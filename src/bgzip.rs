use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::io::ErrorKind;
use std::path::Path;
use std::cmp::min;
use std::fmt::{self, Display, Debug, Formatter};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lru_cache::LruCache;
use miniz_oxide::inflate::stream as miniz_inflate;
use miniz_oxide::deflate;
use crc::{crc32, Hasher32};

use super::index::{Chunk, VirtualOffset};

/// Biggest possible compressed and uncompressed size
pub const MAX_BLOCK_SIZE: usize = 65536;

fn as_u16(buffer: &[u8], start: usize) -> u16 {
    buffer[start] as u16 + ((buffer[start + 1] as u16) << 8)
}

/// io::Error produced while reading a bgzip block.
///
/// # Variants
///
/// * `EndOfFile` - the stream ended before the beginning of the block. This error does not
/// appear if part of the block was read before the end of the stream.
/// * `Corrupted(s)` - the block has incorrect header or contents.
/// `s` contains additional information about the problem.
/// * `IoError(e)` - the stream raised `io::Error`.
pub enum BlockError {
    EndOfFile,
    Corrupted(String),
    IoError(io::Error),
}

impl From<io::Error> for BlockError {
    fn from(e: io::Error) -> BlockError {
        BlockError::IoError(e)
    }
}

impl Into<io::Error> for BlockError {
    fn into(self) -> io::Error {
        use BlockError::*;
        match self {
            EndOfFile => io::Error::new(ErrorKind::UnexpectedEof,
                "EOF: Failed to read bgzip block"),
            Corrupted(s) => io::Error::new(ErrorKind::InvalidData,
                format!("Corrupted bgzip block: {}", s)),
            IoError(e) => e,
        }
    }
}

impl Display for BlockError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        use BlockError::*;
        match self {
            EndOfFile => write!(f, "EOF: Failed to read bgzip block"),
            Corrupted(s) => write!(f, "Corrupted bgzip block: {}", s),
            IoError(e) => write!(f, "{}", e),
        }
    }
}

impl Debug for BlockError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        use BlockError::*;
        match self {
            EndOfFile => write!(f, "EOF: Failed to read bgzip block"),
            Corrupted(s) => write!(f, "Corrupted bgzip block: {}", s),
            IoError(e) => write!(f, "{}", e),
        }
    }
}

/// BGzip block. Both uncompressed and compressed size should not be bigger than
/// `MAX_BLOCK_SIZE = 65536`.
pub struct Block {
    block_size: usize,
    contents_size: usize,
    contents: Vec<u8>,
}

impl Block {
    #[doc(hidden)]
    pub fn new() -> Block {
        Block {
            block_size: 0,
            contents_size: 0,
            contents: vec![0; MAX_BLOCK_SIZE],
        }
    }

    /// Load bgzip block from `stream` into `self`.
    ///
    /// # Arguments
    ///
    /// * `reading_buffer` - Buffer for reading the compressed block.
    ///     It should have length >= `MAX_BLOCK_SIZE`
    #[doc(hidden)]
    pub fn fill<R: Read>(&mut self, stream: &mut R, reading_buffer: &mut Vec<u8>) 
            -> Result<(), BlockError> {
        assert!(reading_buffer.len() >= MAX_BLOCK_SIZE);
        let extra_length = {
            let header = &mut reading_buffer[..12];
            match stream.read_exact(header) {
                Ok(()) => {},
                Err(e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        return Err(BlockError::EndOfFile);
                    } else {
                        return Err(BlockError::from(e));
                    }
                }
            }
            // TODO: Try read header and extra fields simultaniously
            Block::analyze_header(header)? as usize
        };
        let block_size = {
            let extra_fields = &mut reading_buffer[12..12 + extra_length];
            stream.read_exact(extra_fields)?;
            Block::analyze_extra_fields(extra_fields)? as usize + 1
        };

        stream.read_exact(&mut reading_buffer[12 + extra_length..block_size])?;
        let inflate_res = miniz_inflate::inflate(
            &mut miniz_inflate::InflateState::new(miniz_oxide::DataFormat::Raw),
            &reading_buffer[12 + extra_length..block_size - 8],
            &mut self.contents, miniz_oxide::MZFlush::Finish);
        match inflate_res.status {
            Err(e) => return Err(BlockError::Corrupted(
                format!("Could not decompress block contents: {:?}", e))),
            Ok(miniz_oxide::MZStatus::StreamEnd) => {},
            Ok(o) => return Err(BlockError::Corrupted(
                format!("Could not decompress block contents: {:?}", o))),
        }
        // This should not happen - therefore assert.
        assert!(inflate_res.bytes_consumed == block_size - 20 - extra_length,
            "Could not decompress block contents: decompressed {} bytes instead of {}",
                inflate_res.bytes_consumed, block_size - 20 - extra_length);
        self.contents_size = inflate_res.bytes_written;

        let exp_contents_size = (&reading_buffer[block_size - 4..block_size])
            .read_u32::<LittleEndian>()?;
        if exp_contents_size as usize > MAX_BLOCK_SIZE {
            return Err(BlockError::Corrupted(format!("Expected contents > MAX_BLOCK_SIZE ({} > {})",
                exp_contents_size, MAX_BLOCK_SIZE)));
        }

        #[cfg(feature = "check_crc")] {
            let exp_crc32 = (&reading_buffer[block_size - 8..block_size - 4])
                .read_u32::<LittleEndian>()?;
            let obs_crc32 = crc32::checksum_ieee(&self.contents[..self.contents_size]);
            if obs_crc32 != exp_crc32 {
                return Err(BlockError::Corrupted(
                    format!("CRC do not match: expected {}, observed {}", exp_crc32, obs_crc32)));
            }
        }
        if exp_contents_size as usize != self.contents_size {
            return Err(BlockError::Corrupted(
                format!("Uncompressed sizes do not match: expected {}, observed {}",
                exp_contents_size, self.contents_size)));
        }
        self.block_size = block_size;
        Ok(())
    }

    fn clear(&mut self) {
        self.block_size = 0;
    }

    /// Analyzes 12 heades bytes of a block.
    /// Returns XLEN - total length of extra subfields.
    fn analyze_header(header: &[u8]) -> Result<u16, BlockError> {
        if header[0] != 31 || header[1] != 139 || header[2] != 8 || header[3] != 4 {
            return Err(BlockError::Corrupted("bgzip block has an invalid header".to_string()));
        }
        Ok(as_u16(header, 10))
    }

    /// Analyzes extra fields following the header.
    /// Returns BSIZE - total block size - 1.
    fn analyze_extra_fields(extra_fields: &[u8]) -> Result<u16, BlockError> {
        let mut i = 0;
        while i + 3 < extra_fields.len() {
            let subfield_id1 = extra_fields[i];
            let subfield_id2 = extra_fields[i + 1];
            let subfield_len = as_u16(extra_fields, i + 2);
            if subfield_id1 == 66 && subfield_id2 == 67 && subfield_len == 2 {
                if subfield_len != 2 || i + 5 >= extra_fields.len() {
                    return Err(BlockError::Corrupted("bgzip block has an invalid header"
                        .to_string()));
                }
                return Ok(as_u16(extra_fields, i + 4));
            }
            i += 4 + subfield_len as usize;
        }
        Err(BlockError::Corrupted("bgzip block has an invalid header".to_string()))
    }

    /// Return the uncompressed contents.
    pub fn contents(&self) -> &[u8] {
        &self.contents[..self.contents_size]
    }

    /// Return the size of the uncompressed data (same as `contents().len()`)
    pub fn contents_size(&self) -> usize {
        self.contents_size
    }

    /// Return the block size (size of the compressed data).
    pub fn block_size(&self) -> usize {
        self.block_size
    }
}

/// Builder of [bgzip file reader](struct.SeekReader.html)
pub struct SeekReaderBuilder {
    cache_capacity: usize,
}

impl SeekReaderBuilder {
    /// Creates a new SeekReaderBuilder. The same as
    /// [SeekReader::build](struct.SeekReader.html#method.build)
    pub fn new() -> SeekReaderBuilder {
        SeekReaderBuilder {
            cache_capacity: 1000,
        }
    }

    /// Sets new LRU cache capacity. The cache stores last accessed bgzip blocks, so the size of the
    /// cache would be `cache_size * (MAX_BLOCK_SIZE = 64 KiB)`. Default cache capacity is 1000.
    /// Cache capacity should not be zero.
    pub fn cache_capacity(&mut self, cache_capacity: usize) -> &mut Self {
        assert!(cache_capacity > 0, "Cache size must be non-zero");
        self.cache_capacity = cache_capacity;
        self
    }

    /// Opens a [SeekReader](struct.SeekReader.html) from the `path`.
    pub fn from_path<P: AsRef<Path>>(&self, path: P) -> io::Result<SeekReader<File>> {
        let stream = File::open(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open bgzip reader: {}", e)))?;
        Ok(self.from_stream(stream))
    }

    /// Opens a [SeekReader](struct.SeekReader.html) from stream. Stream must support random access.
    pub fn from_stream<R: Read + Seek>(&self, stream: R) -> SeekReader<R> {
        SeekReader::new(stream, self.cache_capacity)
    }
}

/// BGzip file reader, which allows to open bgzip blocks given an offset.
pub struct SeekReader<R: Read + Seek> {
    stream: R,
    cache: LruCache<u64, Block>,
    reading_buffer: Vec<u8>,
    empty_blocks: Vec<Block>,
    current_offset: u64,
}

impl SeekReader<File> {
    /// Creates [SeekReaderBuilder](struct.SeekReaderBuilder.html).
    pub fn build() -> SeekReaderBuilder {
        SeekReaderBuilder::new()
    }
}

impl<R: Read + Seek> SeekReader<R> {
    /// Creates a new SeekReader from a stream and a cache capacity.
    /// Consider using `SeekReader::build()` to create
    /// [SeekReaderBuilder](struct.SeekReaderBuilder.html).
    pub fn new(stream: R, cache_capacity: usize) -> Self {
        Self {
            stream,
            cache: LruCache::new(cache_capacity),
            reading_buffer: vec![0; MAX_BLOCK_SIZE],
            empty_blocks: Vec::new(),
            current_offset: 0,
        }
    }

    /// Get a bgzip block using `offset` into the file.
    /// Blocks are cached, so it should be unexpensive to consecutively ask for the same blocks.
    pub fn get_block<'a>(&'a mut self, offset: u64) -> io::Result<&'a Block> {
        if self.cache.contains_key(&offset) {
            return Ok(self.cache.get_mut(&offset)
                .expect("Cache should contain the requested block"));
        }

        if self.current_offset != offset {
            self.stream.seek(SeekFrom::Start(offset))?;
            self.current_offset = offset;
        }
        let mut new_block = self.empty_blocks.pop().unwrap_or_else(|| Block::new());
        new_block.fill(&mut self.stream, &mut self.reading_buffer)
            .map_err(|e| -> io::Error { e.into() })?;
        self.current_offset += new_block.block_size() as u64;

        if let Some(mut old_block) = self.cache.insert(offset, new_block) {
            old_block.clear();
            self.empty_blocks.push(old_block);
        }
        Ok(self.cache.get_mut(&offset).expect("Cache should contain the requested block"))
    }
}

/// Reader of Bgzip blocks given for a vector of [chunks](../index/struct.Chunk.html).
/// Wrapper of [SeekReader](struct.SeekReader.html).
pub struct ChunksReader<'a, R: Read + Seek> {
    reader: &'a mut SeekReader<R>,
    chunks: Vec<Chunk>,
    chunk_ix: usize,

    block_offset: u64,
    block_size: usize,

    buffer: &'a mut Vec<u8>,
    buffer_offset: usize,
}

impl<'a, R: Read + Seek> ChunksReader<'a, R> {
    /// Create a Reader that consecutively returns uncompressed contents of the Bgzip file
    /// corresponding to each [Chunk](../index/struct.Chunk.html).
    ///
    /// `buffer` must have capacity at least `MAX_BLOCK_SIZE`.
    pub fn new(reader: &'a mut SeekReader<R>, chunks: Vec<Chunk>, buffer: &'a mut Vec<u8>) -> Self {
        // Does not change the capacity
        buffer.clear();
        assert!(buffer.capacity() >= MAX_BLOCK_SIZE);
        ChunksReader {
            reader,
            chunks,
            chunk_ix: 0,

            block_offset: 0,
            block_size: std::usize::MAX,

            buffer,
            buffer_offset: 0,
        }
    }

    /// Create a Reader that returns the uncompressed contents of the full Bgzip file.
    ///
    /// `buffer` must have capacity at least `MAX_BLOCK_SIZE`.
    pub fn without_boundaries(reader: &'a mut SeekReader<R>, buffer: &'a mut Vec<u8>) -> Self {
        let chunk = Chunk::new(VirtualOffset::from_raw(0), VirtualOffset::from_raw(std::u64::MAX));
        Self::new(reader, vec![chunk], buffer)
    }
}

impl<'a, R: Read + Seek> Read for ChunksReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.buffer_offset < self.buffer.len() {
            let bytes = min(self.buffer.len() - self.buffer_offset, buf.len());
            buf[..bytes].copy_from_slice(
                &self.buffer[self.buffer_offset..self.buffer_offset + bytes]);
            self.buffer_offset += bytes;
            return Ok(bytes);
        }

        // Here, either a block has ended, or a chunk has ended.
        // Before the first iteration block_size = usize::MAX
        let mut update_chunk = self.block_size == std::usize::MAX;
        loop {
            if self.chunk_ix >= self.chunks.len() {
                return Ok(0);
            }
            let chunk = &self.chunks[self.chunk_ix];

            // Last block in a chunk
            if !update_chunk && self.block_offset == chunk.end().block_offset() {
                self.chunk_ix += 1;
                update_chunk = true;
                continue;
            }

            let contents_start = if update_chunk {
                // Starting a new chunk
                self.block_offset = chunk.start().block_offset();
                chunk.start().contents_offset() as usize
            } else {
                // Starting a new block in the same chunk
                self.block_offset += self.block_size as u64;
                0
            };

            // Chunk ends, no need to load a block
            if chunk.end().equal(self.block_offset, 0) {
                self.chunk_ix += 1;
                update_chunk = true;
                continue;
            }

            // Here we start a new block in any case
            let block = self.reader.get_block(self.block_offset)?;
            self.block_size = block.block_size();

            let contents_end = if chunk.end().block_offset() == self.block_offset {
                // End of the chunk is in the current block
                chunk.end().contents_offset() as usize
            } else {
                block.contents_size()
            };
            unsafe {
                self.buffer.set_len(contents_end - contents_start);
            }
            self.buffer.copy_from_slice(&block.contents()[contents_start..contents_end]);
            self.buffer_offset = 0;

            let bytes = min(self.buffer.len() - self.buffer_offset, buf.len());
            assert!(bytes != 0);
            buf[..bytes].copy_from_slice(
                &self.buffer[self.buffer_offset..self.buffer_offset + bytes]);
            self.buffer_offset += bytes;
            return Ok(bytes);
        }
    }
}

/// Consecutive reader of a bgzip file, does not support random access, but also does not
/// spend memory and time on caching.
pub struct ConsecutiveReader<R: Read> {
    stream: R,
    block: Block,
    contents_offset: usize,
    reading_buffer: Vec<u8>,
    previous_empty: bool,
}

impl ConsecutiveReader<File> {
    /// Open the reader from the `path`.
    pub fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let stream = File::open(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open bgzip reader: {}", e)))?;
        ConsecutiveReader::from_stream(stream)
    }
}

impl<R: Read> ConsecutiveReader<R> {
    /// Open the reader from the `stream`. The stream should be open as long as the reader.
    pub fn from_stream(mut stream: R) -> io::Result<Self> {
        let mut reading_buffer = vec![0; MAX_BLOCK_SIZE];
        let mut block = Block::new();
        block.fill(&mut stream, &mut reading_buffer).map_err(|e| -> io::Error { e.into() })?;
        Ok(ConsecutiveReader {
            stream, block, reading_buffer,
            contents_offset: 0,
            previous_empty: false,
        })
    }
}

impl<R: Read> Read for ConsecutiveReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.contents_offset < self.block.contents_size() {
            let bytes = min(self.block.contents_size() - self.contents_offset, buf.len());
            buf[..bytes].copy_from_slice(
                &self.block.contents()[self.contents_offset..self.contents_offset + bytes]);
            self.contents_offset += bytes;
            return Ok(bytes);
        }

        loop {
            match self.block.fill(&mut self.stream, &mut self.reading_buffer) {
                Ok(()) => {},
                Err(BlockError::EndOfFile) => {
                    if self.previous_empty {
                        return Ok(0);
                    } else {
                        return Err(io::Error::new(ErrorKind::InvalidData, "BAM file truncated!"));
                    }
                },
                Err(e) => return Err(e.into()),
            }

            if self.block.contents_size() == 0 {
                self.previous_empty = true;
                continue;
            }
            let bytes = min(self.block.contents_size(), buf.len());
            buf[..bytes].copy_from_slice(&self.block.contents()[..bytes]);
            self.contents_offset = bytes;
            return Ok(bytes);
        }
    }
}

const COMPRESSED_BLOCK_SIZE: usize = MAX_BLOCK_SIZE - 26;

/// Bgzip writer, that allows to compress and write blocks with uncompressed
/// size at most `MAX_BLOCK_SIZE = 65536`.
pub struct Writer<W: Write> {
    stream: W,
    compressed_buffer: Vec<u8>,
    compressor: deflate::core::CompressorOxide,
}

impl Writer<File> {
    /// Opens a bgzip writer from a path and compression level. Maximal compression level is 10.
    pub fn from_path<P: AsRef<Path>>(path: P, level: u8) -> io::Result<Self> {
        let stream = File::create(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open bgzip writer: {}", e)))?;
        Ok(Writer::from_stream(stream, level))
    }
}

impl<W: Write> Writer<W> {
    /// Opens a bgzip writer from a stream and compression level. Maximal compression level is 10.
    pub fn from_stream(stream: W, level: u8) -> Self {
        assert!(level <= 10, "Compression level should be at most 10");
        let mut compressor = deflate::core::CompressorOxide::new(0);
        compressor.set_format_and_level(miniz_oxide::DataFormat::Raw, level);
        Writer {
            stream,
            compressed_buffer: vec![0; COMPRESSED_BLOCK_SIZE + 1],
            compressor,
        }
    }

    /// Writes an empty block.
    pub fn write_empty(&mut self) -> io::Result<()> {
        const EMPTY_BLOCK: &[u8; 28] = &[0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00];
        self.stream.write_all(EMPTY_BLOCK)?;
        Ok(())
    }

    /// Compresses all slices in the `contents` and writes as a single block.
    ///
    /// Sum length of the `contents` should be at most `MAX_BLOCK_SIZE = 65536`.
    ///
    /// If the compressed block is bigger than `MAX_BLOCK_SIZE`, the function returns an error
    /// `WriteZero`.
    pub fn write_several(&mut self, contents: &[&[u8]]) -> io::Result<()> {
        let contents_size: usize = contents.iter().map(|slice| slice.len()).sum();
        if contents_size == 0 {
            return self.write_empty();
        }
        if contents_size > MAX_BLOCK_SIZE {
            panic!("Cannot write a block: uncompressed size {} > {}", contents_size,
                MAX_BLOCK_SIZE);
        }

        self.compressor.reset();
        let mut bytes_written = 0;
        let mut crc_digest = crc32::Digest::new(crc32::IEEE);
        for (i, subcontents) in contents.iter().enumerate() {
            if subcontents.len() == 0 {
                continue;
            }
            let flush = if i == contents.len() - 1 {
                miniz_oxide::MZFlush::Finish
            } else {
                miniz_oxide::MZFlush::Sync
            };
            let deflate_res = deflate::stream::deflate(&mut self.compressor, subcontents,
                &mut self.compressed_buffer[bytes_written..], flush);
            match deflate_res.status {
                Ok(miniz_oxide::MZStatus::StreamEnd)
                    | Ok(miniz_oxide::MZStatus::Ok)
                    | Err(miniz_oxide::MZError::Buf) => {},
                Ok(o) => return Err(io::Error::new(ErrorKind::InvalidData,
                    format!("Could not compress block contents: {:?}", o))),
                Err(e) => return Err(io::Error::new(ErrorKind::InvalidData,
                    format!("Could not compress block contents: {:?}", e))),
            }
            bytes_written += deflate_res.bytes_written;
            // Compressed size is too big.
            if deflate_res.bytes_consumed != subcontents.len() 
                    || bytes_written > COMPRESSED_BLOCK_SIZE {
                return Err(io::Error::new(ErrorKind::WriteZero,
                    "Compressed size is bigger than MAX_BLOCK_SIZE"));
            }
            crc_digest.write(subcontents);
        }

        const BLOCK_HEADER: &[u8; 16] = &[
             31, 139,   8,   4,  // ID1, ID2, Compression method, Flags
              0,   0,   0,   0,  // Modification time
              0, 255,   6,   0,  // Extra flags, OS (255 = unknown), extra length (2 bytes)
             66,  67,   2,   0]; // SI1, SI2, subfield len (2 bytes)
        self.stream.write_all(BLOCK_HEADER)?;
        let block_size = bytes_written + 26;
        self.stream.write_u16::<LittleEndian>((block_size - 1) as u16)?;

        let compressed_data = &self.compressed_buffer[..bytes_written];
        self.stream.write_all(compressed_data)?;
        self.stream.write_u32::<LittleEndian>(crc_digest.sum32())?;
        self.stream.write_u32::<LittleEndian>(contents_size as u32)?;
        Ok(())
    }

    /// Compresses `contents` and writes as a single block.
    ///
    /// Input `contents` size should be at most `MAX_BLOCK_SIZE = 65536`.
    ///
    /// If the compressed block is bigger than `MAX_BLOCK_SIZE`, the function returns an error
    /// `WriteZero`.
    pub fn write(&mut self, contents: &[u8]) -> io::Result<()> {
        self.write_several(&[contents])
    }
}

/// A struct that allows to write bgzip files in *sentences*.
///
/// It implements a `Write` trait, and works similar to a `BufWriter`, but has two functions:
/// [end_sentence](#method.end_sentence) and [force_end_sentence](#method.force_end_sentence),
/// that indicate the end of a block of the same nature (for example, each sentence is a single
/// BAM record).
///
/// The `SentenceWriter` will then try to start new bgzip blocks only when a new sentence starts.
/// Several sentences can still get to the same bgzip block, if they are small enough.
pub struct SentenceWriter<W: Write> {
    writer: Writer<W>,
    contents: Vec<u8>,
    start: usize,
    position: usize,
    ends: Vec<usize>,
    panicked: bool,
}

impl<W: Write> SentenceWriter<W> {
    pub fn new(writer: Writer<W>) -> Self {
        Self {
            writer,
            contents: vec![0; MAX_BLOCK_SIZE + 1],
            start: 0,
            position: 0,
            ends: Vec::new(),
            panicked: false,
        }
    }

    /// End the current sentence.
    pub fn end_sentence(&mut self) {
        self.ends.push(self.position);
    }

    /// End the current sentence and force end the bgzip block (if possible).
    pub fn force_end_sentence(&mut self) -> io::Result<()> {
        self.end_sentence();
        self.write_bgzip()
    }

    fn update_contents(&mut self, buf: &[u8]) {
        if self.position + buf.len() <= self.contents.len() {
            self.contents[self.position..self.position + buf.len()].copy_from_slice(buf);
            self.position += buf.len();
        } else {
            let split = self.contents.len() - self.position;
            self.contents[self.position..].copy_from_slice(&buf[..split]);
            self.position = buf.len() - split;
            self.contents[..self.position].copy_from_slice(&buf[split..]);
        }
    }

    /// Try to write a bgzip block from contents `[self.start - end)`.
    fn try_write(&mut self, end: usize) -> io::Result<()> {
        self.panicked = true;
        if end > self.start {
            self.writer.write(&self.contents[self.start..end])?
        } else {
            self.writer.write_several(&[&self.contents[self.start..], &self.contents[..end]])?
        };
        self.start = end;
        self.panicked = false;
        Ok(())
    }

    fn get_len(&self, end: usize) -> usize {
        if self.start <= end {
            end - self.start
        } else {
            self.contents.len() - self.start + end
        }
    }

    /// Try different ends to write a bgzip block.
    fn write_bgzip(&mut self) -> io::Result<()> {
        let mut prev_end = std::usize::MAX;

        for i in (0..self.ends.len()).rev() {
            let end = self.ends[i];
            if prev_end == end {
                continue;
            }
            prev_end = end;
            match self.try_write(end) {
                Ok(()) => {
                    self.ends.drain(..i + 1);
                    return Ok(());
                },
                Err(ref e) if e.kind() == ErrorKind::WriteZero => {},
                Err(e) => return Err(e),
            }
        }
        self.ends.clear();

        let last_end_len = self.get_len(prev_end);
        let mut curr_len = self.get_len(self.position);
        if curr_len >= last_end_len {
            curr_len /= 2;
        }
        while curr_len >= 1 {
            let end = (self.start + curr_len) % self.contents.len();
            match self.try_write(end) {
                Ok(()) => return Ok(()),
                Err(ref e) if e.kind() == ErrorKind::WriteZero => {},
                Err(e) => return Err(e),
            }
            curr_len /= 2;
        }
        panic!("Failed to compress data");
    }

    /// Writes all the remaining contents to bgzip and an empty block.
    pub fn finish(&mut self) -> io::Result<()> {
        self.flush()?;
        self.writer.write_empty()
    }
}

impl<W: Write> Write for SentenceWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.get_len(self.position) + buf.len() <= MAX_BLOCK_SIZE {
            self.update_contents(buf);
            return Ok(buf.len());
        }

        self.write_bgzip()?;
        self.update_contents(buf);
        return Ok(buf.len());
    }

    fn flush(&mut self) -> io::Result<()> {
        self.ends.clear();
        while self.start != self.position {
            self.write_bgzip()?;
        }
        Ok(())
    }
}

impl<W: Write> Drop for SentenceWriter<W> {
    fn drop(&mut self) {
        if !self.panicked {
            let _ignore = self.finish();
        }
    }
}
