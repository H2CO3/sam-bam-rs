//! Bgzip files (BGZF) and bgzip blocks.
//!
//! This modules contains several readers and writers of Bgzip files:
//!
//! # Readers
//!
//! * [Consecutive Reader](struct.ConsecutiveReader.html) - reads bgzip file consecutively.
//! * [Seek Reader](struct.SeekReader.html) - seeks and reads bgzip blocks given an offset.
//! * [Chunks Reader](struct.ChunksReader.html) - wrapper around the
//! [Seek Reader](struct.SeekReader.html),
//! that reads multiple blocks given a vector of [chunks](../index/struct.Chunk.html).
//!
//! # Writers
//!
//! * [Writer](struct.Writer.html) - writes whole bgzip blocks.
//! * [Sentence Writer](struct.SentenceWriter.html) - wrapper around the
//! [Writer](struct.Writer.html), that implements the `Write` trait. In addition, it allows
//! to *end sentences* - marks points, in which it is preferable to break the stream and start a
//! new bgzip block. For example, each BAM record represents a separate sentence, and
//! `SentenceWriter` will try not to split a record between two blocks.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::io::ErrorKind;
use std::path::Path;
use std::cmp::min;
use std::fmt::{self, Display, Debug, Formatter};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lru_cache::LruCache;
use flate2::Compression;
use flate2::write::{DeflateEncoder, DeflateDecoder};

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

/// Bgzip block. Both uncompressed and compressed size should not be bigger than
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
        let exp_contents_size = (&reading_buffer[block_size - 4..block_size])
            .read_u32::<LittleEndian>()?;
        if exp_contents_size as usize > MAX_BLOCK_SIZE {
            return Err(BlockError::Corrupted(format!("Expected contents > MAX_BLOCK_SIZE ({} > {})",
                exp_contents_size, MAX_BLOCK_SIZE)));
        }

        self.contents_size = {
            let mut decoder = DeflateDecoder::new(&mut self.contents[..]);
            decoder.write_all(&reading_buffer[12 + extra_length..block_size - 8])
                .map_err(|e| BlockError::Corrupted(
                    format!("Could not decompress block contents: {:?}", e)))?;
            let remaining_contents = decoder.finish()
                .map_err(|e| BlockError::Corrupted(
                    format!("Could not decompress block contents: {:?}", e)))?;
            MAX_BLOCK_SIZE - remaining_contents.len()
        };

        let exp_crc32 = (&reading_buffer[block_size - 8..block_size - 4])
            .read_u32::<LittleEndian>()?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.contents[..self.contents_size]);
        let obs_crc32 = hasher.finalize();
        if obs_crc32 != exp_crc32 {
            return Err(BlockError::Corrupted(
                format!("CRC do not match: expected {}, observed {}", exp_crc32, obs_crc32)));
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

/// Bgzip file reader, which allows to open bgzip blocks given an offset.
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
    /// Blocks are cached, so it should be inexpensive to consecutively ask for the same blocks.
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

/// Consecutive reader of a bgzip file, it does not support random access, but also does not
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
    compression: Compression,
}

impl Writer<File> {
    /// Opens a bgzip writer from a path and compression level. Maximal compression level is 9.
    pub fn from_path<P: AsRef<Path>>(path: P, level: u8) -> io::Result<Self> {
        let stream = File::create(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open bgzip writer: {}", e)))?;
        Ok(Writer::from_stream(stream, level))
    }
}

impl<W: Write> Writer<W> {
    /// Opens a bgzip writer from a stream and compression level. Maximal compression level is 9.
    pub fn from_stream(stream: W, level: u8) -> Self {
        assert!(level <= 9, "Compression level should be at most 9");
        Writer {
            stream,
            compressed_buffer: vec![0; COMPRESSED_BLOCK_SIZE],
            compression: Compression::new(level as u32),
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
        assert!(contents_size <= MAX_BLOCK_SIZE, "Cannot write a block: uncompressed size {} > {}",
            contents_size, MAX_BLOCK_SIZE);

        let mut crc_hasher = crc32fast::Hasher::new();
        let bytes_written = {
            let mut encoder = DeflateEncoder::new(&mut self.compressed_buffer[..], self.compression);
            for subcontents in contents.iter() {
                encoder.write_all(subcontents)?;
                crc_hasher.update(subcontents);
            }
            let remaining_buf = encoder.finish()?;
            CONTENTS_SIZE - remaining_buf.len()
        };

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
        self.stream.write_u32::<LittleEndian>(crc_hasher.finalize())?;
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

    /// Flushes inner stream.
    pub fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

const CONTENTS_SIZE: usize = MAX_BLOCK_SIZE + 1;

/// A struct that allows to write bgzip files in *sentences*.
///
/// It implements a `Write` trait, and works similar to a `BufWriter`, but has a method
/// [end_sentence](#method.end_sentence),
/// that indicates the end of a block of the same nature (for example, a BAM record can represent
/// a single sentence). Method `flush` ignores sentences and immediately writes
/// the remaining buffer.
///
/// The `SentenceWriter` will try to start new bgzip blocks only when a new sentence starts.
/// The writer still puts several sentences in the same bgzip block, if possible.
pub struct SentenceWriter<W: Write> {
    writer: Writer<W>,
    contents: Vec<u8>,
    start: usize,
    position: usize,
    end: Option<usize>,
    panicked: bool,
}

impl<W: Write> SentenceWriter<W> {
    pub fn new(writer: Writer<W>) -> Self {
        Self {
            writer,
            contents: vec![0; CONTENTS_SIZE],
            start: 0,
            position: 0,
            end: None,
            panicked: false,
        }
    }

    /// Ends the current sentence.
    pub fn end_sentence(&mut self) {
        self.end = Some(self.position);
    }

    /// Try to write a bgzip block from contents `[self.start - end)`.
    fn try_write(&mut self, end: usize) -> io::Result<()> {
        self.panicked = true;
        if self.start <= end {
            self.writer.write(&self.contents[self.start..end])?
        } else {
            self.writer.write_several(&[&self.contents[self.start..], &self.contents[..end]])?
        };
        self.start = end % CONTENTS_SIZE;
        self.panicked = false;
        Ok(())
    }

    fn get_len(&self, end: usize) -> usize {
        if self.start <= end {
            end - self.start
        } else {
            CONTENTS_SIZE - self.start + end
        }
    }

    /// Try different ends to write a bgzip block.
    fn write_bgzip(&mut self) -> io::Result<()> {
        let mut write_len = self.get_len(self.position);
        if let Some(end) = self.end {
            let end_len = self.get_len(end);
            if end_len >= write_len / 2 {
                // Block until sentence end is not too small.
                write_len = end_len;
            }
        }
        self.end = None;

        loop {
            assert!(write_len != 0);
            match self.try_write((self.start + write_len - 1) % CONTENTS_SIZE + 1) {
                Ok(()) => return Ok(()),
                Err(ref e) if e.kind() == ErrorKind::WriteZero => {},
                Err(e) => return Err(e),
            }
            const DECREASE_BY: usize = 2000;
            if write_len <= DECREASE_BY {
                return Err(io::Error::new(ErrorKind::Other,
                    format!("Compressed size is too big. Last attempt to compress {} bytes failed",
                    write_len)));
            }
            write_len -= DECREASE_BY;
        }
    }

    /// Writes all the remaining contents to bgzip and an empty block.
    pub fn finish(&mut self) -> io::Result<()> {
        self.flush()?;
        self.writer.write_empty()
    }
}

impl<W: Write> Write for SentenceWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut remains = CONTENTS_SIZE - 1 - self.get_len(self.position);
        if remains == 0 {
            self.write_bgzip()?;
            remains = CONTENTS_SIZE - 1 - self.get_len(self.position);
        };
        let write_bytes = min(buf.len(), remains);
        if self.position + write_bytes > CONTENTS_SIZE {
            let split = CONTENTS_SIZE - self.position;
            self.contents[self.position..].copy_from_slice(&buf[..split]);
            self.position = write_bytes - split;
            self.contents[..self.position].copy_from_slice(&buf[split..write_bytes]);
        } else {
            self.contents[self.position..self.position + write_bytes]
                .copy_from_slice(&buf[..write_bytes]);
            self.position = (self.position + write_bytes) % CONTENTS_SIZE;
        }
        Ok(write_bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.end = None;
        while self.start != self.position {
            self.write_bgzip()?;
        }
        self.writer.flush()
    }
}

impl<W: Write> Drop for SentenceWriter<W> {
    fn drop(&mut self) {
        if !self.panicked {
            let _ignore = self.finish();
        }
    }
}
