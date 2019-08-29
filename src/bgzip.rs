use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::io::ErrorKind::{InvalidData, UnexpectedEof};
use std::path::Path;
use std::cmp::min;

use byteorder::{LittleEndian, ReadBytesExt};
use libflate::deflate;
use lru_cache::LruCache;

use super::index::{Chunk, VirtualOffset};

/// Biggest possible compressed and uncompressed size
pub const MAX_BLOCK_SIZE: usize = 65536;

fn as_u16(buffer: &[u8], start: usize) -> u16 {
    buffer[start] as u16 + ((buffer[start + 1] as u16) << 8)
}

/// BGzip block. Both uncompressed and compressed size should not be bigger than
/// `MAX_BLOCK_SIZE = 65536`.
pub struct Block {
    block_size: usize,
    contents: Vec<u8>,
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
            EndOfFile => io::Error::new(UnexpectedEof, "EOF: Failed to read bgzip block"),
            Corrupted(s) => io::Error::new(InvalidData, format!("Corrupted bgzip block: {}", s)),
            IoError(e) => e,
        }
    }
}

impl Block {
    #[doc(hidden)]
    pub fn new() -> Block {
        Block {
            block_size: 0,
            contents: Vec::with_capacity(MAX_BLOCK_SIZE),
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
        self.contents.clear();

        let extra_length = {
            let header = &mut reading_buffer[..12];
            match stream.read_exact(header) {
                Ok(()) => {},
                Err(e) => {
                    if e.kind() == UnexpectedEof {
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
        let mut decoder = deflate::Decoder::new(&reading_buffer[12 + extra_length..block_size - 8]);
        let obs_contents_size = decoder.read_to_end(&mut self.contents)
            .map_err(|e| BlockError::Corrupted(
                format!("Could not decompress block contents: {}", e)))?;
        
        let _exp_crc32 = (&reading_buffer[block_size - 8..block_size - 4])
            .read_u32::<LittleEndian>()?;
        let exp_contents_size = (&reading_buffer[block_size - 4..block_size])
            .read_u32::<LittleEndian>()?;
        if exp_contents_size as usize > MAX_BLOCK_SIZE {
            return Err(BlockError::Corrupted(format!("Expected contents > MAX_BLOCK_SIZE ({} > {})",
                exp_contents_size, MAX_BLOCK_SIZE)));
        }

        #[cfg(feature = "check_crc")] {
            let obs_crc32 = crc::crc32::checksum_ieee(&self.contents);
            if obs_crc32 != _exp_crc32 {
                return Err(BlockError::Corrupted(
                    format!("CRC do not match: expected {}, observed {}", _exp_crc32, obs_crc32)));
            }
        }
        if exp_contents_size as usize != obs_contents_size {
            return Err(BlockError::Corrupted(
                format!("Uncompressed sizes do not match: expected {}, observed {}",
                exp_contents_size, obs_contents_size)));
        }
        debug_assert!(obs_contents_size == self.contents.len());
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
        &self.contents
    }

    /// Return the size of the uncompressed data (same as `contents().len()`)
    pub fn contents_size(&self) -> usize {
        self.contents.len()
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
        }
    }

    /// Get a bgzip block using `offset` into the file.
    /// Blocks are cached, so it should be unexpensive to consecutively ask for the same blocks.
    pub fn get_block<'a>(&'a mut self, offset: u64) -> io::Result<&'a Block> {
        if self.cache.contains_key(&offset) {
            return Ok(self.cache.get_mut(&offset)
                .expect("Cache should contain the requested block"));
        }

        self.stream.seek(SeekFrom::Start(offset))?;
        let mut new_block = self.empty_blocks.pop().unwrap_or_else(|| Block::new());
        new_block.fill(&mut self.stream, &mut self.reading_buffer)
            .map_err(|e| -> io::Error { e.into() })?;

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
                        return Err(io::Error::new(InvalidData, "BAM file truncated!"));
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