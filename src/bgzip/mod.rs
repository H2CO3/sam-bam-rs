use std::io::{self, Read, Write, ErrorKind};
use std::cmp::min;
use std::fmt::{self, Display, Debug, Formatter};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use flate2::write::{DeflateDecoder, DeflateEncoder};

pub mod read;
pub mod write;

/// Error produced while reading or decompressing a bgzip block.
///
/// # Variants
///
/// * `EndOfStream` - failed to read a block because the stream has ended.
/// * `Corrupted(s)` - the block has incorrect header or contents.
/// `s` contains additional information about the problem.
/// * `IoError(e)` - the stream raised `io::Error`.
pub enum BlockError {
    EndOfStream,
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
            EndOfStream => io::Error::new(ErrorKind::UnexpectedEof,
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
            EndOfStream => write!(f, "EOF: Failed to read bgzip block"),
            Corrupted(s) => write!(f, "Corrupted bgzip block: {}", s),
            IoError(e) => write!(f, "{}", e),
        }
    }
}

impl Debug for BlockError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        Display::fmt(self, f)
    }
}

fn as_u16(buffer: &[u8], start: usize) -> u16 {
    buffer[start] as u16 + ((buffer[start + 1] as u16) << 8)
}

/// Analyzes first 12 bytes of a block.
/// Returns the total length of extra subfields (XLEN).
fn analyze_header(header: &[u8]) -> Result<u16, BlockError> {
    if header[0] != 31 || header[1] != 139 || header[2] != 8 || header[3] != 4 {
        return Err(BlockError::Corrupted("bgzip block has an invalid header".to_string()));
    }
    Ok(as_u16(header, 10))
}

/// Analyzes extra fields following the header.
/// Returns total block size - 1 (BSIZE).
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

/// Enum that describes the block state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockState {
    /// Block is empty and contains no information.
    Empty,
    /// Block contains only uncompressed data.
    Uncompressed,
    /// Block contains only compressed data.
    Compressed,
    /// Block contains both uncompressed data and its compressed representation.
    Full,
}

impl BlockState {
    /// Returns `true` if the block contains uncompressed data.
    pub fn contains_uncompressed(self) -> bool {
        match self {
            BlockState::Full | BlockState::Uncompressed => true,
            _ => false,
        }
    }

    /// Returns `true` if the block contains compressed data.
    pub fn contains_compressed(self) -> bool {
        match self {
            BlockState::Full | BlockState::Compressed => true,
            _ => false,
        }
    }
}

/// Biggest possible size of the compressed and uncompressed block.
pub const MAX_BLOCK_SIZE: usize = 65536;

const HEADER_SIZE: usize = 12;
const MIN_EXTRA_SIZE: usize = 6;
const FOOTER_SIZE: usize = 8;
// WRAPPER_SIZE = 26
const WRAPPER_SIZE: usize = HEADER_SIZE + MIN_EXTRA_SIZE + FOOTER_SIZE;

/// Biggest possible length of the compressed data (excluding header + footer).
/// Equal to `MAX_BLOCK_SIZE - 26 = 65510`.
pub const MAX_COMPRESSED_SIZE: usize = MAX_BLOCK_SIZE - WRAPPER_SIZE;

/// Maximal size of the compressed data.
pub struct Block {
    // Uncompressed contents, max size = `MAX_BLOCK_SIZE`.
    uncompressed: Vec<u8>,
    // Store uncompressed size in case we need to write a block without decompressing it.
    uncompressed_size: u32,
    // Compressed contents, max size = `MAX_COMPRESSED_SIZE`.
    compressed: Vec<u8>,

    // CRC32 hash of the contents.
    crc_hash: u32,
    // Buffer used to read the header.
    buffer: Vec<u8>,
    offset: Option<u64>,
}

impl Block {
    /// Creates an empty block.
    pub fn new() -> Self {
        Self {
            uncompressed: Vec::with_capacity(MAX_BLOCK_SIZE),
            uncompressed_size: 0,
            compressed: Vec::with_capacity(MAX_COMPRESSED_SIZE + FOOTER_SIZE),
            crc_hash: 0,
            buffer: Vec::new(),
            offset: None,
        }
    }

    /// Resets a block.
    pub fn reset(&mut self) {
        self.uncompressed.clear();
        self.uncompressed_size = 0;
        self.compressed.clear();
        self.crc_hash = 0;
        self.offset = None;
    }

    /// Resets compressed data, if present. This function is needed if you want to update
    /// uncompressed contents.
    pub fn reset_compression(&mut self) {
        self.compressed.clear();
        self.crc_hash = 0;
    }

    /// Extends uncompressed contents and returns the number of consumed bytes. The only case when
    /// the number of consumed bytes is less then the size of the `buf` is when the contents
    /// reached maximum size `MAX_BLOCK_SIZE`. However, it is not recommended to fill the
    /// contents completely to `MAX_BLOCK_SIZE` as the compressed size may become too big
    /// (bigger than `MAX_COMPRESSED_SIZE`).
    ///
    /// This function panics, if the block contains compressed data. Consider using
    /// [reset_compression](#method.reset_compression).
    pub fn extend_contents(&mut self, buf: &[u8]) -> usize {
        assert!(self.compressed.len() == 0, "Cannot update contents, as the block was compressed. \\
            Consider using reset_compression()");
        let consume_bytes = min(buf.len(), MAX_BLOCK_SIZE - self.uncompressed.len());
        self.uncompressed.extend(&buf[..consume_bytes]);
        self.uncompressed_size += consume_bytes as u32;
        consume_bytes
    }

    /// Returns the size of the uncompressed data
    /// (this works even if the block was never decompressed).
    pub fn uncompressed_size(&self) -> u32 {
        self.uncompressed_size
    }

    /// Returns the size of the compressed data. If the block was not compressed, the function
    /// returns zero. Note, that compressed data does not include
    /// header and footer of the bgzip block.
    pub fn compressed_size(&self) -> u32 {
        self.compressed.len() as u32
    }

    /// Returns the size of the block (compressed data and header and footer of the bgzip block).
    /// Returns None if the block was not compressed yet.
    pub fn block_size(&self) -> Option<u32> {
        if self.compressed.is_empty() {
            None
        } else {
            Some((self.compressed.len() + WRAPPER_SIZE) as u32)
        }
    }

    /// Returns a block offset, if present.
    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    /// Returns the state of the block.
    pub fn state(&self) -> BlockState {
        match (self.uncompressed.is_empty(), self.compressed.is_empty()) {
            (true, true) => BlockState::Empty,
            (true, false) => BlockState::Uncompressed,
            (false, true) => BlockState::Compressed,
            (false, false) => BlockState::Full,
        }
    }

    /// Compressed block contents. This function panics if the block was already compressed or
    /// the uncompressed contents are empty.
    ///
    /// If the compressed size is bigger than `MAX_COMPRESSED_SIZE`, the function returns
    /// `WriteZero`.
    pub fn compress(&mut self, compression: flate2::Compression) -> io::Result<()> {
        assert!(self.uncompressed.len() > 0, "Cannot compress an empty block");
        assert!(self.compressed.len() == 0, "Cannot compress an already compressed block");

        unsafe {
            self.compressed.set_len(MAX_COMPRESSED_SIZE);
        }
        let mut crc_hasher = crc32fast::Hasher::new();
        let compressed_size = {
            let mut encoder = DeflateEncoder::new(&mut self.compressed[..], compression);
            encoder.write_all(&self.uncompressed)?;
            crc_hasher.update(&self.uncompressed);
            let remaining_buf = encoder.finish()?;
            MAX_COMPRESSED_SIZE - remaining_buf.len()
        };
        self.compressed.truncate(compressed_size);
        self.crc_hash = crc_hasher.finalize();
        Ok(())
    }

    /// Writes the block to `stream`. The function panics if the block was not compressed.
    pub fn dump<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        assert!(self.compressed.len() == 0, "Cannot write an uncompressed block");
        const BLOCK_HEADER: &[u8; 16] = &[
             31, 139,   8,   4,  // ID1, ID2, Compression method, Flags
              0,   0,   0,   0,  // Modification time
              0, 255,   6,   0,  // Extra flags, OS (255 = unknown), extra length (2 bytes)
             66,  67,   2,   0]; // SI1, SI2, subfield len (2 bytes)
        stream.write_all(BLOCK_HEADER)?;
        stream.write_u16::<LittleEndian>((self.compressed.len() + 25) as u16)?;
        stream.write_all(&self.compressed)?;
        stream.write_u32::<LittleEndian>(self.crc_hash)?;
        assert!(self.uncompressed.len() == 0
            || self.uncompressed.len() as u32 == self.uncompressed_size);
        stream.write_u32::<LittleEndian>(self.uncompressed_size)?;
        Ok(())
    }

    /// Reads the compressed contents from `stream`. Panics if the block is non-empty
    /// (consider using [reset](#method.reset)).
    pub fn load<R: Read>(&mut self, offset: Option<u64>, stream: &mut R) -> Result<(), BlockError> {
        assert!(self.compressed.is_empty() && self.uncompressed.is_empty(),
            "Cannot load into a non-empty block");
        self.offset = offset;

        let extra_len = {
            self.buffer.resize(HEADER_SIZE + MIN_EXTRA_SIZE, 0);
            match stream.read_exact(&mut self.buffer) {
                Ok(()) => {},
                Err(e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        return Err(BlockError::EndOfStream);
                    } else {
                        return Err(BlockError::from(e));
                    }
                }
            }
            analyze_header(&self.buffer)? as usize
        };

        if extra_len > MIN_EXTRA_SIZE {
            self.buffer.resize(HEADER_SIZE + extra_len, 0);
            stream.read_exact(&mut self.buffer[HEADER_SIZE..])?;
        }
        let block_size = analyze_extra_fields(&self.buffer[HEADER_SIZE..])? as usize + 1;
        if block_size > MAX_BLOCK_SIZE {
            return Err(BlockError::Corrupted(
                format!("Block size {} > {}", block_size, MAX_BLOCK_SIZE)));
        }

        let compressed_size = block_size - WRAPPER_SIZE;
        unsafe {
            // Include footer in self.compressed to read footer in one go.
            self.compressed.set_len(compressed_size + FOOTER_SIZE);
        }
        stream.read_exact(&mut self.compressed)?;
        self.crc_hash = (&self.compressed[compressed_size..compressed_size + 4])
            .read_u32::<LittleEndian>().unwrap();
        self.uncompressed_size = (&self.compressed[compressed_size + 4..compressed_size + 8])
            .read_u32::<LittleEndian>().unwrap();
        if self.uncompressed_size as usize > MAX_BLOCK_SIZE {
            return Err(BlockError::Corrupted(
                format!("Expected uncompressed size {} > {}", block_size, MAX_BLOCK_SIZE)));
        }
        self.compressed.truncate(compressed_size);
        Ok(())
    }

    /// Decompressed block contents. This function panics if the block was already decompressed or
    /// if the block is empty.
    pub fn decompress(&mut self) -> Result<(), BlockError> {
        assert!(self.compressed.len() > 0, "Cannot decompress an empty block");
        assert!(self.uncompressed.len() == 0, "Cannot decompress an already decompressed block");

        unsafe {
            self.uncompressed.set_len(self.uncompressed_size as usize);
        }
        {
            let mut decoder = DeflateDecoder::new(&mut self.uncompressed[..]);
            decoder.write_all(&self.compressed).map_err(|e| BlockError::Corrupted(
                format!("Could not decompress block contents: {:?}", e)))?;
            let remaining_contents = decoder.finish().map_err(|e| BlockError::Corrupted(
                format!("Could not decompress block contents: {:?}", e)))?.len();
            if remaining_contents != 0 {
                return Err(BlockError::Corrupted(
                    format!("Uncompressed sizes do not match: expected {}, observed {}",
                    self.uncompressed_size, self.uncompressed_size as usize - remaining_contents)));
            }
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.uncompressed);
        let obs_crc32 = hasher.finalize();
        if obs_crc32 != self.crc_hash {
            return Err(BlockError::Corrupted(
                format!("CRC do not match: expected {}, observed {}", self.crc_hash, obs_crc32)));
        }
        Ok(())
    }

    /// Access uncompressed data.
    pub fn uncompressed_data(&self) -> &[u8] {
        &self.uncompressed
    }

    /// Access compressed data (without header and footer).
    pub fn compressed_data(&self) -> &[u8] {
        &self.compressed
    }
}

pub(crate) struct ObjectPool<T> {
    objects: Vec<T>,
    constructor: Box<dyn Fn() -> T>,
    taken: u64,
    brought: u64,
}

impl<T> ObjectPool<T> {
    pub fn new<F: 'static + Fn() -> T>(constructor: F) -> Self {
        Self {
            objects: vec![],
            constructor: Box::new(constructor),
            taken: 0,
            brought: 0,
        }
    }

    pub fn take(&mut self) -> T {
        self.taken += 1;
        match self.objects.pop() {
            Some(object) => object,
            None => (self.constructor)(),
        }
    }

    pub fn bring(&mut self, object: T) {
        self.brought += 1;
        self.objects.push(object);
    }
}
