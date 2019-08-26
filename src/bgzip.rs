use std::fs::File;
use std::io::{Read, Seek, Result, Error, SeekFrom};
use std::io::ErrorKind::InvalidData;
use std::path::Path;
use std::cmp::min;

use byteorder::{LittleEndian, ReadBytesExt};
use libflate::deflate;
use lru_cache::LruCache;

use super::index::Chunk;

const MAX_BLOCK_SIZE: usize = 65536_usize;

fn as_u16(buffer: &[u8], start: usize) -> u16 {
    buffer[start] as u16 + ((buffer[start + 1] as u16) << 8)
}

pub struct Block {
    compressed_size: usize,
    uncompr_data: Vec<u8>,
}

impl Block {
    fn new() -> Block {
        Block {
            compressed_size: 0,
            uncompr_data: Vec::with_capacity(MAX_BLOCK_SIZE),
        }
    }

    /// Load bgzip block from `stream` into `self`. Returns the loaded block size
    ///
    /// # Arguments
    ///
    /// * `reading_buffer` - Buffer for reading the compressed block.
    ///     It should have length >= `MAX_BLOCK_SIZE`
    fn fill<R: Read>(&mut self, stream: &mut R, reading_buffer: &mut Vec<u8>) -> Result<()> {
        debug_assert!(reading_buffer.len() >= MAX_BLOCK_SIZE);
        debug_assert!(self.uncompr_data.capacity() >= MAX_BLOCK_SIZE);
        self.uncompr_data.clear();

        let extra_length = {
            let header = &mut reading_buffer[..12];
            stream.read_exact(header)
                .map_err(|e| Error::new(e.kind(), format!("Failed to read bgzip block ({})", e)))?;
            // TODO: Try read header and extra fields simultaniously
            Block::analyze_header(header)? as usize
        };
        let block_size = {
            let extra_fields = &mut reading_buffer[12..12 + extra_length];
            stream.read_exact(extra_fields)
                .map_err(|e| Error::new(e.kind(), format!("Failed to read bgzip block ({})", e)))?;
            Block::analyze_extra_fields(extra_fields)? as usize + 1
        };

        stream.read_exact(&mut reading_buffer[12 + extra_length..block_size])
            .map_err(|e| Error::new(e.kind(), format!("Failed to read bgzip block ({})", e)))?;
        let mut decoder = deflate::Decoder::new(&reading_buffer[12 + extra_length..block_size - 8]);
        let obs_uncompr_size = decoder.read_to_end(&mut self.uncompr_data)
            .map_err(|e| Error::new(e.kind(), format!("Failed to read bgzip block ({})", e)))?;
        
        let exp_crc32 = (&reading_buffer[block_size - 8..block_size - 4])
            .read_u32::<LittleEndian>()
            .map_err(|e| Error::new(e.kind(), format!("Corrupted bgzip block ({})", e)))?;
        let exp_uncompr_size = (&reading_buffer[block_size - 4..block_size])
            .read_u32::<LittleEndian>()
            .map_err(|e| Error::new(e.kind(), format!("Failed to read bgzip block ({})", e)))?;

        let obs_crc32 = crc::crc32::checksum_ieee(&self.uncompr_data);
        if obs_crc32 != exp_crc32 {
            return Err(Error::new(InvalidData,
                format!("Corrupted bgzip block. CRC do not match: expected {}, observed {}",
                exp_crc32, obs_crc32)));
        }
        if exp_uncompr_size as usize != obs_uncompr_size {
            return Err(Error::new(InvalidData,
                format!("Corrupted bgzip block. Uncompressed size do not: expected {}, observed {}",
                exp_uncompr_size, obs_uncompr_size)));
        }
        debug_assert!(obs_uncompr_size == self.uncompr_data.len());
        self.compressed_size = block_size;

        Ok(())
    }

    /// Analyzes 12 heades bytes of a block.
    /// Returns XLEN - total length of extra subfields.
    fn analyze_header(header: &[u8]) -> Result<u16> {
        if header[0] != 31 || header[1] != 139 || header[2] != 8 || header[3] != 4 {
            return Err(Error::new(InvalidData, "bgzip::Block has an invalid header"));
        }
        Ok(as_u16(header, 10))
    }

    /// Analyzes extra fields following the header.
    /// Returns BSIZE - total block size - 1.
    fn analyze_extra_fields(extra_fields: &[u8]) -> Result<u16> {
        let mut i = 0;
        while i + 3 < extra_fields.len() {
            let subfield_id1 = extra_fields[i];
            let subfield_id2 = extra_fields[i + 1];
            let subfield_len = as_u16(extra_fields, i + 2);
            if subfield_id1 == 66 && subfield_id2 == 67 && subfield_len == 2 {
                if subfield_len != 2 || i + 5 >= extra_fields.len() {
                    return Err(Error::new(InvalidData, "bgzip::Block has an invalid header"));
                }
                return Ok(as_u16(extra_fields, i + 4));
            }
            i += 4 + subfield_len as usize;
        }
        Err(Error::new(InvalidData, "bgzip::Block has an invalid header"))
    }

    pub fn contents(&self, start: usize, end: usize) -> &[u8] {
        &self.uncompr_data[start..end]
    }

    pub fn uncompressed_size(&self) -> usize {
        self.uncompr_data.len()
    }

    pub fn compressed_size(&self) -> usize {
        self.compressed_size
    }
}

pub struct Reader<R: Read + Seek> {
    stream: R,
    current_offset: u64,
    cache: LruCache<u64, Block>,
    reading_buffer: Vec<u8>,
}

impl Reader<File> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let stream = File::open(path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open bgzip reader ({})", e)))?;
        Reader::new(stream)
    }
}

const LRU_CAPACITY: usize = 1000_usize;

impl<R: Read + Seek> Reader<R> {
    pub fn new(stream: R) -> Result<Self> {
        Ok(Reader {
            stream,
            current_offset: 0,
            cache: LruCache::new(LRU_CAPACITY),
            reading_buffer: vec![0; MAX_BLOCK_SIZE],
        })
    }

    pub fn get_block<'a>(&'a mut self, offset: u64) -> Result<&'a Block> {
        if self.cache.contains_key(&offset) {
            return Ok(self.cache.get_mut(&offset)
                .expect("Cache should contain the requested block"));
        }

        if offset != self.current_offset {
            self.stream.seek(SeekFrom::Start(offset))?;
            self.current_offset = offset;
        }
        
        let mut block = Block::new();
        block.fill(&mut self.stream, &mut self.reading_buffer)?;
        self.cache.insert(offset, block);
        Ok(self.cache.get_mut(&offset).expect("Cache should contain the requested block"))
    }
}

struct ChunksReader<'a, R: Read + Seek> {
    reader: &'a mut Reader<R>,
    chunks: Vec<Chunk>,
    chunk_ix: usize,
    block_offset: u64,
    in_block_offset: usize,
}

impl<'a, R: Read + Seek> ChunksReader<'a, R> {
    fn new(reader: &'a mut Reader<R>, chunks: Vec<Chunk>) -> Self {
        let (block_offset, in_block_offset) = if chunks.len() > 0 {
            (chunks[0].start().compr_offset(), chunks[0].start().uncompr_offset() as usize)
        } else {
            (0, 0)
        };
        ChunksReader {
            reader, chunks,
            chunk_ix: 0,
            block_offset, in_block_offset,
        }
    }
}

impl<'a, R: Read + Seek> Read for ChunksReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.chunk_ix >= self.chunks.len() {
            return Ok(0);
        }

        let chunk = &self.chunks[self.chunk_ix];
        let block = self.reader.get_block(self.block_offset)?;

        let mut bytes = if chunk.end().compr_offset() == self.block_offset {
            // Last block in a chunk
            chunk.end().uncompr_offset() as usize - self.in_block_offset
        } else {
            block.uncompressed_size() - self.in_block_offset
        };
        bytes = min(bytes, buf.len());
        buf.copy_from_slice(block.contents(self.in_block_offset, self.in_block_offset + bytes));
        
        self.in_block_offset += bytes;
        if (chunk.end().compr_offset() == self.block_offset
                && self.in_block_offset == chunk.end().uncompr_offset() as usize) {
            // Last block in a chunk
            self.chunk_ix += 1;
        } else if block.uncompressed_size() == self.in_block_offset {
            self.block_offset += block.compressed_size() as u64;
        }
        Ok(bytes)
    }
}
