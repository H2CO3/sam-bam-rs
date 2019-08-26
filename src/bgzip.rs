use std::fs::File;
use std::io::{Read, Seek, Result, Error, SeekFrom};
use std::io::ErrorKind::InvalidData;
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt};
use libflate::deflate;

const MAX_BLOCK_SIZE: usize = 65536_usize;

fn as_u16(buffer: &[u8], start: usize) -> u16 {
    buffer[start] as u16 + ((buffer[start + 1] as u16) << 8)
}

pub struct Block {
    uncompr_data: Vec<u8>,
}

impl Block {
    fn new() -> Block {
        Block {
            uncompr_data: Vec::with_capacity(MAX_BLOCK_SIZE),
        }
    }

    /// Load bgzip block from `stream` into `self`. Returns the loaded block size
    ///
    /// # Arguments
    ///
    /// * `reading_buffer` - Buffer for reading the compressed block.
    ///     It should have length >= `MAX_BLOCK_SIZE`
    fn fill<R: Read>(&mut self, stream: &mut R, reading_buffer: &mut Vec<u8>) -> Result<usize> {
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
        Ok(block_size)
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
}

pub struct Reader<R: Read + Seek> {
    stream: R,
    current_offset: u64,
    block: Block,
    reading_buffer: Vec<u8>,
}

impl Reader<File> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let stream = File::open(path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open bgzip reader ({})", e)))?;
        Reader::new(stream)
    }
}

impl<R: Read + Seek> Reader<R> {
    pub fn new(stream: R) -> Result<Self> {
        Ok(Reader {
            stream,
            current_offset: 0,
            block: Block::new(),
            reading_buffer: vec![0; MAX_BLOCK_SIZE],
        })
    }

    pub fn get_block<'a>(&'a mut self, offset: u64) -> Result<&'a Block> {
        if offset != self.current_offset {
            self.stream.seek(SeekFrom::Start(offset))?;
            self.current_offset = offset;
        }
        self.block.fill(&mut self.stream, &mut self.reading_buffer)?;
        Ok(&self.block)
    }
}