use std::io::{self, Read, ErrorKind};

use byteorder::{LittleEndian, ReadBytesExt};

const READ_PAIRED: u16 = 0x1;
const ALL_SEGMENTS_ALIGNED: u16 = 0x2;
const READ_UNMAPPED: u16 = 0x4;
const MATE_UNMAPPED: u16 = 0x8;
const READ_REVERSE_STRAND: u16 = 0x10;
const MATE_REVERSE_STRAND: u16 = 0x20;
const FIRST_IN_PAIR: u16 = 0x40;
const SECOND_IN_PAIR: u16 = 0x80;
const NOT_PRIMARY: u16 = 0x100;
const READ_FAILS_QC: u16 = 0x200;
const PCR_OR_OPTICAL_DUPLICATE: u16 = 0x400;
const SUPPLEMENTARY: u16 = 0x800;

pub struct Record {
    ref_id: i32,
    next_ref_id: i32,
    pos: i32,
    next_pos: i32,
    mapq: u8,
    flag: u16,
    template_len: i32,

    name: Vec<u8>,
    cigar: Vec<u32>,
    seq: Vec<u8>,
    qual: Vec<u8>,
    // tags
}

pub enum Error {
    IoError(io::Error),
    NoMoreReads,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

unsafe fn resize<T>(v: &mut Vec<T>, new_len: usize) {
    if v.capacity() < new_len {
        v.reserve(new_len - v.len());
    }
    v.set_len(new_len);
}

impl Record {
    pub fn new() -> Record {
        Record {
            ref_id: -1,
            next_ref_id: -1,
            pos: -1,
            next_pos: -1,
            mapq: 0,
            flag: 0,
            template_len: 0,

            name: Vec::new(),
            cigar: Vec::new(),
            seq: Vec::new(),
            qual: Vec::new(),
        }
    }

    pub fn fill_from<R: Read>(&mut self, stream: &mut R) -> Result<(), Error> {
        let block_size = match stream.read_i32::<LittleEndian>() {
            Ok(value) => value,
            Err(e) => {
                return Err(if e.kind() == ErrorKind::UnexpectedEof {
                    Error::NoMoreReads
                } else {
                    Error::from(e)
                })
            },
        };
        self.ref_id = stream.read_i32::<LittleEndian>()?;
        self.pos = stream.read_i32::<LittleEndian>()?;
        let name_len = stream.read_u8()? as usize - 1;
        self.mapq = stream.read_u8()?;
        let _bin = stream.read_u16::<LittleEndian>()?;
        let cigar_len = stream.read_u16::<LittleEndian>()? as usize;
        self.flag = stream.read_u16::<LittleEndian>()?;
        let seq_len = stream.read_i32::<LittleEndian>()? as usize;
        self.next_ref_id = stream.read_i32::<LittleEndian>()?;
        self.next_pos = stream.read_i32::<LittleEndian>()?;
        self.template_len = stream.read_i32::<LittleEndian>()?;

        unsafe {
            resize(&mut self.name, name_len);
            resize(&mut self.cigar, cigar_len);
            resize(&mut self.seq, seq_len);
            resize(&mut self.qual, seq_len);
        }

        stream.read_exact(&mut self.name)?;
        let _null_symbol = stream.read_u8()?;
        stream.read_u32_into::<LittleEndian>(&mut self.cigar)?;
        stream.read_exact(&mut self.seq)?;
        stream.read_exact(&mut self.qual)?;

        unimplemented!();
    }
}
