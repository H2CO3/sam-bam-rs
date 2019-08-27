use std::io::{self, Read, ErrorKind};
use std::fmt::{self, Display, Formatter};

use byteorder::{LittleEndian, ReadBytesExt};

pub enum IntegerType {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
}

pub enum TagValue {
    Int(i64, IntegerType),
    Float(f32),
    String(Vec<u8>),
    Hex(Vec<u8>),
    IntArray(Vec<i64>, IntegerType),
    FloatArray(Vec<f32>),
}

impl TagValue {
    fn vec_from_stream<R: Read>(stream: &mut R) -> io::Result<Self> {
        use TagValue::*;
        use IntegerType::*;

        let ty = stream.read_u8()?;
        let size = stream.read_i32::<LittleEndian>()? as usize;
        match ty {
            b'c' => Ok(IntArray((0..size)
                .map(|_| stream.read_i8().map(|el| el as i64))
                .collect::<io::Result<_>>()?, I8)),
            b'C' => Ok(IntArray((0..size)
                .map(|_| stream.read_u8().map(|el| el as i64))
                .collect::<io::Result<_>>()?, U8)),
            b's' => Ok(IntArray((0..size)
                .map(|_| stream.read_i16::<LittleEndian>().map(|el| el as i64))
                .collect::<io::Result<_>>()?, I16)),
            b'S' => Ok(IntArray((0..size)
                .map(|_| stream.read_u16::<LittleEndian>().map(|el| el as i64))
                .collect::<io::Result<_>>()?, U16)),
            b'i' => Ok(IntArray((0..size)
                .map(|_| stream.read_i32::<LittleEndian>().map(|el| el as i64))
                .collect::<io::Result<_>>()?, I16)),
            b'I' => Ok(IntArray((0..size)
                .map(|_| stream.read_u32::<LittleEndian>().map(|el| el as i64))
                .collect::<io::Result<_>>()?, U32)),
            b'f' => {
                let mut float_vec = vec![0.0_f32; size];
                stream.read_f32_into::<LittleEndian>(&mut float_vec)?;
                Ok(FloatArray(float_vec))
            },
            _ => Err(io::Error::new(ErrorKind::InvalidData, "Corrupted record: Failed to read a tag")),
        }
    }

    pub fn from_stream<R: Read>(stream: &mut R) -> io::Result<Self> {
        use TagValue::*;
        use IntegerType::*;

        let ty = stream.read_u8()?;
        match ty {
            b'c' => Ok(Int(stream.read_i8()? as i64, I8)),
            b'C' => Ok(Int(stream.read_u8()? as i64, U8)),
            b's' => Ok(Int(stream.read_i16::<LittleEndian>()? as i64, I16)),
            b'S' => Ok(Int(stream.read_u16::<LittleEndian>()? as i64, U16)),
            b'i' => Ok(Int(stream.read_i32::<LittleEndian>()? as i64, I32)),
            b'I' => Ok(Int(stream.read_u32::<LittleEndian>()? as i64, U32)),
            b'f' => Ok(Float(stream.read_f32::<LittleEndian>()?)),
            b'Z' | b'H' => {
                let mut res = Vec::new();
                loop {
                    let symbol = stream.read_u8()?;
                    if symbol == 0 {
                        break;
                    } else {
                        res.push(symbol);
                    }
                }
                if ty == b'Z' {
                    Ok(String(res))
                } else {
                    Ok(Hex(res))
                }
            },
            b'B' => TagValue::vec_from_stream(stream),
            _ => Err(io::Error::new(ErrorKind::InvalidData, "Corrupted record: Failed to read a tag")),
        }
    }
}

struct Tag {
    key: [u8; 2],
    value: TagValue,
}

impl Tag {
    fn from_stream<R: Read>(stream: &mut R) -> io::Result<Self> {
        let mut key = [0_u8; 2];
        stream.read_exact(&mut key)?;
        let value = TagValue::from_stream(stream)?;
        Ok(Tag { key, value })
    }
}

pub const READ_PAIRED: u16 = 0x1;
pub const ALL_SEGMENTS_ALIGNED: u16 = 0x2;
pub const READ_UNMAPPED: u16 = 0x4;
pub const MATE_UNMAPPED: u16 = 0x8;
pub const READ_REVERSE_STRAND: u16 = 0x10;
pub const MATE_REVERSE_STRAND: u16 = 0x20;
pub const SECOND_IN_PAIR: u16 = 0x80;
pub const FIRST_IN_PAIR: u16 = 0x40;
pub const NOT_PRIMARY: u16 = 0x100;
pub const READ_FAILS_QC: u16 = 0x200;
pub const PCR_OR_OPTICAL_DUPLICATE: u16 = 0x400;
pub const SUPPLEMENTARY: u16 = 0x800;

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
    tags: Vec<Tag>,
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
            tags: Vec::new(),
        }
    }

    pub fn fill_from<R: Read>(&mut self, stream: &mut R) -> Result<(), Error> {
        let block_size = match stream.read_i32::<LittleEndian>() {
            Ok(value) => value as usize,
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

        let remaining_size = block_size - 32 - name_len - 1 - cigar_len - 2 * seq_len;
        let mut tags_vec = vec![0; remaining_size];
        stream.read_exact(&mut tags_vec)?;
        let tags_reader = &tags_vec;
        self.tags.clear();
        while tags_reader.len() > 0 {
            self.tags.push(Tag::from_stream(stream)?);
        }
        Ok(())
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        let name = unsafe {
            std::str::from_utf8_unchecked(&self.name)
        };

        // TODO: Reference name
        write!(f, "{name}\t{flag}\t{ref}\t{pos}\t{mapq}", name=name, flag=self.flag,
            ref=self.ref_id, pos=self.pos, mapq=self.mapq)?;
        writeln!(f)
    }
}