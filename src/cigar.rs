use std::io::{self, Read, ErrorKind};

use byteorder::{LittleEndian, ReadBytesExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    AlnMatch = 0,
    Insertion = 1,
    Deletion = 2,
    Skip = 3,
    Soft = 4,
    Hard = 5,
    Padding = 6,
    SeqMatch = 7,
    SeqMismatch = 8,
}

impl Operation {
    pub fn from_char(symbol: u8) -> Operation {
        use Operation::*;
        match symbol {
            b'M' => AlnMatch,
            b'I' => Insertion,
            b'D' => Deletion,
            b'N' => Skip,
            b'S' => Soft,
            b'H' => Hard,
            b'P' => Padding,
            b'=' => SeqMatch,
            b'X' => SeqMismatch,
            _ => panic!("Unexpected cigar operation: {}", symbol as char),
        }
    }

    pub fn to_char(self) -> u8 {
        b"MIDNSHP=X"[self as usize]
    }
}

impl From<u32> for Operation {
    fn from(value: u32) -> Operation {
        use Operation::*;
        match value {
            0 => AlnMatch,
            1 => Insertion,
            2 => Deletion,
            3 => Skip,
            4 => Soft,
            5 => Hard,
            6 => Padding,
            7 => SeqMatch,
            8 => SeqMismatch,
            _ => panic!("Unexpected cigar operation: {}", value),
        }
    }
}

pub struct Cigar(Vec<u32>);

impl Cigar {
    pub(crate) fn new() -> Self {
        Cigar(Vec::new())
    }

    pub(crate) fn fill_from<R: ReadBytesExt>(&mut self, stream: &mut R, len: usize)
            -> io::Result<()> {
        unsafe {
            super::record::resize(&mut self.0, len);
        }
        stream.read_u32_into::<LittleEndian>(&mut self.0)?;
        Ok(())
    }

    pub fn at(&self, index: usize) -> (u32, Operation) {
        let v = self.0[index];
        (v >> 4, Operation::from(v & 0xff))
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, Operation)> + '_ {
        (0..self.0.len()).map(move |i| self.at(i))
    }
}
