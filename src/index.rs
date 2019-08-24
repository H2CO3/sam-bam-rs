use std::io::{Result, Error};
use std::io::ErrorKind::InvalidData;

use byteorder::{LittleEndian, ReadBytesExt};

struct Bin {
    bin_id: u32,
    chunks: Vec<(u64, u64)>,
}

impl Bin {
    fn new<R: ReadBytesExt>(stream: &mut R) -> Result<Self> {
        let bin_id = stream.read_u32::<LittleEndian>()?;
        let n_chunks = stream.read_i32::<LittleEndian>()?;
        let chunks = (0..n_chunks).map(|_| -> Result<(u64, u64)> {
                Ok((stream.read_u64::<LittleEndian>()?, stream.read_u64::<LittleEndian>()?))
            }).collect::<Result<_>>()?;
        Ok(Bin { bin_id, chunks })
    }
}

struct Reference {
    bins: Vec<Bin>,
    intervals: Vec<u64>,
}

impl Reference {
    fn new<R: ReadBytesExt>(stream: &mut R) -> Result<Self> {
        let n_bins = stream.read_i32::<LittleEndian>()?;
        let bins = (0..n_bins).map(|_| Bin::new(stream)).collect::<Result<_>>()?;
        let n_intervals = stream.read_i32::<LittleEndian>()?;
        let mut intervals = vec![0_u64; n_intervals as usize];
        stream.read_u64_into::<LittleEndian>(&mut intervals)?;
        Ok(Reference { bins, intervals })
    }
}

pub struct Index {
    references: Vec<Reference>,
    n_unmapped: Option<u64>,
}

impl Index {
    pub fn new<R: ReadBytesExt>(mut stream: R) -> Result<Index> {
        let mut magic = [0_u8; 4];
        stream.read_exact(&mut magic)?;
        if magic != [b'B', b'A', b'I', 1] {
            return Err(Error::new(InvalidData, "Input is not in BAI format"));
        }

        let n_ref = stream.read_i32::<LittleEndian>()?;
        let references = (0..n_ref).map(|_| Reference::new(&mut stream)).collect::<Result<_>>()?;
        let n_unmapped = stream.read_u64::<LittleEndian>().ok();
        Ok(Index { references, n_unmapped })
    }
}