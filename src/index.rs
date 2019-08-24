use std::io::{Result, Error};
use std::io::ErrorKind::InvalidData;
use std::path::Path;
use std::fs::File;
use std::fmt::{self, Display, Formatter};
use std::result;

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

impl Display for Bin {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Bin {} {:?}", self.bin_id, self.chunks)
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

impl Display for Reference {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        writeln!(f, "    Bins:")?;
        self.bins.iter().map(|bin| writeln!(f, "        {}", bin))
            .collect::<result::Result<_, _>>()?;
        writeln!(f, "    Intervals: {:?}", self.intervals)
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

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Index> {
        let f = File::open(&path)?;
        Index::new(f)
    }
}

impl Display for Index {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        for (i, reference) in self.references.iter().enumerate() {
            writeln!(f, "Reference {}:", i)?;
            reference.fmt(f)?;
        }
        write!(f, "Unmapped reads: ")?;
        match self.n_unmapped {
            Some(count) => writeln!(f, "{}", count),
            None => writeln!(f, "Unknown")
        }
    }
}

pub fn region_to_bin(beg: i32, end: i32) -> u32 {
    let end = end - 1;
    let mut res = 0_i32;
    for i in (14..27).step_by(3) {
        if beg >> i == end >> i {
            res = ((1 << (29 - i)) - 1) / 7 + (beg >> i);
            break;
        }
    }
    res as u32
}

pub fn region_to_bins(beg: i32, end: i32) -> Vec<u32> {
    let end = end - 1;
    let mut res = vec![0];
    let mut t = 0;
    for i in 0..5 {
        t += 1 << (i * 3);
        res.extend((t + (beg >> (26 - 3 * i))) as u32..=(t + (end >> (26 - 3 * i))) as u32);
    }
    res
}