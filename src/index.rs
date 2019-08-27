use std::io::{Result, Error};
use std::io::ErrorKind::InvalidData;
use std::path::Path;
use std::fs::File;
use std::fmt::{self, Debug, Display, Formatter};
use std::result;
use std::collections::HashMap;
use std::cmp::{min, max};

use byteorder::{LittleEndian, ReadBytesExt};

// Virtual Offset

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VirtualOffset(u64);

impl VirtualOffset {
    fn new<R: ReadBytesExt>(stream: &mut R) -> Result<Self> {
        Ok(VirtualOffset(stream.read_u64::<LittleEndian>()?))
    }

    fn from(compr_offset: u64, uncompr_offset: u16) -> Self {
        VirtualOffset(compr_offset << 16 | uncompr_offset as u64)
    }

    pub fn raw(&self) -> u64 {
        self.0
    }

    pub fn compr_offset(&self) -> u64 {
        self.0 >> 16
    }

    pub fn uncompr_offset(&self) -> u16 {
        self.0 as u16
    }
}

impl Display for VirtualOffset {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "c={},u={}", self.compr_offset(), self.uncompr_offset())
    }
}


// Chunk

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Chunk {
    start: VirtualOffset,
    end: VirtualOffset,
}

impl Chunk {
    fn new<R: ReadBytesExt>(stream: &mut R) -> Result<Self> {
        let start = VirtualOffset::new(stream)?;
        let end = VirtualOffset::new(stream)?;
        Ok(Chunk { start, end })
    }

    fn intersects(&self, other: &Chunk) -> bool {
        self.start < other.end && other.start < self.end
    }

    fn combine(&self, other: &Chunk) -> Chunk {
        assert!(self.intersects(other), "Cannot combine non-intersecting chunks");
        Chunk {
            start: min(self.start, other.start),
            end: max(self.end, other.end),
        }
    }

    pub fn start(&self) -> VirtualOffset {
        self.start
    }

    pub fn end(&self) -> VirtualOffset {
        self.end
    }
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{{{}__{}}}", self.start, self.end)
    }
}

impl Display for Chunk {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{{{}__{}}}", self.start, self.end)
    }
}

// Bin

struct Bin {
    bin_id: u32,
    chunks: Vec<Chunk>,
}

impl Bin {
    fn new<R: ReadBytesExt>(stream: &mut R) -> Result<Self> {
        let bin_id = stream.read_u32::<LittleEndian>()?;
        let n_chunks = stream.read_i32::<LittleEndian>()?;
        let chunks = (0..n_chunks).map(|_| Chunk::new(stream)).collect::<Result<_>>()?;
        Ok(Bin { bin_id, chunks })
    }
}

impl Display for Bin {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Bin {}:  ", self.bin_id)?;
        self.chunks.iter().enumerate()
            .map(|(i, chunk)| write!(f, "{}{}", if i > 0 { ",  " } else { "" }, chunk))
            .collect::<result::Result<_, _>>()
    }
}

struct Reference {
    bins: HashMap<u32, Bin>,
}

const SUMMARY_BIN: u32 = 37450;

impl Reference {
    fn new<R: ReadBytesExt>(stream: &mut R) -> Result<Self> {
        let n_bins = stream.read_i32::<LittleEndian>()?;
        let bins = (0..n_bins).map(|_| {
            let bin = Bin::new(stream)?;
            Ok((bin.bin_id, bin))
        }).collect::<Result<_>>()?;
        let n_intervals = stream.read_i32::<LittleEndian>()?;
        // Linear index is not used in the current version
        let mut intervals = vec![0_u64; n_intervals as usize];
        stream.read_u64_into::<LittleEndian>(&mut intervals)?;
        Ok(Reference { bins })
    }
}

impl Display for Reference {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        writeln!(f, "    Bins:")?;
        self.bins.values().map(|bin| writeln!(f, "        {}", bin))
            .collect::<result::Result<_, _>>()
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

    pub fn fetch_chunks(&self, ref_id: i32, start: i32, end: i32) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        for bin_id in region_to_bins(start, end).into_iter() {
            if let Some(bin) = self.references[ref_id as usize].bins.get(&bin_id) {
                println!("Adding bin {}:   {:?}", bin_id, bin.chunks);
                chunks.extend(bin.chunks.iter());
            }
        }
        let mut res = Vec::new();
        if chunks.is_empty() {
            return res;
        }

        chunks.sort();
        let mut curr = chunks[0].clone();
        for i in 1..chunks.len() {
            if !curr.intersects(&chunks[i]) {
                res.push(curr);
                curr = chunks[i].clone();
            } else {
                curr = curr.combine(&chunks[i]);
            }
        }
        res.push(curr);
        res
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
            res = ((1 << 29 - i) - 1) / 7 + (beg >> i);
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
        let start = (t + (beg >> 26 - 3 * i)) as u32;
        let end = (t + (end >> 26 - 3 * i)) as u32;
        res.extend(start..=end);
    }
    res
}
