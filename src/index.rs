use std::io::{Result, Error, Read};
use std::io::ErrorKind::InvalidData;
use std::path::Path;
use std::fs::File;
use std::fmt::{self, Debug, Display, Formatter};
use std::result;
use std::collections::HashMap;
use std::cmp::{min, max};

use byteorder::{LittleEndian, ReadBytesExt};

/// Virtual offset. Represents `block_offset << 16 | contents_offset`, where
/// `block_offset` is `u48` and represents the offset in the bgzip file to the beginning of th 
/// block (also known as `coffset` or `compressed_offset`).
///
/// `contents_offset` is `u16` and represents offset in the uncompressed data in a single block
/// (also known as `uoffset` or `uncompressed_offset`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VirtualOffset(u64);

impl VirtualOffset {
    /// Construct Virtual offset from raw value.
    pub fn from_raw(raw: u64) -> VirtualOffset {
        VirtualOffset(raw)
    }

    fn from_stream<R: Read>(stream: &mut R) -> Result<Self> {
        Ok(VirtualOffset(stream.read_u64::<LittleEndian>()?))
    }

    /// Construct Virtual offset from `block_offset` and `contents_offset`.
    pub fn from(block_offset: u64, contents_offset: u16) -> Self {
        VirtualOffset(block_offset << 16 | contents_offset as u64)
    }

    /// Get the raw value.
    pub fn raw(&self) -> u64 {
        self.0
    }

    /// Get the block offset. Represents the offset in the BGzip file to the beginning of the block.
    pub fn block_offset(&self) -> u64 {
        self.0 >> 16
    }

    /// Get the contents offset. Represents the offset into the uncompressed contents of the block.
    pub fn contents_offset(&self) -> u16 {
        self.0 as u16
    }

    /// Checks if the `self` is the same as `block_offset << 16 | contents_offset`.
    pub fn equal(&self, block_offset: u64, contents_offset: u16) -> bool {
        self.0 == (block_offset << 16 | contents_offset as u64)
    }
}

impl Display for VirtualOffset {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "c={},u={}", self.block_offset(), self.contents_offset())
    }
}

/// Chunk `[start-end)`, where `start` and `end` are virtual offsets.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Chunk {
    start: VirtualOffset,
    end: VirtualOffset,
}

impl Chunk {
    /// Construct a `Chunk` from two virtual offsets.
    pub fn new(start: VirtualOffset, end: VirtualOffset) -> Self {
        Chunk { start, end }
    }

    fn from_stream<R: Read>(stream: &mut R, check: bool) -> Result<Self> {
        let start = VirtualOffset::from_stream(stream)?;
        let end = VirtualOffset::from_stream(stream)?;
        if check && end <= start {
            Err(Error::new(InvalidData, format!("BAI chunk end < start ({}  <  {})", end, start)))
        } else {
            Ok(Chunk { start, end })
        }
    }

    /// Check if two chunks intersect.
    pub fn intersects(&self, other: &Chunk) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// Combine two intersecting chunks. Fails if chunks do not intersect.
    pub fn combine(&self, other: &Chunk) -> Chunk {
        assert!(self.intersects(other), "Cannot combine non-intersecting chunks");
        Chunk {
            start: min(self.start, other.start),
            end: max(self.end, other.end),
        }
    }

    /// Chunk start.
    pub fn start(&self) -> VirtualOffset {
        self.start
    }

    /// Chunk end.
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

struct Bin {
    bin_id: u32,
    chunks: Vec<Chunk>,
}

impl Bin {
    fn from_stream<R: Read>(stream: &mut R) -> Result<Self> {
        let bin_id = stream.read_u32::<LittleEndian>()?;
        let n_chunks = stream.read_i32::<LittleEndian>()?;
        let check_chunks = bin_id != SUMMARY_BIN;
        let chunks = (0..n_chunks).map(|_| Chunk::from_stream(stream, check_chunks))
            .collect::<Result<_>>()?;
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

/// Per BAM specification, bin with `bin_id == SUMMARY_BIN` contains summary over the reference.
const SUMMARY_BIN: u32 = 37450;

impl Reference {
    fn from_stream<R: Read>(stream: &mut R) -> Result<Self> {
        let n_bins = stream.read_i32::<LittleEndian>()?;
        let bins = (0..n_bins).map(|_| {
            let bin = Bin::from_stream(stream)?;
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

/// BAI Index.
pub struct Index {
    references: Vec<Reference>,
    n_unmapped: Option<u64>,
}

impl Index {
    /// Load index from stream
    pub fn from_stream<R: Read>(mut stream: R) -> Result<Index> {
        let mut magic = [0_u8; 4];
        stream.read_exact(&mut magic)?;
        if magic != [b'B', b'A', b'I', 1] {
            return Err(Error::new(InvalidData, "Input is not in BAI format"));
        }

        let n_ref = stream.read_i32::<LittleEndian>()?;
        let references = (0..n_ref).map(|_| Reference::from_stream(&mut stream))
            .collect::<Result<_>>()?;
        let n_unmapped = stream.read_u64::<LittleEndian>().ok();
        Ok(Index { references, n_unmapped })
    }

    /// Load index from path
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Index> {
        let f = File::open(&path)?;
        Index::from_stream(f)
    }

    /// For a given region the function returns [chunks](struct.Chunk.html) of BAM file that
    /// contain all records in the region.
    pub fn fetch_chunks(&self, ref_id: i32, start: i32, end: i32) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        for bin_id in region_to_bins(start, end).into_iter() {
            if let Some(bin) = self.references[ref_id as usize].bins.get(&bin_id) {
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

/// Get BAI bin for the record with alignment `[beg-end)`.
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

/// Get all possible BAI bins for the region `[beg-end)`.
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
