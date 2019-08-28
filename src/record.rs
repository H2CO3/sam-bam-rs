use std::io::{self, Read, ErrorKind, Write};
use std::io::ErrorKind::InvalidData;
use std::fmt::{self, Display, Debug, Formatter};

use byteorder::{LittleEndian, ReadBytesExt};

use super::cigar::{self, Cigar};
use super::bam_reader::Header;

/// Enum that represents tag type for the cases when a tag contains integer.
///
/// Possible values are `I8` (`c`), `U8` (`C`), `I16` (`s`), `U16` (`S`), `I32` (`i`) and `U32` (`I`).
pub enum IntegerType {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
}

impl IntegerType {
    /// Returns a letter that represents the integer type. For example, `i8` corresponds to `c`.
    pub fn letter(&self) -> u8 {
        use IntegerType::*;
        match self {
            I8 => b'c',
            U8 => b'C',
            I16 => b's',
            U16 => b'S',
            I32 => b'i',
            U32 => b'I',
        }
    }
}

/// Enum with all possible tag values.
///
/// If a tag contains integer value, or array with integer values, this enum will store `i64`,
/// to be able to contain both types `i` (`i32`) and `I` (`u32`).
pub enum TagValue {
    Char(u8),
    Int(i64, IntegerType),
    Float(f32),
    String(Vec<u8>),
    Hex(Vec<u8>),
    IntArray(Vec<i64>, IntegerType),
    FloatArray(Vec<f32>),
}

impl TagValue {
    fn vec_from_stream<R: Read>(stream: &mut R) -> Result<Self, Error> {
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
            _ => Err(Error::Corrupted("Failed to read a tag")),
        }
    }

    fn from_stream<R: Read>(stream: &mut R) -> Result<Self, Error> {
        use TagValue::*;
        use IntegerType::*;

        let ty = stream.read_u8()?;
        match ty {
            b'A' => Ok(Char(stream.read_u8()?)),
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
            _ => Err(Error::Corrupted("Failed to read a tag (unexpected tag type)")),
        }
    }
}

impl Display for TagValue {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        use TagValue::*;
        match self {
            Char(value) => write!(f, "A:{}", *value as char),
            Int(value, _) => write!(f, "i:{}", value),
            Float(value) => write!(f, "f:{}", value),
            String(value) => write!(f, "Z:{}", std::string::String::from_utf8_lossy(value)),
            Hex(value) => write!(f, "H:{}", std::str::from_utf8(value)
                .expect("Corrupted record: Hex tag is not in UTF-8")),
            IntArray(array, ty) => {
                write!(f, "B:{}", ty.letter() as char)?;
                for value in array.iter() {
                    write!(f, ",{}", value)?;
                }
                Ok(())
            },
            FloatArray(array) => {
                write!(f, "B:f")?;
                for value in array.iter() {
                    write!(f, ",{}", value)?;
                }
                Ok(())
            },
        }
    }
}

struct Tag {
    key: [u8; 2],
    value: TagValue,
}

impl Tag {
    fn new(key: [u8; 2], value: TagValue) -> Self {
        Tag { key, value }
    }

    fn from_stream<R: Read>(stream: &mut R) -> Result<Self, Error> {
        let mut key = [0_u8; 2];
        stream.read_exact(&mut key)?;
        let value = TagValue::from_stream(stream)?;
        Ok(Tag { key, value })
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}{}:", self.key[0] as char, self.key[1] as char)?;
        self.value.fmt(f)
    }
}

/// Wrapper around raw sequence, stored as an `[u8; (len + 1) / 2]`. Each four bits encode a
/// nucleotide in the following order: `=ACMGRSVTWYHKDBN`.
///
/// Using `Display` on an empty sequence produces `*`.
pub struct Sequence {
    data: Vec<u8>,
    len: usize,
}

impl Sequence {
    fn new() -> Self {
        Sequence {
            data: Vec::new(),
            len: 0,
        }
    }

    fn fill_from<R: Read>(&mut self, stream: &mut R, expanded_len: usize) -> io::Result<()> {
        let short_len = (expanded_len + 1) / 2;
        unsafe {
            resize(&mut self.data, short_len);
        }
        stream.read_exact(&mut self.data)?;
        self.len = expanded_len;
        Ok(())
    }

    /// Returns raw data.
    pub fn raw(&self) -> &[u8] {
        &self.data
    }

    /// Returns full length of the sequence, O(1).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns transformed data, each byte is a single nucleotde, O(n).
    pub fn to_vec(&self) -> Vec<u8> {
        (0..self.len).map(|i| self.at(i)).collect()
    }

    /// Returns transformed data with only nucleotides `A`, `C`, `G`, `T` and `N`,
    /// all other values are transformed into `N`, each byte is a single nucleotde, O(n).
    pub fn to_vec_acgtn_only(&self) -> Vec<u8> {
        (0..self.len).map(|i| self.at_acgtn_only(i)).collect()
    }

    /// Returns a nucleotide at the position `index`, represented by a single byte, O(1).
    pub fn at(&self, index: usize) -> u8 {
        if index >= self.len {
            panic!("Index out of range ({} >= {})", index, self.len);
        }
        let nt = if index % 2 == 0 {
            self.data[index / 2] >> 4
        } else {
            self.data[index / 2] & 0x0f
        };
        b"=ACMGRSVTWYHKDBN"[nt as usize]
    }

    /// Returns a nucleotide at the position `index`, represented by a single byte, O(1).
    /// If nucleotide is not `A`, `C`, `G` or `T`, the function returns `N`.
    pub fn at_acgtn_only(&self, index: usize) -> u8 {
        if index >= self.len {
            panic!("Index out of range ({} >= {})", index, self.len);
        }
        let nt = if index % 2 == 0 {
            self.data[index / 2] >> 4
        } else {
            self.data[index / 2] & 0x0f
        };
        b"NACNGNNNTNNNNNNN"[nt as usize]
    }
}

impl Display for Sequence {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        if self.data.len() == 0 {
            write!(f, "*")
        } else {
            for i in 0..self.len {
                write!(f, "{}", self.at(i) as char)?;
            }
            Ok(())
        }
    }
}

/// Wrapper around qualities. Raw data can be accessed as `qualities.0`, and it contains
/// values 0-93, without +33 added.
///
/// If `Qualities` are empty or contain `0xff`, the `Display` trait would produce `*`.
pub struct Qualities(pub Vec<u8>);

impl Qualities {
    fn new() -> Self {
        Qualities(Vec::new())
    }

    fn fill_from<R: Read>(&mut self, stream: &mut R, len: usize) -> io::Result<()> {
        unsafe {
            resize(&mut self.0, len);
        }
        stream.read_exact(&mut self.0)?;
        Ok(())
    }

    /// Return vector with +33 added, O(n).
    pub fn to_readable(&self) -> Vec<u8> {
        self.0.iter().map(|qual| qual + 33).collect()
    }
}

impl Display for Qualities {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        if self.0.len() == 0 || self.0[0] == 0xff {
            write!(f, "*")
        } else {
            for &qual in self.0.iter() {
                write!(f, "{}", (qual + 33) as char)?;
            }
            Ok(())
        }
    }
}

pub const READ_PAIRED: u16 = 0x1;
pub const ALL_SEGMENTS_ALIGNED: u16 = 0x2;
pub const READ_UNMAPPED: u16 = 0x4;
pub const MATE_UNMAPPED: u16 = 0x8;
pub const READ_REVERSE_STRAND: u16 = 0x10;
pub const MATE_REVERSE_STRAND: u16 = 0x20;
pub const FIRST_IN_PAIR: u16 = 0x40;
pub const LAST_IN_PAIR: u16 = 0x80;
pub const SECONDARY: u16 = 0x100;
pub const READ_FAILS_QC: u16 = 0x200;
pub const PCR_OR_OPTICAL_DUPLICATE: u16 = 0x400;
pub const SUPPLEMENTARY: u16 = 0x800;

/// Error produced while reading [Record](struct.Record.html).
///
/// # Variants
///
/// * `NoMoreRecords` - represents `StopIteration`,
/// * `Corrupted(s)` - shows that reading the record produced impossible values, like negative length
/// of the sequence. `s` contains additional information about the problem.
/// * `Truncated(e)` - shows that reading the record was interrupted by `io::Error`.
/// `e` contains the causing error.
pub enum Error {
    NoMoreRecords,
    Corrupted(&'static str),
    Truncated(io::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Truncated(e)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            Error::NoMoreRecords => write!(f, "No more records"),
            Error::Corrupted(e) => write!(f, "Corrupted record: {}", e),
            Error::Truncated(e) => write!(f, "Truncated record: {}", e),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            Error::NoMoreRecords => write!(f, "No more records"),
            Error::Corrupted(e) => write!(f, "Corrupted record: {}", e),
            Error::Truncated(e) => write!(f, "Truncated record: {}", e),
        }
    }
}

pub(crate) unsafe fn resize<T>(v: &mut Vec<T>, new_len: usize) {
    if v.capacity() < new_len {
        v.reserve(new_len - v.len());
    }
    v.set_len(new_len);
}

/// BAM Record
pub struct Record {
    ref_id: i32,
    next_ref_id: i32,
    start: i32,
    end: Option<i32>,
    next_start: i32,
    mapq: u8,
    flag: u16,
    template_len: i32,

    name: Vec<u8>,
    cigar: Cigar,
    seq: Sequence,
    qual: Qualities,
    tags: Vec<Tag>,
}

impl Record {
    /// Creates an empty record. Can be filled using
    /// [read_into](../bam_reader/struct.RegionViewer.html#method.read_into).
    pub fn new() -> Record {
        Record {
            ref_id: -1,
            next_ref_id: -1,
            start: -1,
            end: None,
            next_start: -1,
            mapq: 0,
            flag: 0,
            template_len: 0,

            name: Vec::new(),
            cigar: Cigar::new(),
            seq: Sequence::new(),
            qual: Qualities::new(),
            tags: Vec::new(),
        }
    }

    /// Fills the record from a `stream` of uncompressed BAM contents.
    pub(crate) fn fill_from<R: Read>(&mut self, stream: &mut R) -> Result<(), Error> {
        let block_size = match stream.read_i32::<LittleEndian>() {
            Ok(value) => {
                if value < 0 {
                    return Err(Error::Corrupted("Negative block size"));
                }
                value as usize
            },
            Err(e) => {
                return Err(if e.kind() == ErrorKind::UnexpectedEof {
                    Error::NoMoreRecords
                } else {
                    Error::from(e)
                })
            },
        };
        self.ref_id = stream.read_i32::<LittleEndian>()?;
        if self.ref_id < -1 {
            return Err(Error::Corrupted("Reference id < -1"));
        }
        self.start = stream.read_i32::<LittleEndian>()?;
        if self.start < -1 {
            return Err(Error::Corrupted("Start < -1"));
        }
        self.end = None;
        let name_len = stream.read_u8()?;
        if name_len == 0 {
            return Err(Error::Corrupted("Name length == 0"));
        }
        self.mapq = stream.read_u8()?;
        let _bin = stream.read_u16::<LittleEndian>()?;
        let cigar_len = stream.read_u16::<LittleEndian>()?;
        self.flag = stream.read_u16::<LittleEndian>()?;
        let qual_len = stream.read_i32::<LittleEndian>()?;
        if qual_len < 0 {
            return Err(Error::Corrupted("Negative sequence length"));
        }
        let qual_len = qual_len as usize;
        self.next_ref_id = stream.read_i32::<LittleEndian>()?;
        if self.next_ref_id < -1 {
            return Err(Error::Corrupted("Next reference id < -1"));
        }
        self.next_start = stream.read_i32::<LittleEndian>()?;
        if self.next_start < -1 {
            return Err(Error::Corrupted("Next start < -1"));
        }
        self.template_len = stream.read_i32::<LittleEndian>()?;

        unsafe {
            resize(&mut self.name, name_len as usize - 1);
        }
        stream.read_exact(&mut self.name)?;
        let _null_symbol = stream.read_u8()?;

        self.cigar.fill_from(stream, cigar_len as usize)?;
        self.seq.fill_from(stream, qual_len)?;
        self.qual.fill_from(stream, qual_len)?;

        let seq_len = (qual_len + 1) / 2;
        let remaining_size = block_size - 32 - name_len as usize - 4 * cigar_len as usize
            - seq_len - qual_len;
        let mut tags_vec = vec![0; remaining_size];
        stream.read_exact(&mut tags_vec)?;
        self.tags.clear();
        let mut tags_reader = &tags_vec[..];
        while tags_reader.len() > 0 {
            self.tags.push(Tag::from_stream(&mut tags_reader)?);
        }
        self.replace_cigar_if_needed()?;

        Ok(())
    }

    /// Replace Cigar by CG tag if Cigar has placeholder *kSmN*.
    fn replace_cigar_if_needed(&mut self) -> Result<(), Error> {
        if self.cigar.len() > 0 && self.cigar.at(0) ==
                (self.seq.len as u32, cigar::Operation::Soft) {
            if self.cigar.len() != 2 {
                return Err(Error::Corrupted("Record contains invalid Cigar"));
            }
            let (len, op) = self.cigar.at(1);
            if op != cigar::Operation::Skip {
                return Err(Error::Corrupted("Record contains invalid Cigar"));
            }
            self.end = Some(len as i32);

            let cigar_arr = match self.remove_tag(b"CG") {
                Some(TagValue::IntArray(arr, _)) => arr,
                _ => return Err(Error::Corrupted("Record should contain tag CG, but does not")),
            };
            self.cigar.0.clear();
            self.cigar.0.extend(cigar_arr.into_iter().map(|el| el as u32));
        }
        Ok(())
    }

    /// Shrinks record contents. The more records were read into the same `Record` instance, the
    /// bigger would be inner vectors (to save time on memory allocation).
    /// Use this function if you do not plan to read into this record in the future.
    pub fn shrink_to_fit(&mut self) {
        self.name.shrink_to_fit();
        self.cigar.shrink_to_fit();
        self.seq.data.shrink_to_fit();
        self.qual.0.shrink_to_fit();
    }

    /// Returns record name as bytes.
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Returns record sequence.
    pub fn sequence(&self) -> &Sequence {
        &self.seq
    }

    /// Returns record qualities, if present.
    pub fn qualities(&self) -> Option<&Qualities> {
        if self.qual.0.len() == 0 || self.qual.0[0] == 0xff {
            None
        } else {
            Some(&self.qual)
        }
    }

    /// Returns record CIGAR (can be empty).
    pub fn cigar(&self) -> &Cigar {
        &self.cigar
    }

    /// Returns 0-based reference index. Returns -1 for unmapped records.
    pub fn ref_id(&self) -> i32 {
        self.ref_id
    }

    /// Returns 0-based left-most aligned reference position. Same as *POS - 1* in SAM specification.
    /// Returns -1 for unmapped records.
    pub fn start(&self) -> i32 {
        self.start
    }

    /// For a mapped read aligned to reference positions `[start-end)`, the function returns `end`.
    /// The first calculation takes O(n), where *n* is the length of Cigar.
    /// Consecutive calculations take O(1).
    /// If the read was fetched from a specific region, it should have `end` already calculated.
    ///
    /// Returns -1 for unmapped records.
    pub fn calculate_end(&mut self) -> i32 {
        if self.cigar.len() == 0 {
            -1
        } else if let Some(end) = self.end {
            end
        } else {
            let end = self.start + self.cigar.calculate_aligned_len() as i32;
            self.end = Some(end);
            end
        }
    }

    /// Returns record MAPQ.
    pub fn mapq(&self) -> u8 {
        self.mapq
    }

    /// Returns 0-based reference index for the pair record. Returns -1 for unmapped records,
    /// and records without a pair.
    pub fn next_ref_id(&self) -> i32 {
        self.next_ref_id
    }

    /// Returns 0-based left-most aligned reference position for the pair record.
    /// Same as *PNEXT - 1* in SAM specification.
    /// Returns -1 for unmapped records and records without a pair.
    pub fn next_start(&self) -> i32 {
        self.next_start
    }

    /// Observed template length (TLEN in SAM specification).
    pub fn template_len(&self) -> i32 {
        self.template_len
    }

    /// Return [tag value](enum.TagValue.html) with `tag_name`, if there is one. Takes `O(n_tags)`.
    pub fn get_tag(&self, tag_name: &[u8; 2]) -> Option<&TagValue> {
        for tag in self.tags.iter() {
            if &tag.key == tag_name {
                return Some(&tag.value);
            }
        }
        None
    }

    /// Remove tag and return its value. Returns `None`, if the record does not contain such tag.
    /// Takes `O(n_tags)`.
    pub fn remove_tag(&mut self, tag_name: &[u8; 2]) -> Option<TagValue> {
        for i in 0..self.tags.len() {
            if &self.tags[i].key == tag_name {
                return Some(self.tags.remove(i).value);
            }
        }
        None
    }

    /// Add a tag and its value. Does not check if the tag already exists. Takes O(1).
    pub fn push_tag(&mut self, tag_name: &[u8; 2], value: TagValue) {
        self.tags.push(Tag::new(*tag_name, value))
    }

    /// Add a tag and its value. Replaces an existing tag if there was one and pushes
    /// the new tag to the end otherwise. Takes `O(n_tags)`. Returns old value, if there was one.
    pub fn replace_tag(&mut self, tag_name: &[u8; 2], value: TagValue) -> Option<TagValue> {
        for i in 0..self.tags.len() {
            if &self.tags[i].key == tag_name {
                return Some(std::mem::replace(&mut self.tags[i].value, value));
            }
        }
        None
    }

    /// Write the record in SAM format to `f`. The function needs
    /// [header](../bam_reader/struct.Header.html), as the record itself does not store reference
    /// names.
    pub fn write_sam<W: Write>(&self, f: &mut W, header: &Header) -> io::Result<()> {
        let name = unsafe {
            std::str::from_utf8_unchecked(&self.name)
        };
        write!(f, "{}\t{}\t", name, self.flag)?;
        if self.ref_id < 0 {
            write!(f, "*\t")?;
        } else {
            write!(f, "{}\t", header.reference_name(self.ref_id as usize)
                .ok_or_else(|| io::Error::new(InvalidData,
                "Record has a reference id not in the header"))?)?;
        }
        write!(f, "{}\t{}\t", self.start + 1, self.mapq)?;
        write!(f, "{}\t", self.cigar)?;

        if self.next_ref_id < 0 {
            write!(f, "*\t")?;
        } else if self.next_ref_id == self.ref_id {
            write!(f, "=\t")?;
        } else {
            write!(f, "{}\t", header.reference_name(self.next_ref_id as usize)
                .ok_or_else(|| io::Error::new(InvalidData,
                "Record has a reference id not in the header"))?)?;
        }
        write!(f, "{}\t{}\t", self.next_start + 1, self.template_len)?;
        write!(f, "{}\t{}", self.seq, self.qual)?;
        for tag in self.tags.iter() {
            write!(f, "\t{}", tag)?;
        }
        writeln!(f)
    }

    pub fn is_paired(&self) -> bool {
        self.flag & READ_PAIRED != 0
    }

    pub fn all_segments_aligned(&self) -> bool {
        self.flag & ALL_SEGMENTS_ALIGNED != 0
    }

    pub fn is_mapped(&self) -> bool {
        // EQUAL 0
        self.flag & READ_UNMAPPED == 0
    }

    pub fn mate_is_mapped(&self) -> bool {
        // EQUAL 0
        self.flag & MATE_UNMAPPED == 0
    }

    pub fn is_reverse_strand(&self) -> bool {
        self.flag & READ_REVERSE_STRAND != 0
    }

    pub fn mate_is_reverse_strand(&self) -> bool {
        self.flag & MATE_REVERSE_STRAND != 0
    }

    pub fn first_in_pair(&self) -> bool {
        self.flag & FIRST_IN_PAIR != 0
    }

    pub fn last_in_pair(&self) -> bool {
        self.flag & LAST_IN_PAIR != 0
    }

    pub fn is_secondary(&self) -> bool {
        self.flag & SECONDARY != 0
    }

    pub fn fails_quality_controls(&self) -> bool {
        self.flag & READ_FAILS_QC != 0
    }

    pub fn is_duplicate(&self) -> bool {
        self.flag & PCR_OR_OPTICAL_DUPLICATE != 0
    }

    pub fn is_supplementary(&self) -> bool {
        self.flag & SUPPLEMENTARY != 0
    }
}
