//! SAM/BAM record, sequence, qualities and operations on them.

use std::io::{self, Read, ErrorKind, Write};
use std::io::ErrorKind::{InvalidData, UnexpectedEof};
use std::fmt::{self, Display, Debug, Formatter};
use std::cell::Cell;
use std::ops::RangeBounds;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub mod tags;
pub mod cigar;

use cigar::Cigar;
use super::header::Header;
use super::index;

/// Converts nucleotide to BAM u4 (for example `b'T'` -> `8`).
fn nt_to_raw(nt: u8) -> Result<u8, String> {
    match nt {
        b'=' => Ok(0),
        b'A' => Ok(1),
        b'C' => Ok(2),
        b'M' => Ok(3),
        b'G' => Ok(4),
        b'R' => Ok(5),
        b'S' => Ok(6),
        b'V' => Ok(7),
        b'T' => Ok(8),
        b'W' => Ok(9),
        b'Y' => Ok(10),
        b'H' => Ok(11),
        b'K' => Ok(12),
        b'D' => Ok(13),
        b'B' => Ok(14),
        b'N' => Ok(15),
        _ => Err(format!("Nucleotide not expected: {}", nt as char)),
    }
}

/// Wrapper around raw sequence, stored as an `[u8; (len + 1) / 2]`. Each four bits encode a
/// nucleotide in the following order: `=ACMGRSVTWYHKDBN`.
pub struct Sequence {
    raw: Vec<u8>,
    len: usize,
}

impl Sequence {
    fn new() -> Self {
        Sequence {
            raw: Vec::new(),
            len: 0,
        }
    }

    fn fill_from_text<I: IntoIterator<Item = u8>>(&mut self, nucleotides: I) -> Result<(), String> {
        self.raw.clear();
        self.len = 0;
        for nt in nucleotides.into_iter() {
            if self.len % 2 == 0 {
                self.raw.push(nt_to_raw(nt)? << 4);
            } else {
                self.raw[self.len / 2] |= nt_to_raw(nt)?;
            }
            self.len += 1;
        };
        Ok(())
    }

    fn fill_from<R: Read>(&mut self, stream: &mut R, expanded_len: usize) -> io::Result<()> {
        let short_len = (expanded_len + 1) / 2;
        unsafe {
            resize(&mut self.raw, short_len);
        }
        stream.read_exact(&mut self.raw)?;
        self.len = expanded_len;
        Ok(())
    }

    /// Returns raw data.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// Returns full length of the sequence, O(1).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true`, if the sequence is not present.
    pub fn unavailable(&self) -> bool {
        self.len == 0
    }

    /// Returns transformed data, each byte represents a single nucleotde, O(n).
    pub fn to_vec(&self) -> Vec<u8> {
        (0..self.len).map(|i| self.at(i)).collect()
    }

    /// Returns transformed data using only nucleotides A, C, G, T and N,
    /// all other values are transformed into N, each byte represents a single nucleotde, O(n).
    pub fn to_vec_acgtn_only(&self) -> Vec<u8> {
        (0..self.len).map(|i| self.at_acgtn_only(i)).collect()
    }

    /// Returns a nucleotide at the position `index`, represented by a single byte, O(1).
    pub fn at(&self, index: usize) -> u8 {
        assert!(index < self.len, "Index out of range ({} >= {})", index, self.len);
        let nt = if index % 2 == 0 {
            self.raw[index / 2] >> 4
        } else {
            self.raw[index / 2] & 0x0f
        };
        b"=ACMGRSVTWYHKDBN"[nt as usize]
    }

    /// Returns a nucleotide at the position `index`, represented by a single byte, O(1).
    /// If the nucleotide is not A, C, G or T, the function returns N.
    pub fn at_acgtn_only(&self, index: usize) -> u8 {
        assert!(index < self.len, "Index out of range ({} >= {})", index, self.len);
        let nt = if index % 2 == 0 {
            self.raw[index / 2] >> 4
        } else {
            self.raw[index / 2] & 0x0f
        };
        b"NACNGNNNTNNNNNNN"[nt as usize]
    }

    /// Returns an iterator over a subsequence.
    pub fn subseq<R: RangeBounds<usize>>(&self, range: R) -> impl Iterator<Item = u8> + '_ {
        use std::ops::Bound::*;
        let start = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => self.len,
        };
        assert!(start <= end);
        assert!(end <= self.len);
        (start..end).map(move |i| self.at(i))
    }

    /// Returns an iterator over a subsequence using only nucleotides A, C, G, T and N.
    pub fn subseq_acgtn_only<R: RangeBounds<usize>>(&self, range: R)
            -> impl Iterator<Item = u8> + '_ {
        use std::ops::Bound::*;
        let start = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => self.len,
        };
        assert!(start <= end);
        assert!(end <= self.len);
        (start..end).map(move |i| self.at_acgtn_only(i))
    }

    /// Returns an iterator over a reverse complement of a subsequence.
    pub fn rev_compl<R: RangeBounds<usize>>(&self, range: R) -> impl Iterator<Item = u8> + '_ {
        use std::ops::Bound::*;
        let start = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => self.len,
        };
        assert!(start <= end);
        assert!(end <= self.len);
        (start..end).rev().map(move |i| self.compl_at(i))
    }

    /// Returns an iterator over a reverse complement of a subsequence using only
    /// nucleotides A, C, G, T and N.
    pub fn rev_compl_acgtn_only<R: RangeBounds<usize>>(&self, range: R)
            -> impl Iterator<Item = u8> + '_ {
        use std::ops::Bound::*;
        let start = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => self.len,
        };
        assert!(start <= end);
        assert!(end <= self.len);
        (start..end).rev().map(move |i| self.compl_at_acgtn_only(i))
    }

    /// Returns a nucleotide, complement to the nucleotide at the position `index`, O(1).
    pub fn compl_at(&self, index: usize) -> u8 {
        assert!(index < self.len, "Index out of range ({} >= {})", index, self.len);
        let nt = if index % 2 == 0 {
            self.raw[index / 2] >> 4
        } else {
            self.raw[index / 2] & 0x0f
        };
        b"=TGKCYSBAWRDMHVN"[nt as usize]
    }

    /// Returns a nucleotide, complement to the nucleotide at the position `index`, O(1).
    /// If the nucleotide is not A, C, G or T, the function returns N.
    pub fn compl_at_acgtn_only(&self, index: usize) -> u8 {
        assert!(index < self.len, "Index out of range ({} >= {})", index, self.len);
        let nt = if index % 2 == 0 {
            self.raw[index / 2] >> 4
        } else {
            self.raw[index / 2] & 0x0f
        };
        b"NTGNCNNNANNNNNNN"[nt as usize]
    }

    /// Writes in human readable format. Writes `*` if empty.
    pub fn write_readable<W: Write>(&self, f: &mut W) -> io::Result<()> {
        if self.len == 0 {
            return f.write_u8(b'*');
        }
        write_iterator(f, (0..self.len).map(|i| self.at(i)))
    }

    /// Clears the contents but does not touch capacity.
    pub fn clear(&mut self) {
        self.raw.clear();
        self.len = 0;
    }
}

/// Wrapper around qualities.
pub struct Qualities {
    raw: Vec<u8>
}

impl Qualities {
    fn new() -> Self {
        Qualities {
            raw: Vec::new()
        }
    }

    fn fill_from<R: Read>(&mut self, stream: &mut R, len: usize) -> io::Result<()> {
        unsafe {
            resize(&mut self.raw, len);
        }
        stream.read_exact(&mut self.raw)?;
        Ok(())
    }

    /// Returns raw qualities, they contain values 0-93, without +33 added.
    ///
    /// If qualities are empty, they have the same length as `Sequence`, but are filled with `0xff`.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// Returns 0 if qualities are not available.
    pub fn len(&self) -> usize {
        if self.unavailable() {
            0
        } else {
            self.raw.len()
        }
    }

    /// Returns `true` if raw qualities have length 0 or are filled with `0xff`.
    /// Only the first element is checked, O(1).
    pub fn unavailable(&self) -> bool {
        self.raw.len() == 0 || self.raw[0] == 0xff
    }

    /// Returns vector with +33 added, O(n).
    pub fn to_readable(&self) -> Vec<u8> {
        self.raw.iter().map(|qual| qual + 33).collect()
    }

    /// Writes to `f` in human readable format (qual + 33). Writes `*` if empty.
    pub fn write_readable<W: Write>(&self, f: &mut W) -> io::Result<()> {
        if self.unavailable() {
            return f.write_u8(b'*');
        }
        write_iterator(f, self.raw.iter().map(|qual| qual + 33))
    }

    /// Clears the contents but does not touch capacity.
    pub fn clear(&mut self) {
        self.raw.clear();
    }

    /// Fills the qualities from raw qualities (without + 33).
    fn fill_from_raw<I: IntoIterator<Item = u8>>(&mut self, qualities: I) {
        self.raw.clear();
        self.raw.extend(qualities);
    }
}

/// `= 0x1`. Record has a mate.
pub const RECORD_PAIRED: u16 = 0x1;
/// `= 0x2`. Record and its mate mapped properly.
pub const ALL_SEGMENTS_ALIGNED: u16 = 0x2;
/// `= 0x4`. Record is unmapped.
pub const RECORD_UNMAPPED: u16 = 0x4;
/// `= 0x8`. Record mate is unmapped.
pub const MATE_UNMAPPED: u16 = 0x8;
/// `= 0x10`. Record is on the reverse strand.
pub const RECORD_REVERSE_STRAND: u16 = 0x10;
/// `= 0x20`. Record mate is on the reverse strand.
pub const MATE_REVERSE_STRAND: u16 = 0x20;
/// `= 0x40`. Record is the first segment in a template.
pub const FIRST_IN_PAIR: u16 = 0x40;
/// `= 0x80`. Record is the last segment in a template.
pub const LAST_IN_PAIR: u16 = 0x80;
/// `= 0x100`. Alignment is secondary (not primary).
pub const SECONDARY: u16 = 0x100;
/// `= 0x200`. Record fails platform/vendor quality checks.
pub const RECORD_FAILS_QC: u16 = 0x200;
/// `= 0x400`. Record is PCR or optical duplicate.
pub const PCR_OR_OPTICAL_DUPLICATE: u16 = 0x400;
/// `= 0x800`. Alignment is supplementary (chimeric/split).
pub const SUPPLEMENTARY: u16 = 0x800;

/// A wrapper around BAM/SAM flag.
///
/// You can check flag as `record.flag().is_paired()` or `record.flag().0 | IS_PAIRED == 0`.
/// You can also modify the flag as `record.flag_mut().set_paired(true)` or
/// `record.flag_mut().0 |= IS_PAIRED`.
#[derive(Clone, Copy, Debug)]
pub struct Flag(pub u16);

impl Flag {
    pub fn is_paired(&self) -> bool {
        self.0 & RECORD_PAIRED != 0
    }

    pub fn all_segments_aligned(&self) -> bool {
        self.0 & ALL_SEGMENTS_ALIGNED != 0
    }

    pub fn is_mapped(&self) -> bool {
        // EQUAL 0
        self.0 & RECORD_UNMAPPED == 0
    }

    pub fn mate_is_mapped(&self) -> bool {
        // EQUAL 0
        self.0 & MATE_UNMAPPED == 0
    }

    pub fn is_reverse_strand(&self) -> bool {
        self.0 & RECORD_REVERSE_STRAND != 0
    }

    pub fn mate_is_reverse_strand(&self) -> bool {
        self.0 & MATE_REVERSE_STRAND != 0
    }

    pub fn first_in_pair(&self) -> bool {
        self.0 & FIRST_IN_PAIR != 0
    }

    pub fn last_in_pair(&self) -> bool {
        self.0 & LAST_IN_PAIR != 0
    }

    pub fn is_secondary(&self) -> bool {
        self.0 & SECONDARY != 0
    }

    /// Returns `true` if the record fails filters, such as platform/vendor quality controls.
    pub fn fails_quality_controls(&self) -> bool {
        self.0 & RECORD_FAILS_QC != 0
    }

    /// Returns `true` if the record is PCR or optical duplicate.
    pub fn is_duplicate(&self) -> bool {
        self.0 & PCR_OR_OPTICAL_DUPLICATE != 0
    }

    pub fn is_supplementary(&self) -> bool {
        self.0 & SUPPLEMENTARY != 0
    }

    /// Modifies the record flag. This function does not do any checks.
    pub fn set_paired(&mut self, paired: bool) {
        if paired {
            self.0 |= RECORD_PAIRED;
        } else {
            self.0 &= !RECORD_PAIRED;
        }
    }

    /// Modifies the record flag. This function does not do any checks.
    pub fn set_all_segments_aligned(&mut self, all_segments_aligned: bool) {
        if all_segments_aligned {
            self.0 |= ALL_SEGMENTS_ALIGNED;
        } else {
            self.0 &= !ALL_SEGMENTS_ALIGNED;
        }
    }

    /// Modifies the record flag. This function does not do any checks.
    pub fn set_mapped(&mut self, mapped: bool) {
        if mapped {
            self.0 &= !RECORD_UNMAPPED;
        } else {
            self.0 |= RECORD_UNMAPPED;
        }
    }

    /// Modifies the record flag. This function does not do any checks and does not modify the
    /// mate record.
    pub fn set_mate_mapped(&mut self, mate_mapped: bool) {
        if mate_mapped {
            self.0 &= !MATE_UNMAPPED;
        } else {
            self.0 |= MATE_UNMAPPED;
        }
    }

    /// Sets the record strand. Use `true` to set to forward strand, and `false` for the
    /// reverse strand.
    pub fn set_strand(&mut self, forward_strand: bool) {
        if forward_strand {
            self.0 &= !RECORD_REVERSE_STRAND;
        } else {
            self.0 |= RECORD_REVERSE_STRAND;
        }
    }

    /// Sets the strand of the mate. Use `true` to set to forward strand, and `false` for the
    /// reverse strand. This function does not do any checks and does not modify the
    /// mate record.
    pub fn set_mate_strand(&mut self, forward_strand: bool) {
        if forward_strand {
            self.0 &= !MATE_REVERSE_STRAND;
        } else {
            self.0 |= MATE_REVERSE_STRAND;
        }
    }

    /// Modifies the record flag. This function does not do any checks.
    ///
    /// Use `true` to set the flag bit and `false` to unset.
    pub fn set_first_in_pair(&mut self, is_first: bool) {
        if is_first {
            self.0 |= FIRST_IN_PAIR;
        } else {
            self.0 &= !FIRST_IN_PAIR;
        }
    }

    /// Modifies the record flag. This function does not do any checks.
    ///
    /// Use `true` to set the flag bit and `false` to unset.
    pub fn set_last_in_pair(&mut self, is_last: bool) {
        if is_last {
            self.0 |= LAST_IN_PAIR;
        } else {
            self.0 &= !LAST_IN_PAIR;
        }
    }

    /// Modifies the record flag. This function does not do any checks.
    ///
    /// Use `true` to set the flag bit and `false` to unset.
    pub fn set_secondary(&mut self, is_secondary: bool) {
        if is_secondary {
            self.0 |= SECONDARY;
        } else {
            self.0 &= !SECONDARY;
        }
    }

    /// Sets the record flag to fail or pass filters, such as platform/vendor quality controls.
    ///
    /// Use `true` to set the flag bit and `false` to unset.
    pub fn make_fail_quality_controls(&mut self, fails: bool) {
        if fails {
            self.0 |= RECORD_FAILS_QC;
        } else {
            self.0 &= !RECORD_FAILS_QC;
        }
    }

    /// Sets the record as PCR or optical duplicate.
    pub fn set_duplicate(&mut self, is_duplicate: bool) {
        if is_duplicate {
            self.0 |= PCR_OR_OPTICAL_DUPLICATE;
        } else {
            self.0 &= !PCR_OR_OPTICAL_DUPLICATE;
        }
    }

    pub fn set_supplementary(&mut self, is_supplementary: bool) {
        if is_supplementary {
            self.0 |= SUPPLEMENTARY;
        } else {
            self.0 &= !SUPPLEMENTARY;
        }
    }
}

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
    Corrupted(String),
    Truncated(io::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Truncated(e)
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Error {
        Error::Corrupted(e.to_string())
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

pub(crate) fn write_iterator<W, I>(writer: &mut W, mut iterator: I) -> io::Result<()>
where W: Write,
      I: Iterator<Item = u8>,
{
    const SIZE: usize = 1024;
    let mut buffer = [0_u8; SIZE];
    loop {
        for i in 0..SIZE {
            match iterator.next() {
                Some(value) => buffer[i] = value,
                None => {
                    return writer.write_all(&buffer[..i]);
                }
            }
        }
        writer.write_all(&buffer)?;
    }
}

/// A helper trait to get next from a splitted string.
trait NextToErr<'a> {
    fn try_next(&mut self, field: &'static str) -> Result<&'a str, Error>;
}

impl<'a> NextToErr<'a> for std::str::Split<'a, char> {
    fn try_next(&mut self, field: &'static str) -> Result<&'a str, Error> {
        self.next().ok_or_else(|| Error::Truncated(io::Error::new(UnexpectedEof,
            format!("Cannot extract {}", field))))
    }
}

/// BAM Record.
///
/// Allows to get and set name, [sequence](struct.Sequence.html), [qualities](struct.Qualities.html),
/// [CIGAR](cigar/struct.Cigar.html), [flag](struct.Flag.html), [tags](tags/struct.TagViewer.html)
/// and all other BAM/SAM record fields.
///
/// You can use [aligned_pairs](#method.aligned_pairs) and [matching_pairs](#method.matching_pairs)
/// to iterate over record/reference aligned indices.
pub struct Record {
    ref_id: i32,
    mate_ref_id: i32,
    start: i32,
    end: Cell<i32>,
    mate_start: i32,
    bin: Cell<u16>,
    mapq: u8,
    flag: Flag,
    template_len: i32,

    name: Vec<u8>,
    cigar: Cigar,
    seq: Sequence,
    qual: Qualities,
    tags: tags::TagViewer,
}

const BIN_UNKNOWN: u16 = std::u16::MAX;

impl Record {
    /// Creates an empty record. Can be filled using
    /// [read_into](../bam_reader/struct.RegionViewer.html#method.read_into).
    pub fn new() -> Record {
        Record {
            ref_id: -1,
            mate_ref_id: -1,
            start: -1,
            end: Cell::new(0),
            mate_start: -1,
            bin: Cell::new(BIN_UNKNOWN),
            mapq: 0,
            flag: Flag(0),
            template_len: 0,

            name: Vec::new(),
            cigar: Cigar::new(),
            seq: Sequence::new(),
            qual: Qualities::new(),
            tags: tags::TagViewer::new(),
        }
    }

    /// Clears the record.
    pub fn clear(&mut self) {
        self.ref_id = -1;
        self.mate_ref_id = -1;
        self.start = -1;
        self.end.set(0);
        self.mate_start = -1;
        self.bin.set(BIN_UNKNOWN);
        self.mapq = 0;
        self.flag.0 = 0;
        self.template_len = 0;

        self.name.clear();
        self.cigar.clear();
        self.seq.clear();
        self.qual.clear();
        self.tags.clear();
    }

    fn corrupt(&mut self, text: String) -> Error {
        if self.name.is_empty() {
            Error::Corrupted(format!("Record {}: {}",
                std::str::from_utf8(&self.name).unwrap_or("_"), text))
        } else {
            Error::Corrupted(text)
        }
    }

    /// Fills the record from a `stream` of uncompressed BAM contents.
    pub(crate) fn fill_from_bam<R: Read>(&mut self, stream: &mut R) -> Result<(), Error> {
        self.name.clear();
        let block_size = match stream.read_i32::<LittleEndian>() {
            Ok(value) => {
                if value < 0 {
                    return Err(self.corrupt("Negative block size".to_string()));
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
        self.set_ref_id(stream.read_i32::<LittleEndian>()?)?;
        self.end.set(0);
        self.set_start(stream.read_i32::<LittleEndian>()?)?;
        let name_len = stream.read_u8()?;
        if name_len == 0 {
            return Err(self.corrupt("Name length == 0".to_string()));
        }

        self.set_mapq(stream.read_u8()?);
        self.bin.set(stream.read_u16::<LittleEndian>()?);
        let cigar_len = stream.read_u16::<LittleEndian>()?;
        self.set_flag(stream.read_u16::<LittleEndian>()?);
        let qual_len = stream.read_i32::<LittleEndian>()?;
        if qual_len < 0 {
            return Err(self.corrupt("Negative sequence length".to_string()));
        }
        let qual_len = qual_len as usize;
        self.set_mate_ref_id(stream.read_i32::<LittleEndian>()?)?;
        self.set_mate_start(stream.read_i32::<LittleEndian>()?)?;
        self.set_template_len(stream.read_i32::<LittleEndian>()?);

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
        self.tags.fill_from(stream, remaining_size)?;
        self.replace_cigar_if_needed()?;

        if self.flag().is_mapped() == (self.ref_id == -1) {
            return Err(self.corrupt("Record (flag & 0x4) and ref_id do not match".to_string()));
        }
        Ok(())
    }

    /// Fills the record from SAM. If an error is return, the record may be corrupted.
    pub fn fill_from_sam(&mut self, line: &str, header: &Header) -> Result<(), Error> {
        let mut split = line.split('\t');
        self.set_name(split.try_next("record name (QNAME)")?.bytes());
        let flag = split.try_next("flag")?;
        let flag = flag.parse()
            .map_err(|_| self.corrupt(format!("Cannot convert flag '{}' to int", flag)))?;
        self.set_flag(flag);

        let rname = split.try_next("reference name")?;
        if rname == "*" {
            self.set_ref_id(-1).unwrap();
        } else {
            let r_id = header.reference_id(rname).ok_or_else(||
                self.corrupt(format!("Reference '{}' is not in the header", rname)))?;
            self.set_ref_id(r_id as i32)?;
        }

        let start = split.try_next("start (POS)")?;
        let start = start.parse::<i32>()
            .map_err(|_| self.corrupt(format!("Cannot convert POS '{}' to int", start)))? - 1;
        self.set_start(start)?;

        let mapq = split.try_next("mapq")?;
        let mapq = mapq.parse()
            .map_err(|_| self.corrupt(format!("Cannot convert MAPQ '{}' to int", mapq)))?;
        self.set_mapq(mapq);

        self.set_cigar(split.try_next("CIGAR")?.bytes()).map_err(|e| self.corrupt(e))?;

        let rnext = split.try_next("mate reference name (RNEXT)")?;
        if rnext == "*" {
            self.set_mate_ref_id(-1).unwrap();
        } else if rnext == "=" {
            self.set_mate_ref_id(self.ref_id).unwrap();
        } else {
            let nr_id = header.reference_id(rnext).ok_or_else(||
                self.corrupt(format!("Reference '{}' is not in the header", rnext)))?;
            self.set_mate_ref_id(nr_id as i32)?;
        }

        let mate_start = split.try_next("mate start (PNEXT)")?;
        let mate_start = mate_start.parse::<i32>().map_err(|_|
            self.corrupt(format!("Cannot convert PNEXT '{}' to int", mate_start)))? - 1;
        self.set_mate_start(mate_start)?;

        let template_len = split.try_next("template length (TLEN)")?;
        let template_len = template_len.parse::<i32>().map_err(|_|
            self.corrupt(format!("Cannot convert TLEN '{}' to int", template_len)))?;
        self.set_template_len(template_len);

        let seq = split.try_next("sequence")?;
        let qual = split.try_next("qualities")?;
        if seq == "*" {
            self.reset_seq();
        } else if qual == "*" {
            self.set_seq(seq.bytes()).map_err(|e| self.corrupt(e))?;
        } else {
            self.set_seq_qual(seq.bytes(), qual.bytes().map(|q| q - 33))
                .map_err(|e| self.corrupt(e))?;
        }

        self.tags.clear();
        for tag in split {
            self.tags.push_sam(tag)?;
        }
        Ok(())
    }

    /// Replace Cigar by CG tag if Cigar has placeholder *kSmN*.
    fn replace_cigar_if_needed(&mut self) -> Result<(), Error> {
        if self.cigar.len() > 0 && self.cigar.at(0) ==
                (self.seq.len as u32, cigar::Operation::Soft) {
            if self.cigar.len() != 2 {
                return Err(self.corrupt("Record contains invalid Cigar".to_string()));
            }
            let (len, op) = self.cigar.at(1);
            if op != cigar::Operation::Skip {
                return Err(self.corrupt("Record contains invalid Cigar".to_string()));
            }
            self.end.set(self.start + len as i32);

            let cigar_arr = match self.tags.get(b"CG") {
                Some(tags::TagValue::IntArray(array_view)) => {
                    if array_view.int_type() != tags::IntegerType::U32 {
                        return Err(self.corrupt("CG tag has an incorrect type".to_string()));
                    }
                    array_view
                },
                _ => return Err(
                    self.corrupt("Record should contain tag CG, but does not".to_string())),
            };
            self.cigar.fill_from_raw(cigar_arr.iter().map(|el| el as u32));
            std::mem::drop(cigar_arr);
            self.tags.remove(b"CG");
        }
        Ok(())
    }

    /// Shrinks record contents. The more records were read into the same `Record` instance, the
    /// bigger would be inner vectors (to save time on memory allocation).
    /// Use this function if you do not plan to read into this record in the future.
    pub fn shrink_to_fit(&mut self) {
        self.name.shrink_to_fit();
        self.cigar.shrink_to_fit();
        self.seq.raw.shrink_to_fit();
        self.qual.raw.shrink_to_fit();
        self.tags.shrink_to_fit();
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
        if self.qual.unavailable() {
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

    /// For a mapped record aligned to reference positions `[start-end)`, the function returns `end`.
    /// The first calculation takes O(n), where *n* is the length of Cigar.
    /// Consecutive calculations take O(1).
    /// If the record was fetched from a specific region, it should have `end` already calculated.
    ///
    /// Returns 0 for unmapped records.
    pub fn calculate_end(&self) -> i32 {
        if self.cigar.len() == 0 {
            return 0;
        }

        let end = self.end.get();
        if end != 0 {
            return end;
        }

        let end = self.start + self.cigar.calculate_ref_len() as i32;
        self.end.set(end);
        end
    }

    /// Returns BAI bin. If the bin is unknown and the end has not been calculated,
    /// the bin will be calculated in `O(n_cigar)`, otherwise `O(1)`.
    ///
    /// Returns 4680 for unmapped reads.
    pub fn calculate_bin(&self) -> u16 {
        let bin = self.bin.get();
        if bin != BIN_UNKNOWN {
            return bin;
        }
        let end = self.calculate_end();
        let bin = index::region_to_bin(self.start, end) as u16;
        self.bin.set(bin);
        bin
    }

    /// Returns record MAPQ.
    pub fn mapq(&self) -> u8 {
        self.mapq
    }

    /// Returns 0-based reference index for the pair record. Returns -1 for unmapped records,
    /// and records without a pair.
    pub fn mate_ref_id(&self) -> i32 {
        self.mate_ref_id
    }

    /// Returns 0-based left-most aligned reference position for the pair record.
    /// Same as *PNEXT - 1* in SAM specification.
    /// Returns -1 for unmapped records and records without a pair.
    pub fn mate_start(&self) -> i32 {
        self.mate_start
    }

    /// Observed template length (TLEN in SAM specification).
    pub fn template_len(&self) -> i32 {
        self.template_len
    }

    /// Returns [TagViewer](tags/struct.TagViewer.html), which provides operations of tags.
    pub fn tags(&self) -> &tags::TagViewer {
        &self.tags
    }

    /// Returns mutable [TagViewer](tags/struct.TagViewer.html), which provides operations of tags.
    pub fn tags_mut(&mut self) -> &mut tags::TagViewer {
        &mut self.tags
    }

    /// Write the record in SAM format to `f`. The function needs
    /// [header](../header/struct.Header.html), as the record itself does not store reference
    /// names.
    pub fn write_sam<W: Write>(&self, f: &mut W, header: &Header) -> io::Result<()> {
        f.write_all(&self.name)?;
        write!(f, "\t{}\t", self.flag.0)?;
        if self.ref_id < 0 {
            f.write_all(b"*\t")?;
        } else {
            write!(f, "{}\t", header.reference_name(self.ref_id as usize)
                .ok_or_else(|| io::Error::new(InvalidData,
                "Record has a reference id not in the header"))?)?;
        }
        write!(f, "{}\t{}\t", self.start + 1, self.mapq)?;
        self.cigar.write_readable(f)?;

        if self.mate_ref_id < 0 {
            f.write_all(b"\t*\t")?;
        } else if self.mate_ref_id == self.ref_id {
            f.write_all(b"\t=\t")?;
        } else {
            write!(f, "\t{}\t", header.reference_name(self.mate_ref_id as usize)
                .ok_or_else(|| io::Error::new(InvalidData,
                "Record has a reference id not in the header"))?)?;
        }
        write!(f, "{}\t{}\t", self.mate_start + 1, self.template_len)?;
        self.seq.write_readable(f)?;
        f.write_u8(b'\t')?;
        self.qual.write_readable(f)?;
        self.tags.write_sam(f)?;
        writeln!(f)
    }

    /// Writes a record in BAM format.
    pub fn write_bam<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        let raw_cigar_len = if self.cigar.len() <= 0xffff {
            4 * self.cigar.len()
        } else {
            16 + 4 * self.cigar.len()
        };
        let total_block_len = 32 + self.name.len() + 1 + raw_cigar_len + self.seq.raw.len()
            + self.qual.len() + self.tags.raw().len();
        stream.write_i32::<LittleEndian>(total_block_len as i32)?;

        stream.write_i32::<LittleEndian>(self.ref_id)?;
        stream.write_i32::<LittleEndian>(self.start)?;
        stream.write_u8(self.name.len() as u8 + 1)?;
        stream.write_u8(self.mapq)?;
        stream.write_u16::<LittleEndian>(self.calculate_bin())?;

        if self.cigar.len() <= 0xffff {
            stream.write_u16::<LittleEndian>(self.cigar.len() as u16)?;
        } else {
            stream.write_u16::<LittleEndian>(2)?;
        }

        stream.write_u16::<LittleEndian>(self.flag.0)?;
        stream.write_i32::<LittleEndian>(self.seq.len() as i32)?;
        stream.write_i32::<LittleEndian>(self.mate_ref_id)?;
        stream.write_i32::<LittleEndian>(self.mate_start)?;
        stream.write_i32::<LittleEndian>(self.template_len)?;
        
        stream.write_all(&self.name)?;
        stream.write_u8(0)?;
        if self.cigar.len() <= 0xffff {
            for &el in self.cigar.raw() {
                stream.write_u32::<LittleEndian>(el)?;
            }
        } else {
            let seq_len = if self.seq.len() != 0 {
                self.seq.len() as u32
            } else {
                self.cigar.calculate_query_len()
            };
            stream.write_u32::<LittleEndian>(seq_len << 4 | 4)?;
            let ref_len = (self.calculate_end() - self.start) as u32;
            stream.write_u32::<LittleEndian>(ref_len << 4 | 3)?;
        }
        stream.write_all(&self.seq.raw)?;
        stream.write_all(&self.qual.raw)?;
        stream.write_all(self.tags.raw())?;

        if self.cigar.len() > 0xffff {
            stream.write_all(b"CGBI")?;
            stream.write_i32::<LittleEndian>(self.cigar.len() as i32)?;
            for &el in self.cigar.raw() {
                stream.write_u32::<LittleEndian>(el)?;
            }
        }
        Ok(())
    }

    /// Sets record name (only first 254 letters will be used).
    pub fn set_name<T: IntoIterator<Item = u8>>(&mut self, name: T) {
        self.name.clear();
        self.name.extend(name.into_iter().take(254));
    }

    /// Returns record [flag](struct.Flag.html).
    ///
    /// It supports predicates like `record.flag().is_paired()`. You can compare with flags directly
    /// using `record.flag().0 & RECORD_PAIRED != 0`.
    pub fn flag(&self) -> Flag {
        self.flag
    }

    /// Returns mutable record [flag](struct.Flag.html).
    ///
    /// It can be changed like `record.flag_mut().set_paired(true)`.
    /// You can change it directly `record.flag_mut().0 |= RECORD_PAIRED`. You can also you
    /// [set_flag](#method.set_flag).
    pub fn flag_mut(&mut self) -> &mut Flag {
        &mut self.flag
    }

    pub fn set_flag(&mut self, flag: u16) {
        self.flag.0 = flag;
    }

    /// Sets reference id. It cannot be less than -1.
    pub fn set_ref_id(&mut self, ref_id: i32) -> Result<(), &'static str> {
        if ref_id < -1 {
            Err("Reference id < -1")
        } else {
            self.ref_id = ref_id;
            Ok(())
        }
    }

    /// Sets record 0-based start. It cannot be less than -1.
    ///
    /// If the end position was already calculated, it is updated.
    pub fn set_start(&mut self, start: i32) -> Result<(), &'static str> {
        if start < -1 {
            Err("Start < -1")
        } else {
            let difference = start - self.start;
            self.start = start;
            if self.end.get() != 0 {
                *self.end.get_mut() += difference;
            }
            self.bin.set(BIN_UNKNOWN);
            Ok(())
        }
    }

    pub fn set_mapq(&mut self, mapq: u8) {
        self.mapq = mapq;
    }

    /// Sets reference id of the mate record. It cannot be less than -1.
    pub fn set_mate_ref_id(&mut self, mate_ref_id: i32) -> Result<(), &'static str> {
        if mate_ref_id < -1 {
            Err("Mate reference id < -1")
        } else {
            self.mate_ref_id = mate_ref_id;
            Ok(())
        }
    }

    /// Sets record 0-based start of the mate record. It cannot be less than -1.
    pub fn set_mate_start(&mut self, mate_start: i32) -> Result<(), &'static str> {
        if mate_start < -1 {
            Err("Mate start < -1")
        } else {
            self.mate_start = mate_start;
            Ok(())
        }
    }

    pub fn set_template_len(&mut self, template_len: i32) {
        self.template_len = template_len;
    }

    /// Sets empty sequence and qualities.
    pub fn reset_seq(&mut self) {
        self.seq.clear();
        self.qual.clear();
    }

    /// Sets a sequence for a record and removes record qualities.
    /// Sequence should be in text format (for example `b"ACGT"`).
    ///
    /// The function returns an error if there was an unexpected nucleotide.
    /// In that case the sequence and qualities are cleared.
    pub fn set_seq<T: IntoIterator<Item = u8>>(&mut self, sequence: T) -> Result<(), String> {
        if let Err(e) = self.seq.fill_from_text(sequence) {
            self.seq.clear();
            self.qual.clear();
            return Err(e);
        }
        self.qual.fill_from_raw(std::iter::repeat(0xff_u8).take(self.seq.len()));
        Ok(())
    }

    /// Sets a sequence and qualities for a record. If you do not need to set qualities, use
    /// [seq_seq](#method.set_seq). Both iterators should have the same length.
    ///
    /// # Arguments
    /// * sequence - in text format (for example `b"ACGT"`),
    /// * qualities - in raw format (without +33 added).
    ///
    /// If the function returns an error, the sequence and qualities are cleared.
    pub fn set_seq_qual<T, U>(&mut self, sequence: T, qualities: U) -> Result<(), String>
    where T: IntoIterator<Item = u8>,
          U: IntoIterator<Item = u8>,
    {
        if let Err(e) = self.seq.fill_from_text(sequence) {
            self.seq.clear();
            self.qual.clear();
            return Err(e);
        }
        self.qual.fill_from_raw(qualities);
        if self.seq.len() != self.qual.len() {
            let err = Err(format!("Trying to set sequence and qualities of different lengths: \
                {} and {}", self.seq.len(), self.qual.len()));
            self.seq.clear();
            self.qual.clear();
            err
        } else {
            Ok(())
        }
    }

    /// Sets raw sequence (4 bits for a nucleotide) for a record and removes record qualities.
    ///
    /// The function panics if the length does not match sequence length.
    pub fn set_raw_seq(&mut self, raw_seq: &[u8], len: usize) {
        assert!(len + 1 / 2 == raw_seq.len(), "Sequence length can be {} or {}, but not {}",
            raw_seq.len() * 2 - 1, raw_seq.len() * 2, len);
        unsafe {
            resize(&mut self.seq.raw, raw_seq.len());
        }
        self.seq.raw.copy_from_slice(raw_seq);
        self.seq.len = len;
    }

    /// Sets raw sequence (4 bits for a nucleotide) for a record and record qualities.
    /// Qualities should be in raw format (without +33 added), and non-empty.
    ///
    /// If the sequence and qualities length do not match,
    /// the function returns an error clears the record sequence and qualities.
    pub fn set_raw_seq_qual<U>(&mut self, raw_seq: &[u8], qualities: U) -> Result<(), String>
    where U: IntoIterator<Item = u8>,
    {
        self.qual.fill_from_raw(qualities);
        let len = self.qual.len();
        if len + 1 / 2 == raw_seq.len() {
            self.seq.clear();
            self.qual.clear();
            return Err(format!("Sequence length can be {} or {}, but not {}",
                raw_seq.len() * 2 - 1, raw_seq.len() * 2, len));
        }
        unsafe {
            resize(&mut self.seq.raw, raw_seq.len());
        }
        self.seq.raw.copy_from_slice(raw_seq);
        self.seq.len = len;
        Ok(())
    }

    /// Sets record cigar. This resets end position and BAI bin.
    pub fn set_cigar<I: IntoIterator<Item = u8>>(&mut self, cigar: I) -> Result<(), String> {
        self.end.set(0);
        self.bin.set(BIN_UNKNOWN);
        self.cigar.fill_from_text(cigar)
    }

    /// Sets raw record cigar. This resets end position and BAI bin.
    pub fn set_raw_cigar<I: IntoIterator<Item = u32>>(&mut self, cigar: I) {
        self.end.set(0);
        self.bin.set(BIN_UNKNOWN);
        self.cigar.fill_from_raw(cigar);
    }

    /// Returns an iterator over pairs `(Option<u32>, Option<u32>)`.
    /// The first element represents a sequence index, and the second element represents a
    /// reference index. If the current operation is an insertion or a deletion, the respective
    /// element will be `None.`
    ///
    /// If the record is unmapped, returns an empty iterator.
    pub fn aligned_pairs(&self) -> cigar::AlignedPairs {
        self.cigar.aligned_pairs(self.start as u32)
    }

    /// Returns an iterator over pairs `(u32, u32)`.
    /// The first element represents a sequence index, and the second element represents a
    /// reference index. This iterator skips insertions and deletions.
    ///
    /// If the record is unmapped, returns an empty iterator.
    pub fn matching_pairs(&self) -> cigar::MatchingPairs {
        self.cigar.matching_pairs(self.start as u32)
    }
}
