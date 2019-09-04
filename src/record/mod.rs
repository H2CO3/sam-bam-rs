use std::io::{self, Read, ErrorKind, Write};
use std::io::ErrorKind::InvalidData;
use std::fmt::{self, Display, Debug, Formatter};
use std::cell::RefCell;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::cigar::{self, Cigar};
use super::bam_reader::Header;

pub mod tags;

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
            self.raw[index / 2] >> 4
        } else {
            self.raw[index / 2] & 0x0f
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
            self.raw[index / 2] >> 4
        } else {
            self.raw[index / 2] & 0x0f
        };
        b"NACNGNNNTNNNNNNN"[nt as usize]
    }

    /// Writes in human readable format. Writes `*` if empty.
    pub fn write_readable<W: Write>(&self, f: &mut W) -> io::Result<()> {
        if self.len == 0 {
            return f.write_u8(b'*');
        }
        write_iterator(f, (0..self.len).map(|i| self.at(i)))
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
        if self.is_empty() {
            0
        } else {
            self.raw.len()
        }
    }

    /// Returns `true` if raw qualities have length 0 or are filled with `0xff`.
    /// Only the first element is checked, O(1).
    pub fn is_empty(&self) -> bool {
        self.raw.len() == 0 || self.raw[0] == 0xff
    }

    /// Returns vector with +33 added, O(n).
    pub fn to_readable(&self) -> Vec<u8> {
        self.raw.iter().map(|qual| qual + 33).collect()
    }

    /// Writes to `f` in human readable format (qual + 33). Writes `*` if empty.
    pub fn write_readable<W: Write>(&self, f: &mut W) -> io::Result<()> {
        if self.is_empty() {
            return f.write_u8(b'*');
        }
        write_iterator(f, self.raw.iter().map(|qual| qual + 33))
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

/// BAM Record
pub struct Record {
    ref_id: i32,
    next_ref_id: i32,
    start: i32,
    end: RefCell<i32>,
    next_start: i32,
    bin: u16,
    mapq: u8,
    flag: u16,
    template_len: i32,

    name: Vec<u8>,
    cigar: Cigar,
    seq: Sequence,
    qual: Qualities,
    tags: tags::TagViewer,
}

impl Record {
    /// Creates an empty record. Can be filled using
    /// [read_into](../bam_reader/struct.RegionViewer.html#method.read_into).
    pub fn new() -> Record {
        Record {
            ref_id: -1,
            next_ref_id: -1,
            start: -1,
            end: RefCell::new(-1),
            next_start: -1,
            bin: 0,
            mapq: 0,
            flag: 0,
            template_len: 0,

            name: Vec::new(),
            cigar: Cigar::new(),
            seq: Sequence::new(),
            qual: Qualities::new(),
            tags: tags::TagViewer::new(),
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
        *self.end.get_mut() = -1;
        let name_len = stream.read_u8()?;
        if name_len == 0 {
            return Err(Error::Corrupted("Name length == 0"));
        }
        self.mapq = stream.read_u8()?;
        self.bin = stream.read_u16::<LittleEndian>()?;
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
        self.tags.fill_from(stream, remaining_size)?;
        self.replace_cigar_if_needed()?;

        if self.is_mapped() == (self.ref_id == -1) {
            return Err(Error::Corrupted("Record (flag & 0x4) and ref_id do not match"));
        }

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
            *self.end.get_mut() = self.start + len as i32;

            let cigar_arr = match self.tags.get(b"CG") {
                Some(tags::TagValue::IntArray(array_view)) => {
                    if array_view.int_type() != tags::IntegerType::U32 {
                        return Err(Error::Corrupted("CG tag has an incorrect type"));
                    }
                    array_view
                },
                _ => return Err(Error::Corrupted("Record should contain tag CG, but does not")),
            };
            self.cigar.0.clear();
            self.cigar.0.extend(cigar_arr.iter().map(|el| el as u32));
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
        if self.qual.is_empty() {
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
    pub fn calculate_end(&self) -> i32 {
        if self.cigar.len() == 0 {
            return -1;
        }

        let end = *self.end.borrow();
        if end != -1 {
            return end;
        }

        let end = self.start + self.cigar.calculate_aligned_len() as i32;
        *self.end.borrow_mut() = end;
        end
    }

    /// Returns BAI bin.
    pub fn bin(&self) -> u16 {
        self.bin
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

    /// Returns [TagViewer](tags/struct.TagViewer.html), which provides operations of tags.
    pub fn tags(&self) -> &tags::TagViewer {
        &self.tags
    }

    /// Returns mutable [TagViewer](tags/struct.TagViewer.html), which provides operations of tags.
    pub fn tags_mut(&mut self) -> &mut tags::TagViewer {
        &mut self.tags
    }

    /// Write the record in SAM format to `f`. The function needs
    /// [header](../bam_reader/struct.Header.html), as the record itself does not store reference
    /// names.
    pub fn write_sam<W: Write>(&self, f: &mut W, header: &Header) -> io::Result<()> {
        f.write_all(&self.name)?;
        write!(f, "\t{}\t", self.flag)?;
        if self.ref_id < 0 {
            f.write_all(b"*\t")?;
        } else {
            write!(f, "{}\t", header.reference_name(self.ref_id as usize)
                .ok_or_else(|| io::Error::new(InvalidData,
                "Record has a reference id not in the header"))?)?;
        }
        write!(f, "{}\t{}\t", self.start + 1, self.mapq)?;
        self.cigar.write_readable(f)?;

        if self.next_ref_id < 0 {
            f.write_all(b"\t*\t")?;
        } else if self.next_ref_id == self.ref_id {
            f.write_all(b"\t=\t")?;
        } else {
            write!(f, "\t{}\t", header.reference_name(self.next_ref_id as usize)
                .ok_or_else(|| io::Error::new(InvalidData,
                "Record has a reference id not in the header"))?)?;
        }
        write!(f, "{}\t{}\t", self.next_start + 1, self.template_len)?;
        self.seq.write_readable(f)?;
        f.write_u8(b'\t')?;
        self.qual.write_readable(f)?;
        self.tags.write_sam(f)?;
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
