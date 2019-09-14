use std::io::{self, Read, ErrorKind, Write};
use std::io::ErrorKind::{InvalidData, UnexpectedEof};
use std::fmt::{self, Display, Debug, Formatter};
use std::cell::Cell;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::cigar::{self, Cigar};
use super::header::Header;
use super::index;

pub mod tags;

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

/// BAM Record
pub struct Record {
    ref_id: i32,
    next_ref_id: i32,
    start: i32,
    end: Cell<i32>,
    next_start: i32,
    bin: Cell<u16>,
    mapq: u8,
    flag: u16,
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
            next_ref_id: -1,
            start: -1,
            end: Cell::new(0),
            next_start: -1,
            bin: Cell::new(BIN_UNKNOWN),
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

    /// Clears the record.
    pub fn clear(&mut self) {
        self.ref_id = -1;
        self.next_ref_id = -1;
        self.start = -1;
        self.end.set(0);
        self.next_start = -1;
        self.bin.set(BIN_UNKNOWN);
        self.mapq = 0;
        self.flag = 0;
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
        self.set_next_ref_id(stream.read_i32::<LittleEndian>()?)?;
        self.set_next_start(stream.read_i32::<LittleEndian>()?)?;
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

        if self.is_mapped() == (self.ref_id == -1) {
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

        let rnext = split.try_next("next reference name (RNEXT)")?;
        if rnext == "*" {
            self.set_next_ref_id(-1).unwrap();
        } else if rnext == "=" {
            self.set_next_ref_id(self.ref_id).unwrap();
        } else {
            let nr_id = header.reference_id(rnext).ok_or_else(||
                self.corrupt(format!("Reference '{}' is not in the header", rnext)))?;
            self.set_next_ref_id(nr_id as i32)?;
        }

        let next_start = split.try_next("next start (PNEXT)")?;
        let next_start = next_start.parse::<i32>().map_err(|_|
            self.corrupt(format!("Cannot convert PNEXT '{}' to int", next_start)))? - 1;
        self.set_next_start(next_start)?;

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

    /// Writes a record in BAM format.
    pub fn write_bam<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        let raw_cigar_len = if self.cigar.len() <= 0xffff {
            4 * self.cigar.len()
        } else {
            10 + 4 * self.cigar.len()
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

        stream.write_u16::<LittleEndian>(self.flag)?;
        stream.write_i32::<LittleEndian>(self.seq.len() as i32)?;
        stream.write_i32::<LittleEndian>(self.next_ref_id)?;
        stream.write_i32::<LittleEndian>(self.next_start)?;
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

    pub fn set_flag(&mut self, flag: u16) {
        self.flag = flag;
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
    pub fn set_next_ref_id(&mut self, next_ref_id: i32) -> Result<(), &'static str> {
        if next_ref_id < -1 {
            Err("Next reference id < -1")
        } else {
            self.next_ref_id = next_ref_id;
            Ok(())
        }
    }

    /// Sets record 0-based start of the mate record. It cannot be less than -1.
    pub fn set_next_start(&mut self, next_start: i32) -> Result<(), &'static str> {
        if next_start < -1 {
            Err("Next start < -1")
        } else {
            self.next_start = next_start;
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
