use std::io::{self, Read, Write};
use std::ops::RangeBounds;

use byteorder::WriteBytesExt;

use super::{resize, write_iterator};

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
    /// Creates an empty sequence.
    pub fn new() -> Self {
        Sequence {
            raw: Vec::new(),
            len: 0,
        }
    }

    /// Clears sequence but does not touch capacity.
    pub fn clear(&mut self) {
        self.raw.clear();
        self.len = 0;
    }

    /// Shrinks inner vector.
    pub fn shrink_to_fit(&mut self) {
        self.raw.shrink_to_fit();
    }

    /// Extends sequence from the text representation.
    pub fn extend_from_text<I: IntoIterator<Item = u8>>(&mut self, nucleotides: I)
            -> Result<(), String> {
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

    /// Clears sequence and fills from a raw stream. `new_len` represents the number of nucleotides,
    /// not the number of bytes.
    pub fn fill_from<R: Read>(&mut self, stream: &mut R, new_len: usize)
            -> io::Result<()> {
        let short_len = (new_len + 1) / 2;
        unsafe {
            resize(&mut self.raw, short_len);
        }
        stream.read_exact(&mut self.raw)?;
        self.len = new_len;
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
}