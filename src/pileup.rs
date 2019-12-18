//! BAM file pileup.

use std::rc::Rc;
use std::io;

use super::{Record, RecordReader, Region};

/// Record sequence and qualities matching a single reference position. It can be
/// - `Nucleotide(nt: u8, qual: u8)`, where `nt` is a letter, like `b'A'`, and quality does not have +33 added.
/// if the qualities are not present, `qual` is equal to `255`.
/// - `Insertion(seq: Vec<u8>, qual: Vec<u8>)`, `seq` and `qual` have the same format as in `Nucleotide(nt, qual)`.
/// First letter represents the nucleotide matching to the reference position, and the next letters represent the
/// insertion.
/// - `Deletion` - this reference position is absent from the record.
#[derive(Clone)]
pub enum PosSequence {
    Nucleotide(u8, u8),
    Insertion(Vec<u8>, Vec<u8>),
    Deletion,
}

impl PosSequence {
    /// Returns 1 for `Nucleotide`, 0 for `Deletion`, and 1 + length of the insertion for `Insertion`.
    pub fn len(&self) -> u32 {
        match self {
            PosSequence::Nucleotide(_, _) => 1,
            PosSequence::Insertion(seq, _) => 1 + seq.len() as u32,
            PosSequence::Deletion => 0,
        }
    }

    /// Returns `true` for `Nucleotide` variant, and `false` otherwise.
    pub fn is_single_nt(&self) -> bool {
        match self {
            PosSequence::Nucleotide(_, _) => true,
            _ => false,
        }
    }

    /// Returns `true` for `Insertion` variant, and `false` otherwise.
    pub fn is_insertion(&self) -> bool {
        match self {
            PosSequence::Insertion(_, _) => true,
            _ => false,
        }
    }

    /// Returns `true` for `Deletion` variant, and `false` otherwise.
    pub fn is_deletion(&self) -> bool {
        match self {
            PosSequence::Deletion => true,
            _ => false,
        }
    }

    /// Returns sequence and qualities iterator. It has [len()](#method.len) number of elements.
    pub fn seq_qual<'a>(&'a self) -> PosSequenceIterator<'a> {
        PosSequenceIterator {
            parent: self,
            i: 0,
            j: self.len(),
        }
    }
}

/// Iterator over pairs (nt, qual) for a single record and a single reference [position](struct.PosSequence.html).
#[derive(Clone)]
pub struct PosSequenceIterator<'a> {
    parent: &'a PosSequence,
    i: u32,
    j: u32,
}

impl<'a> Iterator for PosSequenceIterator<'a> {
    type Item = (u8, u8);

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.j {
            self.i += 1;
            match self.parent {
                PosSequence::Insertion(seq, qual) => {
                    let index = self.i as usize - 1;
                    Some((seq[index], qual[index]))
                },
                PosSequence::Nucleotide(nt, qual) => {
                    Some((*nt, *qual))
                },
                PosSequence::Deletion => unreachable!(),
            }
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = (self.j - self.i) as usize;
        (len, Some(len))
    }
}

impl<'a> DoubleEndedIterator for PosSequenceIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.i < self.j {
            self.j -= 1;
            match self.parent {
                PosSequence::Insertion(seq, qual) => {
                    Some((seq[self.j as usize], qual[self.j as usize]))
                },
                PosSequence::Nucleotide(nt, qual) => {
                    Some((*nt, *qual))
                },
                PosSequence::Deletion => unreachable!(),
            }
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for PosSequenceIterator<'a> {}

impl<'a> std::iter::FusedIterator for PosSequenceIterator<'a> {}

/// Entry in a pileup column, that stores information for a single record.
pub struct PileupEntry {
    record: Rc<Record>,
    cigar_index: usize,
    cigar_remaining: u32,
    q_pos: u32,
}

pub struct PileupColumn {
    entries: Vec<PileupEntry>,
}

/// [Pileup](struct.Pileup.html) builder.
pub struct PileupBuilder<'a, R: RecordReader> {
    reader: &'a mut R,
    region: Option<Region>,
    all_covered_pos: bool,
    read_filter: Option<Box<dyn Fn(&Record) -> bool>>,
}

impl<'a, R: RecordReader> PileupBuilder<'a, R> {
    pub(crate) fn new(reader: &'a mut R, region: Option<Region>) -> Self {
        Self {
            reader, region,
            all_covered_pos: region.is_none(),
            read_filter: None,
        }
    }
}

pub struct Pileup<'a, R: RecordReader> {
    reader: &'a mut R,
    reads: Vec<PileupEntry>,
    ref_id: u32,
    ref_pos: u32,
    next_read: Result<Record, io::Error>,
    read_filter: Box<dyn Fn(&Record) -> bool>,
}
