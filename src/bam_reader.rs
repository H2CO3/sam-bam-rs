use std::fs::File;
use std::io::{Read, Seek, Result, Error, Write};
use std::io::ErrorKind::InvalidData;
use std::path::Path;
use std::result;

use byteorder::{LittleEndian, ReadBytesExt};

use super::index::Index;
use super::record;
use super::bgzip::{self, SeekReader, ChunksReader};

/// BAM file header. Contains names and lengths of reference sequences.
#[derive(Clone)]
pub struct Header {
    text: Vec<u8>,
    references: Vec<String>,
    lengths: Vec<i32>,
}

impl Header {
    fn from_stream<R: Read>(stream: &mut R) -> Result<Header> {
        let mut magic = [0_u8; 4];
        stream.read_exact(&mut magic)?;
        if magic != [b'B', b'A', b'M', 1] {
            return Err(Error::new(InvalidData, "Input is not in BAM format"));
        }

        let l_text = stream.read_i32::<LittleEndian>()?;
        if l_text < 0 {
            return Err(Error::new(InvalidData, "BAM file corrupted: negative header length"));
        }
        let mut text = vec![0_u8; l_text as usize];
        stream.read_exact(&mut text)?;
        let mut res = Header {
            text,
            references: Vec::new(),
            lengths: Vec::new(),
        };

        let n_refs = stream.read_i32::<LittleEndian>()?;
        if n_refs < 0 {
            return Err(Error::new(InvalidData, "BAM file corrupted: negative number of references"));
        }
        for _ in 0..n_refs {
            let l_name = stream.read_i32::<LittleEndian>()?;
            if l_name <= 0 {
                return Err(Error::new(InvalidData,
                    "BAM file corrupted: negative reference name length"));
            }
            let mut name = vec![0_u8; l_name as usize - 1];
            stream.read_exact(&mut name)?;
            let _null = stream.read_u8()?;
            let name = std::string::String::from_utf8(name)
                .map_err(|_| Error::new(InvalidData,
                "BAM file corrupted: reference name not in UTF-8"))?;

            let l_ref = stream.read_i32::<LittleEndian>()?;
            if l_ref < 0 {
                return Err(Error::new(InvalidData,
                    "BAM file corrupted: negative reference length"));
            }
            res.references.push(name);
            res.lengths.push(l_ref);
        }
        Ok(res)
    }

    /// Number of reference sequences in the bam file
    pub fn n_references(&self) -> usize {
        self.references.len()
    }

    /// Get the name of the reference with `ref_id` (0-based).
    /// Returns None if there is no such reference
    pub fn reference_name(&self, ref_id: usize) -> Option<&str> {
        if ref_id > self.references.len() {
            None
        } else {
            Some(&self.references[ref_id])
        }
    }

    /// Get the length of the reference with `ref_id` (0-based).
    /// Returns None if there is no such reference
    pub fn reference_len(&self, ref_id: usize) -> Option<i32> {
        if ref_id > self.lengths.len() {
            None
        } else {
            Some(self.lengths[ref_id])
        }
    }

    /// Full header text, as in BAM file
    pub fn text(&self) -> &[u8] {
        &self.text
    }
}

/// Iterator over records in a specific region.
///
/// If possible, create a single record using [Record::new](../record/struct.Record.html#method.new)
/// and then use [read_into](#method.read_into) instead of iterating, as it saves time on allocation.
pub struct RegionViewer<'a, R: Read + Seek> {
    chunks_reader: ChunksReader<'a, R>,
    start: i32,
    end: i32,
    predicate: Box<Fn(&record::Record) -> bool>,
}

impl<'a, R: Read + Seek> RegionViewer<'a, R> {
    /// Write the next record into `record`.
    ///
    /// # Errors
    ///
    /// If there are no more reads to iterate over, the function returns
    /// [NoMoreRecords](../record/enum.Error.html#variant.NoMoreRecords) error.
    ///
    /// If the record was corrupted, the function returns
    /// [Corrupted](../record/enum.Error.html#variant.Corrupted) error.
    /// If the record was truncated or the reading failed for a different reason, the function
    /// returns [Truncated](../record/enum.Error.html#variant.Truncated) error.
    pub fn read_into(&mut self, record: &mut record::Record) -> result::Result<(), record::Error> {
        loop {
            record.fill_from(&mut self.chunks_reader)?;
            if (self.predicate)(&record) && record.start() < self.end {
                let record_end = record.calculate_end();
                if record_end != -1 && record_end < record.start() {
                    return Err(record::Error::Corrupted("aln_end < aln_start"));
                }
                if record_end > self.start {
                    return Ok(());
                }
            }
        }
    }
}

/// Iterate over records in the region. Consider using [read_into](#method.read_into), if only
/// one record is needed at each time.
///
/// # Errors
///
/// If the record was corrupted, the function returns
/// [Corrupted](../record/enum.Error.html#variant.Corrupted) error.
/// If the record was truncated or the reading failed for a different reason, the function
/// returns [Truncated](../record/enum.Error.html#variant.Truncated) error.
impl<'a, R: Read + Seek> Iterator for RegionViewer<'a, R> {
    type Item = result::Result<record::Record, record::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = record::Record::new();
        match self.read_into(&mut record) {
            Ok(()) => Some(Ok(record)),
            Err(record::Error::NoMoreRecords) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// BAM file reader. `IndexedReader` allows to fetch records from arbitrary positions,
/// but does not allow to read all reads consecutively.
pub struct IndexedReader<R: Read + Seek> {
    reader: SeekReader<R>,
    header: Header,
    index: Index,
    buffer: Vec<u8>,
}

impl IndexedReader<File> {
    /// Open bam file from `path`. Bai index will be loaded from `{path}.bai`.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        IndexedReader::from_path_and_index(&path, format!("{}.bai", path.display()))
    }

    /// Open bam file from `bam_path` and load bai index from `bai_path`.
    pub fn from_path_and_index<P: AsRef<Path>, U: AsRef<Path>>(bam_path: P, bai_path: U)
            -> Result<Self> {
        let reader = SeekReader::from_path(bam_path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAM file: {}", e)))?;
        let index = Index::from_path(bai_path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAI index: {}", e)))?;
        IndexedReader::new(reader, index)
    }
}

impl<R: Read + Seek> IndexedReader<R> {
    /// Open `IndexedReader` using two streams. `reader_stream` should be opened during the
    /// whole work with the reader, while `index_stream` will be read once.
    pub fn from_streams<T: Read>(reader_stream: R, index_stream: &mut T) -> Result<Self> {
        let reader = SeekReader::from_stream(reader_stream)?;
        let index = Index::from_stream(index_stream)?;
        IndexedReader::new(reader, index)
    }

    fn new(mut reader: SeekReader<R>, index: Index) -> Result<Self> {
        let mut buffer = Vec::with_capacity(bgzip::MAX_BLOCK_SIZE);
        let header = {
            let mut header_reader = ChunksReader::without_boundaries(&mut reader, &mut buffer);
            Header::from_stream(&mut header_reader)?
        };
        Ok(Self {
            reader, header, index, buffer,
        })
    }

    /// Get an iterator over records aligned to the reference `ref_id` (0-based),
    /// and intersecting half-open interval `[start-end)`.
    pub fn fetch<'a>(&'a mut self, ref_id: i32, start: i32, end: i32) -> RegionViewer<'a, R> {
        self.fetch_by(ref_id, start, end, |_| true)
    }

    /// Get an iterator over records aligned to the reference `ref_id` (0-based),
    /// and intersecting half-open interval `[start-end)`.
    ///
    /// Records will be filtered by `predicate`. It helps to slightly reduce fetching time,
    /// as some records will be removed without allocating new memory and without calculating
    /// alignment length.
    pub fn fetch_by<'a, F>(&'a mut self, ref_id: i32, start: i32, end: i32, predicate: F)
        -> RegionViewer<'a, R>
    where F: 'static + Fn(&record::Record) -> bool
    {
        let chunks = self.index.fetch_chunks(ref_id, start, end);
        RegionViewer {
            chunks_reader: ChunksReader::new(&mut self.reader, chunks, &mut self.buffer),
            start, end,
            predicate: Box::new(predicate),
        }
    }

    /// Return BAM header
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Write record in sam format.
    /// Same as [Record::write_sam](../record/struct.Record.html#method.write_sam)
    pub fn write_record_as_sam<W: Write>(&self, writer: &mut W, record: &record::Record)
            -> Result<()> {
        record.write_sam(writer, self.header())
    }
}
