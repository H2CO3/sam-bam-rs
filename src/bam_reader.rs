use std::fs::File;
use std::io::{Read, Seek, Result, Error, Write};
use std::io::ErrorKind::{self, InvalidData};
use std::path::{Path, PathBuf};
use std::result;

use byteorder::{LittleEndian, ReadBytesExt};

use super::index::{self, Index};
use super::record;
use super::bgzip;

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

    /// Returns the number of reference sequences in the BAM file.
    pub fn n_references(&self) -> usize {
        self.references.len()
    }

    /// Returns the name of the reference with `ref_id` (0-based).
    /// Returns None if there is no such reference
    pub fn reference_name(&self, ref_id: usize) -> Option<&str> {
        if ref_id > self.references.len() {
            None
        } else {
            Some(&self.references[ref_id])
        }
    }

    /// Returns the length of the reference with `ref_id` (0-based).
    /// Returns None if there is no such reference
    pub fn reference_len(&self, ref_id: usize) -> Option<i32> {
        if ref_id > self.lengths.len() {
            None
        } else {
            Some(self.lengths[ref_id])
        }
    }

    /// Returns full header text, as in BAM file
    pub fn text(&self) -> &[u8] {
        &self.text
    }
}

/// Iterator over bam records.
///
/// You can use the single record:
/// ```rust
///let mut record = bam::Record::new();
///loop {
///    // reader: impl BamReader
///    match reader.read_into(&mut record) {
///    // New record is saved into record
///        Ok(()) => {},
///        // NoMoreRecords represents stop iteration
///        Err(bam::Error::NoMoreRecords) => break,
///        Err(e) => panic!("{}", e),
///    }
///    // Do somethind with the record
///}
///```
/// Or you can just iterate over records:
/// ```rust
///for record in reader {
///    let record = record.unwrap();
///    // Do somethind with the record
///}
///```
pub trait BamReader: Iterator<Item = result::Result<record::Record, record::Error>> {
    /// Writes the next record into `record`. It allows to skip excessive memory allocation.
    ///
    /// # Errors
    ///
    /// If there are no more records to iterate over, the function returns
    /// [NoMoreRecords](../record/enum.Error.html#variant.NoMoreRecords) error.
    ///
    /// If the record was corrupted, the function returns
    /// [Corrupted](../record/enum.Error.html#variant.Corrupted) error.
    /// If the record was truncated or the reading failed for a different reason, the function
    /// returns [Truncated](../record/enum.Error.html#variant.Truncated) error.
    fn read_into(&mut self, record: &mut record::Record) -> result::Result<(), record::Error>;
}

/// Iterator over records in a specific region. Implements [BamReader](trait.BamReader.html) trait.
///
/// If possible, create a single record using [Record::new](../record/struct.Record.html#method.new)
/// and then use [read_into](trait.BamReader.html#method.read_into) instead of iterating,
/// as it saves time on allocation.
pub struct RegionViewer<'a, R: Read + Seek> {
    chunks_reader: bgzip::ChunksReader<'a, R>,
    start: i32,
    end: i32,
    predicate: Box<Fn(&record::Record) -> bool>,
}

impl<'a, R: Read + Seek> BamReader for RegionViewer<'a, R> {
    fn read_into(&mut self, record: &mut record::Record) -> result::Result<(), record::Error> {
        loop {
            record.fill_from(&mut self.chunks_reader)?;
            if !record.is_mapped() || !(self.predicate)(&record) || record.start() >= self.end {
                continue;
            }
            if record.bin() > index::MAX_BIN {
                return Err(record::Error::Corrupted(
                    "Read has BAI bin bigger than max possible value"));
            }
            let (min_start, max_end) = index::bin_to_region(record.bin());
            if min_start >= self.start && max_end <= self.end {
                return Ok(());
            }

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

/// Iterator over records.
///
/// # Errors
///
/// If the record was corrupted, the function returns
/// [Corrupted](../record/enum.Error.html#variant.Corrupted) error.
/// If the record was truncated or the reading failed for a different reason, the function
/// returns [Truncated](../record/enum.Error.html#variant.Truncated) error.
impl<'a, R: Seek + Read> Iterator for RegionViewer<'a, R> {
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

/// [IndexedReader](struct.IndexedReader.html) builder. Allows to specify paths to BAM and BAI
/// files, as well as LRU cache size and the option to skip BAI modification time check.
pub struct IndexedReaderBuilder {
    cache_capacity: Option<usize>,
    bai_path: Option<PathBuf>,
    check_time: bool,
}

impl IndexedReaderBuilder {
    /// Creates a new indexed reader builder.
    pub fn new() -> Self {
        Self {
            cache_capacity: None,
            bai_path: None,
            check_time: true,
        }
    }

    /// Sets a path to a BAI index. By default, it is `{bam_path}.bai`.
    /// Overwrites the last value, if any.
    pub fn bai_path<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.bai_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets or unsets BAI modification time check (set by default).
    ///
    /// If on, the build will fail if the BAI index is younger than the BAM file.
    pub fn check_time(&mut self, value: bool) -> &mut Self {
        self.check_time = value;
        self
    }

    /// Sets new LRU cache capacity. See
    /// [cache_capacity](../bgzip/struct.SeekReaderBuilder.html#method.cache_capacity)
    /// for more details.
    pub fn cache_capacity(&mut self, cache_capacity: usize) -> &mut Self {
        assert!(cache_capacity > 0, "Cache size must be non-zero");
        self.cache_capacity = Some(cache_capacity);
        self
    }

    /// Creates a new [IndexedReader](struct.IndexedReader.html) from `bam_path`.
    /// If BAI path was not specified, the functions tries to open `{bam_path}.bai`.
    pub fn from_path<P: AsRef<Path>>(&self, bam_path: P) -> Result<IndexedReader<File>> {
        let bam_path = bam_path.as_ref();
        let bai_path = self.bai_path.as_ref().map(PathBuf::clone)
            .unwrap_or_else(|| PathBuf::from(format!("{}.bai", bam_path.display())));

        if self.check_time {
            let bam_modified = bam_path.metadata().and_then(|metadata| metadata.modified());
            let bai_modified = bai_path.metadata().and_then(|metadata| metadata.modified());
            match (bam_modified, bai_modified) {
                (Ok(bam_time), Ok(bai_time)) => {
                    if bai_time < bam_time {
                        return Err(Error::new(ErrorKind::InvalidInput,
                            "BAI file is younger than BAM file"));
                    }
                },
                _ => {
                    // Modification time not available, nothing we can do.
                }
            }
        }

        let mut reader_builder = bgzip::SeekReader::build();
        if let Some(cache_capacity) = self.cache_capacity {
            reader_builder.cache_capacity(cache_capacity);
        }
        let reader = reader_builder.from_path(bam_path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAM file: {}", e)))?;

        let index = Index::from_path(bai_path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAI index: {}", e)))?;
        IndexedReader::new(reader, index)
    }

    /// Creates a new [IndexedReader](struct.IndexedReader.html) from two streams.
    /// BAM stream should support random access, while BAI stream does not need to.
    /// `check_time` and `bai_path` values are ignored.
    pub fn from_streams<R: Seek + Read, T: Read>(&self, bam_stream: R, bai_stream: T)
            -> Result<IndexedReader<R>> {
        let mut reader_builder = bgzip::SeekReader::build();
        if let Some(cache_capacity) = self.cache_capacity {
            reader_builder.cache_capacity(cache_capacity);
        }
        let reader = reader_builder.from_stream(bam_stream);

        let index = Index::from_stream(bai_stream)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAI index: {}", e)))?;
        IndexedReader::new(reader, index)
    }
}

/// BAM file reader. In contrast to [Reader](struct.Reader.html) the `IndexedReader`
/// allows to fetch records from arbitrary positions,
/// but does not allow to read all records consecutively.
///
/// The following code would load BAM file `test.bam` and its index `test.bam.bai`, take all records
/// from `2:100001-200000` and print them on the stdout.
///
/// ```rust
/// extern crate bam;
///
/// fn main() {
///     let mut reader = bam::IndexedReader::from_path("test.bam").unwrap();
///
///     // We need to clone the header to have access to reference names as the
///     // reader will be blocked during fetch.
///     let header = reader.header().clone();
///     let mut stdout = std::io::BufWriter::new(std::io::stdout());
///
///     for record in reader.fetch(1, 100_000, 200_000) {
///         record.unwrap().write_sam(&mut stdout, &header).unwrap();
///     }
/// }
/// ```
///
/// You can find more detailed help on the [main page](../index.html#indexedreader).
pub struct IndexedReader<R: Read + Seek> {
    reader: bgzip::SeekReader<R>,
    header: Header,
    index: Index,
    buffer: Vec<u8>,
}

impl IndexedReader<File> {
    /// Creates [IndexedReaderBuilder](struct.IndexedReaderBuilder.html).
    pub fn build() -> IndexedReaderBuilder {
        IndexedReaderBuilder::new()
    }

    /// Opens bam file from `path`. Bai index will be loaded from `{path}.bai`.
    ///
    /// Same as `Self::build().from_path(path)`.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::build().from_path(path)
    }
}

impl<R: Read + Seek> IndexedReader<R> {
    fn new(mut reader: bgzip::SeekReader<R>, index: Index) -> Result<Self> {
        let mut buffer = Vec::with_capacity(bgzip::MAX_BLOCK_SIZE);
        let header = {
            let mut header_reader = bgzip::ChunksReader::without_boundaries(
                &mut reader, &mut buffer);
            Header::from_stream(&mut header_reader)?
        };
        Ok(Self {
            reader, header, index, buffer,
        })
    }

    /// Returns an iterator over records aligned to the reference `ref_id` (0-based),
    /// and intersecting half-open interval `[start-end)`.
    pub fn fetch<'a>(&'a mut self, ref_id: i32, start: i32, end: i32) -> RegionViewer<'a, R> {
        self.fetch_by(ref_id, start, end, |_| true)
    }

    /// Returns an iterator over records aligned to the reference `ref_id` (0-based),
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
            chunks_reader: bgzip::ChunksReader::new(&mut self.reader, chunks, &mut self.buffer),
            start, end,
            predicate: Box::new(predicate),
        }
    }

    /// Returns BAM header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Writes record in sam format.
    /// Same as [Record::write_sam](../record/struct.Record.html#method.write_sam).
    pub fn write_record_as_sam<W: Write>(&self, writer: &mut W, record: &record::Record)
            -> Result<()> {
        record.write_sam(writer, self.header())
    }
}

/// BAM file reader. In contrast to [IndexedReader](struct.IndexedReader.html) the `Reader`
/// allows to read all records consecutively, but does not allow random access.
///
/// Implements [BamReader](struct.BamReader.html) trait.
///
/// ```rust
/// extern crate bam;
///
/// fn main() {
///     let reader = bam::Reader::from_path("test.bam").unwrap();
///
///     let header = reader.header().clone();
///     let mut stdout = std::io::BufWriter::new(std::io::stdout());
///
///     for record in reader {
///         record.unwrap().write_sam(&mut stdout, &header).unwrap();
///     }
/// }
/// ```
///
/// You can find more detailed help on the [main page](../index.html#reader).
pub struct Reader<R: Read> {
    bgzip_reader: bgzip::ConsecutiveReader<R>,
    header: Header,
}

impl Reader<File> {
    /// Creates BAM file reader from `path`.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let stream = File::open(path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAM file: {}", e)))?;
        Self::from_stream(stream)
    }
}

impl<R: Read> Reader<R> {
    /// Creates BAM file reader from `stream`. The stream does not have to support random access.
    pub fn from_stream(stream: R) -> Result<Self> {
        let mut bgzip_reader = bgzip::ConsecutiveReader::from_stream(stream)?;
        let header = Header::from_stream(&mut bgzip_reader)?;
        Ok(Self {
            bgzip_reader, header,
        })
    }
}

impl<R: Read> Reader<R> {
    /// Returns BAM header.
    pub fn header(&self) -> &Header {
        &self.header
    }
}

impl<R: Read> BamReader for Reader<R> {
    fn read_into(&mut self, record: &mut record::Record) -> result::Result<(), record::Error> {
        record.fill_from(&mut self.bgzip_reader)
    }
}

/// Iterator over records.
///
/// # Errors
///
/// If the record was corrupted, the function returns
/// [Corrupted](../record/enum.Error.html#variant.Corrupted) error.
/// If the record was truncated or the reading failed for a different reason, the function
/// returns [Truncated](../record/enum.Error.html#variant.Truncated) error.
impl<R: Read> Iterator for Reader<R> {
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
