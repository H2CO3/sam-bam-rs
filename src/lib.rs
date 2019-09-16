//! *bam* is a crate that allows to read BAM files, written completely in Rust. Currently, it
//! allows to read BAM files in *indexed* and *consecutive* modes ([bam::IndexedReader](bam_reader/struct.IndexedReader.html)
//! and [bam::Reader](bam_reader/struct.Reader.html)). The future versions will support writing BAM files.
//!
//! ## Why?
//!
//! Having a crate written completely in Rust reduces the number of dependencies and compilation time.
//! Additionally, it removes the need to install additional C libraries.
//!
//! Errors produced by this crate are more readable and easier to catch and fix on-the-fly.
//!
//! ## Usage
//!
//! Currently, there are two readers:
//! * [bam::IndexedReader](bam_reader/struct.IndexedReader.html), which allows to fetch records from
//! random genomic regions,
//! * [bam::Reader](bam_reader/struct.Reader.html), which allows to read the BAM file consecutively.
//!
//! ### IndexedReader
//!
//! The following code would load BAM file `test.bam` and its index `test.bam.bai`, take all records
//! from `2:100001-200000` and print them on the stdout.
//!
//! ```rust
//! extern crate bam;
//!
//! fn main() {
//!     let mut reader = bam::IndexedReader::from_path("test.bam").unwrap();
//!
//!     // We need to clone the header to have access to reference names as the
//!     // reader will be blocked during fetch.
//!     let header = reader.header().clone();
//!     let mut stdout = std::io::BufWriter::new(std::io::stdout());
//!
//!     for record in reader.fetch(1, 100_000, 200_000).unwrap() {
//!         record.unwrap().write_sam(&mut stdout, &header).unwrap();
//!     }
//! }
//! ```
//!
//! Find more at [IndexedReader](bam_reader/struct.IndexedReader.html).
//!
//! ### Reader
//!
//! [Reader](bam_reader/struct.Reader.html) allows to read all records from the BAM file
//! consecutively. [Reader](bam_reader/struct.Reader.html) itself is an iterator
//! and implements the same trait [RecordReader](bam_reader/trait.RecordReader.html), which allows
//! to load records similarly:
//! ```rust
//! extern crate bam;
//!
//! fn main() {
//!     let reader = bam::Reader::from_path("test.bam").unwrap();
//!
//!     let header = reader.header().clone();
//!     let mut stdout = std::io::BufWriter::new(std::io::stdout());
//!
//!     for record in reader {
//!         record.unwrap().write_sam(&mut stdout, &header).unwrap();
//!     }
//! }
//! ```
//!
//! Find more at [Reader](bam_reader/struct.Reader.html).

extern crate byteorder;
extern crate lru_cache;
extern crate crc32fast;
extern crate flate2;

/// A module that works with BAI index.
pub mod index;
/// A module that works with Bgzip files (BGZF) and bgzip blocks.
pub mod bgzip;
/// A module that supports Cigar and operations on it.
pub mod cigar;
/// A module with BAM records and operations on them.
pub mod record;
/// A module with indexed and consecutive BAM readers.
pub mod bam_reader;
/// A module with a BAM writer.
pub mod bam_writer;
/// A module that describes SAM/BAM header.
pub mod header;
/// A module that describes SAM reader and writer.
pub mod sam;

pub use bam_reader::IndexedReader;
pub use bam_reader::BamReader;
pub use bam_writer::BamWriter;

pub use header::Header;

pub use record::Record;
pub use record::Error;

pub use sam::SamWriter;
pub use sam::SamReader;


/// A trait for reading BAM/SAM records.
///
/// You can use the single record:
/// ```rust
///let mut record = bam::Record::new();
///loop {
///    // reader: impl RecordReader
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
pub trait RecordReader: Iterator<Item = Result<Record, Error>> {
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
    ///
    /// If the function returns an error, the record is supposed to be cleared.
    fn read_into(&mut self, record: &mut Record) -> Result<(), Error>;
}

/// A trait for writing BAM/SAM records.
pub trait RecordWriter {
    /// Writes a single record.
    fn write(&mut self, record: &Record) -> std::io::Result<()>;
}
