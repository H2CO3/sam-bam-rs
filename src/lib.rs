//! *bam* is a crate that allows to read and write BAM, SAM and BGZIP files,
//! written completely in Rust.
//!
//! ## Overview
//!
//! Currently, there are three readers and two writers:
//! * [bam::IndexedReader](bam_reader/struct.IndexedReader.html) - fetches records from
//! random genomic regions.
//! * [bam::BamReader](bam_reader/struct.BamReader.html) - reads a BAM file consecutively.
//! * [bam::SamReader](sam/struct.SamReader.html) - reads a SAM file consecutively.
//! * [bam::BamWriter](bam_writer/struct.BamWriter.html) - writes a BAM file.
//! * [bam::SamWriter](sam/struct.SamWriter.html) - writes a SAM file.
//!
//! BAM readers and writers have single-thread and multi-thread modes.
//!
//! The [bgzip](bgzip/index.html) module to interact directly with bgzip files (BGZF).
//!
//! The crate also allows to conviniently work with SAM/BAM [records](record/struct.Record.html)
//! and their fields, such as [CIGAR](record/cigar/struct.Cigar.html) or
//! [tags](record/tags/struct.TagViewer.html).
//!
//! ## Usage
//!
//! The following code would load BAM file `in.bam` and its index `in.bam.bai`, take all records
//! from `3:600001-700000` and print them on the stdout.
//!
//! ```rust
//! extern crate bam;
//!
//! use std::io;
//! use bam::RecordWriter;
//!
//! fn main() {
//!     let mut reader = bam::IndexedReader::from_path("in.bam").unwrap();
//!     let output = io::BufWriter::new(io::stdout());
//!     let mut writer = bam::SamWriter::build()
//!         .write_header(false)
//!         .from_stream(output, reader.header().clone()).unwrap();
//!
//!     for record in reader.fetch(&bam::Region::new(2, 600_000, 700_000)).unwrap() {
//!         let record = record.unwrap();
//!         writer.write(&record).unwrap();
//!     }
//! }
//! ```
//!
//! More complicated example with completely created header and record:
//!
//! ```rust
//! extern crate bam;
//! 
//! use std::io;
//! use bam::RecordWriter;
//! use bam::header::{Header, HeaderEntry};
//! 
//! fn main() {
//!     // Creating a header.
//!     let mut header = Header::new();
//!     // Header line          "@HD  VN:1.6  SO:Coordinate".
//!     let mut header_line = HeaderEntry::header_line("1.6".to_string());
//!     header_line.push(b"SO", "Coordinate".to_string());
//!     header.push_entry(header_line).unwrap();
//!     // Reference line       "@SQ  SN:chr1  LN:10000".
//!     header.push_entry(HeaderEntry::ref_sequence("chr1".to_string(), 10000)).unwrap();
//! 
//!     // Write SAM to stdout.
//!     let output = io::BufWriter::new(io::stdout());
//!     let mut writer = bam::SamWriter::from_stream(output, header).unwrap();
//! 
//!     // Create a new record, set its name to "Read_1",
//!     // reference id to 0, start to 10 (both 0-based).
//!     let mut record = bam::Record::new();
//!     record.set_name("Read_1".bytes());
//!     record.set_ref_id(0).unwrap();
//!     record.set_start(10).unwrap();
//!     // Set the record to be on reverse strand.
//!     record.flag_mut().set_strand(false);
//!     // Set sequence and qualities (qualities without +33), and cigar.
//!     record.set_seq_qual("ACGT".bytes(), [10_u8, 20, 30, 10].iter().cloned()).unwrap();
//!     record.set_cigar("2M1I1M".bytes()).unwrap();
//!     // Add NM tag.
//!     record.tags_mut().push(b"NM", 1).unwrap();
//! 
//!     writer.write(&record).unwrap();
//!     writer.finish().unwrap();
//!     // Above code would print the following SAM file:
//!     // @HD VN:1.6  SO:Coordinate
//!     // @SQ SN:chr1 LN:10000
//!     // Read_1  16  chr1    11  0   2M1I1M  *   0   0   ACGT    +5?+    NM:i:1
//! 
//!     println!("Aligned pairs:");
//!     for (read_pos, ref_pos) in record.aligned_pairs() {
//!         println!("    {:?} {:?}", read_pos, ref_pos);
//!     }
//!     // Aligned pairs:
//!     //     Some(0) Some(10)
//!     //     Some(1) Some(11)
//!     //     Some(2) None
//!     //     Some(3) Some(12)
//! }
//! ```


extern crate byteorder;
extern crate crc32fast;
extern crate flate2;

pub mod index;
pub mod bgzip;
pub mod record;
pub mod bam_reader;
pub mod bam_writer;
pub mod header;
pub mod sam;
pub mod pileup;

pub use bam_reader::IndexedReader;
pub use bam_reader::BamReader;
pub use bam_reader::Region;
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
///    // New record is saved into record.
///        Ok(()) => {},
///        // NoMoreRecords represents stop iteration.
///        Err(bam::Error::NoMoreRecords) => break,
///        Err(e) => panic!("{}", e),
///    }
///    // Do somethind with the record.
///}
///```
/// Or you can just iterate over records:
/// ```rust
///for record in reader {
///    let record = record.unwrap();
///    // Do somethind with the record.
///}
///```
pub trait RecordReader: Iterator<Item = Result<Record, Error>> {
    /// Writes the next record into `record`. It allows to skip excessive memory allocation.
    ///
    /// # Errors
    ///
    /// If there are no more records to iterate over, the function returns
    /// [NoMoreRecords](record/enum.Error.html#variant.NoMoreRecords) error.
    ///
    /// If the record was corrupted, the function returns
    /// [Corrupted](record/enum.Error.html#variant.Corrupted) error.
    /// If the record was truncated or the reading failed for a different reason, the function
    /// returns [Truncated](record/enum.Error.html#variant.Truncated) error.
    ///
    /// If the function returns an error, the record is supposed to be cleared.
    fn read_into(&mut self, record: &mut Record) -> Result<(), Error>;
}

/// A trait for writing BAM/SAM records.
pub trait RecordWriter {
    /// Writes a single record.
    fn write(&mut self, record: &Record) -> std::io::Result<()>;

    /// Finishes the stream, same as `std::mem::drop(writer)`, but can return an error.
    fn finish(&mut self) -> std::io::Result<()>;

    /// Flushes contents.
    fn flush(&mut self) -> std::io::Result<()>;
}
