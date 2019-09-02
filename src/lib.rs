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
//! and implements the same trait [BamReader](bam_reader/trait.BamReader.html), which allows
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
//!
//! ## CRC32
//!
//! Each bgzip block contains a CRC32 checksum. By default, the *bam* crate does not compare
//! checksums to save time.
//! However, you can compare checksums by adding the following line to your Cargo.toml:
//! ```
//! bam = { version = "*", features = ["check_crc"] }
//! ```
//!

extern crate byteorder;
extern crate inflate;
extern crate lru_cache;
#[cfg(feature = "check_crc")]
extern crate crc;

/// A module that works with BAI index.
pub mod index;
/// A module that works with Bgzip files (BGZF) and bgzip blocks.
pub mod bgzip;
/// A module that supports Cigar and operations on it.
pub mod cigar;
/// A module with BAM records and operations on them.
pub mod record;
/// A module with various BAM readers.
pub mod bam_reader;

pub use bam_reader::Header;
pub use bam_reader::IndexedReader;
pub use bam_reader::Reader;
pub use bam_reader::BamReader;

pub use record::Record;
pub use record::Error;
