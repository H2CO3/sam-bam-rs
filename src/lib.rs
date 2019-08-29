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
//!     for record in reader.fetch(1, 100_000, 200_000) {
//!         record.unwrap().write_sam(&mut stdout, &header).unwrap();
//!     }
//! }
//! ```
//!
//! Additionally, you can use `read_into(&mut record)` to save time on record allocation:
//! ```rust
//! extern crate bam;
//!
//! // You need to import BamReader trait
//! use bam::BamReader;
//!
//! fn main() {
//!     let mut reader = bam::IndexedReader::from_path("test.bam").unwrap();
//!
//!     let header = reader.header().clone();
//!     let mut stdout = std::io::BufWriter::new(std::io::stdout());
//!
//!     let mut viewer = reader.fetch(1, 100_000, 200_000);
//!     let mut record = bam::Record::new();
//!     loop {
//!         match viewer.read_into(&mut record) {
//!             Ok(()) => {},
//!             Err(bam::Error::NoMoreRecords) => break,
//!             Err(e) => panic!("{}", e),
//!         }
//!         record.write_sam(&mut stdout, &header).unwrap();
//!     }
//! }
//! ```
//!
//! Note that currently printing the read is much slower than loading it. Without printing, it
//! takes almost the same time to load records using *bam* crate and `samtools view`.
//!
//! If only records with specific MAPQ or FLAGs are needed, you can use `fetch_by`. For example,
//! ```rust
//! reader.fetch_by(1, 100_000, 200_000,
//!     |record| record.mapq() >= 30 && !record.is_secondary())
//! ```
//! to load only records with MAPQ at least 30 and skip all secondary alignments. In some cases it
//! helps to save time by not calculating the right-most aligned read position, as well as
//! remove additional allocations.
//!
//! You can also use [IndexedReaderBuilder](bam_reader/struct.IndexedReaderBuilder.html),
//! which gives more control over loading
//! [IndexedReader](bam_reader/struct.IndexedReader.html).
//! For example you can create a reader using a different BAI path, and a different cache capacity:
//! ```rust
//! let mut reader = bam::IndexedReader::build()
//!     .bai_path("other_dir/test.bai")
//!     .cache_capacity(10000)
//!     .from_path("test.bam").unwrap();
//! ```
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
//! Similarly, you can skip allocation with
//! ```rust
//! extern crate bam;
//!
//! // You need to import BamReader trait
//! use bam::BamReader;
//!
//! fn main() {
//!     let mut reader = bam::Reader::from_path("test.bam").unwrap();
//!
//!     let header = reader.header().clone();
//!     let mut stdout = std::io::BufWriter::new(std::io::stdout());
//!
//!     let mut record = bam::Record::new();
//!     loop {
//!         match reader.read_into(&mut record) {
//!             Ok(()) => {},
//!             Err(bam::Error::NoMoreRecords) => break,
//!             Err(e) => panic!("{}", e),
//!         }
//!         record.write_sam(&mut stdout, &header).unwrap();
//!     }
//! }
//! ```
//!
//! However, there is no way to skip records using a predicate like `fetch_by`.
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
//! ## Changelog
//! * 0.0.3 - Switched to a new `inflate` crate, additional reading speedup,
//! * 0.0.2 - Support for consecutive reader [bam::Reader](bam_reader/struct.Reader.html),
//! and a [bam::BamReader](bam_reader/trait.BamReader.html) trait.
//! * 0.0.1 - Support for indexed reader [bam::IndexedReader](bam_reader/struct.IndexedReader.html).
//!
//! ## Future versions
//! * Support for `bam::Writer`,
//! * Optimized writing of SAM records,
//! * Support for multi-thread loading and writing,
//! * Additional features for [bam::Record](record/struct.Record.html)
//! and other structures, like [Cigar](cigar/struct.Cigar.html).
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
