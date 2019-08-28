extern crate byteorder;
extern crate libflate;
extern crate lru_cache;
#[cfg(feature = "check_crc")]
extern crate crc;

pub mod index;
pub mod bgzip;
pub mod cigar;
pub mod record;
pub mod bam_reader;

pub use bam_reader::{Header, IndexedReader};
pub use record::{Record, Error};
