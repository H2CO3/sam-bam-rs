pub mod index;
pub mod bgzip;
pub mod cigar;
pub mod record;
pub mod bam_reader;

use std::time::Instant;

fn main() {
    let mut reader = bam_reader::IndexedReader::from_path(
        "/home/timofey/Documents/Study/18/Vikas/Data/Without1/Reads/generated_minimap/reads.bam")
        .unwrap_or_else(|e| panic!("{}", e));
    let header = reader.header().clone();

    let mut viewer = reader.fetch(1, 100_000, 200_000);
    let mut rec = record::Record::new();
    loop {
        match viewer.read_into(&mut rec) {
            // New record is saved into record
            Ok(()) => {},
            // NoMoreRecords represents stop iteration
            Err(record::Error::NoMoreRecords) => break,
            Err(e) => panic!("{}", e),
        }

        rec.write_sam(&mut std::io::stdout(), &header)
            .unwrap_or_else(|e| panic!("{}", e));
    }
}