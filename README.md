# bam

*bam* is a library that allows to read BAM files, written completely in Rust. Currently, it
allows to only fetch records from a region in an indexed bam file. In the future versions,
non-indexed reader will be supported, as well as writing BAM files.

## Why?

Having a library written completely in Rust reduces the number of dependencies and compilation time.
Additionally, it removes the need to install additional C libraries.

Errors produced by this crate are more readable and easier to catch and fix on-the-fly.

## Usage

Currently, there is only one available reader: `bam::IndexedReader`. The following code would
print SAM entries for all records in a region:

```rust
extern crate bam;

fn main() {
    // If the index is not located at test.bam.bai, use
    // IndexedReader::from_path_and_index(bam_path, bai_path)
    let mut reader = bam::IndexedReader::from_path("test.bam").unwrap();

    // Need to clone header to have access to reference names as the
    // reader will be blocked durint fetch.
    let header = reader.header().clone();

    for record in reader.fetch(1, 100_000, 200_000) {
        let record = record.unwrap();
        record.write_sam(&mut std::io::stdout(), &header).unwrap();
    }
}
```

Additionally, you can use `read_into(&mut record)` to save time on record allocation:
```rust
    let mut viewer = reader.fetch(1, 100_000, 200_000);
    let mut record = bam::Record::new();
    loop {
        match viewer.read_into(&mut record) {
            // New record is saved into record
            Ok(()) => {},
            // NoMoreRecords represents stop iteration
            Err(bam::Error::NoMoreRecords) => break,
            Err(e) => panic!("{}", e),
        }
        record.write_sam(&mut std::io::stdout(), &header).unwrap();
    }
```

## CRC32

Each bgzip block contains a CRC32 checksum. By default, the *bam* crate does not compare 
checksums to save time.
However, you can compare checksums by adding the following line to your Cargo.toml:
```
bam = { version = "*", features = ["check_crc"] }
```