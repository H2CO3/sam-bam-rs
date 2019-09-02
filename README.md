*bam* is a crate that allows to read BAM files, written completely in Rust. Currently, it
allows to read BAM files in *indexed* and *consecutive* modes (`bam::IndexedReader`
and `bam::Reader`). The future versions will support writing BAM files.

## Why?

Having a crate written completely in Rust reduces the number of dependencies and compilation time.
Additionally, it removes the need to install additional C libraries.

Errors produced by this crate are more readable and easier to catch and fix on-the-fly.

## Usage

Currently, there are two readers:
* `bam::IndexedReader`, which allows to fetch records from
random genomic regions,
* `bam::Reader`, which allows to read the BAM file consecutively.

The following code would load BAM file `test.bam` and its index `test.bam.bai`, take all records
from `2:100001-200000` and print them on the stdout.

```rust
extern crate bam;

fn main() {
    let mut reader = bam::IndexedReader::from_path("test.bam").unwrap();

    // We need to clone the header to have access to reference names as the
    // reader will be blocked during fetch.
    let header = reader.header().clone();
    let mut stdout = std::io::BufWriter::new(std::io::stdout());

    for record in reader.fetch(1, 100_000, 200_000).unwrap() {
        record.unwrap().write_sam(&mut stdout, &header).unwrap();
    }
}
```

The following code will print the whole contents of `test.bam`:

```rust
extern crate bam;

fn main() {
    let reader = bam::Reader::from_path("test.bam").unwrap();

    let header = reader.header().clone();
    let mut stdout = std::io::BufWriter::new(std::io::stdout());

    for record in reader {
        record.unwrap().write_sam(&mut stdout, &header).unwrap();
    }
}
```

You can find more detailed usage [here](https://docs.rs/bam).

## Changelog
* 0.0.5 - Improved interaction with tags, in addition:
    - now `fetch` and `fetch_by` return an error, if the fetched region is out of bounds,
    - during the construction of `IndexedReader`, you can specify how to handle a situation when
    BAI index is younger than a BAM file (ignore, return error, warn).
* 0.0.4 - Bug fixes, optimized writing in SAM format,
* 0.0.3 - Switched to a new `inflate` crate, additional reading speedup,
* 0.0.2 - Support for consecutive reader `bam::Reader`,
and a `bam::BamReader` trait.
* 0.0.1 - Support for indexed reader `bam::IndexedReader`.

## Future versions
* Support for `bam::Writer`,
* Optimized writing of SAM records,
* Support for multi-thread loading and writing,
* Additional features for `bam::Record`
and other structures, like `Cigar`.
