//! BAM writer.

use std::io::{Write, Result};
use std::fs::File;
use std::path::Path;

use super::bgzip;
use super::{Header, Record, RecordWriter};

/// Builder of the [BamWriter](struct.BamWriter.html).
pub struct BamWriterBuilder {
    header: Option<Header>,
    write_header: bool,
    level: u8,
    additional_threads: u16,
}

impl BamWriterBuilder {
    pub fn new() -> Self {
        Self {
            header: None,
            write_header: true,
            level: 6,
            additional_threads: 0,
        }
    }

    /// Specifies BAM header.
    pub fn header(&mut self, header: Header) -> &mut Self {
        self.header = Some(header);
        self
    }

    /// The option to write or skip header when creating the BAM writer (writing by default).
    pub fn write_header(&mut self, write: bool) -> &mut Self {
        self.write_header = write;
        self
    }

    /// Specify compression level from 0 to 9 (6 by default).
    pub fn compression_level(&mut self, level: u8) -> &mut Self {
        assert!(level <= 9, "Maximal compression level is 9");
        self.level = level;
        self
    }

    /// Specify the number of additional threads.
    /// Additional threads are used to compress blocks, while the
    /// main thread reads the writes to a file/stream.
    /// If `additional_threads` is 0 (default), the main thread
    /// will compress blocks itself.
    pub fn additional_threads(&mut self, additional_threads: u16) -> &mut Self {
        self.additional_threads = additional_threads;
        self
    }

    /// Creates a BAM writer from a file. If you want to use the same instance of
    /// [BamWriterBuilder](struct.BamWriterBuilder.html) again, you need to specify header again.
    ///
    /// Panics if the header was not specified.
    pub fn from_path<P: AsRef<Path>>(&mut self, path: P) -> Result<BamWriter<File>> {
        let stream = File::create(path)?;
        self.from_stream(stream)
    }

    /// Creates a BAM writer from stream. If you want to use the same instance of
    /// [BamWriterBuilder](struct.BamWriterBuilder.html) again, you need to specify header again.
    ///
    /// Panics if the header was not specified.
    pub fn from_stream<W: Write>(&mut self, stream: W) -> Result<BamWriter<W>> {
        let header = match std::mem::replace(&mut self.header, None) {
            None => panic!("Cannot construct BAM writer without a header"),
            Some(header) => header,
        };
        let mut writer = bgzip::Writer::build()
            .additional_threads(self.additional_threads)
            .compression_level(self.level)
            .from_stream(stream);
        if self.write_header {
            header.write_bam(&mut writer)?;
        }
        writer.flush()?;
        Ok(BamWriter { writer, header })
    }
}

pub struct BamWriter<W: Write> {
    writer: bgzip::Writer<W>,
    header: Header,
}

impl BamWriter<File> {
    /// Creates a [BamWriterBuilder](struct.BamWriterBuilder.html).
    pub fn build() -> BamWriterBuilder {
        BamWriterBuilder::new()
    }

    /// Creates a new `BamWriter` from a path and header.
    pub fn from_path<P: AsRef<Path>>(path: P, header: Header) -> Result<Self> {
        Self::build().header(header).from_path(path)
    }
}

impl<W: Write> BamWriter<W> {
    /// Creates a new `BamWriter` from a stream and header.
    pub fn from_stream(stream: W, header: Header) -> Result<Self> {
        BamWriter::build().header(header).from_stream(stream)
    }

    /// Returns BAM header.
    pub fn header(&self) -> &Header {
        &self.header
    }
}

impl<W: Write> RecordWriter for BamWriter<W> {
    fn write(&mut self, record: &Record) -> std::io::Result<()> {
        record.write_bam(&mut self.writer)?;
        self.writer.end_context();
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.finish()
    }
}
