//! BAM writer.

use std::io::{Write, BufWriter, Result};
use std::fs::File;
use std::path::Path;

use super::bgzip;
use super::{Header, Record, RecordWriter};

/// Builder of the [BamWriter](struct.BamWriter.html).
pub struct BamWriterBuilder {
    header: Option<Header>,
    write_header: bool,
    level: u8,
}

impl BamWriterBuilder {
    pub fn new() -> Self {
        Self {
            header: None,
            write_header: true,
            level: 6,
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
        assert!(level <= 9, "Compression level should be at most 9");
        self.level = level;
        self
    }

    /// Creates a BAM writer from a file. If you want to use the same instance of
    /// [BamWriterBuilder](struct.BamWriterBuilder.html) again, you need to specify header again.
    ///
    /// Panics if the header was not specified.
    pub fn from_path<P: AsRef<Path>>(&mut self, path: P) -> Result<BamWriter<BufWriter<File>>> {
        let stream = BufWriter::new(File::create(path)?);
        self.from_stream(stream)
    }

    /// Creates a BAM writer from stream. Preferably the stream should be wrapped
    /// in a buffer writer, such as `BufWriter`.
    ///
    /// If you want to use the same instance of
    /// [BamWriterBuilder](struct.BamWriterBuilder.html) again, you need to specify header again.
    ///
    /// Panics if the header was not specified.
    pub fn from_stream<W: Write>(&mut self, stream: W) -> Result<BamWriter<W>> {
        let header = match std::mem::replace(&mut self.header, None) {
            None => panic!("Cannot construct BAM writer without a header"),
            Some(header) => header,
        };
        let mut writer = bgzip::write::SentenceWriter::new(
            bgzip::write::Writer::from_stream(stream, self.level));
        if self.write_header {
            header.write_bam(&mut writer)?;
        }
        writer.flush()?;
        Ok(BamWriter { writer, header })
    }
}

pub struct BamWriter<W: Write> {
    writer: bgzip::write::SentenceWriter<W>,
    header: Header,
}

impl BamWriter<BufWriter<File>> {
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
    /// Creates a new `BamWriter` from a stream and header. Preferably the stream should be wrapped
    /// in a buffer writer, such as `BufWriter`.
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
        self.writer.end_sentence();
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.finish()
    }
}
