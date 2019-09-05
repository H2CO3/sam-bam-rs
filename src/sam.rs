use std::io::{Write, BufWriter, Result};
use std::fs::File;
use std::path::Path;

use super::header::Header;
use super::record::Record;

/// Writes records in SAM format.
pub struct SamWriter<W: Write> {
    stream: W,
    header: Header,
}

impl SamWriter<BufWriter<File>> {
    /// Creates a SAM writer from a path and a header.
    pub fn from_path<P: AsRef<Path>>(path: P, header: Header) -> Result<Self> {
        let stream = BufWriter::new(File::create(path)?);
        SamWriter::from_stream(stream, header)
    }
}

impl<W: Write> SamWriter<W> {
    /// Creates a SAM writer from a stream and a header. Preferably the stream should be wrapped
    /// in a buffer writer, such as `BufWriter`.
    pub fn from_stream(mut stream: W, header: Header) -> Result<Self> {
        header.write_text(&mut stream)?;
        Ok(SamWriter { stream, header })
    }

    /// Returns [header](../header/struct.Header.html).
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Writes a single record in SAM format.
    pub fn write(&mut self, record: &Record) -> Result<()> {
        record.write_sam(&mut self.stream, &self.header)
    }
}