use std::io::{Write, BufWriter, Result, BufReader, BufRead};
use std::fs::File;
use std::path::Path;

use super::header::Header;
use super::record::Record;
use super::RecordReader;

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

/// Reads records from SAM format.
pub struct SamReader<R: BufRead> {
    stream: R,
    header: Header,
    buffer: String,
}

impl SamReader<BufReader<File>> {
    /// Opens SAM reader from `path`.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let stream = BufReader::new(File::open(path)?);
        SamReader::from_stream(stream)
    }
}

// TODO: Check empty SAM file.

impl<R: BufRead> SamReader<R> {
    /// Opens SAM reader from a buffered stream.
    pub fn from_stream(mut stream: R) -> Result<Self> {
        let mut header = Header::new();
        let mut buffer = String::new();
        loop {
            buffer.clear();
            if stream.read_line(&mut buffer)? == 0 {
                break;
            };
            if buffer.starts_with('@') {
                header.push_line(&buffer)?;
            } else {
                break;
            }
        }
        Ok(SamReader { stream, header, buffer })
    }

    /// Returns [header](../header/struct.Header.html).
    pub fn header(&self) -> &Header {
        &self.header
    }
}
