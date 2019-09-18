use std::io::{Write, BufWriter, Result, BufReader, BufRead};
use std::fs::File;
use std::path::Path;
use std::result;

use super::header::Header;
use super::record::{Record, Error};
use super::{RecordReader, RecordWriter};

/// Builder of the [SamWriter](struct.SamWriter.html).
pub struct SamWriterBuilder {
    header: Option<Header>,
    skip_header: bool,
}

impl SamWriterBuilder {
    pub fn new() -> Self {
        Self {
            header: None,
            skip_header: false,
        }
    }

    /// Specifies SAM header.
    pub fn header(&mut self, header: Header) -> &mut Self {
        self.header = Some(header);
        self
    }

    /// Option to skip header when creating the SAM writer (false by default).
    pub fn skip_header(&mut self, skip: bool) -> &mut Self {
        self.skip_header = skip;
        self
    }

    /// Creates a SAM writer from a file. If you want to use the same instance of
    /// [SamWriterBuilder](struct.SamWriterBuilder.html) again, you need to specify header again.
    ///
    /// Panics if the header was not specified.
    pub fn from_path<P: AsRef<Path>>(&mut self, path: P) -> Result<SamWriter<BufWriter<File>>> {
        let stream = BufWriter::new(File::create(path)?);
        self.from_stream(stream)
    }

    /// Creates a SAM writer from stream. Preferably the stream should be wrapped
    /// in a buffer writer, such as `BufWriter`.
    ///
    /// If you want to use the same instance of
    /// [SamWriterBuilder](struct.SamWriterBuilder.html) again, you need to specify header again.
    ///
    /// Panics if the header was not specified.
    pub fn from_stream<W: Write>(&mut self, mut stream: W) -> Result<SamWriter<W>> {
        let header = match std::mem::replace(&mut self.header, None) {
            None => panic!("Cannot construct SAM writer without a header"),
            Some(header) => header,
        };
        if !self.skip_header {
            header.write_text(&mut stream)?;
        }
        Ok(SamWriter { stream, header })
    }
}

/// Writes records in SAM format.
pub struct SamWriter<W: Write> {
    stream: W,
    header: Header,
}

impl SamWriter<BufWriter<File>> {
    /// Create a [builder](struct.SamWriterBuilder.html).
    pub fn build() -> SamWriterBuilder {
        SamWriterBuilder::new()
    }

    /// Creates a SAM writer from a path and a header.
    pub fn from_path<P: AsRef<Path>>(path: P, header: Header) -> Result<Self> {
        SamWriterBuilder::new().header(header).from_path(path)
    }
}

impl<W: Write> SamWriter<W> {
    /// Creates a SAM writer from a stream and a header. Preferably the stream should be wrapped
    /// in a buffer writer, such as `BufWriter`.
    pub fn from_stream(stream: W, header: Header) -> Result<Self> {
        SamWriterBuilder::new().header(header).from_stream(stream)
    }

    /// Returns [header](../header/struct.Header.html).
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Flushes contents to output.
    pub fn flush(&mut self) -> Result<()> {
        self.stream.flush()
    }
}

impl<W: Write> RecordWriter for SamWriter<W> {
    /// Writes a single record in SAM format.
    fn write(&mut self, record: &Record) -> Result<()> {
        record.write_sam(&mut self.stream, &self.header)
    }

    fn finish(&mut self) -> Result<()> {
        self.flush()
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
                header.push_line(buffer.trim_end())?;
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

impl<R: BufRead> RecordReader for SamReader<R> {
    fn read_into(&mut self, record: &mut Record) -> result::Result<(), Error> {
        if self.buffer.is_empty() {
            return Err(Error::NoMoreRecords);
        }
        let res = match record.fill_from_sam(self.buffer.trim(), &self.header) {
            Ok(()) => Ok(()),
            Err(e) => {
                record.clear();
                Err(e)
            },
        };
        self.buffer.clear();
        match self.stream.read_line(&mut self.buffer) {
            Ok(_) => res,
            Err(e) => res.or(Err(Error::Truncated(e))),
        }
    }
}

/// Iterator over records.
///
/// # Errors
///
/// If the record was corrupted, the function returns
/// [Corrupted](../record/enum.Error.html#variant.Corrupted) error.
/// If the record was truncated or the reading failed for a different reason, the function
/// returns [Truncated](../record/enum.Error.html#variant.Truncated) error.
impl<R: BufRead> Iterator for SamReader<R> {
    type Item = result::Result<Record, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = Record::new();
        match self.read_into(&mut record) {
            Ok(()) => Some(Ok(record)),
            Err(Error::NoMoreRecords) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
