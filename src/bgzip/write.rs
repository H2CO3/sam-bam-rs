//! Bgzip files (BGZF) writers.
//!
//! This module will change in future versions to support multi-thread writing.
//!
//! # Writers
//!
//! * [Writer](struct.Writer.html) - writes whole bgzip blocks.
//! * [Sentence Writer](struct.SentenceWriter.html) - wrapper around the
//! [Writer](struct.Writer.html), that implements the `Write` trait. In addition, it allows
//! to *end sentences* - marks points, in which it is preferable to break the stream and start a
//! new bgzip block. For example, each BAM record represents a separate sentence, and
//! `SentenceWriter` will try not to split a record between two blocks.

use std::fs::File;
use std::io::{self, Write, ErrorKind};
use std::path::Path;
use std::cmp::min;

use byteorder::{LittleEndian, WriteBytesExt};
use flate2::Compression;
use flate2::write::DeflateEncoder;

pub const MAX_BLOCK_SIZE: usize = 65536;
pub const COMPRESSED_BLOCK_SIZE: usize = MAX_BLOCK_SIZE - 26;

/// Bgzip writer, that allows to compress and write blocks with uncompressed
/// size at most `MAX_BLOCK_SIZE = 65536`.
pub struct Writer<W: Write> {
    stream: W,
    compressed_buffer: Vec<u8>,
    compression: Compression,
}

impl Writer<File> {
    /// Opens a bgzip writer from a path and compression level. Maximal compression level is 9.
    pub fn from_path<P: AsRef<Path>>(path: P, level: u8) -> io::Result<Self> {
        let stream = File::create(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open bgzip writer: {}", e)))?;
        Ok(Writer::from_stream(stream, level))
    }
}

impl<W: Write> Writer<W> {
    /// Opens a bgzip writer from a stream and compression level. Maximal compression level is 9.
    pub fn from_stream(stream: W, level: u8) -> Self {
        assert!(level <= 9, "Compression level should be at most 9");
        Writer {
            stream,
            compressed_buffer: vec![0; COMPRESSED_BLOCK_SIZE],
            compression: Compression::new(level as u32),
        }
    }

    /// Writes an empty block.
    pub fn write_empty(&mut self) -> io::Result<()> {
        const EMPTY_BLOCK: &[u8; 28] = &[0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00];
        self.stream.write_all(EMPTY_BLOCK)?;
        Ok(())
    }

    /// Compresses all slices in the `contents` and writes as a single block.
    ///
    /// Sum length of the `contents` should be at most `MAX_BLOCK_SIZE = 65536`.
    ///
    /// If the compressed block is bigger than `MAX_BLOCK_SIZE`, the function returns an error
    /// `WriteZero`.
    pub fn write_several(&mut self, contents: &[&[u8]]) -> io::Result<()> {
        let contents_size: usize = contents.iter().map(|slice| slice.len()).sum();
        if contents_size == 0 {
            return self.write_empty();
        }
        assert!(contents_size <= MAX_BLOCK_SIZE, "Cannot write a block: uncompressed size {} > {}",
            contents_size, MAX_BLOCK_SIZE);

        let mut crc_hasher = crc32fast::Hasher::new();
        let bytes_written = {
            let mut encoder = DeflateEncoder::new(&mut self.compressed_buffer[..], self.compression);
            for subcontents in contents.iter() {
                encoder.write_all(subcontents)?;
                crc_hasher.update(subcontents);
            }
            let remaining_buf = encoder.finish()?;
            CONTENTS_SIZE - remaining_buf.len()
        };

        const BLOCK_HEADER: &[u8; 16] = &[
             31, 139,   8,   4,  // ID1, ID2, Compression method, Flags
              0,   0,   0,   0,  // Modification time
              0, 255,   6,   0,  // Extra flags, OS (255 = unknown), extra length (2 bytes)
             66,  67,   2,   0]; // SI1, SI2, subfield len (2 bytes)
        self.stream.write_all(BLOCK_HEADER)?;
        let block_size = bytes_written + 26;
        self.stream.write_u16::<LittleEndian>((block_size - 1) as u16)?;

        let compressed_data = &self.compressed_buffer[..bytes_written];
        self.stream.write_all(compressed_data)?;
        self.stream.write_u32::<LittleEndian>(crc_hasher.finalize())?;
        self.stream.write_u32::<LittleEndian>(contents_size as u32)?;
        Ok(())
    }

    /// Compresses `contents` and writes as a single block.
    ///
    /// Input `contents` size should be at most `MAX_BLOCK_SIZE = 65536`.
    ///
    /// If the compressed block is bigger than `MAX_BLOCK_SIZE`, the function returns an error
    /// `WriteZero`.
    pub fn write(&mut self, contents: &[u8]) -> io::Result<()> {
        self.write_several(&[contents])
    }

    /// Flushes inner stream.
    pub fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

const CONTENTS_SIZE: usize = MAX_BLOCK_SIZE + 1;

/// A struct that allows to write bgzip files in *sentences*.
///
/// It implements a `Write` trait, and works similar to a `BufWriter`, but has a method
/// [end_sentence](#method.end_sentence),
/// that indicates the end of a block of the same nature (for example, a BAM record can represent
/// a single sentence). Method `flush` ignores sentences and immediately writes
/// the remaining buffer.
///
/// The `SentenceWriter` will try to start new bgzip blocks only when a new sentence starts.
/// The writer still puts several sentences in the same bgzip block, if possible.
pub struct SentenceWriter<W: Write> {
    writer: Writer<W>,
    contents: Vec<u8>,
    start: usize,
    position: usize,
    end: Option<usize>,
    panicked: bool,
    finished: bool,
}

impl<W: Write> SentenceWriter<W> {
    pub fn new(writer: Writer<W>) -> Self {
        Self {
            writer,
            contents: vec![0; CONTENTS_SIZE],
            start: 0,
            position: 0,
            end: None,
            panicked: false,
            finished: false,
        }
    }

    /// Ends the current sentence.
    pub fn end_sentence(&mut self) {
        self.end = Some(self.position);
    }

    /// Try to write a bgzip block from contents `[self.start - end)`.
    fn try_write(&mut self, end: usize) -> io::Result<()> {
        self.finished = false;
        self.panicked = true;
        if self.start <= end {
            self.writer.write(&self.contents[self.start..end])?
        } else {
            self.writer.write_several(&[&self.contents[self.start..], &self.contents[..end]])?
        };
        self.start = end % CONTENTS_SIZE;
        self.panicked = false;
        Ok(())
    }

    fn get_len(&self, end: usize) -> usize {
        if self.start <= end {
            end - self.start
        } else {
            CONTENTS_SIZE - self.start + end
        }
    }

    /// Try different ends to write a bgzip block.
    fn write_bgzip(&mut self) -> io::Result<()> {
        let mut write_len = self.get_len(self.position);
        if let Some(end) = self.end {
            let end_len = self.get_len(end);
            if end_len >= write_len / 2 {
                // Block until sentence end is not too small.
                write_len = end_len;
            }
        }
        self.end = None;

        loop {
            assert!(write_len != 0);
            match self.try_write((self.start + write_len - 1) % CONTENTS_SIZE + 1) {
                Ok(()) => return Ok(()),
                Err(ref e) if e.kind() == ErrorKind::WriteZero => {},
                Err(e) => return Err(e),
            }
            const DECREASE_BY: usize = 2000;
            if write_len <= DECREASE_BY {
                return Err(io::Error::new(ErrorKind::Other,
                    format!("Compressed size is too big. Last attempt to compress {} bytes failed",
                    write_len)));
            }
            write_len -= DECREASE_BY;
        }
    }

    /// Flushes all the remaining contents to bgzip and writes an empty block.
    pub fn finish(&mut self) -> io::Result<()> {
        self.flush()?;
        if !self.finished {
            self.writer.write_empty()?;
            self.writer.flush()?;
            self.finished = true;
        }
        Ok(())
    }
}

impl<W: Write> Write for SentenceWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut remains = CONTENTS_SIZE - 1 - self.get_len(self.position);
        if remains == 0 {
            self.write_bgzip()?;
            remains = CONTENTS_SIZE - 1 - self.get_len(self.position);
        };
        let write_bytes = min(buf.len(), remains);
        if self.position + write_bytes > CONTENTS_SIZE {
            let split = CONTENTS_SIZE - self.position;
            self.contents[self.position..].copy_from_slice(&buf[..split]);
            self.position = write_bytes - split;
            self.contents[..self.position].copy_from_slice(&buf[split..write_bytes]);
        } else {
            self.contents[self.position..self.position + write_bytes]
                .copy_from_slice(&buf[..write_bytes]);
            self.position = (self.position + write_bytes) % CONTENTS_SIZE;
        }
        Ok(write_bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.end = None;
        while self.start != self.position {
            self.write_bgzip()?;
        }
        self.writer.flush()
    }
}

impl<W: Write> Drop for SentenceWriter<W> {
    fn drop(&mut self) {
        if !self.panicked {
            let _ignore = self.finish();
        }
    }
}
