use std::fs::File;
use std::io::{Read, Seek, Result, Error};
use std::path::Path;
use std::cmp::min;
use std::result;

use super::index::{Chunk, Index};
use super::record;
use super::bgzip;

struct ChunksReader<'a, R: Read + Seek> {
    reader: &'a mut bgzip::Reader<R>,
    chunks: Vec<Chunk>,
    chunk_ix: usize,
    block_offset: u64,
    in_block_offset: usize,
}

impl<'a, R: Read + Seek> ChunksReader<'a, R> {
    fn new(reader: &'a mut bgzip::Reader<R>, chunks: Vec<Chunk>) -> Self {
        let (block_offset, in_block_offset) = if chunks.len() > 0 {
            (chunks[0].start().compr_offset(), chunks[0].start().uncompr_offset() as usize)
        } else {
            (0, 0)
        };
        ChunksReader {
            reader, chunks,
            chunk_ix: 0,
            block_offset, in_block_offset,
        }
    }
}

impl<'a, R: Read + Seek> Read for ChunksReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.chunk_ix >= self.chunks.len() {
            return Ok(0);
        }

        let chunk = &self.chunks[self.chunk_ix];
        let block = self.reader.get_block(self.block_offset)?;

        let mut bytes = if chunk.end().compr_offset() == self.block_offset {
            // Last block in a chunk
            chunk.end().uncompr_offset() as usize - self.in_block_offset
        } else {
            block.uncompressed_size() - self.in_block_offset
        };
        bytes = min(bytes, buf.len());
        buf.copy_from_slice(block.contents(self.in_block_offset, self.in_block_offset + bytes));
        
        self.in_block_offset += bytes;
        if chunk.end().compr_offset() == self.block_offset
                && self.in_block_offset == chunk.end().uncompr_offset() as usize {
            // Last block in a chunk
            self.chunk_ix += 1;
        } else if block.uncompressed_size() == self.in_block_offset {
            self.block_offset += block.compressed_size() as u64;
        }
        Ok(bytes)
    }
}

pub struct RegionViewer<'a, R: Read + Seek> {
    chunks_reader: ChunksReader<'a, R>,
    start: i32,
    end: i32,
}

impl<'a, R: Read + Seek> RegionViewer<'a, R> {
    pub fn read_into(&mut self, record: &mut record::Record) -> result::Result<(), record::Error> {
        record.fill_from(&mut self.chunks_reader)
    }

    pub fn records<'b>(&'b mut self) -> RecordsIter<'b, 'a, R> {
        RecordsIter {
            region_viewer: self,
        }
    }
}

pub struct RecordsIter<'b, 'a: 'b, R: Read + Seek> {
    region_viewer: &'b mut RegionViewer<'a, R>,
}

impl<'b, 'a: 'b, R: Read + Seek> Iterator for RecordsIter<'b, 'a, R> {
    type Item = Result<record::Record>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = record::Record::new();
        match self.region_viewer.read_into(&mut record) {
            Ok(()) => Some(Ok(record)),
            Err(record::Error::IoError(e)) => Some(Err(e)),
            Err(record::Error::NoMoreReads) => None,
        }
    }
}

pub struct IndexedReader<R: Read + Seek> {
    reader: bgzip::Reader<R>,
    index: Index,
}

impl IndexedReader<File> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let reader = bgzip::Reader::from_path(&path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAM file: {}", e)))?;
        let index = Index::from_path(format!("{}.bai", path.display()))
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAI index: {}", e)))?;
        Ok(IndexedReader::new(reader, index))
    }
}

impl<R: Read + Seek> IndexedReader<R> {
    pub fn new(reader: bgzip::Reader<R>, index: Index) -> Self {
        Self { reader, index }
    }

    pub fn fetch<'a>(&'a mut self, ref_id: i32, start: i32, end: i32) -> RegionViewer<'a, R> {
        let chunks = self.index.fetch_chunks(ref_id, start, end);
        RegionViewer {
            chunks_reader: ChunksReader::new(&mut self.reader, chunks),
            start, end,
        }
    }
}
