use std::fs::File;
use std::io::{Read, Seek, Result, Error, BufReader};
use std::path::Path;
use std::result;

use super::index::Index;
use super::record;
use super::bgzip;

pub struct RegionViewer<'a, R: Read + Seek> {
    chunks_reader: BufReader<bgzip::ChunksReader<'a, R>>,
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
        println!("Calling next");
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
        println!("Loaded reader and index");
        println!("Index: {}\n\n", index);
        Ok(IndexedReader::new(reader, index))
    }
}

impl<R: Read + Seek> IndexedReader<R> {
    pub fn new(reader: bgzip::Reader<R>, index: Index) -> Self {
        Self { reader, index }
    }

    pub fn fetch<'a>(&'a mut self, ref_id: i32, start: i32, end: i32) -> RegionViewer<'a, R> {
        let chunks = self.index.fetch_chunks(ref_id, start, end);
        println!("\n\n\nFetch chunks: {:?}", chunks);
        RegionViewer {
            chunks_reader: BufReader::new(bgzip::ChunksReader::new(&mut self.reader, chunks)),
            start, end,
        }
    }
}
