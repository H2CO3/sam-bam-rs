use std::fs::File;
use std::io::{Read, Seek, Result, Error, BufReader, Write};
use std::io::ErrorKind::InvalidData;
use std::path::Path;
use std::result;

use byteorder::{LittleEndian, ReadBytesExt};

use super::index::Index;
use super::record;
use super::bgzip;

#[derive(Clone)]
pub struct Header {
    text: Vec<u8>,
    references: Vec<String>,
    lengths: Vec<i32>,
}

impl Header {
    pub fn from_stream<R: Read>(stream: &mut R) -> Result<Header> {
        let mut magic = [0_u8; 4];
        stream.read_exact(&mut magic)?;
        if magic != [b'B', b'A', b'M', 1] {
            return Err(Error::new(InvalidData, "Input is not in BAM format"));
        }

        let l_text = stream.read_i32::<LittleEndian>()?;
        if l_text < 0 {
            return Err(Error::new(InvalidData, "BAM file corrupted: negative header length"));
        }
        let mut text = vec![0_u8; l_text as usize];
        stream.read_exact(&mut text)?;
        let mut res = Header {
            text,
            references: Vec::new(),
            lengths: Vec::new(),
        };

        let n_refs = stream.read_i32::<LittleEndian>()?;
        if n_refs < 0 {
            return Err(Error::new(InvalidData, "BAM file corrupted: negative number of references"));
        }
        for _ in 0..n_refs {
            let l_name = stream.read_i32::<LittleEndian>()?;
            if l_name <= 0 {
                return Err(Error::new(InvalidData,
                    "BAM file corrupted: negative reference name length"));
            }
            let mut name = vec![0_u8; l_name as usize - 1];
            stream.read_exact(&mut name)?;
            let _null = stream.read_u8()?;
            let name = std::string::String::from_utf8(name)
                .map_err(|_| Error::new(InvalidData,
                "BAM file corrupted: reference name not in UTF-8"))?;

            let l_ref = stream.read_i32::<LittleEndian>()?;
            if l_ref < 0 {
                return Err(Error::new(InvalidData,
                    "BAM file corrupted: negative reference length"));
            }
            res.references.push(name);
            res.lengths.push(l_ref);
        }
        Ok(res)
    }

    pub fn n_references(&self) -> usize {
        self.references.len()
    }

    pub fn reference_name(&self, ref_id: usize) -> Option<&str> {
        if ref_id > self.references.len() {
            None
        } else {
            Some(&self.references[ref_id])
        }
    }

    pub fn reference_len(&self, ref_id: usize) -> Option<i32> {
        if ref_id > self.lengths.len() {
            None
        } else {
            Some(self.lengths[ref_id])
        }
    }
}

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
        let mut record = record::Record::new();
        match self.region_viewer.read_into(&mut record) {
            Ok(()) => Some(Ok(record)),
            Err(record::Error::IoError(e)) => Some(Err(e)),
            Err(record::Error::NoMoreReads) => None,
        }
    }
}

pub struct IndexedReader<R: Read + Seek> {
    reader: bgzip::SeekReader<R>,
    header: Header,
    index: Index,
}

impl IndexedReader<File> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let reader = bgzip::SeekReader::from_path(&path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAM file: {}", e)))?;
        let index = Index::from_path(format!("{}.bai", path.display()))
            .map_err(|e| Error::new(e.kind(), format!("Failed to open BAI index: {}", e)))?;
        IndexedReader::new(reader, index)
    }
}

impl<R: Read + Seek> IndexedReader<R> {
    pub fn new(mut reader: bgzip::SeekReader<R>, index: Index) -> Result<Self> {
        let header = {
            let mut header_reader = BufReader::new(
                bgzip::ChunksReader::without_boundaries(&mut reader));
            Header::from_stream(&mut header_reader)?
        };
        Ok(Self { reader, header, index })
    }

    pub fn fetch<'a>(&'a mut self, ref_id: i32, start: i32, end: i32) -> RegionViewer<'a, R> {
        let chunks = self.index.fetch_chunks(ref_id, start, end);
        RegionViewer {
            chunks_reader: BufReader::new(bgzip::ChunksReader::new(&mut self.reader, chunks)),
            start, end,
        }
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn write_record_as_sam<W: Write>(&self, writer: &mut W, record: &record::Record)
            -> Result<()> {
        record.write_sam(writer, self.header())
    }
}
