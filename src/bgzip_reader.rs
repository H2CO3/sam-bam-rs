use std::sync::{Arc, Mutex, RwLock};
use std::collections::VecDeque;
use std::io::{self, Read, Write, ErrorKind};
use std::thread;
use std::time::Duration;
use std::fmt::{self, Display, Debug, Formatter};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use flate2::write::DeflateDecoder;

struct ObjectPool<T> {
    objects: Vec<T>,
    constructor: Box<dyn Fn() -> T>,
}

impl<T> ObjectPool<T> {
    fn new<F: 'static + Fn() -> T>(constructor: F) -> Self {
        Self {
            objects: vec![],
            constructor: Box::new(constructor),
        }
    }

    fn take(&mut self) -> T {
        match self.objects.pop() {
            Some(object) => object,
            None => (self.constructor)(),
        }
    }

    fn bring(&mut self, object: T) {
        self.objects.push(object);
    }
}

/// Biggest possible compressed and uncompressed size
pub const MAX_BLOCK_SIZE: usize = 65536;

/// io::Error produced while reading a bgzip block.
///
/// # Variants
///
/// * `EndOfFile` - the stream ended before the beginning of the block. This error does not
/// appear if part of the block was read before the end of the stream.
/// * `Corrupted(s)` - the block has incorrect header or contents.
/// `s` contains additional information about the problem.
/// * `IoError(e)` - the stream raised `io::Error`.
pub enum BlockError {
    EndOfFile,
    Corrupted(String),
    IoError(io::Error),
}

impl From<io::Error> for BlockError {
    fn from(e: io::Error) -> BlockError {
        BlockError::IoError(e)
    }
}

impl Into<io::Error> for BlockError {
    fn into(self) -> io::Error {
        use BlockError::*;
        match self {
            EndOfFile => io::Error::new(ErrorKind::UnexpectedEof,
                "EOF: Failed to read bgzip block"),
            Corrupted(s) => io::Error::new(ErrorKind::InvalidData,
                format!("Corrupted bgzip block: {}", s)),
            IoError(e) => e,
        }
    }
}

impl Display for BlockError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        use BlockError::*;
        match self {
            EndOfFile => write!(f, "EOF: Failed to read bgzip block"),
            Corrupted(s) => write!(f, "Corrupted bgzip block: {}", s),
            IoError(e) => write!(f, "{}", e),
        }
    }
}

impl Debug for BlockError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        Display::fmt(self, f)
    }
}

fn as_u16(buffer: &[u8], start: usize) -> u16 {
    buffer[start] as u16 + ((buffer[start + 1] as u16) << 8)
}

/// Bgzip block. Both uncompressed and compressed size should not be bigger than
/// `MAX_BLOCK_SIZE = 65536`.
pub struct Block {
    // Compressed BGZF block
    block: Vec<u8>,
    // Decompressed contents
    contents: Vec<u8>,
    extra_len: u16,
}

impl Block {
    fn new() -> Block {
        Block {
            block: Vec::with_capacity(MAX_BLOCK_SIZE),
            contents: Vec::with_capacity(MAX_BLOCK_SIZE),
            extra_len: 0,
        }
    }

    /// Fills the block, but does not decompress its contents.
    fn fill<R: Read>(&mut self, stream: &mut R) -> Result<(), BlockError> {
        unsafe {
            self.block.set_len(MAX_BLOCK_SIZE);
        }
        self.contents.clear();

        self.extra_len = {
            let header = &mut self.block[..12];
            match stream.read_exact(header) {
                Ok(()) => {},
                Err(e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        return Err(BlockError::EndOfFile);
                    } else {
                        return Err(BlockError::from(e));
                    }
                }
            }
            // TODO: Try read header and extra fields simultaniously
            Block::analyze_header(header)?
        };
        let block_size = {
            let extra_fields = &mut self.block[12..12 + self.extra_len as usize];
            stream.read_exact(extra_fields)?;
            Block::analyze_extra_fields(extra_fields)? as usize + 1
        };
        self.block.truncate(block_size);
        stream.read_exact(&mut self.block[12 + self.extra_len as usize..])?;
        Ok(())
    }

    fn decompress(&mut self) -> Result<(), BlockError> {
        let block_size = self.block.len();
        let exp_contents_size = (&self.block[block_size - 4..block_size])
            .read_u32::<LittleEndian>()?;
        if exp_contents_size as usize > MAX_BLOCK_SIZE {
            return Err(BlockError::Corrupted(format!("Expected contents > MAX_BLOCK_SIZE ({} > {})",
                exp_contents_size, MAX_BLOCK_SIZE)));
        }
        unsafe {
            self.contents.set_len(exp_contents_size as usize);
        }
        let contents_size = {
            let mut decoder = DeflateDecoder::new(&mut self.contents[..]);
            decoder.write_all(&self.block[12 + self.extra_len as usize..block_size - 8])
                .map_err(|e| BlockError::Corrupted(
                    format!("Could not decompress block contents: {:?}", e)))?;
            let remaining_contents = decoder.finish()
                .map_err(|e| BlockError::Corrupted(
                    format!("Could not decompress block contents: {:?}", e)))?;
            MAX_BLOCK_SIZE - remaining_contents.len()
        };
        if exp_contents_size as usize != contents_size {
            return Err(BlockError::Corrupted(
                format!("Uncompressed sizes do not match: expected {}, observed {}",
                exp_contents_size, contents_size)));
        }

        let exp_crc32 = (&self.block[block_size - 8..block_size - 4]).read_u32::<LittleEndian>()?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.contents);
        let obs_crc32 = hasher.finalize();
        if obs_crc32 != exp_crc32 {
            return Err(BlockError::Corrupted(
                format!("CRC do not match: expected {}, observed {}", exp_crc32, obs_crc32)));
        }
        Ok(())
    }

    /// Analyzes 12 heades bytes of a block.
    /// Returns XLEN - total length of extra subfields.
    fn analyze_header(header: &[u8]) -> Result<u16, BlockError> {
        if header[0] != 31 || header[1] != 139 || header[2] != 8 || header[3] != 4 {
            return Err(BlockError::Corrupted("bgzip block has an invalid header".to_string()));
        }
        Ok(as_u16(header, 10))
    }

    /// Analyzes extra fields following the header.
    /// Returns BSIZE - total block size - 1.
    fn analyze_extra_fields(extra_fields: &[u8]) -> Result<u16, BlockError> {
        let mut i = 0;
        while i + 3 < extra_fields.len() {
            let subfield_id1 = extra_fields[i];
            let subfield_id2 = extra_fields[i + 1];
            let subfield_len = as_u16(extra_fields, i + 2);
            if subfield_id1 == 66 && subfield_id2 == 67 && subfield_len == 2 {
                if subfield_len != 2 || i + 5 >= extra_fields.len() {
                    return Err(BlockError::Corrupted("bgzip block has an invalid header"
                        .to_string()));
                }
                return Ok(as_u16(extra_fields, i + 4));
            }
            i += 4 + subfield_len as usize;
        }
        Err(BlockError::Corrupted("bgzip block has an invalid header".to_string()))
    }

    /// Return the uncompressed contents.
    pub fn contents(&self) -> &[u8] {
        &self.contents
    }

    /// Return the size of the uncompressed data (same as `contents().len()`)
    pub fn contents_size(&self) -> usize {
        self.contents.len()
    }

    /// Return the block size (size of the compressed data).
    pub fn block_size(&self) -> usize {
        self.block.len()
    }
}

const WORKER_SLEEP: Duration = Duration::from_millis(100);

#[derive(PartialEq, Eq, Clone, Copy)]
struct WorkerId(u16);

struct Worker {
    worker_id: WorkerId,
    working_queue: Arc<Mutex<WorkingQueue>>,
    is_finished: Arc<RwLock<bool>>,
}

impl Worker {
    fn run(&mut self) {
        while !self.is_finished.read().map(|guard| *guard).unwrap_or(true) {
            let mut block = if let Ok(mut guard) = self.working_queue.lock() {
                if let Some(block) = guard.blocks.pop_front() {
                    guard.tasks.push_back(Task::NotReady(self.worker_id));
                    block
                } else {
                    thread::sleep(WORKER_SLEEP);
                    continue;
                }
            } else {
                // Panic in another thread
                break
            };

            let res = block.decompress().map(|_| block);
            if let Ok(mut guard) = self.working_queue.lock() {
                for task in guard.tasks.iter_mut().rev() {
                    if let Task::NotReady(worker_id) = task {
                        if *worker_id == self.worker_id {
                            std::mem::replace(task, Task::Ready(res));
                            break;
                        }
                    }
                }
                panic!("Task handler not found for worker {}", self.worker_id.0);
            } else {
                // Panic in another thread
                break
            };
        }
    }
}

enum Task {
    NotReady(WorkerId),
    Ready(Result<Block, BlockError>),
}

#[derive(Default)]
struct WorkingQueue {
    blocks: VecDeque<Block>,
    tasks: VecDeque<Task>,
}

struct MultiThreadReader<R: Read> {
    working_queue: Arc<Mutex<WorkingQueue>>,
    is_finished: Arc<RwLock<bool>>,
    blocks_pool: ObjectPool<Block>,
    workers: Vec<thread::JoinHandle<()>>,
    stream: R,
}

impl<R: Read> MultiThreadReader<R> {
    pub fn from_stream(stream: R, threads: u16) -> Self {
        assert!(threads > 0);
        let working_queue = Arc::new(Mutex::new(WorkingQueue::default()));
        let is_finished = Arc::new(RwLock::new(false));
        let workers = (0..threads).map(|i| {
            let mut worker = Worker {
                worker_id: WorkerId(i),
                working_queue: Arc::clone(&working_queue),
                is_finished: Arc::clone(&is_finished),
            };
            thread::Builder::new()
                .name(format!("worker{}", i))
                .spawn(move || worker.run())
                .expect("Cannot create a thread")
        }).collect();

        Self {
            working_queue,
            is_finished,
            blocks_pool: ObjectPool::new(|| Block::new()),
            workers,
            stream,
        }
    }

    /// Joins workers after they finish their current job.
    pub fn join(self) -> thread::Result<()> {
        *self.is_finished.write()
            .map_err(|e| Box::new(e.to_string()) as Box<std::any::Any + Send>)? = true;
        for worker in self.workers {
            worker.join()?;
        }
        Ok(())
    }
}
