use std::sync::{Arc, Mutex, RwLock};
use std::collections::VecDeque;
use std::io::{self, Read, Write, ErrorKind, Seek, SeekFrom};
use std::thread;
use std::time::Duration;
use std::fmt::{self, Display, Debug, Formatter};

use byteorder::{LittleEndian, ReadBytesExt};
use flate2::write::DeflateDecoder;

use super::index::{Chunk, VirtualOffset};

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
/// * `EndOfStream` - no blocks read because the stream ended.
/// * `Corrupted(s)` - the block has incorrect header or contents.
/// `s` contains additional information about the problem.
/// * `IoError(e)` - the stream raised `io::Error`.
pub enum BlockError {
    EndOfStream,
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
            EndOfStream => io::Error::new(ErrorKind::UnexpectedEof,
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
            EndOfStream => write!(f, "EOF: Failed to read bgzip block"),
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
    offset: u64,
}

impl Block {
    fn new() -> Block {
        Block {
            block: Vec::with_capacity(MAX_BLOCK_SIZE),
            contents: Vec::with_capacity(MAX_BLOCK_SIZE),
            extra_len: 0,
            offset: 0,
        }
    }

    /// Fills the block, but does not decompress its contents.
    fn fill<R: Read>(&mut self, new_offset: u64, stream: &mut R) -> Result<(), BlockError> {
        self.offset = new_offset;
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
                        return Err(BlockError::EndOfStream);
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

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

const SLEEP_TIME: Duration = Duration::from_millis(100);

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
            let block = if let Ok(mut guard) = self.working_queue.lock() {
                if let Some(block) = guard.blocks.pop_front() {
                    guard.tasks.push_back(Task::NotReady((self.worker_id, TaskStatus::Waiting)));
                    Some(block)
                } else {
                    None
                }
            } else {
                // Panic in another thread
                break
            };

            let mut block = if let Some(value) = block {
                value
            } else {
                thread::sleep(SLEEP_TIME);
                continue;
            };

            let res = block.decompress();
            if let Ok(mut guard) = self.working_queue.lock() {
                for task in guard.tasks.iter_mut().rev() {
                    match task {
                        Task::NotReady((worker_id, task_status))
                                if *worker_id == self.worker_id => {
                            let new_value = if *task_status == TaskStatus::Waiting {
                                Task::Ready((block, res))
                            } else {
                                Task::Interrupted(block)
                            };
                            std::mem::replace(task, new_value);
                            break;
                        },
                        _ => {},
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskStatus {
    Waiting,
    Interrupted,
}

enum Task {
    Ready((Block, Result<(), BlockError>)),
    // Second element = true if the block is no longer needed.
    NotReady((WorkerId, TaskStatus)),
    Interrupted(Block),
}

#[doc(hidden)]
pub trait ReadingMode {
    /// `next_offset` - is a function that returns the next offset given a current offset, and None,
    /// if the reader shlould stop.
    fn next_offset(&mut self, offset: u64) -> Option<u64>;
}

pub struct Consecutive {}

impl ReadingMode for Consecutive {
    fn next_offset(&mut self, offset: u64) -> Option<u64> {
        Some(offset)
    }
}

pub struct Bouncing {
    chunks: Vec<Chunk>,
    index: usize,
    started: bool,
}

impl Bouncing {
    fn new() -> Self {
        Self {
            chunks: vec![],
            index: 0,
            started: false,
        }
    }

    fn set_chunks<I: IntoIterator<Item = Chunk>>(&mut self, chunks: I) {
        self.chunks.clear();
        self.chunks.extend(chunks);
        self.index = 0;
        self.started = false;
    }
}

impl ReadingMode for Bouncing {
    fn next_offset(&mut self, offset: u64) -> Option<u64> {
        if self.index >= self.chunks.len() {
            return None;
        }
        if !self.started {
            self.started = true;
            return Some(self.chunks[0].start().block_offset());
        }

        if VirtualOffset::new(offset, 0) >= self.chunks[self.index].end() {
            self.index += 1;
            if self.index >= self.chunks.len() {
                None
            } else {
                Some(self.chunks[self.index].start().block_offset())
            }
        } else {
            Some(offset)
        }
    }
}

#[doc(hidden)]
pub trait DecompressBlock {
    fn next_block<R: ReadBlockInto>(&mut self, reader: &mut R) -> Result<&Block, BlockError>;
}

#[derive(Default)]
struct WorkingQueue {
    blocks: VecDeque<Block>,
    tasks: VecDeque<Task>,
}

pub struct MultiThread {
    working_queue: Arc<Mutex<WorkingQueue>>,
    is_finished: Arc<RwLock<bool>>,
    blocks_pool: ObjectPool<Block>,
    workers: Vec<thread::JoinHandle<()>>,
    reached_end: bool,
    current_block: Block,
}

impl MultiThread {
    /// Creates a multi-thread reader from a stream.
    fn new(threads: u16) -> Self {
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
            reached_end: false,
            current_block: Block::new(),
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

impl DecompressBlock for MultiThread {
    fn next_block<R: ReadBlockInto>(&mut self, reader: &mut R) -> Result<&Block, BlockError> {
        let blocks_to_read = if self.reached_end {
            0
        } else if let Ok(guard) = self.working_queue.lock() {
            self.workers.len() - guard.blocks.len()
        } else {
            return Err(BlockError::IoError(io::Error::new(ErrorKind::Other,
                "Panic in one of the threads")));
        };

        for _ in 0..blocks_to_read {
            let mut block = self.blocks_pool.take();
            match reader.read_block_into(&mut block) {
                Err(BlockError::EndOfStream) => {
                    self.reached_end = true;
                    self.blocks_pool.bring(block);
                    break;
                },
                Err(e) => {
                    self.blocks_pool.bring(block);
                    return Err(e)
                },
                Ok(()) => {},
            }
            if let Ok(mut guard) = self.working_queue.lock() {
                guard.blocks.push_back(block);
            } else {
                self.blocks_pool.bring(block);
                return Err(BlockError::IoError(io::Error::new(ErrorKind::Other,
                    "Panic in one of the threads")));
            }
        }

        let mut time_waited = Duration::from_secs(0);
        let timeout = Duration::from_secs(10);

        let (block, result) = loop {
            if let Ok(mut guard) = self.working_queue.lock() {
                let need_pop = match guard.tasks.get(0) {
                    Some(Task::Ready(_)) => true,
                    Some(Task::NotReady(_)) => false,
                    Some(Task::Interrupted(_)) => true,
                    None => {
                        if guard.blocks.len() > 0 {
                            false
                        } else {
                            assert!(self.reached_end);
                            return Err(BlockError::EndOfStream);
                        }
                    },
                };
                if need_pop {
                    match guard.tasks.pop_front() {
                        Some(Task::Ready(value)) => break value,
                        Some(Task::Interrupted(block)) => self.blocks_pool.bring(block),
                        _ => unreachable!(),
                    }
                }
            } else {
                return Err(BlockError::IoError(io::Error::new(ErrorKind::Other,
                    "Panic in one of the threads")));
            }

            thread::sleep(SLEEP_TIME);
            time_waited += SLEEP_TIME;
            if time_waited > timeout {
                return Err(BlockError::IoError(io::Error::new(ErrorKind::TimedOut,
                    "Decompression takes more than 10 seconds")));
            }
        };

        match result {
            Ok(()) => {
                self.blocks_pool.bring(std::mem::replace(&mut self.current_block, block));
                Ok(&self.current_block)
            },
            Err(e) => {
                self.blocks_pool.bring(block);
                Err(e)
            },
        }
    }
}

pub struct SingleThread {
    block: Block,
}

impl SingleThread {
    fn new() -> Self {
        Self {
            block: Block::new(),
        }
    }
}

impl DecompressBlock for SingleThread {
    fn next_block<R: ReadBlockInto>(&mut self, reader: &mut R) -> Result<&Block, BlockError> {
        reader.read_block_into(&mut self.block)?;
        self.block.decompress()?;
        Ok(&self.block)
    }
}

struct BlockReader<R: Read, M: ReadingMode, T: DecompressBlock> {
    stream: R,
    current_offset: u64,
    reading_mode: M,
    get_block: T,
}

#[doc(hidden)]
pub trait ReadBlockInto {
    fn read_block_into(&mut self, block: &mut Block) -> Result<(), BlockError>;
}

impl<R: Read + Seek, T: DecompressBlock> ReadBlockInto for BlockReader<R, Bouncing, T> {
    fn read_block_into(&mut self, block: &mut Block) -> Result<(), BlockError> {
        if let Some(new_offset) = self.reading_mode.next_offset(self.current_offset) {
            if new_offset != self.current_offset {
                self.stream.seek(SeekFrom::Start(new_offset))?;
                self.current_offset = new_offset;
            }
            block.fill(self.current_offset, &mut self.stream)?;
            self.current_offset += block.block_size() as u64;
            Ok(())
        } else {
            Err(BlockError::EndOfStream)
        }
    }
}

impl<R: Read, T: DecompressBlock> ReadBlockInto for BlockReader<R, Consecutive, T> {
    fn read_block_into(&mut self, block: &mut Block) -> Result<(), BlockError> {
        block.fill(self.current_offset, &mut self.stream)?;
        self.current_offset += block.block_size() as u64;
        Ok(())
    }
}
