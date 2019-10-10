use std::sync::{Arc, Mutex, RwLock};
use std::collections::VecDeque;
use std::io::{self, Write, ErrorKind};
use std::thread;
use std::time::Duration;

use super::{Block, ObjectPool};
use super::{SLEEP_TIME, TIMEOUT};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
struct WorkerId(u16);

enum Task {
    Ready((Block, Result<(), io::Error>)),
    NotReady(WorkerId),
}

impl Task {
    fn is_not_ready(&self, worker_id: WorkerId) -> bool {
        match self {
            Task::NotReady(task_worker_id) if *task_worker_id == worker_id => true,
            _ => false,
        }
    }
}

#[derive(Default)]
struct WorkingQueue {
    blocks: VecDeque<Block>,
    tasks: VecDeque<Task>,
}

struct Worker {
    worker_id: WorkerId,
    working_queue: Arc<Mutex<WorkingQueue>>,
    is_finished: Arc<RwLock<bool>>,
    compression: flate2::Compression,
}

impl Worker {
    fn run(&mut self) {
        'outer: while !self.is_finished.read().map(|guard| *guard).unwrap_or(true) {
            let block = if let Ok(mut guard) = self.working_queue.lock() {
                if let Some(block) = guard.blocks.pop_front() {
                    guard.tasks.push_back(Task::NotReady(self.worker_id));
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

            let res = match block.compress(self.compression) {
                Err(ref e) if e.kind() == ErrorKind::WriteZero => {
                    // Compressed size is too big.
                    block.reset_compression();
                    let mut second_half = block.split_into_two();

                    let res1 = block.compress(self.compression);
                    let res2 = second_half.compress(self.compression);

                    if let Ok(mut guard) = self.working_queue.lock() {
                        // Insert two Ready Tasks.
                        for i in 0..guard.tasks.len() {
                            if guard.tasks[i].is_not_ready(self.worker_id) {
                                std::mem::replace(&mut guard.tasks[i], Task::Ready((block, res1)));
                                guard.tasks.insert(i + 1, Task::Ready((second_half, res2)));
                                continue 'outer;
                            }
                        }
                        panic!("Task handler not found for worker {}", self.worker_id.0);
                    } else {
                        // Panic in another thread
                        break
                    };
                },
                res => res,
            };

            if let Ok(mut guard) = self.working_queue.lock() {
                for task in guard.tasks.iter_mut().rev() {
                    if task.is_not_ready(self.worker_id) {
                        std::mem::replace(task, Task::Ready((block, res)));
                        continue 'outer;
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

trait CompressionQueue<W: Write> {
    /// Adds the next uncompressed block to the queue, and tries to write to the output.
    /// The function returns an empty block and the result of the writing.
    ///
    /// It is highly not recommended to try to rewrite a block if the function returned an error.
    /// In that case it is possible that the resulting stream would have missing blocks because
    /// * If the block is too big, it may be split in two and only one of them could be written,
    /// * For multi-thread writer, any of the previous blocks could raise an error.
    fn add_block_and_write(&mut self, block: Block) -> (Block, io::Result<()>);

    /// Flush contents to strem. Multi-thread writer would wait until all blocks are compressed
    /// unless it encounters an error.
    fn flush(&mut self) -> io::Result<()>;
    fn join(&mut self);
}

struct SingleThread<W: Write> {
    stream: W,
    compression: flate2::Compression,
}

impl<W: Write> CompressionQueue<W> for SingleThread<W> {
    fn add_block_and_write(&mut self, mut block: Block) -> (Block, io::Result<()>) {
        match block.compress(self.compression) {
            Ok(()) => {},
            Err(ref e) if e.kind() == ErrorKind::WriteZero => {
                // Compressed size is too big.
                block.reset_compression();
                let second_half = block.split_into_two();
                if let Err(e) = block.dump(&mut self.stream) {
                    block.reset();
                    return (block, Err(e));
                }
                block = second_half;
            },
            Err(e) => {
                block.reset();
                return (block, Err(e));
            },
        }

        let res = block.dump(&mut self.stream);
        block.reset();
        (block, res)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }

    fn join(&mut self) {}
}

struct MultiThread<W: Write> {
    stream: W,
    working_queue: Arc<Mutex<WorkingQueue>>,
    is_finished: Arc<RwLock<bool>>,
    blocks_pool: ObjectPool<Block>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl<W: Write> MultiThread<W> {
    /// Creates a multi-thread writer from a stream.
    fn new(stream: W, threads: u16, compression: flate2::Compression) -> Self {
        assert!(threads > 0);
        let working_queue = Arc::new(Mutex::new(WorkingQueue::default()));
        let is_finished = Arc::new(RwLock::new(false));
        let workers = (0..threads).map(|i| {
            let mut worker = Worker {
                worker_id: WorkerId(i),
                working_queue: Arc::clone(&working_queue),
                is_finished: Arc::clone(&is_finished),
                compression,
            };
            thread::Builder::new()
                .name(format!("bgzip_write{}", i + 1))
                .spawn(move || worker.run())
                .expect("Cannot create a thread")
        }).collect();

        Self {
            stream,
            working_queue,
            is_finished,
            blocks_pool: ObjectPool::new(|| Block::new()),
            workers,
        }
    }

    fn write_compressed(&mut self, stop_if_not_ready: bool) -> io::Result<()> {
        let mut time_waited = Duration::new(0, 0);
        loop {
            let queue_top = if let Ok(mut guard) = self.working_queue.lock() {
                let need_pop = match guard.tasks.get(0) {
                    Some(Task::NotReady(_)) => {
                        if stop_if_not_ready {
                            return Ok(());
                        } else {
                            false
                        }
                    },
                    Some(Task::Ready(_)) => true,
                    None => return Ok(()),
                };

                if need_pop {
                    match guard.tasks.pop_front() {
                        Some(Task::Ready(value)) => Some(value),
                        _ => unreachable!(),
                    }
                } else {
                    None
                }
            } else {
                return Err(io::Error::new(ErrorKind::Other, "Panic in one of the threads"));
            };

            if queue_top.is_none() {
                thread::sleep(SLEEP_TIME);
                time_waited += SLEEP_TIME;
                if time_waited > TIMEOUT {
                    return Err(io::Error::new(ErrorKind::TimedOut,
                        format!("Compression takes more than {:?}", TIMEOUT)));
                }
                continue;
            }

            time_waited = Duration::new(0, 0);
            let (block, res) = queue_top.unwrap();
            let res = res.and_then(|_| block.dump(&mut self.stream));
            self.blocks_pool.bring(block);
            if res.is_err() {
                return res;
            }
        }
    }
}

impl<W: Write> CompressionQueue<W> for MultiThread<W> {
    fn add_block_and_write(&mut self, block: Block) -> (Block, io::Result<()>) {
        assert!(self.workers.len() > 0, "Cannot compress blocks: threads were already joined");

        if let Ok(mut guard) = self.working_queue.lock() {
            guard.blocks.push_back(block);
        } else {
            return (block, Err(io::Error::new(ErrorKind::Other, "Panic in one of the threads")));
        };

        let res = self.write_compressed(true);
        let mut block = self.blocks_pool.take();
        block.reset();
        (block, res)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.write_compressed(false)?;
        self.stream.flush()
    }

    fn join(&mut self) {
        *self.is_finished.write()
            .unwrap_or_else(|e| panic!("Panic in one of the threads: {:?}", e)) = true;
        for worker in self.workers.drain(..) {
            worker.join().unwrap_or_else(|e| panic!("Panic in one of the threads: {:?}", e));
        }
    }
}