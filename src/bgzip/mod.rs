pub mod read;
pub mod write;

pub const MAX_BLOCK_SIZE: usize = 65536;
pub const COMPRESSED_BLOCK_SIZE: usize = MAX_BLOCK_SIZE - 26;
