//! Partial file implementation.

use std::io::{self, Seek, SeekFrom};
use std::path::Path;
use std::fs::File;
use std::fmt;

use byteorder::{WriteBytesExt, LE, ReadBytesExt};

use crate::range::RangeVec;


/// Block size for partial files.
pub const BLOCK_SIZE: u64 = 4096;


/// A partial file, used to store a file blocks by blocks.
pub struct PartialFile {
    /// Underlying memory mapped file (for seek and read/write).
    file: File,
    dirty: bool,
    size: u64,
    partial_blocks: Option<RangeVec<u64>>
}

impl PartialFile {

    pub fn create<P: AsRef<Path>>(path: P, size: u64) -> io::Result<Self> {
        let mut ret = PartialFile {
            file: File::create(path)?,
            dirty: true,
            size,
            partial_blocks: Some(RangeVec::new())
        };
        ret.flush_partial()?;
        Ok(ret)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {

        let mut file = File::options().read(true).write(true).open(path)?;
        let file_len = file.metadata()?.len();

        // Here we check if this file is in partial mode.
        file.seek(SeekFrom::End(-8))?;
        let footer_length = file.read_u64::<LE>()?;

        file.seek(SeekFrom::End(-(footer_length as i64)))?;
        let size = file.read_u64::<LE>()?;

        let partial = size + footer_length == file_len;

        let size = if partial { size } else { file_len };

        let partial_blocks = if partial {
            // If we guessed that this file is partially filled, parse ranges.
            let mut ranges = RangeVec::new();
            let ranges_count = file.read_u64::<LE>()?;
            for _ in 0..ranges_count {
                let from = file.read_u64::<LE>()?;
                let to = file.read_u64::<LE>()?;
                ranges.push(from, to);
            }
            Some(ranges)
        } else {
            None
        };

        Ok(PartialFile {
            file,
            dirty: false,
            size,
            partial_blocks
        })

    }

    fn flush_partial(&mut self) -> io::Result<()> {

        let blocks = match self.partial_blocks {
            Some(ref mut blocks) => blocks,
            None => panic!("can't flush partial if not in partial mode")
        };

        // Write actual footer.
        self.file.seek(SeekFrom::Start(self.size))?;
        self.file.write_u64::<LE>(self.size)?;

        let ranges = blocks.get_ranges();
        self.file.write_u64::<LE>(ranges.len() as u64)?;

        for &(from, to) in ranges {
            self.file.write_u64::<LE>(from)?;
            self.file.write_u64::<LE>(to)?;
        }

        // Write footer length.
        let real_size = self.file.seek(SeekFrom::Current(0))?;
        let footer_length = real_size - self.size;
        self.file.write_u64::<LE>(footer_length + 8)?; // + 8 for the footer length itself

        self.dirty = false;

        Ok(())

    }

    fn complete_partial(&mut self) -> io::Result<()> {
        self.partial_blocks = None;
        self.file.set_len(self.size)
    }

    #[inline]
    pub fn is_partial(&self) -> bool {
        self.partial_blocks.is_some()
    }

    #[inline]
    pub fn get_partial_blocks(&self) -> Option<&RangeVec<u64>> {
        self.partial_blocks.as_ref()
    }

    fn calc_last_block_index(size: u64) -> u64 {
        (size + BLOCK_SIZE - 1) / BLOCK_SIZE
    }

    fn calc_block_offset(block: u64) -> u64 {
        block * BLOCK_SIZE
    }

}

impl Drop for PartialFile {
    fn drop(&mut self) {
        if self.dirty && self.partial_blocks.is_some() {
            let _ = self.flush_partial();
        }
    }
}

impl fmt::Debug for PartialFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialFile")
            .field("file", &self.file)
            .field("size", &self.size)
            .field("partial_blocks", &self.partial_blocks)
            .finish()
    }
}
