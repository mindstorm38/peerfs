//! Partial file implementation.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::fs::File;
use std::fmt;

use byteorder::{WriteBytesExt, LE, ReadBytesExt};

use crate::range::RangeVec;


/// Block size for partial files.
pub const BLOCK_LEN: usize = 4096;


/// A partial file, used to store a file blocks by blocks.
pub struct PartialFile<F: PartialFiller> {
    /// Underlying memory mapped file (for seek and read/write).
    file: File,
    /// Set to `true` when the partial file has been modified and need
    /// to be save its partial metadata footer.
    dirty: bool,
    /// Real size of this file, partial or not.
    size: u64,
    /// Internal mode of this file.
    mode: PartialMode,
    /// The block provider used to try to fill missing blocks if file
    /// is in partial mode.
    filler: F,
}

/// The internal mode of this file.
#[derive(Debug)]
enum PartialMode {
    /// The file is partially filled.
    Partial {
        /// Ranges of filled blocks in this partial file.
        blocks: RangeVec<u64>,
        /// True if the block at the cursor has already been fetched.
        block_state: BlockState,
        /// Current block's length.
        block_len: usize
    },
    /// The partial file is fully filled.
    Full
}

/// State for the current block, only used in partial mode.
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum BlockState {
    /// Unknown state, to check on next data read.
    Unknown,
    /// The current block has been fetched but is invalid.
    Invalid,
    /// The current block has been fetched but doesn't provide data.
    Valid
}

impl PartialMode {

    #[inline]
    fn new_partial(blocks: RangeVec<u64>) -> PartialMode {
        PartialMode::Partial {
            blocks,
            block_state: BlockState::Unknown,
            block_len: 0
        }
    }

    #[inline]
    fn is_partial(&self) -> bool {
        matches!(self, PartialMode::Partial { .. })
    }

    #[inline]
    fn is_full(&self) -> bool {
        matches!(self, PartialMode::Full)
    }

}

impl<F: PartialFiller> PartialFile<F> {

    pub fn create<P: AsRef<Path>>(path: P, size: u64, filler: F) -> io::Result<Self> {
        let mut ret = PartialFile {
            file: File::create(path)?,
            dirty: true,
            size,
            mode: PartialMode::new_partial(RangeVec::new()),
            filler,
        };
        ret.flush_partial()?;
        Ok(ret)
    }

    pub fn open<P: AsRef<Path>>(path: P, filler: F) -> io::Result<Self> {

        let mut file = File::options().read(true).write(true).open(path)?;
        let file_len = file.metadata()?.len();

        // Here we check if this file is in partial mode.
        file.seek(SeekFrom::End(-8))?;
        let footer_length = file.read_u64::<LE>()?;

        file.seek(SeekFrom::End(-(footer_length as i64)))?;
        let size = file.read_u64::<LE>()?;

        let partial = size + footer_length == file_len;
        let mode = if partial {

            // If we guessed that this file is partially filled, parse ranges.
            let mut blocks = RangeVec::new();
            let ranges_count = file.read_u64::<LE>()?;

            // Test if the remaining header length is strictly equal to the
            // expected length for ranges.
            let expected_ranges_size = ranges_count * 16;
            let actual_ranges_size = footer_length - 24; // -(file_size + header_size + ranges_count)
            if expected_ranges_size != actual_ranges_size {
                for _ in 0..ranges_count {
                    let from = file.read_u64::<LE>()?;
                    let to = file.read_u64::<LE>()?;
                    blocks.push(from, to);
                }
                PartialMode::new_partial(blocks)
            } else {
                PartialMode::Full
            }

        } else {
            PartialMode::Full
        };

        let size = if mode.is_partial() { size } else { file_len };

        Ok(PartialFile {
            file,
            dirty: false,
            size,
            mode,
            filler
        })

    }

    fn flush_partial(&mut self) -> io::Result<()> {

        debug_assert!(self.mode.is_partial(), "expected partial mode");

        if let PartialMode::Partial { ref blocks, .. } = self.mode {

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

        }

        Ok(())

    }

    fn complete_partial(&mut self) -> io::Result<()> {
        self.mode = PartialMode::Full;
        self.file.set_len(self.size)
    }

    #[inline]
    pub fn is_partial(&self) -> bool {
        self.mode.is_partial()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.mode.is_full()
    }

    #[inline]
    pub fn get_partial_blocks(&self) -> Option<&RangeVec<u64>> {
        match self.mode {
            PartialMode::Partial { ref blocks, .. } => Some(blocks),
            _ => None
        }
    }

    #[inline]
    fn calc_block_len(size: u64, block: u64) -> usize {
        let block_offset = block * BLOCK_LEN as u64;
        if block_offset < size {
            let remaining_size = size - block_offset;
            remaining_size.min(BLOCK_LEN as u64) as usize
        } else {
            0
        }
    }

    #[inline]
    fn calc_last_block_index(size: u64) -> u64 {
        (size + BLOCK_LEN as u64 - 1) / BLOCK_LEN as u64
    }

    #[inline]
    fn calc_block_offset(block: u64) -> u64 {
        block * BLOCK_LEN as u64
    }

}

impl<F: PartialFiller> Drop for PartialFile<F> {
    fn drop(&mut self) {
        if self.dirty && self.mode.is_partial() {
            let _ = self.flush_partial();
        }
    }
}

impl<F: PartialFiller> fmt::Debug for PartialFile<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialFile")
            .field("file", &self.file)
            .field("size", &self.size)
            .field("mode", &self.mode)
            .finish()
    }
}

impl<F: PartialFiller> Read for PartialFile<F> {

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {

        match self.mode {
            PartialMode::Partial {
                ref mut blocks ,
                ref mut block_state,
                ref mut block_len,
            } => {

                if let BlockState::Unknown = block_state {

                    let pos = self.file.stream_position()?;
                    let block = pos / BLOCK_LEN as u64;

                    *block_len = Self::calc_block_len(self.size, block);

                    if blocks.contains(block) {
                        *block_state = BlockState::Valid;
                    } else {

                        self.file.seek(SeekFrom::Start(block * BLOCK_LEN as u64))?;

                        let mut writer = LimitedWriter {
                            inner: &mut self.file,
                            len: *block_len
                        };

                        match self.filler.provide(block, *block_len, &mut writer) {
                            Ok(_) if writer.len == 0 => {
                                *block_state = BlockState::Valid;
                                blocks.push(block, block + 1);
                                self.file.seek(SeekFrom::Start(pos))?;
                            }
                            Ok(_) => {
                                *block_state = BlockState::Invalid;
                            }
                            Err(err) => {
                                *block_state = BlockState::Invalid;
                                return Err(err);
                            }
                        }

                    }

                }

                if let BlockState::Valid = block_state {
                    let len = buf.len().min(*block_len);
                    self.file.read(&mut buf[..len])
                } else {
                    Err(io::ErrorKind::InvalidData.into())
                }

            }
            PartialMode::Full => {
                // If the file is full, we use the file's internal cursor.
                self.file.read(buf)
            }
        }

    }

}

impl<F: PartialFiller> Seek for PartialFile<F> {

    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {

        match self.mode {
            PartialMode::Partial {
                ref mut block_state,
                ..
            } => {
                let abs_pos = match pos {
                    SeekFrom::Start(pos) => pos,
                    SeekFrom::End(pos) => {
                        (self.size as i64).saturating_add(pos).max(0) as u64
                    }
                    SeekFrom::Current(pos) => {
                        (self.file.stream_position()? as i64).saturating_add(pos).max(0) as u64
                    }
                }.min(self.size);
                *block_state = BlockState::Unknown;
                self.file.seek(SeekFrom::Start(abs_pos))
            }
            PartialMode::Full => {
                self.file.seek(pos)
            }
        }

    }

}


/// A block provider used to complete missing blocks from [`PartialFile`]s.
pub trait PartialFiller {
    fn provide<W: Write>(&self, block_index: u64, block_len: usize, dest: W) -> io::Result<()>;
}

impl PartialFiller for () {
    fn provide<W: Write>(&self, _block_index: u64, block_len: usize, mut dest: W) -> io::Result<()> {
        static RES: [u8; BLOCK_LEN] = [0; BLOCK_LEN];
        dest.write_all(&RES[..block_len])
    }
}


struct LimitedWriter<W: Write> {
    inner: W,
    len: usize
}

impl<W: Write> Write for LimitedWriter<W> {

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len().min(self.len);
        let read = self.inner.write(&buf[..len])?;
        self.len -= read;
        Ok(read)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

}