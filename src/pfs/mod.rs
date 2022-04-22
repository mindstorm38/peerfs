//! Partial file system implementation.

use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::io::{self};
use std::fs::File;


mod file;
pub use file::*;


pub type Handle = u64;


/// A partial filesystem view, used to store partial files and manage handles to them.
pub struct PartialFileSystem {
    root: PathBuf,
    handles: HashMap<Handle, File>,
    next_handle: Handle
}

impl PartialFileSystem {

    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            handles: HashMap::new(),
            next_handle: 1
        }
    }

    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> io::Result<Handle> {
        let path = self.root.join(path);
        let file = File::open(path)?;
        let handle = self.next_handle;
        self.handles.insert(handle, file);
        Ok(handle)
    }

}


