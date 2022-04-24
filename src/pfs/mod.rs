//! Partial file system implementation. Not related to peerfs protocol,
//! but used by it.

use std::collections::hash_map::Entry;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::io::{self};


mod file;
pub use file::*;



/// A partial filesystem view, used to store partial files and manage handles to them.
pub struct PartialFileSystem {
    root: PathBuf,
    files: HashMap<u64, PartialFile>,
    handles: HashMap<PathBuf, u64>,
    next_handle: u64
}

impl PartialFileSystem {

    pub fn new<P: AsRef<Path>>(root: P) -> io::Result<Self> {
        Ok(Self {
            root: root.as_ref().canonicalize()?,
            files: HashMap::new(),
            handles: HashMap::new(),
            next_handle: 1
        })
    }

    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> io::Result<u64> {

        let path = self.root.join(path).canonicalize()?;

        match self.handles.entry(path) {
            Entry::Occupied(o) => {
                Ok(*o.get())
            }
            Entry::Vacant(v) => {

                // If the path escaped root, return error.
                if !v.key().starts_with(&self.root) {
                    return Err(io::ErrorKind::InvalidInput.into());
                }

                let file = PartialFile::open(v.key())?;
                let handle = self.next_handle;
                self.next_handle += 1;
                self.files.insert(handle, file);
                v.insert(handle);
                Ok(handle)

            }
        }

    }

}
