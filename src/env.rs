//! An Env is an interface used by the leveldb implementation to access
//! operating system functionality like the filesystem etc.  Callers
//! may wish to provide a custom Env object when opening a database to
//! get fine gain control; e.g., to rate limit file system operations.
//! 
//! All Env implementations are safe for concurrent access from
//! multiple threads without any external synchronization.

use std::rc::Rc;

use crate::{slice::Slice, status::Status};

pub trait Env {

    /// Create an object that writes to a new file with the specified
    /// name.  Deletes any existing file with the same name and creates a
    /// new file.  On success, stores a pointer to the new file in
    /// *result and returns OK.  On failure stores nullptr in *result and
    /// returns non-OK.
    /// 
    /// The returned file will only be accessed by one thread at a time.
    fn new_writable_file(&self, fname: &str) -> Result<Rc<dyn WritableFile>, Status>;

    /// Returns true iff the named file exists.
    fn file_exists(&self, fname: &str) -> bool;

    /// Delete the named file.
    /// 
    /// The default implementation calls DeleteFile, to support legacy Env
    /// implementations. Updated Env implementations must override RemoveFile and
    /// ignore the existence of DeleteFile. Updated code calling into the Env API
    /// must call RemoveFile instead of DeleteFile.
    /// 
    /// A future release will remove DeleteDir and the default implementation of
    /// RemoveDir.
    fn remove_file(&self, fname: &str) -> Status;

    /// Create the specified directory.
    fn create_dir(&self, dirname: &str) -> Result<(), Status>;

    /// Rename file src to target.
    fn rename_file(&self, src: &str, target: &str) -> Status;

    /// Lock the specified file.  Used to prevent concurrent access to
    /// the same db by multiple processes.  On failure, stores nullptr in
    /// *lock and returns non-OK.
    /// 
    /// On success, stores a pointer to the object that represents the
    /// acquired lock in *lock and returns OK.  The caller should call
    /// UnlockFile(*lock) to release the lock.  If the process exits,
    /// the lock will be automatically released.
    /// 
    /// If somebody else already holds the lock, finishes immediately
    /// with a failure.  I.e., this call does not wait for existing locks
    /// to go away.
    /// 
    /// May create the named file if it does not already exist.
    fn lock_file(&self, fname: &str) -> Result<FileLock, Status>;
}

/// Identifies a locked file.
pub struct FileLock;

/// A file abstraction for sequential writing.  The implementation
/// must provide buffering since callers may append small fragments
/// at a time to the file.
pub trait WritableFile {
    fn append(&self, data: &Slice) -> Status;
    fn close(&self) -> Status;
    fn flush(&self) -> Status;
    fn sync(&self) -> Status;
}

/// An interface for writing log messages.
pub trait Logger {
    /// Write an entry to the log file with the specified format.
    fn logv(&self, msg: &str);
}

pub(crate) fn log(info_log: Option<Rc<dyn Logger>>, msg: &str) {
    if let Some(logger) = info_log {
        logger.logv(msg);
    }
}
