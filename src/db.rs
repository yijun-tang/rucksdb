use crate::{options::Options, status::Status};




/// A DB is a persistent ordered map from keys to values.
/// A DB is safe for concurrent access from multiple threads without
/// any external synchronization.
pub struct DB;

impl DB {
    /// Open the database with the specified "name".
    /// Returns boxed DB on success and a non-OK status on error.
    pub fn open(options: Options, name: &str) -> Result<Box<DB>, Status> {
        if options.block_cache.is_some() && options.no_block_cache {
            return Err(Status::invalid_argument(
                "no_block_cache is true while block_cache is not NULL", ""));
        }
        todo!()
    }

    fn new(options: &Options, dbname: &str) {
        
    }
}


