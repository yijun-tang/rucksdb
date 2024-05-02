use crate::cache::Cache;


/// Options to control the behavior of a database (passed to DB::Open)
pub struct Options {


    /// Control over blocks (user data is stored in a set of blocks, and
    /// a block is the unit of reading from disk).
    /// 
    /// If non-NULL, use the specified cache for blocks.
    /// If NULL, leveldb will automatically create and use an 8MB internal cache.
    /// Default: NULL
    pub block_cache: Option<Box<dyn Cache>>,

    /// Disable block cache. If this is set to true,
    /// then no block cache should be used, and the block_cache should
    /// point to a NULL object.
    pub no_block_cache: bool,
}
