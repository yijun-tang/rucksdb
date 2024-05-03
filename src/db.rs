use std::{rc::Rc, sync::Mutex};

use crate::{comparator::Comparator, db::{filename::{current_file_name, descriptor_file_name, lock_file_name}, version_edit::VersionEdit}, env::{log, Env, FileLock}, filter_policy::FilterPolicy, memtable::MemTable, options::Options, status::Status};

mod version_edit;
mod dbformat;
mod filename;
mod log_writer;
mod log_format;


/// A DB is a persistent ordered map from keys to values.
/// A DB is safe for concurrent access from multiple threads without
/// any external synchronization.
pub struct DB {
    // Lock over the persistent DB state.  Non-null iff successfully acquired.
    db_lock_: Option<FileLock>,

    env_: Rc<dyn Env>,
    internal_comparator_: Rc<dyn Comparator>,
    internal_filter_policy_: Option<Rc<dyn FilterPolicy>>,
    options_: Options,  // options_.comparator == &internal_comparator_
    dbname_: String,
    mutex_: Mutex<InnerState>,
}

struct InnerState {
    imm_: MemTable,
}

impl InnerState {
    fn new() -> Self {
        Self {
            imm_: MemTable,
        }
    }
}

impl DB {
    /// Open the database with the specified "name".
    /// Returns boxed DB on success and a non-OK status on error.
    pub fn open(options: &Options, name: &str) -> Result<Box<DB>, Status> {
        let db = Box::new(Self::new(options, name));
        {
            let guard = db.mutex_.lock().expect("failed to acquire lock");

        }
        
        
        todo!()
    }

    fn new(raw_options: &Options, dbname: &str) -> DB {
        Self {
            db_lock_: None,
            env_: raw_options.env.clone(),
            internal_comparator_: raw_options.comparator.clone(),
            internal_filter_policy_: raw_options.filter_policy.clone(),
            options_: sanitize_options(dbname, raw_options.comparator.clone(), raw_options.filter_policy.clone(), raw_options),
            dbname_: dbname.to_string(),
            mutex_: Mutex::new(InnerState::new()),
            
        }
    }

    fn new_db(&self) -> Status {
        let mut new_db = VersionEdit::new();
        new_db.set_comparator_name(self.internal_comparator_.name());
        new_db.set_log_number(0);
        new_db.set_next_file(2);
        new_db.set_last_sequence(0);

        let manifest = descriptor_file_name(&self.dbname_, 1);
        match self.env_.new_writable_file(&manifest) {
            Ok(file) => {

            },
            Err(s) => { return s; },
        }
        todo!()
    }

    /// The mutex should be acquired before calling it.
    fn recover(&mut self) -> Status {
        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        let _ = self.env_.create_dir(&self.dbname_);
        assert!(self.db_lock_.is_none());
        match self.env_.lock_file(&lock_file_name(&self.dbname_)) {
            Ok(f) => { self.db_lock_ = Some(f); },
            Err(s) => { return s; },
        };

        if !self.env_.file_exists(&current_file_name(&self.dbname_)) {
            if self.options_.create_if_missing {
                log(self.options_.info_log.clone(), &format!("Creating DB {} since it was missing.", &self.dbname_));

            } else {
                return Status::invalid_argument(&self.dbname_, "does not exist (create_if_missing is false)");
            }
        } else {

        }

        todo!()
    }
}

fn sanitize_options(dbname: &str, icmp: Rc<dyn Comparator>, ipolicy: Option<Rc<dyn FilterPolicy>>, src: &Options) -> Options {

    todo!()
}
