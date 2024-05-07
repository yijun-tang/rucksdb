use std::{cell::RefCell, rc::Rc, sync::Mutex};

use crate::{comparator::Comparator, db::{filename::{current_file_name, descriptor_file_name, lock_file_name, log_file_name, set_current_file}, log_writer::Writer, version_edit::VersionEdit}, env::{log, Env, FileLock, WritableFile}, filter_policy::FilterPolicy, options::Options, slice::Slice, status::Status};

use self::{dbformat::InternalKeyComparator, memtable::MemTable, version_set::VersionSet};

pub(crate) mod version_edit;
pub(crate) mod version_set;
pub(crate) mod dbformat;
pub(crate) mod filename;
pub(crate) mod log_writer;
pub(crate) mod log_format;
pub(crate) mod memtable;
pub(crate) mod skiplist;


/// A DB is a persistent ordered map from keys to values.
/// A DB is safe for concurrent access from multiple threads without
/// any external synchronization.
pub struct DB {
    // Lock over the persistent DB state.  Non-null iff successfully acquired.
    db_lock_: RefCell<Option<FileLock>>,

    env_: Rc<dyn Env>,
    internal_comparator_: InternalKeyComparator,
    internal_filter_policy_: Option<Rc<dyn FilterPolicy>>,
    options_: Options,  // options_.comparator == &internal_comparator_
    dbname_: String,

    // State below is protected by mutex_
    mutex_: Mutex<()>,
    mem_: Option<Rc<MemTable>>,
    imm_: Option<Rc<MemTable>>,
    logfile_: Option<Rc<dyn WritableFile>>,
    logfile_number_: u64,
    log_: Option<Writer>,

    versions_: RefCell<VersionSet>,
}

impl DB {
    /// Open the database with the specified "name".
    /// Returns boxed DB on success and a non-OK status on error.
    pub fn open(options: &Options, name: &str) -> Result<Box<DB>, Status> {
        let mut db = Box::new(Self::new(options, name));
        {
            let _unused = db.mutex_.lock().expect("failed to acquire lock");
            let mut edit = VersionEdit::new();
            // Recover handles create_if_missing, error_if_exists
            let mut save_manifest = false;
            let mut s = db.recover(&mut edit, &mut save_manifest);
            if s.ok() && db.mem_.is_none() {
                // Create new log and a corresponding memtable.
                let new_log_number = db.versions_.borrow_mut().new_file_number();
                match options.env.new_writable_file(&log_file_name(name, new_log_number)) {
                    Ok(file) => {
                        edit.set_log_number(new_log_number);
                        db.logfile_ = Some(file.clone());
                        db.logfile_number_ = new_log_number;
                        db.log_ = Some(Writer::new(file));
                        db.mem_ = Some(Rc::new(MemTable::new(&db.internal_comparator_)));
                    },
                    Err(s_) => { s = s_; },
                }
            }
        }
        
        
        todo!()
    }

    fn new(raw_options: &Options, dbname: &str) -> DB {
        let icmp = InternalKeyComparator::new(raw_options.comparator.clone());
        Self {
            db_lock_: RefCell::new(None),
            env_: raw_options.env.clone(),
            internal_comparator_: icmp.clone(),
            internal_filter_policy_: raw_options.filter_policy.clone(),
            options_: sanitize_options(dbname, &icmp, raw_options.filter_policy.clone(), raw_options),
            dbname_: dbname.to_string(),
            mutex_: Mutex::new(()),
            mem_: None,
            imm_: None,
            logfile_: None,
            logfile_number_: 0,
            log_: None,
            versions_: RefCell::new(VersionSet::new()),
        }
    }

    fn new_db(&self) -> Status {
        let mut new_db = VersionEdit::new();
        new_db.set_comparator_name(self.internal_comparator_.name());
        new_db.set_log_number(0);
        new_db.set_next_file(2);
        new_db.set_last_sequence(0);

        let manifest = descriptor_file_name(&self.dbname_, 1);
        let mut s = Status::new_ok();
        match self.env_.new_writable_file(&manifest) {
            Ok(file) => {
                let mut log = Writer::new(file.clone());
                let mut record = Vec::new();
                new_db.encode_to(&mut record);
                s = log.add_record(&Slice::new(&record));
                if s.ok() {
                    s = file.sync();
                }
                if s.ok() {
                    s = file.close();
                }
            },
            Err(s) => { return s; },
        }
        if s.ok() {
            // Make "CURRENT" file that points to the new manifest file.
            s = set_current_file(self.env_.clone(), &self.dbname_, 1);
        } else {
            self.env_.remove_file(&manifest);
        }
        s
    }

    /// The mutex should be acquired before calling it.
    fn recover(&self, edit: &mut VersionEdit, save_manifest: &mut bool) -> Status {
        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        let _ = self.env_.create_dir(&self.dbname_);
        debug_assert!(self.db_lock_.borrow().is_none());
        match self.env_.lock_file(&lock_file_name(&self.dbname_)) {
            Ok(f) => { self.db_lock_.borrow_mut().replace(f); },
            Err(s) => { return s; },
        };

        if !self.env_.file_exists(&current_file_name(&self.dbname_)) {
            if self.options_.create_if_missing {
                log(self.options_.info_log.clone(), &format!("Creating DB {} since it was missing.", &self.dbname_));
                let s = self.new_db();
                if !s.ok() {
                    return s;
                }
            } else {
                return Status::invalid_argument(&self.dbname_, "does not exist (create_if_missing is false)");
            }
        } else {
            return Status::invalid_argument(&self.dbname_, "exists (error_if_exists is true)");
        }

        let mut save_manifest = false;
        match self.versions_.borrow_mut().recover() {
            Ok(save) => { save_manifest = save; },
            Err(s) => { return s; },
        }

        todo!()
    }
}

fn sanitize_options(dbname: &str, icmp: &InternalKeyComparator, ipolicy: Option<Rc<dyn FilterPolicy>>, src: &Options) -> Options {

    todo!()
}
