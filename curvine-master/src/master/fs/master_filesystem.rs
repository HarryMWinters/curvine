// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::master::fs::context::ValidateAddBlock;
use crate::master::fs::policy::ChooseContext;
use crate::master::journal::JournalSystem;
use crate::master::meta::inode::{InodeFile, InodePath, InodePtr, InodeView, PATH_SEPARATOR};
use crate::master::meta::{CacheInvalidationResult, FsDir, InodeId};

use crate::master::fs::DeleteResult;
use crate::master::meta::parse_glob_pattern;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::*;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::GroupExecutor;
use curvine_runtime::sync::ArcRwLock;
use log::{error, info, warn};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

pub struct CvMetadataSnapshotEntry {
    pub status: FileStatus,
    pub blocks: Option<FileBlocks>,
}

pub struct CvMetadataSnapshotPage {
    pub entries: Vec<CvMetadataSnapshotEntry>,
    pub next_page_token: Option<String>,
    pub epoch: u64,
}

pub struct CvMetadataDeltaEntry {
    pub path: String,
    pub entry: Option<CvMetadataSnapshotEntry>,
}

pub struct CvMetadataDeltaPage {
    pub entries: Vec<CvMetadataDeltaEntry>,
    pub next_page_token: Option<String>,
    pub from_epoch: u64,
    pub to_epoch: u64,
    pub full_snapshot_required: bool,
}

struct CompleteFileOptions {
    only_flush: bool,
    return_file_blocks: bool,
    set_attr_opts: Option<SetAttrOpts>,
}

#[derive(Clone)]
pub struct MasterFilesystem {
    pub fs_dir: SyncFsDir,
    pub worker_manager: SyncWorkerManager,
    pub master_monitor: MasterMonitor,
    pub conf: Arc<MasterConf>,
    full_block_reports: Arc<Mutex<HashMap<u32, FullBlockReportState>>>,
    full_block_reconciles: Arc<Mutex<HashMap<u32, FullBlockReconcileState>>>,
    lost_worker_cleanups: Arc<Mutex<HashMap<u32, LostWorkerCleanupState>>>,
    full_block_reconcile_executor: Arc<GroupExecutor>,
    worker_lifecycle_locks: Arc<Mutex<HashMap<u32, Arc<Mutex<()>>>>>,
}

pub struct BlockReportResult {
    pub delete_blocks: Vec<i64>,
}

#[derive(Default)]
pub struct LostWorkerLocationCleanup {
    pub removed_block_ids: Vec<i64>,
    pub replication_block_ids: Vec<i64>,
}

fn child_snapshot_path(parent: &str, child_name: &str) -> String {
    if parent == "/" {
        format!("/{child_name}")
    } else {
        format!("{parent}/{child_name}")
    }
}

fn snapshot_token_in_subtree(token: &str, subtree_path: &str) -> bool {
    token == subtree_path
        || (subtree_path != "/"
            && token
                .strip_prefix(subtree_path)
                .map(|rest| rest.starts_with(PATH_SEPARATOR))
                .unwrap_or(false))
}

struct FullBlockReportState {
    total_len: u64,
    update_time_ms: u64,
    reported_blocks: HashSet<i64>,
    deleted_blocks: HashSet<i64>,
    incremental_updates: HashMap<i64, bool>,
}

#[derive(Default)]
struct LostWorkerCleanupState {
    affected_block_ids: HashSet<i64>,
    invalidation_block_ids: HashSet<i64>,
    invalidated_block_ids: HashSet<i64>,
}

struct FullBlockReconcileState {
    running: bool,
    retry_at_ms: u64,
    generation: u64,
    pending: Option<FullBlockReconcileJob>,
}

struct FullBlockReconcileJob {
    generation: u64,
    reported_blocks: HashSet<i64>,
    worker_session_id: String,
    replication_handler: Option<MasterReplicationHandler>,
}

const FULL_BLOCK_REPORT_TTL_MS: u64 = 60 * 60 * 1000;
const FULL_BLOCK_RECONCILE_THREADS: usize = 2;
const FULL_BLOCK_RECONCILE_QUEUE_SIZE: usize = 128;

impl MasterFilesystem {
    // Max block-report location updates applied under a single fs_dir write lock.
    const BLOCK_REPORT_WRITE_CHUNK: usize = 4096;
    // Max lost-worker block ids inspected under a single fs_dir write lock.
    const LOST_WORKER_INVALIDATION_CHUNK: usize = Self::BLOCK_REPORT_WRITE_CHUNK;

    fn validate_alloc_capacity(
        current_len: i64,
        replicas: u8,
        opts: &FileAllocOpts,
        available: i64,
    ) -> FsResult<()> {
        if opts.truncate || opts.len <= current_len {
            return Ok(());
        }

        let logical_growth = opts.len - current_len;
        let required = logical_growth.saturating_mul(i64::from(replicas));
        if required > available {
            return err_ext!(FsError::disk_out_of_space(format!(
                "fallocate requires {} bytes for {} replicas, but only {} bytes are available",
                logical_growth, replicas, available
            )));
        }

        Ok(())
    }

    pub fn new(
        conf: &ClusterConf,
        fs_dir: SyncFsDir,
        worker_manager: SyncWorkerManager,
        master_monitor: MasterMonitor,
    ) -> Self {
        Self {
            fs_dir,
            worker_manager,
            master_monitor,
            conf: Arc::new(conf.master.clone()),
            full_block_reports: Default::default(),
            full_block_reconciles: Default::default(),
            lost_worker_cleanups: Default::default(),
            worker_lifecycle_locks: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
        }
    }

    pub fn with_js(conf: &ClusterConf, js: &JournalSystem) -> Self {
        let fs = js.fs();
        Self {
            fs_dir: fs.fs_dir.clone(),
            worker_manager: js.worker_manager(),
            master_monitor: js.master_monitor(),
            conf: Arc::new(conf.master.clone()),
            full_block_reports: fs.full_block_reports.clone(),
            full_block_reconciles: fs.full_block_reconciles.clone(),
            lost_worker_cleanups: fs.lost_worker_cleanups.clone(),
            worker_lifecycle_locks: fs.worker_lifecycle_locks.clone(),
            full_block_reconcile_executor: fs.full_block_reconcile_executor.clone(),
        }
    }

    pub fn check_parent(path: &InodePath) -> FsResult<()> {
        // The root directory must exist.All /a does not require verification
        if path.len() > 2 {
            if let Some(v) = path.get_inode(-2) {
                if !v.is_dir() {
                    err_box!(
                        "Parent path is not a directory:: {}",
                        path.get_parent_path()
                    )
                } else {
                    Ok(())
                }
            } else {
                err_box!("Parent directory doesn't exist: {}", path.get_parent_path())
            }
        } else {
            Ok(())
        }
    }

    pub fn print_tree(&self) {
        let fs_dir = self.fs_dir.read();
        fs_dir.print_tree();
    }

    pub fn mkdir_with_opts<T: AsRef<str>>(&self, path: T, opts: MkdirOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        // Creation of root directory is not allowed
        if inp.is_root() {
            return err_box!("Not allowed to create existing root path: {}", inp.path());
        }

        if inp.is_full() {
            if opts.create_parent {
                if let Some(last_inode) = inp.get_last_inode() {
                    if last_inode.is_dir() {
                        let status = last_inode.to_file_status(inp.path())?;
                        return Ok(status);
                    }
                }
            }
            return err_ext!(FsError::file_exists(inp.path()));
        }

        // Check whether the directory can be created recursively.
        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let inp = fs_dir.mkdir(inp, opts)?;
        let last = try_option!(
            inp.get_last_inode(),
            "Path {} has no inode after mkdir",
            inp.path()
        );
        let status = last.to_file_status(inp.path())?;
        Ok(status)
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<DeleteResult> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        let mut delete_res = fs_dir.delete(&inp, recursive)?;
        drop(fs_dir);

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks: std::mem::take(&mut delete_res.blocks),
        });

        Ok(delete_res)
    }

    pub fn free<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<FreeResult> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        let mut free_res = fs_dir.free(&inp, recursive)?;
        drop(fs_dir);

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks: std::mem::take(&mut free_res.blocks),
        });

        Ok(free_res)
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T, flags: RenameFlags) -> FsResult<bool> {
        let src = src.as_ref();
        let dst = dst.as_ref();

        let mut fs_dir = self.fs_dir.write();
        let src_inp = Self::resolve_path(&fs_dir, src)?;
        let dst_inp = Self::resolve_path(&fs_dir, dst)?;

        if src_inp.is_root() {
            return err_box!("Cannot rename root path");
        }

        if src == dst {
            return Ok(false);
        }

        // dst cannot be in the src directory, /a/b -> /a/b/c is not allowed (POSIX EINVAL).
        if let Some(rest) = dst.strip_prefix(src) {
            if rest.starts_with(PATH_SEPARATOR) {
                return err_ext!(FsError::invalid_argument(format!(
                    "cannot rename {} to {}: destination is under source",
                    src, dst
                )));
            }
        }

        // EXCHANGE also rejects src under dst (/a/b <-> /a would make /a its own descendant).
        if flags.exchange_mode() {
            if let Some(rest) = src.strip_prefix(dst) {
                if rest.starts_with(PATH_SEPARATOR) {
                    return err_ext!(FsError::invalid_argument(format!(
                        "cannot exchange {} with {}: source is under destination",
                        src, dst
                    )));
                }
            }
        }

        if let Some(del_res) = fs_dir.rename(&src_inp, &dst_inp, flags)? {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&del_res);
        }

        Ok(true)
    }

    pub fn create<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let ctx = CreateFileOpts::with_create(create_parent);
        self.create_with_opts(path, ctx, OpenFlags::new_create().set_overwrite(true))
    }

    fn truncate(&self, fs_dir: &mut FsDir, inp: &InodePath, opts: CreateFileOpts) -> FsResult<()> {
        let clean_result = fs_dir.overwrite_file(inp, opts)?;
        if !clean_result.blocks.is_empty() {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&clean_result);
        }
        Ok(())
    }

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        if !flags.create() {
            return err_box!("create_with_opts requires O_CREAT flag");
        }
        let path = path.as_ref();

        // Check the path length
        self.check_path_length(path)?;

        if opts.replicas < self.conf.min_replication || opts.replicas >= self.conf.max_replication {
            return err_box!(
                "The replica number {} needs to be between {} and {}",
                opts.replicas,
                self.conf.min_replication,
                self.conf.max_replication
            );
        }

        if opts.block_size < self.conf.min_block_size || opts.block_size >= self.conf.max_block_size
        {
            return err_box!(
                "Block size needs to be between {} and {}",
                self.conf.min_block_size,
                self.conf.max_block_size
            );
        }

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let last_inode = inp.get_last_inode();
        if let Some(inode) = &last_inode {
            if inode.is_dir() {
                return err_box!("{}  already exists as a dir", inp.path());
            }

            if flags.exclusive() {
                return err_ext!(FsError::file_exists(inp.path()));
            }
        }

        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let inp = if last_inode.is_some() {
            if flags.overwrite() {
                self.truncate(&mut fs_dir, &inp, opts)?;
            } else {
                return err_ext!(FsError::file_exists(inp.path()));
            }
            inp
        } else {
            fs_dir.create_file(inp, opts)?
        };

        let status = fs_dir.file_status(&inp)?;

        Ok(status)
    }

    pub fn open_file<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        let path = path.as_ref();

        if flags.read_only() {
            if flags.truncate() {
                return err_box!("cannot combine O_RDONLY with O_TRUNC");
            }
            if flags.create() {
                return err_box!("cannot combine O_RDONLY with O_CREAT");
            }
            return self.get_block_locations(path);
        }

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            None => {
                return if flags.create() {
                    drop(fs_dir);
                    let status = self.create_with_opts(path, opts, flags)?;
                    Ok(FileBlocks::new(status, vec![]))
                } else {
                    err_ext!(FsError::file_not_found(inp.path()))
                }
            }

            Some(inode) => {
                if inode.is_dir() {
                    return err_box!("{} is a directory", inp.path());
                }
                inode
            }
        };

        if flags.truncate() {
            self.truncate(&mut fs_dir, &inp, opts)?;
            let status = fs_dir.file_status(&inp)?;
            return Ok(FileBlocks::new(status, vec![]));
        }

        let status = fs_dir.reopen_file(&inp, opts.client_name)?;
        let file = inode.as_file_ref()?;
        let blocks = if !file.blocks.is_empty() {
            self.get_block_locs(path, &fs_dir, file)?
        } else {
            vec![]
        };
        Ok(FileBlocks::new(status, blocks))
    }

    pub fn file_status<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        self.file_status_by_id(path, None)
    }

    pub fn file_status_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
    ) -> FsResult<FileStatus> {
        let path = path.as_ref();
        let fs_dir = self.fs_dir.read();
        let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        Ok(inode.to_file_status(path)?)
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        Ok(inp.get_last_inode().is_some())
    }

    pub fn list_status<T: AsRef<str>>(&self, path: T) -> FsResult<Vec<FileStatus>> {
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path.as_ref());
        if is_glob_pattern {
            let paths = Self::resolve_path_by_glob_pattern(&fs_dir, path.as_ref())?;
            let mut all_statuses = Vec::new();
            for path in &paths {
                let statuses = fs_dir.list_status(path)?;
                all_statuses.extend(statuses);
            }
            Ok(all_statuses)
        } else {
            let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
            fs_dir.list_status(&inp)
        }
    }

    pub fn list_options<T: AsRef<str>>(
        &self,
        path: T,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        let path = path.as_ref();
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path);
        if is_glob_pattern {
            err_box!("list_options does not support glob pattern, path {}", path)
        } else {
            let inp = Self::resolve_path(&fs_dir, path)?;
            fs_dir.list_options(&inp, &opts)
        }
    }

    fn resolve_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    fn resolve_path_by_glob_pattern(fs_dir: &FsDir, path: &str) -> CommonResult<Vec<InodePath>> {
        InodePath::resolve_for_glob_pattern(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    pub fn check_path_length(&self, path: &str) -> CommonResult<()> {
        if path.len() > self.conf.max_path_len {
            return err_box!(
                "create: Path too long, limit {} characters",
                self.conf.max_path_len
            );
        }

        let depth = path.split(PATH_SEPARATOR).count();
        if depth > self.conf.max_path_depth {
            return err_box!(
                "create: Path too long, limit {} levels",
                self.conf.max_path_depth
            );
        }

        Ok(())
    }

    pub fn validate_add_block(
        file: &InodeFile,
        client_addr: &ClientAddress,
        previous: Option<&CommitBlock>,
    ) -> FsResult<ValidateAddBlock> {
        if let Some(v) = previous {
            if v.block_len != file.block_size as i64 {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    file.block_size,
                    v.block_len
                );
            }
        }

        let res = ValidateAddBlock {
            replicas: file.replicas as u16,
            block_size: file.block_size as i64,
            storage_policy: file.storage_policy.clone(),
            client_host: client_addr.hostname.clone(),
        };

        Ok(res)
    }

    pub fn choose_worker(
        &self,
        inp: &InodePath,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        let file = inode.as_file_mut()?;
        self.choose_worker_for_file(file, client_addr, exclude_workers)
    }

    pub fn choose_worker_for_file(
        &self,
        file: &InodeFile,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let wm = self.worker_manager.read();
        let validate_block = Self::validate_add_block(file, &client_addr, None)?;
        let choose_ctx = ChooseContext::with_block(validate_block, exclude_workers);
        Ok(wm.choose_worker(choose_ctx)?)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        self.worker_manager
            .read()
            .create_locate_block(path, block, locs)
    }

    pub fn resolve_file_inode(
        fs_dir: &FsDir,
        path: &str,
        inode_id: Option<i64>,
    ) -> FsResult<InodePtr> {
        match inode_id {
            Some(v) if v > 0 => match fs_dir.store.get_inode(v, None)? {
                Some(view) => Ok(InodePtr::from_owned(view)),
                None => err_ext!(FsError::file_not_found(path).ctx(format!("inode_id={}", v))),
            },

            _ => {
                let inp = Self::resolve_path(fs_dir, path)?;
                match inp.task_last() {
                    Some(ptr) => Ok(ptr),
                    None => err_ext!(FsError::file_not_found(path)),
                }
            }
        }
    }

    /// Document application to allocate a new block.
    #[allow(clippy::too_many_arguments)]
    pub fn add_block<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        let file = inode.as_file_ref()?;

        // File allows concurrent writes, 'previous' is the previous block,
        // need to check if the next block has already been allocated。
        // If it has been allocated, return that block
        if let Some(next) = file.search_next_block(last_block.map(|v| v.id)) {
            let locs = fs_dir.get_block_locations(next.id)?;
            let extend_block = ExtendedBlock {
                id: next.id,
                len: next.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: next.alloc_opts.clone(),
            };

            return self.create_locate_block(path, extend_block, &locs);
        }

        let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block =
            fs_dir.acquire_new_block(path, inode, commit_blocks, &choose_workers, file_len)?;
        let located = LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        };

        Ok(located)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<Option<FileBlocks>> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush,
                return_file_blocks: true,
                set_attr_opts,
            },
        )
    }

    /// Flushes file metadata without building the full block-location snapshot.
    pub fn flush_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
    ) -> FsResult<()> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush: true,
                return_file_blocks: false,
                set_attr_opts: None,
            },
        )
        .map(|_| ())
    }

    fn complete_file0<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        options: CompleteFileOptions,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        fs_dir.complete_file(
            path,
            &mut inode,
            len,
            commit_blocks,
            client_name,
            options.only_flush,
            options.set_attr_opts,
        )?;

        if options.only_flush && options.return_file_blocks {
            let file = inode.as_file_ref()?;
            let locs = self.get_block_locs(path, &fs_dir, file)?;
            let status = inode.to_file_status(path)?;
            return Ok(Some(FileBlocks::new(status, locs)));
        }

        Ok(None)
    }

    pub fn get_file_blocks(
        &self,
        path: &str,
        fs_dir: &FsDir,
        inp: &InodePath,
    ) -> FsResult<FileBlocks> {
        let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
        let file = inode.as_file_ref()?;
        let blocks = self.get_block_locs(path, fs_dir, file)?;
        Ok(FileBlocks::new(inode.to_file_status(path)?, blocks))
    }

    fn get_block_locs(
        &self,
        path: &str,
        fs_dir: &FsDir,
        file: &InodeFile,
    ) -> FsResult<Vec<LocatedBlock>> {
        let wm = self.worker_manager.read();
        let file_locs = fs_dir.get_file_locations(file)?;
        let mut block_locs = Vec::with_capacity(file_locs.len());

        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                return err_box!(
                    "block status abnormal, block id {}, block len {}, expected block size {}",
                    meta.id,
                    meta.len(),
                    file.block_size
                );
            }

            let extend_block = ExtendedBlock {
                id: meta.id,
                len: meta.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: meta.alloc_opts.clone(),
            };

            let lc = try_option!(
                file_locs.get(&meta.id),
                "File {}, block {} Lost (no worker can read)",
                path,
                meta.id
            );
            let lb = wm.create_locate_block(path, extend_block, lc)?;
            block_locs.push(lb);
        }

        Ok(block_locs)
    }

    pub fn get_block_locations<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlocks> {
        let fs_dir = self.fs_dir.read();
        let path = path.as_ref();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(path)),
        };
        let file = inode.as_file_ref()?;
        let block_locs = self.get_block_locs(path, &fs_dir, file)?;
        let locate_blocks = FileBlocks {
            status: inode.to_file_status(path)?,
            block_locs,
        };

        Ok(locate_blocks)
    }

    pub fn cv_metadata_snapshot_page(
        &self,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataSnapshotPage> {
        if page_size == 0 {
            return err_box!("cv metadata snapshot page_size must be greater than 0");
        }

        let fs_dir = self.fs_dir.read();
        let epoch = fs_dir.op_id.get();
        let start_after = page_token.filter(|token| !token.is_empty());
        let mut entries = Vec::with_capacity(page_size.saturating_add(1));
        self.collect_cv_metadata_snapshot_page(
            &fs_dir,
            fs_dir.root_dir(),
            "/",
            start_after.as_deref(),
            page_size.saturating_add(1),
            &mut entries,
        )?;

        let next_page_token = if entries.len() > page_size {
            let token = entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.status.path.clone());
            entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataSnapshotPage {
            entries,
            next_page_token,
            epoch,
        })
    }

    pub fn cv_metadata_delta_page(
        &self,
        from_epoch: u64,
        target_epoch: Option<u64>,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataDeltaPage> {
        if page_size == 0 {
            return err_box!("cv metadata delta page_size must be greater than 0");
        }

        let fs_dir = self.fs_dir.read();
        let current_epoch = fs_dir.op_id.get();
        let to_epoch = target_epoch.unwrap_or(current_epoch);
        if to_epoch > current_epoch {
            return err_box!(
                "cv metadata delta target_epoch {} is newer than current epoch {}",
                to_epoch,
                current_epoch
            );
        }
        if from_epoch > to_epoch {
            return err_box!(
                "cv metadata delta from_epoch {} is newer than target epoch {}",
                from_epoch,
                to_epoch
            );
        }
        if target_epoch.is_some() && to_epoch < current_epoch {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: true,
            });
        }
        if from_epoch == to_epoch {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: false,
            });
        }

        let Some(changes) = fs_dir
            .journal_writer
            .cv_metadata_changes_since(from_epoch, to_epoch)
        else {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: true,
            });
        };

        let mut changed_paths = BTreeMap::new();
        for change in changes {
            changed_paths
                .entry(change.path)
                .and_modify(|include_subtree| *include_subtree |= change.include_subtree)
                .or_insert(change.include_subtree);
        }

        let mut delta_entries = BTreeMap::new();
        for (path, include_subtree) in changed_paths {
            if include_subtree {
                self.collect_cv_metadata_delta_subtree(&fs_dir, &path, &mut delta_entries)?;
            } else {
                let entry = self.cv_metadata_entry_for_path(&fs_dir, &path)?;
                delta_entries.insert(path, entry);
            }
        }

        let start_after = page_token.filter(|token| !token.is_empty());
        let mut page_entries = Vec::with_capacity(page_size.saturating_add(1));
        for (path, entry) in delta_entries {
            if start_after
                .as_deref()
                .map(|token| path.as_str() <= token)
                .unwrap_or(false)
            {
                continue;
            }
            page_entries.push(CvMetadataDeltaEntry { path, entry });
            if page_entries.len() > page_size {
                break;
            }
        }

        let next_page_token = if page_entries.len() > page_size {
            let token = page_entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.path.clone());
            page_entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataDeltaPage {
            entries: page_entries,
            next_page_token,
            from_epoch,
            to_epoch,
            full_snapshot_required: false,
        })
    }

    fn collect_cv_metadata_delta_subtree(
        &self,
        fs_dir: &FsDir,
        path: &str,
        entries: &mut BTreeMap<String, Option<CvMetadataSnapshotEntry>>,
    ) -> FsResult<()> {
        let Some(entry) = self.cv_metadata_entry_for_path(fs_dir, path)? else {
            entries.insert(path.to_string(), None);
            return Ok(());
        };

        let is_dir = entry.status.is_dir;
        entries.insert(path.to_string(), Some(entry));
        if !is_dir {
            return Ok(());
        }

        let inp = Self::resolve_path(fs_dir, path)?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(());
        };
        let resolved = self.resolve_snapshot_inode(fs_dir, &inode)?;
        if let InodeView::Dir(dir) = resolved {
            for child in dir.children_iter() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_delta_subtree(fs_dir, &child_path, entries)?;
            }
        }
        Ok(())
    }

    fn cv_metadata_entry_for_path(
        &self,
        fs_dir: &FsDir,
        path: &str,
    ) -> FsResult<Option<CvMetadataSnapshotEntry>> {
        let inp = match Self::resolve_path(fs_dir, path) {
            Ok(inp) => inp,
            Err(_) => return Ok(None),
        };
        let Some(inode) = inp.get_last_inode() else {
            return Ok(None);
        };
        let resolved = self.resolve_snapshot_inode(fs_dir, &inode)?;
        let status = resolved.to_file_status(path)?;
        let blocks = if let Ok(file) = resolved.as_file_ref() {
            Some(FileBlocks::new(
                status.clone(),
                self.get_block_locs(path, fs_dir, file)?,
            ))
        } else {
            None
        };
        Ok(Some(CvMetadataSnapshotEntry { status, blocks }))
    }

    fn collect_cv_metadata_snapshot_page(
        &self,
        fs_dir: &FsDir,
        inode: &InodeView,
        path: &str,
        start_after: Option<&str>,
        limit: usize,
        entries: &mut Vec<CvMetadataSnapshotEntry>,
    ) -> FsResult<()> {
        if entries.len() >= limit {
            return Ok(());
        }
        if let Some(token) = start_after {
            if path != "/" && token > path && !snapshot_token_in_subtree(token, path) {
                return Ok(());
            }
        }

        let resolved = self.resolve_snapshot_inode(fs_dir, inode)?;
        if start_after.map(|token| path > token).unwrap_or(true) {
            let status = resolved.to_file_status(path)?;
            let blocks = if let Ok(file) = resolved.as_file_ref() {
                Some(FileBlocks::new(
                    status.clone(),
                    self.get_block_locs(path, fs_dir, file)?,
                ))
            } else {
                None
            };
            entries.push(CvMetadataSnapshotEntry { status, blocks });
            if entries.len() >= limit {
                return Ok(());
            }
        }

        if let InodeView::Dir(dir) = resolved {
            for child in dir.children_iter() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_snapshot_page(
                    fs_dir,
                    child,
                    &child_path,
                    start_after,
                    limit,
                    entries,
                )?;
                if entries.len() >= limit {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn resolve_snapshot_inode(&self, fs_dir: &FsDir, inode: &InodeView) -> FsResult<InodeView> {
        if let InodeView::FileEntry(entry) = inode {
            return fs_dir
                .store
                .get_inode(entry.id, Some(&entry.name))?
                .ok_or_else(|| FsError::file_not_found(entry.name.clone()));
        }
        Ok(inode.clone())
    }

    pub fn filesystem_info(&self) -> FsResult<FilesystemInfo> {
        let metrics = Master::get_metrics()?;
        let mut info = FilesystemInfo {
            inode_dir_num: metrics.inode_dir_num.get(),
            inode_file_num: metrics.inode_file_num.get(),
            ..Default::default()
        };

        let wm = self.worker_manager.read();

        // Requests can only reach active master
        info.active_master = wm.conf.master_addr().to_string();
        for peer in &wm.conf.journal.journal_addrs {
            info.journal_nodes.push(peer.to_string())
        }

        for (_, worker) in wm.worker_map.workers() {
            info.capacity += worker.capacity;
            info.available += worker.available;
            info.fs_used += worker.fs_used;
            info.non_fs_used += worker.non_fs_used;
            info.reserved_bytes += worker.reserved_bytes;
            info.block_num += worker.block_num;

            match worker.status {
                WorkerStatus::Live => {
                    info.live_workers.push(worker.clone());
                    // Only Live workers are eligible for new allocations, so the
                    // allocatable view mirrors the allocation policy. Failed
                    // storage dirs are already excluded from worker.capacity.
                    info.allocatable_capacity += worker.capacity;
                    info.allocatable_available += worker.available;
                }
                WorkerStatus::Blacklist => info.blacklist_workers.push(worker.clone()),
                WorkerStatus::Decommission => info.decommission_workers.push(worker.clone()),
                _ => (),
            }
        }

        for (_, worker) in wm.worker_map.lost_workers() {
            info.lost_workers.push(worker.clone());
        }

        Ok(info)
    }

    pub fn fs_dir(&self) -> ArcRwLock<FsDir> {
        self.fs_dir.clone()
    }

    // Add a test worker and unit tests will use it.
    pub fn add_test_worker(&self, worker: WorkerInfo) {
        let mut wm = self.worker_manager.write();
        wm.add_test_worker(worker);
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let fs_dir = self.fs_dir.read();
        fs_dir.sum_hash()
    }

    pub fn last_inode_id(&self) -> i64 {
        let fs_dir = self.fs_dir.read();
        fs_dir.last_inode_id()
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        let fs_dir = self.fs_dir.read();
        fs_dir.get_file_counts()
    }

    // Create a directory number based on rocksdb data for testing.
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        let fs_dir = self.fs_dir.read();
        fs_dir.create_tree()
    }

    // Restore in-memory tree from RocksDB (for testing without Raft).
    // In production, Raft automatically restores via apply_snapshot().
    pub fn restore_from_rocksdb(&self) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        fs_dir.restore_from_rocksdb()
    }

    /// Serialize one worker's restart, reports, and cleanup without holding the
    /// global filesystem lock across its entire inventory.
    pub(crate) fn worker_lifecycle_lock(&self, worker_id: u32) -> Arc<Mutex<()>> {
        self.worker_lifecycle_locks
            .lock()
            .entry(worker_id)
            .or_default()
            .clone()
    }

    fn collect_full_block_report(
        &self,
        list: &BlockReportList,
        updates: &[(i64, BlockReportStatus)],
    ) -> Option<HashSet<i64>> {
        if !list.full_report {
            return None;
        }

        let now = LocalTime::mills();
        let mut reports = self.full_block_reports.lock();
        reports.retain(|_, report| {
            now.saturating_sub(report.update_time_ms) <= FULL_BLOCK_REPORT_TTL_MS
        });
        let report = reports
            .entry(list.worker_id)
            .or_insert_with(|| FullBlockReportState {
                total_len: list.total_len,
                update_time_ms: now,
                reported_blocks: HashSet::with_capacity(list.total_len as usize),
                deleted_blocks: HashSet::new(),
                incremental_updates: HashMap::new(),
            });

        if report.total_len != list.total_len {
            warn!(
                "full block report for worker {} restarted because total_len changed from {} to {}; discarding {} accumulated block ids",
                list.worker_id,
                report.total_len,
                list.total_len,
                report.reported_blocks.len()
            );
            report.total_len = list.total_len;
            report.reported_blocks.clear();
            report.reported_blocks.reserve(list.total_len as usize);
            report.deleted_blocks.clear();
            report.incremental_updates.clear();
        }
        report.update_time_ms = now;
        for &(id, status) in updates {
            report.reported_blocks.insert(id);
            if status == BlockReportStatus::Deleted {
                report.deleted_blocks.insert(id);
            }
        }

        // Incremental updates must not count toward the full inventory length.
        // Apply them only once all chunks of the original snapshot have arrived.
        if report.reported_blocks.len() as u64 >= report.total_len {
            let report = reports.remove(&list.worker_id)?;
            let mut blocks = report.reported_blocks;
            for id in report.deleted_blocks {
                blocks.remove(&id);
            }
            for (id, present) in report.incremental_updates {
                if present {
                    blocks.insert(id);
                } else {
                    blocks.remove(&id);
                }
            }
            Some(blocks)
        } else {
            None
        }
    }

    fn merge_incremental_block_report(&self, worker_id: u32, updates: &[(i64, BlockReportStatus)]) {
        if let Some(report) = self.full_block_reports.lock().get_mut(&worker_id) {
            report.update_time_ms = LocalTime::mills();
            for &(id, status) in updates {
                report
                    .incremental_updates
                    .insert(id, status != BlockReportStatus::Deleted);
            }
        }
        if let Some(job) = self
            .full_block_reconciles
            .lock()
            .get_mut(&worker_id)
            .and_then(|state| state.pending.as_mut())
        {
            for &(id, status) in updates {
                if status == BlockReportStatus::Deleted {
                    job.reported_blocks.remove(&id);
                } else {
                    job.reported_blocks.insert(id);
                }
            }
        }
    }

    pub fn reset_full_block_report(&self, worker_id: u32) {
        self.full_block_reports.lock().remove(&worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    fn invalidate_full_block_reconcile(&self, worker_id: u32) {
        let mut reconciles = self.full_block_reconciles.lock();
        if let Some(state) = reconciles.get_mut(&worker_id) {
            state.generation = state.generation.saturating_add(1);
            state.pending = None;
            if !state.running {
                reconciles.remove(&worker_id);
            }
        }
    }

    /// Process block reports
    pub fn block_report(
        &self,
        mut list: BlockReportList,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<BlockReportResult> {
        let lifecycle = self.worker_lifecycle_lock(list.worker_id);
        let _lifecycle = lifecycle.lock();
        {
            let mut wm = self.worker_manager.write();
            if !wm.block_report_session_matches(list.worker_id, &list.worker_session_id) {
                return err_box!(
                    "block report from a stale worker session: {}",
                    list.worker_id
                );
            }
            wm.ensure_report_cleanup(list.worker_id);
        }

        let updates: Vec<_> = list
            .blocks
            .iter()
            .map(|block| (block.id, block.status))
            .collect();
        let worker_id = list.worker_id;
        let full_report = list.full_report;
        let reconciling = full_report
            || self.full_block_reports.lock().contains_key(&worker_id)
            || self.full_block_reconciles.lock().contains_key(&worker_id);
        if reconciling {
            // Deleted updates can remove a location before the full inventory's
            // comparison sees it. Retain those IDs for cache invalidation too.
            self.lost_worker_cleanups
                .lock()
                .entry(worker_id)
                .or_default()
                .affected_block_ids
                .extend(updates.iter().filter_map(|&(id, status)| {
                    (status == BlockReportStatus::Deleted).then_some(id)
                }));
        }
        let mut delete_blocks = Vec::new();
        let mut blocks = std::mem::take(&mut list.blocks).into_iter();
        loop {
            let chunk: Vec<_> = blocks
                .by_ref()
                .take(Self::BLOCK_REPORT_WRITE_CHUNK)
                .collect();
            if chunk.is_empty() {
                break;
            }
            let chunk_updates: Vec<_> =
                chunk.iter().map(|block| (block.id, block.status)).collect();
            let deleted: Vec<_> = chunk
                .iter()
                .filter(|block| block.status == BlockReportStatus::Deleted)
                .map(|block| block.id)
                .collect();
            crate::fault_point! {
                sync,
                name: "master.block_report.before_apply_chunk",
                description: "Fail a block report before applying one bounded location chunk",
                context: {
                    "worker_id" => worker_id,
                    "chunk_first_block_id" => chunk[0].id,
                },
                return_error: |fault| Err(FsError::common(fault.message)),
            }
            let obsolete =
                self.fs_dir
                    .write()
                    .apply_reported_blocks(worker_id, full_report, chunk)?;
            if !full_report {
                // A later chunk can fail after these writes committed. Preserve
                // every successful update in the queued inventory immediately.
                self.merge_incremental_block_report(worker_id, &chunk_updates);
            }
            let mut wm = self.worker_manager.write();
            for id in deleted {
                wm.deleted_block(worker_id, id);
            }
            for id in &obsolete {
                wm.remove_block(worker_id, *id);
            }
            delete_blocks.extend(obsolete);
        }

        // Do not acknowledge full-inventory progress before all location writes
        // succeed. Retried full chunks are deduplicated by the accumulator.
        if let Some(reported_blocks) = self.collect_full_block_report(&list, &updates) {
            self.submit_full_block_reconcile(
                worker_id,
                list.worker_session_id,
                reported_blocks,
                replication_handler,
            )?;
        }
        Ok(BlockReportResult { delete_blocks })
    }

    fn submit_full_block_reconcile(
        &self,
        worker_id: u32,
        worker_session_id: String,
        reported_blocks: HashSet<i64>,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<()> {
        let should_spawn = {
            let mut reconciles = self.full_block_reconciles.lock();
            let state = reconciles
                .entry(worker_id)
                .or_insert_with(|| FullBlockReconcileState {
                    running: false,
                    retry_at_ms: 0,
                    generation: 0,
                    pending: None,
                });
            state.generation = state.generation.saturating_add(1);
            state.retry_at_ms = 0;
            state.pending = Some(FullBlockReconcileJob {
                generation: state.generation,
                reported_blocks,
                worker_session_id,
                replication_handler,
            });
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if should_spawn {
            self.spawn_full_block_reconcile(worker_id)?;
        }
        Ok(())
    }

    fn full_block_reconcile_retry_at(&self) -> u64 {
        LocalTime::mills().saturating_add(self.conf.worker_check_interval_ms().max(1))
    }

    #[cfg(test)]
    pub(crate) fn full_block_reconcile_executor_for_test(&self) -> Arc<GroupExecutor> {
        self.full_block_reconcile_executor.clone()
    }

    fn spawn_full_block_reconcile(&self, worker_id: u32) -> FsResult<()> {
        let fs = self.clone();
        let res = self
            .full_block_reconcile_executor
            .fixed_try_spawn(worker_id as i64, move || {
                fs.run_full_block_reconcile(worker_id);
            });
        if let Err(e) = &res {
            // The queued inventory remains retryable, including a newer report
            // that may have replaced the job while submission was attempted.
            if let Some(state) = self.full_block_reconciles.lock().get_mut(&worker_id) {
                state.running = false;
                state.retry_at_ms = self.full_block_reconcile_retry_at();
            }
            error!("submit full block report reconcile for worker {worker_id} failed: {e}");
        }
        res?;
        Ok(())
    }

    pub(crate) fn retry_full_block_reconciles(&self, now_ms: u64) {
        let workers: Vec<_> = {
            let mut reconciles = self.full_block_reconciles.lock();
            reconciles
                .iter_mut()
                .filter_map(|(&worker_id, state)| {
                    if !state.running && state.pending.is_some() && now_ms >= state.retry_at_ms {
                        state.running = true;
                        Some(worker_id)
                    } else {
                        None
                    }
                })
                .collect()
        };
        for worker_id in workers {
            // A full executor queue is retried at the next check interval.
            let _ = self.spawn_full_block_reconcile(worker_id);
        }
    }

    fn run_full_block_reconcile(&self, worker_id: u32) {
        loop {
            let lifecycle = self.worker_lifecycle_lock(worker_id);
            let _lifecycle = lifecycle.lock();
            let job = {
                let mut reconciles = self.full_block_reconciles.lock();
                match reconciles.get_mut(&worker_id) {
                    Some(state) => match state.pending.take() {
                        Some(v) => v,
                        None => {
                            reconciles.remove(&worker_id);
                            return;
                        }
                    },
                    None => return,
                }
            };

            if !self.is_full_block_reconcile_current(worker_id, job.generation)
                || !self
                    .worker_manager
                    .read()
                    .block_report_session_matches(worker_id, &job.worker_session_id)
            {
                info!(
                    "skip stale full block report reconcile for worker {}, generation {}",
                    worker_id, job.generation
                );
                continue;
            }

            match self.reconcile_full_block_report(worker_id, job.generation, &job.reported_blocks)
            {
                Ok(stale_block_ids) => {
                    if !self.is_full_block_reconcile_current(worker_id, job.generation) {
                        continue;
                    }
                    self.worker_manager
                        .write()
                        .complete_worker_block_report(worker_id, &job.worker_session_id);
                    let stale_block_count = stale_block_ids.len();
                    if stale_block_count > 0 {
                        info!(
                            "full block report reconciled {} stale block locations for worker {}",
                            stale_block_count, worker_id
                        );
                        if let Some(replication_handler) = &job.replication_handler {
                            if let Err(e) = replication_handler
                                .report_under_replicated_blocks(worker_id, stale_block_ids)
                            {
                                error!(
                                    "Errors on reporting under-replicated {} blocks from full block report reconciliation. err: {:?}",
                                    stale_block_count, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "full block report reconcile for worker {} failed: {}",
                        worker_id, e
                    );
                    if let Some(state) = self.full_block_reconciles.lock().get_mut(&worker_id) {
                        // Never overwrite an inventory submitted after this job.
                        if state.generation == job.generation && state.pending.is_none() {
                            state.pending = Some(job);
                            state.retry_at_ms = self.full_block_reconcile_retry_at();
                        }
                        state.running = false;
                    }
                    return;
                }
            }
            // Reports for this worker are still fenced by the lifecycle lock.
            // Retire the completed job before an incremental report can mistake
            // its empty state for a reconciliation that still owns cleanup work.
            self.full_block_reconciles.lock().remove(&worker_id);
            drop(_lifecycle);
            #[cfg(all(test, feature = "fault-injection"))]
            partial_report_tests::pause_completed_reconcile_for_test(worker_id);
            return;
        }
    }

    fn reconcile_full_block_report(
        &self,
        worker_id: u32,
        generation: u64,
        reported_blocks: &HashSet<i64>,
    ) -> FsResult<Vec<i64>> {
        let existing_blocks = self.fs_dir.read().get_worker_block_ids(worker_id)?;
        let stale_block_ids = existing_blocks
            .into_iter()
            .filter(|block_id| !reported_blocks.contains(block_id))
            .collect();

        if !self.is_full_block_reconcile_current(worker_id, generation) {
            return Ok(Vec::new());
        }
        // Retained cleanup IDs survive a prior attempt that already removed
        // locations. Re-check all of them before making this worker ready.
        let cleanup = self.cleanup_worker_locations(worker_id, stale_block_ids)?;
        Ok(cleanup.replication_block_ids)
    }

    /// Applies block-report location updates in bounded chunks so the global
    /// fs_dir write lock is held only briefly per chunk. Each entry is an
    /// independent add/remove for one block location, so chunk boundaries do
    /// not break cross-entry invariants.
    fn apply_block_report_batch(&self, batch: Vec<(bool, i64, BlockLocation)>) -> FsResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut iter = batch.into_iter();
        loop {
            let chunk: Vec<_> = iter.by_ref().take(Self::BLOCK_REPORT_WRITE_CHUNK).collect();
            if chunk.is_empty() {
                break;
            }
            let mut fs_dir = self.fs_dir.write();
            fs_dir.block_report(chunk)?;
        }
        Ok(())
    }

    fn is_full_block_reconcile_current(&self, worker_id: u32, generation: u64) -> bool {
        self.full_block_reconciles
            .lock()
            .get(&worker_id)
            .map(|state| state.generation == generation)
            .unwrap_or(false)
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<LostWorkerLocationCleanup> {
        let lifecycle = self.worker_lifecycle_lock(worker_id);
        let _lifecycle = lifecycle.lock();
        let block_ids = self.fs_dir.read().get_worker_block_ids(worker_id)?;
        self.cleanup_worker_locations(worker_id, block_ids)
    }

    /// The caller holds the worker lifecycle lock so registration cannot race
    /// with a cleanup that has already removed locations but still needs retry.
    pub(crate) fn has_pending_worker_cleanup(&self, worker_id: u32) -> bool {
        self.lost_worker_cleanups.lock().contains_key(&worker_id)
    }

    pub(crate) fn delete_lost_worker_locations(
        &self,
        worker: &WorkerInfo,
    ) -> FsResult<LostWorkerLocationCleanup> {
        let lifecycle = self.worker_lifecycle_lock(worker.worker_id());
        let _lifecycle = lifecycle.lock();
        let block_ids = {
            // Follow the filesystem -> worker-manager lock order. The shared
            // lifecycle lock fences registration throughout the batched cleanup.
            let fs_dir = self.fs_dir.read();
            let wm = self.worker_manager.read();
            if !wm.is_lost_worker(worker) {
                return Ok(LostWorkerLocationCleanup::default());
            }
            fs_dir.get_worker_block_ids(worker.worker_id())?
        };
        let cleanup = self.cleanup_worker_locations(worker.worker_id(), block_ids)?;
        self.worker_manager
            .write()
            .finish_offline_worker_cleanup(worker);
        Ok(cleanup)
    }

    fn cleanup_worker_locations(
        &self,
        worker_id: u32,
        block_ids: Vec<i64>,
    ) -> FsResult<LostWorkerLocationCleanup> {
        // Capture the complete affected inventory before deleting any index
        // entries. Exact batched removal updates both indexes atomically; after
        // a failure, the remaining index entries and these IDs allow a retry.
        self.lost_worker_cleanups
            .lock()
            .entry(worker_id)
            .or_default()
            .affected_block_ids
            .extend(block_ids.iter().copied());
        self.apply_block_report_batch(
            block_ids
                .into_iter()
                .map(|id| (false, id, BlockLocation::with_id(worker_id)))
                .collect(),
        )?;
        self.invalidate_lost_worker_cache(worker_id)
    }

    fn invalidate_lost_worker_cache(&self, worker_id: u32) -> FsResult<LostWorkerLocationCleanup> {
        let (affected_ids, mut invalidation_ids, mut invalidated_ids) = {
            let cleanups = self.lost_worker_cleanups.lock();
            let state = cleanups
                .get(&worker_id)
                .expect("cleanup inventory captured");
            (
                state.affected_block_ids.clone(),
                state.invalidation_block_ids.clone(),
                state.invalidated_block_ids.clone(),
            )
        };
        let mut removed_block_ids: Vec<_> = affected_ids.iter().copied().collect();
        removed_block_ids.sort_unstable();

        invalidation_ids.extend(affected_ids.iter().copied());
        let mut invalidation_ids: Vec<_> = invalidation_ids.into_iter().collect();
        invalidation_ids.sort_unstable();
        for chunk in invalidation_ids.chunks(Self::LOST_WORKER_INVALIDATION_CHUNK) {
            crate::fault_point! {
                sync,
                name: "master.cache.before_invalidate_lost_chunk",
                description: "Fail lost-worker cache invalidation after location removal",
                context: {
                    "worker_id" => worker_id,
                    "chunk_first_block_id" => chunk[0],
                },
                return_error: |fault| Err(FsError::common(fault.message)),
            }
            let result = {
                let mut fs_dir = self.fs_dir.write();
                // A previous chunk (or a journal failure after applying it) may
                // already have discarded these IDs. Reconstruct their deletion
                // work from exact membership, never from the inode number alone.
                let mut result = Self::discarded_cleanup_blocks(&fs_dir, chunk)?;
                let mut prepared_ids = HashSet::new();
                let invalidation =
                    fs_dir.invalidate_lost_cache_files(worker_id, chunk, &mut prepared_ids);
                // A journal error can follow the inode update. Retain every
                // discarded file block, including replicas on other workers,
                // before propagating that error to the retry scheduler.
                self.lost_worker_cleanups
                    .lock()
                    .get_mut(&worker_id)
                    .expect("cleanup inventory retained")
                    .invalidation_block_ids
                    .extend(prepared_ids);
                result.extend(invalidation?);
                result
            };
            invalidated_ids.extend(result.invalidated_block_ids.iter().copied());
            self.lost_worker_cleanups
                .lock()
                .get_mut(&worker_id)
                .expect("cleanup inventory retained")
                .invalidated_block_ids
                .extend(result.invalidated_block_ids.iter().copied());

            // Queue each successful chunk immediately: a later chunk can fail
            // after these inodes have already lost their old block IDs.
            let mut wm = self.worker_manager.write();
            wm.remove_blocks(&result.delete_result);
            for id in result.invalidated_block_ids.intersection(&affected_ids) {
                // The removed worker is absent from get_locs() by this point.
                // Its physical copies still need deletion if it returns later.
                wm.remove_block(worker_id, *id);
            }
        }

        let replication_block_ids = removed_block_ids
            .iter()
            .copied()
            .filter(|block_id| !invalidated_ids.contains(block_id))
            .collect();
        self.lost_worker_cleanups.lock().remove(&worker_id);
        Ok(LostWorkerLocationCleanup {
            removed_block_ids,
            replication_block_ids,
        })
    }

    fn discarded_cleanup_blocks(
        fs_dir: &FsDir,
        block_ids: &[i64],
    ) -> FsResult<CacheInvalidationResult> {
        let mut result = CacheInvalidationResult::default();
        let mut current_blocks = HashMap::new();
        for &id in block_ids {
            let inode_id = InodeId::get_id(id);
            if let std::collections::hash_map::Entry::Vacant(entry) = current_blocks.entry(inode_id)
            {
                let ids: HashSet<i64> = match fs_dir.store.get_inode(inode_id, None)? {
                    Some(InodeView::File(file)) => {
                        file.blocks.iter().map(|block| block.id).collect()
                    }
                    _ => HashSet::new(),
                };
                entry.insert(ids);
            }
            if !current_blocks[&inode_id].contains(&id) {
                result.invalidated_block_ids.insert(id);
                result
                    .delete_result
                    .blocks
                    .insert(id, fs_dir.get_block_locations(id)?);
            }
        }
        Ok(result)
    }

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        fs_dir.set_attr(inp, opts)
    }

    pub fn symlink<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
    ) -> FsResult<()> {
        self.symlink_with_owner_group(target, link, force, mode, None, None)
    }

    pub fn symlink_with_owner_group<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let target = target.as_ref().to_string();
        let link = Self::resolve_path(&fs_dir, link.as_ref())?;
        fs_dir.symlink(target, link, force, mode, owner, group)
    }

    pub fn link<T: AsRef<str>>(&self, src_path: T, dst_path: T) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let src_path = Self::resolve_path(&fs_dir, src_path.as_ref())?;
        let dst_path = Self::resolve_path(&fs_dir, dst_path.as_ref())?;
        fs_dir.link(src_path, dst_path)
    }

    pub fn resize<T: AsRef<str>>(&self, path: T, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        self.resize_by_id(path, None, opts)
    }

    pub fn resize_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        opts: FileAllocOpts,
    ) -> FsResult<FileBlocks> {
        opts.validate()?;

        let path = path.as_ref();
        // This snapshot only rejects individually impossible requests; it is not a
        // reservation, so concurrent fallocates may observe the same capacity.
        // Worker-side block allocation remains the hard enforcement point.
        let available = if opts.truncate {
            i64::MAX
        } else {
            self.worker_manager.read().available_bytes()
        };

        let (del_res, inode_id) = {
            let mut fs_dir = self.fs_dir.write();
            let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
            let file = inode.as_file_ref()?;
            Self::validate_alloc_capacity(file.len, file.replicas, &opts, available)?;
            let inode_id = inode.id();
            let del_res = fs_dir.resize_inode(path, &mut inode, opts)?;
            (del_res, inode_id)
        };

        if !del_res.blocks.is_empty() {
            self.worker_manager.write().remove_blocks(&del_res);
        }

        let blocks = {
            let fs_dir = self.fs_dir.read();
            let inode = Self::resolve_file_inode(&fs_dir, path, Some(inode_id))?;
            let file = inode.as_file_ref()?;
            let locs = self.get_block_locs(path, &fs_dir, file)?;
            FileBlocks::new(inode.to_file_status(path)?, locs)
        };

        Ok(blocks)
    }

    pub fn assign_worker<T: AsRef<str>>(
        &self,
        path: T,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        self.assign_worker_by_id(path, None, block, client_addr, exclude_workers)
    }

    pub fn assign_worker_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;

        let choose_workers =
            self.choose_worker_for_file(inode.as_file_ref()?, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block = fs_dir.assign_worker_inode(path, &mut inode, block.id, &choose_workers)?;

        Ok(LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        })
    }

    pub fn get_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();

        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path)?;
        let expire_ms = self.conf.lock_expire_time_ms();

        fs_dir.get_lock(inp, &lock, expire_ms)
    }

    pub fn set_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();

        let fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        fs_dir.set_lock(inp, lock, self.conf.lock_expire_time_ms())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_core_error::ErrorExt;
    use curvine_error::ErrorKind;
    use curvine_runtime::common::Utils;

    fn test_fs(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn assert_file_not_found_roundtrip(err: &FsError) {
        assert!(
            matches!(err.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound, got {:?}",
            err.kind()
        );
        let decoded = FsError::decode(err.encode());
        assert!(
            matches!(decoded.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound after encode/decode, got {:?}",
            decoded.kind()
        );
        assert!(
            matches!(decoded, FsError::FileNotFound(_)),
            "decoded error collapsed away from FileNotFound: {}",
            decoded
        );
    }

    #[test]
    fn fallocate_rejects_growth_larger_than_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        let err = MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 359).unwrap_err();
        assert!(matches!(err, FsError::DiskOutOfSpace(_)));
    }

    #[test]
    fn fallocate_accepts_exact_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 360).is_ok());
    }

    #[test]
    fn truncate_growth_does_not_require_physical_capacity() {
        let opts = FileAllocOpts::with_truncate(200);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 0).is_ok());
    }

    #[test]
    fn resolve_file_inode_missing_inode_id_returns_file_not_found() {
        let fs = test_fs("missing-inode-id");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        let missing_id = 9_999_999_i64;
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, "/missing", Some(missing_id))
            .unwrap_err();

        assert_file_not_found_roundtrip(&err);
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            msg
        );
    }

    #[test]
    fn resolve_file_inode_unresolved_path_returns_file_not_found() {
        let fs = test_fs("unresolved-path");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        let path = "/does/not/exist";
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, path, None).unwrap_err();

        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string().contains(path),
            "expected path in error, got: {}",
            err
        );
    }

    #[test]
    fn get_inode_path_follows_parent_links() {
        let fs = test_fs("get-inode-path");
        let status = fs.create("/dir/file.log", true).unwrap();
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        assert_eq!(fs_dir.get_inode_path(status.id).unwrap(), "/dir/file.log");
    }

    #[test]
    fn get_inode_path_after_rename_returns_new_path() {
        let fs = test_fs("get-inode-path-rename");
        let status = fs.create("/dir/old.log", true).unwrap();
        fs.rename("/dir/old.log", "/dir/new.log", RenameFlags::empty())
            .unwrap();

        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        assert_eq!(fs_dir.get_inode_path(status.id).unwrap(), "/dir/new.log");
    }

    #[test]
    fn file_status_by_id_after_rename_resolves_inode_keeps_request_path() {
        // By-id status resolves the inode (id/name) but keeps the request path.
        let fs = test_fs("file-status-by-id-stale-path");
        let created = fs.create("/a/old.log", true).unwrap();
        fs.rename("/a/old.log", "/a/new.log", RenameFlags::empty())
            .unwrap();

        let status = fs
            .file_status_by_id("/a/old.log", Some(created.id))
            .unwrap();
        assert_eq!(status.id, created.id);
        assert_eq!(status.path, "/a/old.log");
        assert_eq!(status.name, "new.log");

        // Path-based status still requires the live path.
        let by_path = fs.file_status("/a/new.log").unwrap();
        assert_eq!(by_path.id, created.id);
        assert_eq!(by_path.path, "/a/new.log");

        // Write path still journals/returns the request path (no hot-path rebuild).
        let blocks = fs
            .resize_by_id(
                "/a/old.log",
                Some(created.id),
                FileAllocOpts::with_truncate(128),
            )
            .unwrap();
        assert_eq!(blocks.status.id, created.id);
        assert_eq!(blocks.status.path, "/a/old.log");
        assert_eq!(blocks.status.name, "new.log");
        assert_eq!(blocks.status.len, 128);
    }

    #[test]
    fn file_status_by_id_missing_inode_returns_file_not_found() {
        let fs = test_fs("file-status-missing-inode");
        let missing_id = 9_999_999_i64;
        let err = fs
            .file_status_by_id("/missing.log", Some(missing_id))
            .unwrap_err();
        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string()
                .contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            err
        );
    }

    #[test]
    fn complete_file_by_id_keeps_request_path() {
        let fs = test_fs("complete-by-id-stale-path");
        let created = fs.create("/b/old.log", true).unwrap();
        fs.rename("/b/old.log", "/b/new.log", RenameFlags::empty())
            .unwrap();

        let blocks = fs
            .complete_file(
                "/b/old.log",
                Some(created.id),
                64,
                vec![],
                "test-client",
                true,
                None,
            )
            .unwrap()
            .expect("only_flush should return FileBlocks");

        assert_eq!(blocks.status.id, created.id);
        assert_eq!(blocks.status.path, "/b/old.log");
        assert_eq!(blocks.status.name, "new.log");
    }

    fn worker_with_status(
        id: u32,
        status: WorkerStatus,
        capacity: i64,
        available: i64,
    ) -> WorkerInfo {
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            1,
        );
        worker.status = status;
        worker.capacity = capacity;
        worker.available = available;
        worker
    }

    #[test]
    fn filesystem_info_allocatable_only_counts_live_workers() {
        // See issue #1460: statfs must report capacity eligible for new writes,
        // i.e. only Live workers. Blacklist/Decommission workers must not
        // contribute to the allocatable view (they still count toward the total
        // physical capacity).
        let fs = test_fs("allocatable-live-only");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.add_test_worker(worker_with_status(2, WorkerStatus::Blacklist, 2000, 1500));
        fs.add_test_worker(worker_with_status(
            3,
            WorkerStatus::Decommission,
            3000,
            2500,
        ));

        let info = fs.filesystem_info().unwrap();

        // Total physical capacity across all non-lost worker states.
        assert_eq!(info.capacity, 6000, "total capacity should sum all workers");
        assert_eq!(
            info.available, 4800,
            "total available should sum all workers"
        );
        assert_eq!(info.live_workers.len(), 1);
        assert_eq!(info.blacklist_workers.len(), 1);
        assert_eq!(info.decommission_workers.len(), 1);

        // Allocatable view mirrors allocation eligibility: Live workers only.
        assert_eq!(
            info.allocatable_capacity, 1000,
            "allocatable capacity must be Live workers only"
        );
        assert_eq!(
            info.allocatable_available, 800,
            "allocatable available must be Live workers only"
        );
    }

    #[test]
    fn filesystem_info_allocatable_excludes_failed_storage_dirs() {
        // A Live worker with a healthy dir plus a failed dir: add_storage already
        // skips failed dirs, so neither total nor allocatable capacity should
        // include the failed dir's bytes.
        let fs = test_fs("allocatable-excludes-failed");
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: 10,
                ..Default::default()
            },
            1,
        );
        worker.status = WorkerStatus::Live;
        worker.add_storage(StorageInfo {
            dir_id: 1,
            storage_id: "healthy".into(),
            failed: false,
            capacity: 1000,
            available: 800,
            ..Default::default()
        });
        worker.add_storage(StorageInfo {
            dir_id: 2,
            storage_id: "failed".into(),
            failed: true,
            capacity: 500,
            available: 400,
            ..Default::default()
        });
        fs.add_test_worker(worker);

        let info = fs.filesystem_info().unwrap();

        assert_eq!(
            info.capacity, 1000,
            "failed dir must not count toward total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(info.allocatable_capacity, 1000);
        assert_eq!(info.allocatable_available, 800);
    }

    #[test]
    fn filesystem_info_allocatable_excludes_lost_workers() {
        // Lost workers live in a separate map and must not contribute to either
        // the total or the allocatable view. Insert a lost worker directly into
        // the lost map (add_test_worker only touches the live map) and assert its
        // capacity never leaks into aggregation.
        let fs = test_fs("allocatable-excludes-lost");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.worker_manager
            .write()
            .worker_map
            .lost_workers
            .insert(99, worker_with_status(99, WorkerStatus::Lost, 9999, 9999));

        let info = fs.filesystem_info().unwrap();

        // The lost worker is reported but its bytes stay out of both views.
        assert_eq!(info.lost_workers.len(), 1);
        assert_eq!(
            info.capacity, 1000,
            "lost worker capacity must not leak into total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(
            info.allocatable_capacity, 1000,
            "lost worker capacity must not leak into allocatable"
        );
        assert_eq!(info.allocatable_available, 800);
    }
}

#[cfg(all(test, feature = "fault-injection"))]
#[path = "lost_worker_cleanup_tests.rs"]
mod lost_worker_cleanup_tests;

#[cfg(all(test, feature = "fault-injection"))]
#[path = "partial_report_tests.rs"]
mod partial_report_tests;
