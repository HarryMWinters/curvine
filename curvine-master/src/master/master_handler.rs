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

use crate::master::fs::{FsRetryCache, MasterFilesystem, OperationStatus};
use crate::master::job::JobHandler;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::MountManager;
use crate::master::{Master, MasterMetrics, RpcContext};
use curvine_config::ClusterConf;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_fs_api::RpcCode;
use curvine_model::ProtoUtils;
use curvine_model::{
    CompatibilityMode, CompatibilityPolicy, CompatibilityVerdict, CreateFileOpts, DeleteBlockCmd,
    DeleteResult, FileBlocks, FileStatus, FilesystemInfo, FreeResult, HeartbeatStatus, ListOptions,
    OpenFlags, RenameFlags, WorkerCommand, WorkerInfo,
};
use curvine_net::net::ConnState;
use curvine_proto::*;
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::Message;
use curvine_runtime::runtime::{GroupExecutor, Runtime};
use dashmap::DashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use tokio::sync::oneshot;

pub struct MasterHandler {
    pub(crate) fs: MasterFilesystem,
    pub(crate) retry_cache: Option<FsRetryCache>,
    pub(crate) metrics: &'static MasterMetrics,
    pub(crate) audit_logging_enabled: bool,
    pub(crate) conn_state: Option<ConnState>,
    pub(crate) job_handler: JobHandler,
    pub(crate) mount_manager: Arc<MountManager>,
    pub(crate) control_rpc_executor: Arc<GroupExecutor>,
    pub(crate) replication_handler: Option<MasterReplicationHandler>,
    pub(crate) actor_rt: Arc<Runtime>,
    // Master's own version + compatibility contract, built once at startup.
    // GetFilesystemInfo backs statfs and is called frequently, so we reuse
    // this instead of recomputing component_version() on every call.
    master_compatibility: ServerCompatibilityInfoProto,
    // Compatibility policy derived from the master configuration. Used to
    // evaluate worker heartbeats and client handshakes with diagnose/enforce
    // semantics (lenient diagnose by default).
    compatibility_policy: CompatibilityPolicy,
    // Last compatibility verdict warned about per peer (worker id / client
    // address). Diagnose-mode warnings are deduped so a persistently
    // incompatible or legacy peer does not spam identical warnings on every
    // heartbeat or statfs call.
    compat_warned: DashMap<String, CompatibilityVerdict>,
}

impl MasterHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conf: &ClusterConf,
        fs: MasterFilesystem,
        retry_cache: Option<FsRetryCache>,
        conn_state: Option<ConnState>,
        mount_manager: Arc<MountManager>,
        job_handler: JobHandler,
        control_rpc_executor: Arc<GroupExecutor>,
        replication_manager: Arc<MasterReplicationManager>,
        actor_rt: Arc<Runtime>,
        metrics: &'static MasterMetrics,
    ) -> Self {
        metrics.active_connections.inc();
        // Build the master's compatibility payload once; GetFilesystemInfo can
        // be hot (statfs) and the underlying version metadata is immutable.
        let master_version = curvine_sys::version::component_version("master");
        // The advertised contract and the enforcement policy both come from
        // the configured compatibility section; defaults are lenient diagnose
        // with no bounds, so old components are never rejected by default.
        let compatibility_policy = conf.master.compatibility.to_policy();
        let master_compatibility =
            ProtoUtils::compatibility_to_pb(&master_version, &compatibility_policy);
        Self {
            fs,
            retry_cache,
            metrics,
            audit_logging_enabled: conf.master.audit_logging_enabled,
            conn_state,
            mount_manager,
            job_handler,
            control_rpc_executor,
            replication_handler: Some(MasterReplicationHandler::new(replication_manager)),
            actor_rt,
            master_compatibility,
            compatibility_policy,
            compat_warned: DashMap::new(),
        }
    }

    pub fn get_req_cache(&self, id: i64) -> Option<OperationStatus> {
        if let Some(ref c) = self.retry_cache {
            c.get(&id)
        } else {
            None
        }
    }

    pub fn set_req_cache<T>(&self, id: i64, res: FsResult<T>) -> FsResult<T> {
        if let Some(ref c) = self.retry_cache {
            c.set_status(id, res.is_ok())
        }
        res
    }

    pub fn check_is_retry(&self, id: i64) -> FsResult<bool> {
        if let Some(ref c) = self.retry_cache {
            c.check_is_retry(id)
        } else {
            Ok(false)
        }
    }

    pub fn mkdir(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: MkdirRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::mkdir_opts_from_pb(header.opts);
        let status = self.fs.mkdir_with_opts(&header.path, opts)?;
        let rep_header = MkdirResponse {
            status: ProtoUtils::file_status_to_pb(status),
            ..Default::default()
        };
        ctx.response(rep_header)
    }

    fn create_file0(
        &self,
        req_id: i64,
        path: String,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        if self.check_is_retry(req_id)? {
            // HDFS retries return the results of the last calculation
            // Alluxio Retry will re-query the status.
            // The same solution as alluxio is adopted here. In fact, the hdfs solution is better, but rust requires an additional memory copy to achieve it.
            // Re-querying the file status may cause the request to be unidempotent.
            return self.fs.file_status(&path);
        }

        let res = self.fs.create_with_opts(&path, opts, flags);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_create_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CreateFileRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::create_opts_from_pb(header.opts);
        let flags = OpenFlags::new(header.flags);
        let status = self.create_file0(ctx.msg.req_id(), header.path, opts, flags)?;

        let rep_header = CreateFileResponse {
            file_status: ProtoUtils::file_status_to_pb(status),
        };
        ctx.response(rep_header)
    }

    pub fn retry_check_open_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: OpenFileRequest = ctx.parse_header()?;

        let opts = ProtoUtils::create_opts_from_pb(header.opts);
        let flags = OpenFlags::new(header.flags);
        let audit_path = format!("{}:{}", flags.access_mark(), header.path);
        ctx.set_audit(Some(audit_path), None);

        let file_blocks = self.open_file0(ctx.msg.req_id(), header.path, opts, flags)?;

        let rep_header = OpenFileResponse {
            file_blocks: ProtoUtils::file_blocks_to_pb(file_blocks),
        };
        ctx.response(rep_header)
    }

    fn open_file0(
        &self,
        req_id: i64,
        path: String,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        if flags.read_only() {
            return self.fs.get_block_locations(&path);
        }

        if self.check_is_retry(req_id)? {
            return self.fs.get_block_locations(&path);
        }

        let res = self.fs.open_file(path, opts, flags);
        self.set_req_cache(req_id, res)
    }

    pub fn file_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: GetFileStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let status = self
            .fs
            .file_status_by_id(header.path.as_str(), header.inode_id)?;
        let rep_header = GetFileStatusResponse {
            status: ProtoUtils::file_status_to_pb(status),
        };

        ctx.response(rep_header)
    }

    pub fn exists(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ExistsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let exists = self.fs.exists(&header.path)?;
        let rep_header = ExistsResponse { exists };
        ctx.response(rep_header)
    }

    pub fn delete0(&self, req_id: i64, header: DeleteRequest) -> FsResult<DeleteResult> {
        if self.check_is_retry(req_id)? {
            return Ok(DeleteResult::default());
        }

        let path = Path::from_str(&header.path)?;
        if let Some(info) = self.mount_manager.get_mount_info(&path)? {
            if path.path() == info.cv_path {
                return err_box!("cannot delete mount point root: {}", info.cv_path);
            }
        }

        let res = self.fs.delete(&header.path, header.recursive);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_delete(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: DeleteRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let res = self.delete0(ctx.msg.req_id(), header)?;
        let rep_header = DeleteResponse {
            res: Some(ProtoUtils::delete_res_to_pb(res)),
        };
        ctx.response(rep_header)
    }

    pub fn retry_check_free(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: FreeRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let res = self.free0(ctx.msg.req_id(), header)?;
        ctx.response(FreeResponse {
            res: ProtoUtils::free_res_to_pb(res),
        })
    }

    pub fn free0(&self, req_id: i64, header: FreeRequest) -> FsResult<FreeResult> {
        if self.check_is_retry(req_id)? {
            return Ok(FreeResult::default());
        }

        let res = self.fs.free(&header.path, header.recursive);
        self.set_req_cache(req_id, res)
    }

    pub fn rename0(&self, req_id: i64, header: RenameRequest) -> FsResult<bool> {
        if self.check_is_retry(req_id)? {
            return Ok(true);
        }
        let flags = RenameFlags::new(header.flags);
        let res = self.fs.rename(&header.src, &header.dst, flags);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_rename(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: RenameRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.src.to_string()), Some(header.dst.to_string()));

        let result = self.rename0(ctx.msg.req_id(), header)?;
        let rep_header = RenameResponse { result };
        ctx.response(rep_header)
    }

    pub fn list_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let list = Self::process_list_status(self.fs.clone(), header.path)?;
        let res = list
            .into_iter()
            .map(ProtoUtils::file_status_to_pb)
            .collect();

        let rep_header = ListStatusResponse { statuses: res };
        ctx.response(rep_header)
    }

    fn process_list_status(fs: MasterFilesystem, path: String) -> FsResult<Vec<FileStatus>> {
        fs.list_status(&path)
    }

    // The add block internally determines whether it is a retry request.
    pub fn add_block(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: AddBlockRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.path.to_string()), None);

        let path = req.path;
        let client_addr = ProtoUtils::client_address_from_pb(req.client_address);
        let commit_blocks = req
            .commit_blocks
            .into_iter()
            .map(ProtoUtils::commit_block_from_pb)
            .collect();

        let last_block = req.last_block.map(ProtoUtils::extend_block_from_pb);
        let located_block = self.fs.add_block(
            path,
            req.inode_id,
            client_addr,
            commit_blocks,
            req.exclude_workers,
            req.file_len,
            last_block,
        )?;
        let rep_header = ProtoUtils::located_block_to_pb(located_block);
        ctx.response(rep_header)
    }

    // Complete_file internally determines whether it is a retry request.
    pub fn complete_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: CompleteFileRequest = ctx.parse_header()?;

        let audit_path = if req.only_flush {
            format!("flush:{}", req.path)
        } else {
            format!("close:{}", req.path)
        };
        ctx.set_audit(Some(audit_path), None);

        let return_file_blocks = req.return_file_blocks.unwrap_or(true);
        let file_blocks = self.complete_file0(req, return_file_blocks)?;
        let rep_header = CompleteFileResponse {
            result: true,
            file_blocks: file_blocks.map(ProtoUtils::file_blocks_to_pb),
        };
        ctx.response(rep_header)
    }

    fn complete_file0(
        &self,
        req: CompleteFileRequest,
        return_file_blocks: bool,
    ) -> FsResult<Option<FileBlocks>> {
        let commit_blocks = req
            .commit_blocks
            .into_iter()
            .map(ProtoUtils::commit_block_from_pb)
            .collect();
        if req.only_flush && !return_file_blocks {
            self.fs.flush_file(
                req.path,
                req.inode_id,
                req.len,
                commit_blocks,
                req.client_name,
            )?;
            Ok(None)
        } else {
            self.fs.complete_file(
                req.path,
                req.inode_id,
                req.len,
                commit_blocks,
                req.client_name,
                req.only_flush,
                req.set_attr_opts.map(ProtoUtils::set_attr_opts_from_pb),
            )
        }
    }

    pub fn create_files_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CreateFilesBatchRequest = ctx.parse_header()?;

        let mut results = Vec::with_capacity(header.requests.len());
        for (index, req) in header.requests.into_iter().enumerate() {
            let opts = ProtoUtils::create_opts_from_pb(req.opts);
            let flags = OpenFlags::new(req.flags);

            // Generate unique req_id for each file in batch
            let unique_req_id = ctx.msg.req_id() + index as i64;
            let status = self.create_file0(unique_req_id, req.path, opts, flags)?;
            results.push(status);
        }

        let rep_header = CreateFilesBatchResponse {
            file_statuses: results
                .into_iter()
                .map(ProtoUtils::file_status_to_pb)
                .collect(),
        };
        ctx.response(rep_header)
    }

    pub fn add_blocks_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: AddBlocksBatchRequest = ctx.parse_header()?;
        let mut results = Vec::with_capacity(header.requests.len());
        for req in header.requests {
            let path = req.path;
            let client_addr = ProtoUtils::client_address_from_pb(req.client_address);
            let commit_blocks = req
                .commit_blocks
                .into_iter()
                .map(ProtoUtils::commit_block_from_pb)
                .collect();

            let last_block = req.last_block.map(ProtoUtils::extend_block_from_pb);
            let located_block = self.fs.add_block(
                path,
                req.inode_id,
                client_addr,
                commit_blocks,
                req.exclude_workers,
                req.file_len,
                last_block,
            )?;
            results.push(ProtoUtils::located_block_to_pb(located_block));
        }

        let rep_header = AddBlocksBatchResponse { blocks: results };
        ctx.response(rep_header)
    }

    pub fn complete_files_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CompleteFilesBatchRequest = ctx.parse_header()?;

        let mut results = Vec::with_capacity(header.requests.len());
        for req in header.requests {
            let result = self.complete_file0(req, false).is_ok();
            results.push(result);
        }

        let rep_header = CompleteFilesBatchResponse { results };
        ctx.response(rep_header)
    }

    pub fn get_block_locations(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetBlockLocationsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.path.to_string()), None);

        let blocks = Self::process_get_block_locations(self.fs.clone(), req.path)?;
        let rep_header = GetBlockLocationsResponse {
            blocks: ProtoUtils::file_blocks_to_pb(blocks),
        };
        ctx.response(rep_header)
    }

    fn process_get_block_locations(fs: MasterFilesystem, path: String) -> FsResult<FileBlocks> {
        fs.get_block_locations(path)
    }

    async fn run_master_rpc_task<T, F>(executor: Arc<GroupExecutor>, task: F) -> FsResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> FsResult<T> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        executor.try_spawn(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(task))
                .unwrap_or_else(|_| err_box!("master control RPC task panicked"));
            let _ = tx.send(result);
        })?;
        rx.await?
    }

    async fn async_get_filesystem_info(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetFilesystemInfoRequest = ctx.parse_header()?;
        // GetFilesystemInfo backs statfs and is called frequently. Only
        // evaluate the compatibility policy when the result can actually be
        // acted upon (enforce mode, configured bounds/blocklist, or the client
        // reported component_info); otherwise a legacy client would hit a
        // MissingInfo verdict and log a warning on every statfs call.
        if self
            .compatibility_policy
            .should_evaluate(req.component_info.is_some())
        {
            Self::check_peer_compatibility(
                "client",
                &format!("client:{}", self.client_ip()),
                &self.compat_warned,
                self.compatibility_policy.mode,
                self.compatibility_policy
                    .check_client(req.component_info.as_ref()),
                self.metrics,
            )?;
        }
        let fs = self.fs.clone();
        let info = Self::run_master_rpc_task(self.control_rpc_executor.clone(), move || {
            Self::process_get_filesystem_info(fs)
        })
        .await?;
        let rep_header = Self::build_filesystem_info_response(info, &self.master_compatibility);
        ctx.response(rep_header)
    }

    /// Build the GetFilesystemInfo response, attaching the master's own version
    /// and the default (lenient) compatibility contract on the reserved 1000+
    /// field range. Legacy clients that do not know the field simply skip it,
    /// so this never breaks older peers. The compatibility payload is built
    /// once at handler construction and reused across requests.
    fn build_filesystem_info_response(
        info: FilesystemInfo,
        master_compatibility: &ServerCompatibilityInfoProto,
    ) -> GetFilesystemInfoResponse {
        let mut rep_header = ProtoUtils::filesystem_info_to_pb(info);
        rep_header.compatibility = Some(master_compatibility.clone());
        rep_header
    }

    async fn async_get_cv_metadata_snapshot_page(
        &self,
        ctx: &mut RpcContext<'_>,
    ) -> FsResult<Message> {
        let req: GetCvMetadataSnapshotPageRequest = ctx.parse_header()?;
        ctx.set_audit(Some("cv-metadata-snapshot".to_string()), None);
        let fs = self.fs.clone();
        let response = Self::run_master_rpc_task(self.control_rpc_executor.clone(), move || {
            let page = fs.cv_metadata_snapshot_page(
                req.page_token,
                req.page_size.unwrap_or(10_000) as usize,
            )?;
            Ok(GetCvMetadataSnapshotPageResponse {
                entries: page
                    .entries
                    .into_iter()
                    .map(|entry| CvMetadataSnapshotEntryProto {
                        status: ProtoUtils::file_status_to_pb(entry.status),
                        blocks: entry.blocks.map(ProtoUtils::file_blocks_to_pb),
                    })
                    .collect(),
                next_page_token: page.next_page_token,
                epoch: page.epoch,
            })
        })
        .await?;
        ctx.response(response)
    }

    async fn async_get_cv_metadata_delta_page(
        &self,
        ctx: &mut RpcContext<'_>,
    ) -> FsResult<Message> {
        let req: GetCvMetadataDeltaPageRequest = ctx.parse_header()?;
        ctx.set_audit(Some("cv-metadata-delta".to_string()), None);
        let fs = self.fs.clone();
        let response = Self::run_master_rpc_task(self.control_rpc_executor.clone(), move || {
            let page = fs.cv_metadata_delta_page(
                req.from_epoch,
                req.target_epoch,
                req.page_token,
                req.page_size.unwrap_or(10_000) as usize,
            )?;
            Ok(GetCvMetadataDeltaPageResponse {
                entries: page
                    .entries
                    .into_iter()
                    .map(|entry| CvMetadataDeltaEntryProto {
                        path: entry.path,
                        entry: entry.entry.map(|entry| CvMetadataSnapshotEntryProto {
                            status: ProtoUtils::file_status_to_pb(entry.status),
                            blocks: entry.blocks.map(ProtoUtils::file_blocks_to_pb),
                        }),
                    })
                    .collect(),
                next_page_token: page.next_page_token,
                from_epoch: page.from_epoch,
                to_epoch: page.to_epoch,
                full_snapshot_required: page.full_snapshot_required,
            })
        })
        .await?;
        ctx.response(response)
    }

    fn process_get_filesystem_info(fs: MasterFilesystem) -> FsResult<FilesystemInfo> {
        fs.filesystem_info()
    }

    pub fn worker_heartbeat(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: WorkerHeartbeatRequest = ctx.parse_header()?;
        // Evaluate the compatibility policy only when the result can actually
        // be acted upon (enforce mode, configured bounds/blocklist, or the
        // worker reported component_info); otherwise a legacy worker would hit
        // a MissingInfo verdict and log a warning on every heartbeat.
        if self
            .compatibility_policy
            .should_evaluate(header.component_info.is_some())
        {
            Self::check_peer_compatibility(
                "worker",
                &format!("worker:{}", header.worker_id),
                &self.compat_warned,
                self.compatibility_policy.mode,
                self.compatibility_policy
                    .check_worker(header.component_info.as_ref()),
                self.metrics,
            )?;
        }
        let cmds = Self::process_worker_heartbeat(self.fs.clone(), header)?;
        let rep_header = WorkerHeartbeatResponse {
            cmds: ProtoUtils::worker_cmd_to_pb(cmds),
        };
        ctx.response(rep_header)
    }

    /// Evaluate a compatibility verdict against the configured mode.
    ///
    /// - `diagnose` (default): log a warning for non-compatible peers and
    ///   allow the request, so old components are never rejected without
    ///   explicit configuration.
    /// - `enforce`: reject with an explicit error describing the actual peer
    ///   version, the expected bound and the upgrade suggestion.
    ///
    /// Diagnose-mode warnings are deduped per peer: a persistently
    /// incompatible or legacy peer (heartbeats run every few seconds, statfs
    /// every call) warns on the first occurrence and again only when its
    /// verdict changes, so repeated identical warnings do not flood
    /// operational logs.
    fn check_peer_compatibility(
        peer: &str,
        dedup_key: &str,
        warned: &DashMap<String, CompatibilityVerdict>,
        mode: CompatibilityMode,
        verdict: CompatibilityVerdict,
        metrics: &MasterMetrics,
    ) -> FsResult<()> {
        // Record the compatibility verdict as a metric. Only one verdict
        // label per peer is active at a time, so set the current label to 1
        // and clear every other label for the peer; otherwise a peer whose
        // verdict changes over time leaves stale series stuck at 1.
        let verdict_label = Self::verdict_label(&verdict);
        let is_worker = dedup_key.starts_with("worker:");
        for label in Self::VERDICT_LABELS {
            let active = if label == verdict_label { 1 } else { 0 };
            if is_worker {
                let worker_id = &dedup_key["worker:".len()..];
                metrics
                    .compat_worker_verdict
                    .with_label_values(&[worker_id, label])
                    .set(active);
            } else {
                metrics
                    .compat_client_verdict
                    .with_label_values(&[dedup_key, label])
                    .set(active);
            }
        }

        if !verdict.rejects(mode) {
            if !verdict.is_compatible() {
                // Warn on the first occurrence and whenever the verdict
                // changes for this peer; suppress identical repeats.
                let changed = warned
                    .get(dedup_key)
                    .map(|last| *last != verdict)
                    .unwrap_or(true);
                if changed {
                    warned.insert(dedup_key.to_string(), verdict.clone());
                    log::warn!("{} compatibility: {}", peer, verdict.describe());
                }
            } else {
                // The peer is compatible again; forget the previous warning so
                // a future incompatibility is surfaced.
                warned.remove(dedup_key);
            }
            return Ok(());
        }
        // Enforce-mode rejection: record the counter and return the error.
        metrics
            .compat_enforce_rejected_total
            .with_label_values(&[peer, verdict_label])
            .inc();
        err_box!(
            "{} rejected by compatibility policy: {}; upgrade the {} or set master.compatibility.mode = \"diagnose\" to allow it",
            peer,
            verdict.describe(),
            peer
        )
    }

    /// All verdict label values for the compat_*_verdict gauge vectors, kept
    /// in sync with [`Self::verdict_label`]. Only one label per peer is active
    /// at a time: recording a verdict sets the current label to 1 and clears
    /// every other label for that peer.
    const VERDICT_LABELS: [&str; 6] = [
        "compatible",
        "missing_info",
        "blocked",
        "protocol_mismatch",
        "version_too_old",
        "version_unknown",
    ];

    /// Short human-readable label for a compatibility verdict, used as a
    /// Prometheus label value in compat_*_verdict and compat_enforce_rejected_total.
    fn verdict_label(verdict: &CompatibilityVerdict) -> &'static str {
        match verdict {
            CompatibilityVerdict::Compatible => "compatible",
            CompatibilityVerdict::MissingInfo => "missing_info",
            CompatibilityVerdict::Blocked(_) => "blocked",
            CompatibilityVerdict::ProtocolMismatch { .. } => "protocol_mismatch",
            CompatibilityVerdict::VersionTooOld { .. } => "version_too_old",
            CompatibilityVerdict::VersionUnknown { .. } => "version_unknown",
        }
    }

    fn process_worker_heartbeat(
        fs: MasterFilesystem,
        header: WorkerHeartbeatRequest,
    ) -> FsResult<Vec<WorkerCommand>> {
        let status = HeartbeatStatus::from(header.status);
        let address = ProtoUtils::worker_address_from_pb(&header.address);
        let lifecycle = fs.worker_lifecycle_lock(address.worker_id);
        let _lifecycle = lifecycle.lock();
        // Worker weight comes from trusted administrator configuration. Preserve the
        // configured u32 value so the master does not silently alter allocation ratios.
        let weight = header.weight.unwrap_or_else(WorkerInfo::default_weight);
        let startup_time_ms = u64::try_from(header.fs_ctime).unwrap_or_default();
        let can_resume = matches!(status, HeartbeatStatus::Running) && {
            let wm = fs.worker_manager.read();
            header.cluster_id == wm.cluster_id
                && wm.accepts_running_heartbeat(
                    &address,
                    header.worker_session_id.as_deref().unwrap_or_default(),
                    startup_time_ms,
                )
        };
        if can_resume && fs.has_pending_worker_cleanup(address.worker_id) {
            // Keep the checker responsible for completing invalidation and
            // forwarding replication work before registration cancels its retry.
            return err_box!("Worker {} cleanup is pending; retry heartbeat", address);
        }
        let mut wm = fs.worker_manager.write();
        if matches!(status, HeartbeatStatus::Start) {
            wm.validate_worker_start(
                &header.cluster_id,
                &address,
                header.worker_session_id.as_deref().unwrap_or_default(),
                startup_time_ms,
            )?;
            if !wm.is_duplicate_worker_start(
                &address,
                header.worker_session_id.as_deref().unwrap_or_default(),
                startup_time_ms,
            ) {
                fs.reset_full_block_report(address.worker_id);
            }
        }
        let cmds = wm.heartbeat(
            &header.cluster_id,
            status,
            address,
            weight,
            header.worker_session_id.unwrap_or_default(),
            curvine_model::TransferWorkerCapabilities {
                task_submit: header.transfer_task_submit.unwrap_or(false),
                report_target: header.transfer_report_target.unwrap_or(false),
                query_task: header.transfer_query_task.unwrap_or(false),
                attempt_safe_output: header.transfer_attempt_safe_output.unwrap_or(false),
                source_read_plan: header.transfer_source_read_plan.unwrap_or(false),
            },
            header.software_version,
            startup_time_ms,
            ProtoUtils::storage_info_list_from_pb(header.storages),
            header.component_info,
        )?;
        Ok(cmds)
    }

    pub fn block_report(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: BlockReportListRequest = ctx.parse_header()?;
        let cmds =
            Self::process_block_report(self.fs.clone(), self.replication_handler.clone(), header)?;
        let rep_header = BlockReportListResponse {
            cmds: ProtoUtils::worker_cmd_to_pb(cmds),
        };
        ctx.response(rep_header)
    }

    fn process_block_report(
        fs: MasterFilesystem,
        replication_handler: Option<MasterReplicationHandler>,
        header: BlockReportListRequest,
    ) -> FsResult<Vec<WorkerCommand>> {
        let list = ProtoUtils::block_report_list_from_pb(header);
        let result = fs.block_report(list, replication_handler)?;

        if result.delete_blocks.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![WorkerCommand::DeleteBlock(DeleteBlockCmd {
                blocks: result.delete_blocks,
            })])
        }
    }

    fn client_ip(&self) -> &str {
        match &self.conn_state {
            None => "",
            Some(v) => &v.remote_addr.hostname,
        }
    }

    pub fn clone_fs(&self) -> MasterFilesystem {
        self.fs.clone()
    }

    fn mount(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: MountRequest = ctx.parse_header()?;
        let mnt_opt = ProtoUtils::mount_options_from_pb(request.mount_options);

        ctx.set_audit(
            Some(request.cv_path.to_string()),
            Some(request.ufs_path.to_string()),
        );

        self.mount_manager
            .mount(None, &request.cv_path, &request.ufs_path, &mnt_opt)?;
        let rep_header = MountResponse::default();
        ctx.response(rep_header)
    }

    fn umount(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: UnMountRequest = ctx.parse_header()?;
        ctx.set_audit(Some(request.cv_path.to_string()), None);

        self.mount_manager.umount(&request.cv_path)?;
        let rep_header = UnMountResponse::default();
        ctx.response(rep_header)
    }

    fn get_mount_info(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: GetMountInfoRequest = ctx.parse_header()?;
        ctx.set_audit(Some(request.path.to_string()), None);

        let path = Path::from_str(request.path)?;
        let ret = self.mount_manager.get_mount_info(&path)?;
        let rep_header = GetMountInfoResponse {
            mount_info: ret.map(ProtoUtils::mount_info_to_pb),
        };
        ctx.response(rep_header)
    }

    fn get_mount_table(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let _: GetMountTableRequest = ctx.parse_header()?;
        let table = self.mount_manager.get_mount_table()?;

        let mount_table: Vec<MountInfoProto> = table
            .into_iter()
            .map(ProtoUtils::mount_info_to_pb)
            .collect();
        let rep_header = GetMountTableResponse { mount_table };
        ctx.response(rep_header)
    }

    fn set_attr_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(SetAttrResponse::default());
        }

        let header: SetAttrRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::set_attr_opts_from_pb(header.opts);
        let status = self.fs.set_attr(header.path, opts)?;

        ctx.response(SetAttrResponse {
            status: ProtoUtils::file_status_to_pb(status),
        })
    }

    fn symlink_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: SymlinkRequest = ctx.parse_header()?;
        ctx.set_audit(
            Some(header.target.to_string()),
            Some(header.link.to_string()),
        );

        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(SymlinkResponse::default());
        }

        self.fs.symlink_with_owner_group(
            &header.target,
            &header.link,
            header.force,
            header.mode,
            header.owner,
            header.group,
        )?;

        ctx.response(SymlinkResponse::default())
    }

    fn metrics_report(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: MetricsReportRequest = ctx.parse_header()?;

        let metrics = ProtoUtils::metrics_report_from_pb(header.metrics);
        Master::get_metrics()?.metrics_report(metrics)?;

        ctx.response(MetricsReportResponse {})
    }

    fn link_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: LinkRequest = ctx.parse_header()?;
        ctx.set_audit(
            Some(header.src_path.to_string()),
            Some(header.dst_path.to_string()),
        );

        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(LinkResponse::default());
        }

        self.fs.link(&header.src_path, &header.dst_path)?;

        ctx.response(LinkResponse::default())
    }

    pub fn resize_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: FileResizeRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let file_blocks = self.fs.resize_by_id(
            &header.path,
            header.inode_id,
            ProtoUtils::file_alloc_opts_from_pb(header.opts),
        )?;
        let rep_header = FileResizeResponse {
            file_blocks: ProtoUtils::file_blocks_to_pb(file_blocks),
        };
        ctx.response(rep_header)
    }

    pub fn assign_worker(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: AssignWorkerRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let block = self.fs.assign_worker_by_id(
            &header.path,
            header.inode_id,
            ProtoUtils::extend_block_from_pb(header.block),
            ProtoUtils::client_address_from_pb(header.client_address),
            header.exclude_workers,
        )?;
        let rep_header = AssignWorkerResponse {
            block: ProtoUtils::located_block_to_pb(block),
        };
        ctx.response(rep_header)
    }

    pub fn get_lock(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: GetLockRequest = ctx.parse_header()?;
        let lock = ProtoUtils::file_lock_from_pb(header.lock);
        ctx.set_audit(Some(header.path.to_string()), None);

        let conflict = self.fs.get_lock(header.path, lock)?;
        let rep_header = GetLockResponse {
            conflict: conflict.map(ProtoUtils::file_lock_to_pb),
        };
        ctx.response(rep_header)
    }

    pub fn set_lock(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: SetLockRequest = ctx.parse_header()?;
        let lock = ProtoUtils::file_lock_from_pb(header.lock);

        let audit = format!(
            "[{:?}-{:?}]{}",
            lock.lock_flags, lock.lock_type, header.path
        );
        ctx.set_audit(Some(audit), None);

        let conflict = self.fs.set_lock(header.path, lock)?;
        let rep_header = SetLockResponse {
            conflict: conflict.map(ProtoUtils::file_lock_to_pb),
        };
        ctx.response(rep_header)
    }

    pub fn list_options(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListOptionsRequest = ctx.parse_header()?;
        if header.options.limit.unwrap_or(0) < 0 {
            return err_box!("list options limit must be greater than 0");
        }
        let opts = ProtoUtils::list_options_from_pb(header.options);
        let audit_path = format!("{}[{}]", header.path, opts);
        ctx.set_audit(Some(audit_path), None);

        let list = Self::process_list_options(self.fs.clone(), header.path, opts)?;
        let res = list
            .into_iter()
            .map(ProtoUtils::file_status_to_pb)
            .collect();
        let rep_header = ListOptionsResponse { statuses: res };
        ctx.response(rep_header)
    }

    fn process_list_options(
        fs: MasterFilesystem,
        path: String,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        fs.list_options(&path, opts)
    }

    fn record_rpc_observability(&self, ctx: &RpcContext<'_>, response: &FsResult<Message>) {
        let used_us = ctx.spent.used_us();
        if self.audit_logging_enabled {
            ctx.audit_log(response, used_us, self.conn_state.as_ref());
        }

        let code_label = format!("{:?}", ctx.code);
        self.metrics.rpc_request_total_time.inc_by(used_us as i64);
        self.metrics.rpc_request_total_count.inc();

        if ctx.code != RpcCode::WorkerHeartbeat {
            self.metrics
                .operation_duration
                .with_label_values(&[&code_label])
                .observe(used_us as f64);
        };
    }
}

impl Drop for MasterHandler {
    fn drop(&mut self) {
        self.metrics.active_connections.dec();
    }
}

impl MessageHandler for MasterHandler {
    type Error = FsError;

    fn is_sync(&self, msg: &Message) -> bool {
        let code = RpcCode::from(msg.code());
        !matches!(
            code,
            RpcCode::SubmitJob
                | RpcCode::GetJobStatus
                | RpcCode::CancelJob
                | RpcCode::ReportTask
                | RpcCode::GetFilesystemInfo
                | RpcCode::GetCvMetadataSnapshotPage
                | RpcCode::GetCvMetadataDeltaPage
        )
    }

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        crate::fault_point! {
            sync,
            name: "master.rpc.before_sync_dispatch",
            description: "Before a synchronous Master RPC is dispatched to its business handler",
            context: {
                "req_id" => msg.req_id(),
                "rpc_code" => msg.code() as i32,
            },
            return_error: |fault| Ok(msg.error_ext(&FsError::common(fault.message))),
        }

        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;
        let code = RpcCode::from(msg.code());

        // Unified processing of all RPC requests (standby NotLeader uses the same
        // observability + error_ext conversion path as async_handle).
        let response = if !self.fs.master_monitor.is_active() {
            Err(FsError::not_leader_master(ctx.code, self.client_ip()))
        } else {
            match code {
                // File system operation request
                RpcCode::Mkdir => self.mkdir(ctx),
                RpcCode::CreateFile => self.retry_check_create_file(ctx),
                RpcCode::OpenFile => self.retry_check_open_file(ctx),
                RpcCode::FileStatus => self.file_status(ctx),
                RpcCode::AddBlock => self.add_block(ctx),
                RpcCode::CompleteFile => self.complete_file(ctx),
                RpcCode::CreateFilesBatch => self.create_files_batch(ctx),
                RpcCode::AddBlocksBatch => self.add_blocks_batch(ctx),
                RpcCode::CompleteFilesBatch => self.complete_files_batch(ctx),
                RpcCode::Exists => self.exists(ctx),
                RpcCode::Delete => self.retry_check_delete(ctx),
                RpcCode::Free => self.retry_check_free(ctx),
                RpcCode::Rename => self.retry_check_rename(ctx),
                RpcCode::ListStatus => self.list_status(ctx),
                RpcCode::ListOptions => self.list_options(ctx),
                RpcCode::GetBlockLocations => self.get_block_locations(ctx),
                RpcCode::SetAttr => self.set_attr_retry_check(ctx),
                RpcCode::Symlink => self.symlink_retry_check(ctx),
                RpcCode::Link => self.link_retry_check(ctx),
                RpcCode::ResizeFile => self.resize_file(ctx),
                RpcCode::AssignWorker => self.assign_worker(ctx),
                RpcCode::GetLock => self.get_lock(ctx),
                RpcCode::SetLock => self.set_lock(ctx),

                RpcCode::Mount => self.mount(ctx),
                RpcCode::UnMount => self.umount(ctx),
                RpcCode::GetMountTable => self.get_mount_table(ctx),
                RpcCode::GetMountInfo => self.get_mount_info(ctx),

                RpcCode::MetricsReport => self.metrics_report(ctx),

                // Worker related requests
                RpcCode::WorkerHeartbeat => self.worker_heartbeat(ctx),
                RpcCode::WorkerBlockReport => self.block_report(ctx),

                RpcCode::ReportBlockReplicationResult => {
                    if let Some(ref replication_service) = self.replication_handler {
                        return replication_service.handle(msg);
                    } else {
                        return Err(FsError::common("Replication service not initialized"));
                    }
                }

                // Unsupported request
                _ => err_box!("Unsupported operation"),
            }
        };

        self.record_rpc_observability(ctx, &response);

        match response {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }

    async fn async_handle(&self, msg: Message) -> FsResult<Message> {
        crate::fault_point! {
            async,
            name: "master.rpc.before_async_dispatch",
            description: "Before an asynchronous Master RPC is dispatched to its business handler",
            context: {
                "req_id" => msg.req_id(),
                "rpc_code" => msg.code() as i32,
            },
            return_error: |fault| async {
                Ok(msg.error_ext(&FsError::common(fault.message)))
            },
        }

        let mut rpc_context = RpcContext::new(&msg);
        let ctx = &mut rpc_context;
        let code = RpcCode::from(msg.code());

        let res = if !self.fs.master_monitor.is_active() {
            Err(FsError::not_leader_master(ctx.code, self.client_ip()))
        } else {
            match code {
                RpcCode::SubmitJob => self.job_handler.submit_job(ctx).await,
                RpcCode::GetJobStatus => self.job_handler.get_load_status(ctx),
                RpcCode::CancelJob => self.job_handler.cancel_job(ctx).await,
                RpcCode::ReportTask => self.job_handler.task_report(ctx),
                RpcCode::GetFilesystemInfo => self.async_get_filesystem_info(ctx).await,
                RpcCode::GetCvMetadataSnapshotPage => {
                    self.async_get_cv_metadata_snapshot_page(ctx).await
                }
                RpcCode::GetCvMetadataDeltaPage => self.async_get_cv_metadata_delta_page(ctx).await,

                v => err_box!("unsupported operation {:?}", v),
            }
        };

        self.record_rpc_observability(ctx, &res);

        match res {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }

    fn get_rt(&self, msg: &Message) -> Option<&Runtime> {
        let code = RpcCode::from(msg.code());
        if matches!(
            code,
            RpcCode::WorkerHeartbeat | RpcCode::WorkerBlockReport | RpcCode::GetFilesystemInfo
        ) {
            Some(&self.actor_rt)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use curvine_model::{
        BlockLocation, ClientAddress, CommitBlock, CreateFileOptsBuilder, LocatedBlock, OpenFlags,
        SetAttrOptsBuilder, StorageInfo, TtlAction, WorkerAddress,
    };
    use curvine_runtime::common::Utils;

    fn offline_worker_fs() -> (MasterFilesystem, WorkerHeartbeatRequest) {
        offline_worker_fs_with_id(7)
    }

    fn offline_worker_fs_with_id(worker_id: u32) -> (MasterFilesystem, WorkerHeartbeatRequest) {
        Master::init_test_metrics();
        let name = Utils::rand_str(10);
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!("offline-worker/meta-{name}"));
        conf.journal.journal_dir = Utils::test_sub_dir(format!("offline-worker/journal-{name}"));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        let address = WorkerAddress {
            worker_id,
            hostname: "offline-worker".into(),
            ip_addr: "127.0.0.1".into(),
            rpc_port: 1234,
            web_port: 5678,
        };
        let header = WorkerHeartbeatRequest {
            cluster_id: conf.cluster_id,
            worker_id: address.worker_id,
            address: ProtoUtils::worker_address_to_pb(&address),
            worker_session_id: Some("original-session".into()),
            fs_ctime: 123_456,
            storages: vec![ProtoUtils::storage_info_to_pb(StorageInfo {
                capacity: 1 << 40,
                available: 1 << 40,
                ..Default::default()
            })],
            ..Default::default()
        };
        offline_heartbeat(&fs, &header, HeartbeatStatus::Running);
        (fs, header)
    }

    fn offline_heartbeat(
        fs: &MasterFilesystem,
        header: &WorkerHeartbeatRequest,
        status: HeartbeatStatus,
    ) {
        let mut header = header.clone();
        header.status = status.into();
        MasterHandler::process_worker_heartbeat(fs.clone(), header).unwrap();
    }

    fn offline_cache_file(
        fs: &MasterFilesystem,
        path: &str,
        ttl_action: TtlAction,
        ufs_backed: bool,
    ) -> (FileStatus, LocatedBlock) {
        let client = ClientAddress::default();
        let status = fs
            .create_with_opts(
                path,
                CreateFileOptsBuilder::new().ttl_action(ttl_action).build(),
                OpenFlags::new_create(),
            )
            .unwrap();
        let block = fs
            .add_block(path, None, client.clone(), vec![], vec![], 0, None)
            .unwrap();
        fs.complete_file(
            path,
            None,
            status.block_size,
            vec![CommitBlock {
                block_id: block.block.id,
                block_len: status.block_size,
                locations: vec![BlockLocation::with_id(block.locs[0].worker_id)],
            }],
            &client.client_name,
            false,
            None,
        )
        .unwrap();
        if ufs_backed {
            fs.set_attr(path, SetAttrOptsBuilder::new().ufs_mtime(12_345).build())
                .unwrap();
        }
        (fs.file_status(path).unwrap(), block)
    }

    fn offline_report_request(
        header: &WorkerHeartbeatRequest,
        full_report: bool,
        blocks: &[(i64, i64)],
    ) -> BlockReportListRequest {
        BlockReportListRequest {
            cluster_id: header.cluster_id.clone(),
            worker_id: header.worker_id,
            worker_session_id: header.worker_session_id.clone(),
            full_report,
            total_len: blocks.len() as u64,
            blocks: blocks
                .iter()
                .map(|&(id, block_size)| BlockReportInfoProto {
                    id,
                    status: curvine_model::BlockReportStatus::Finalized.into(),
                    block_size,
                    storage_type: curvine_model::StorageType::Disk.into(),
                })
                .collect(),
        }
    }

    fn offline_report(
        fs: &MasterFilesystem,
        header: &WorkerHeartbeatRequest,
        full_report: bool,
        blocks: &[(i64, i64)],
    ) -> Vec<i64> {
        let report = offline_report_request(header, full_report, blocks);
        MasterHandler::process_block_report(fs.clone(), None, report)
            .unwrap()
            .into_iter()
            .flat_map(|command| match command {
                WorkerCommand::DeleteBlock(command) => command.blocks,
            })
            .collect()
    }

    fn offline_wait_for_reconcile(mut completed: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !completed() {
            assert!(
                std::time::Instant::now() < deadline,
                "full block report reconciliation did not complete within five seconds"
            );
            std::thread::yield_now();
        }
    }

    fn offline_wait_for_full_report(fs: &MasterFilesystem, header: &WorkerHeartbeatRequest) {
        offline_wait_for_reconcile(|| {
            fs.worker_manager.read().worker_block_report_complete(
                header.worker_id,
                header.worker_session_id.as_deref().unwrap_or_default(),
            )
        });
    }

    #[test]
    fn offline_worker_restart_full_report_preserves_reported_cache() {
        let (fs, header) = offline_worker_fs();
        let path = "/retained-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("retained-storage-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        assert!(offline_report(
            &fs,
            &restarted,
            true,
            &[(block.block.id, before.block_size)],
        )
        .is_empty());
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);

        let retained = fs.get_block_locations(path).unwrap();
        assert!(retained.status.cv_valid(None));
        assert_eq!(
            retained.status.storage_policy.state,
            before.storage_policy.state
        );
        assert_eq!(retained.block_locs.len(), 1);
        assert_eq!(retained.block_locs[0].block.id, block.block.id);
        assert_eq!(retained.block_locs[0].locs.len(), 1);
        assert_eq!(retained.block_locs[0].locs[0].worker_id, header.worker_id);
    }

    #[test]
    fn offline_worker_expiry_after_full_report_deletes_cache_on_first_running() {
        let (fs, header) = offline_worker_fs();
        let path = "/expired-before-running-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("reported-before-expiry-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        assert!(offline_report(
            &fs,
            &restarted,
            true,
            &[(block.block.id, before.block_size)],
        )
        .is_empty());
        offline_wait_for_full_report(&fs, &restarted);
        assert!(fs.file_status(path).unwrap().cv_valid(None));
        assert!(fs
            .worker_manager
            .read()
            .get_worker(header.worker_id)
            .is_none());

        // A completed report does not cancel the deadline until the worker is
        // ready. Expire the retained cache before its first Running heartbeat.
        let expired = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(expired.len(), 1);
        let cleanup = fs.delete_lost_worker_locations(&expired[0]).unwrap();
        assert_eq!(cleanup.removed_block_ids, vec![block.block.id]);
        assert!(cleanup.replication_block_ids.is_empty());
        assert!(!fs.file_status(path).unwrap().cv_valid(None));

        restarted.status = HeartbeatStatus::Running.into();
        let deleted: Vec<i64> = MasterHandler::process_worker_heartbeat(fs.clone(), restarted)
            .unwrap()
            .into_iter()
            .flat_map(|command| match command {
                WorkerCommand::DeleteBlock(command) => command.blocks,
            })
            .collect();
        assert_eq!(deleted, vec![block.block.id]);
        assert!(fs
            .worker_manager
            .read()
            .get_worker(header.worker_id)
            .is_some());
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .is_empty());
        let after = fs.get_block_locations(path).unwrap();
        assert!(!after.status.cv_valid(None));
        assert!(after.status.ufs_exists());
        assert_eq!(after.status.len, before.len);
        assert_eq!(
            after.status.storage_policy.ufs_mtime,
            before.storage_policy.ufs_mtime
        );
        assert!(after.block_locs.is_empty());
    }

    #[test]
    #[cfg(feature = "fault-injection")]
    fn offline_worker_running_waits_for_partial_timeout_cleanup_before_registering() {
        use curvine_fault::{FaultRuleBuilder, FaultRuntime};

        struct RuleGuard(String);
        impl Drop for RuleGuard {
            fn drop(&mut self) {
                let _ = FaultRuntime::process().remove(&self.0);
            }
        }

        let (fs, mut header) = offline_worker_fs_with_id(92_001);
        let path = "/partial-timeout-cleanup-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        let expired = fs
            .worker_manager
            .write()
            .remove_expired_worker(header.worker_id)
            .unwrap();
        let rule_id = format!("partial-timeout-running-{}", header.worker_id);
        let rule = FaultRuleBuilder::named("master.cache.before_invalidate_lost_chunk")
            .matches("worker_id", header.worker_id)
            .unwrap()
            .times(1)
            .unwrap()
            .return_error("injected timeout invalidation failure")
            .unwrap();
        FaultRuntime::process().configure(&rule_id, rule).unwrap();
        let _rule = RuleGuard(rule_id);
        let failure = match fs.delete_lost_worker_locations(&expired) {
            Ok(_) => panic!("expected injected timeout cleanup failure"),
            Err(error) => error,
        };
        assert!(failure
            .to_string()
            .contains("injected timeout invalidation failure"));
        assert!(fs.file_status(path).unwrap().cv_valid(None));
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .is_empty());
        fs.worker_manager.write().queue_offline_worker(expired);

        // A worker recovering from a heartbeat timeout sends Running directly.
        // Defer registration until the checker finishes its captured work, so
        // the normal background path also handles any required replication.
        header.status = HeartbeatStatus::Running.into();
        assert!(MasterHandler::process_worker_heartbeat(fs.clone(), header.clone()).is_err());
        assert!(fs
            .worker_manager
            .read()
            .get_worker(header.worker_id)
            .is_none());
        assert!(fs.file_status(path).unwrap().cv_valid(None));
        let pending = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(pending.len(), 1, "Running must preserve the cleanup retry");
        let cleanup = fs.delete_lost_worker_locations(&pending[0]).unwrap();
        assert_eq!(cleanup.removed_block_ids, vec![block.block.id]);
        assert!(cleanup.replication_block_ids.is_empty());

        let commands = MasterHandler::process_worker_heartbeat(fs.clone(), header.clone()).unwrap();
        assert!(commands.iter().any(|command| match command {
            WorkerCommand::DeleteBlock(command) => command.blocks.contains(&block.block.id),
        }));
        let after = fs.get_block_locations(path).unwrap();
        assert!(!after.status.cv_valid(None));
        assert!(after.status.ufs_exists());
        assert_eq!(after.status.len, before.len);
        assert_eq!(
            after.status.storage_policy.ufs_mtime,
            before.storage_policy.ufs_mtime
        );
        assert!(after.block_locs.is_empty());
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .is_empty());
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(header.worker_id)
                .unwrap()
                .worker_session_id,
            header.worker_session_id.unwrap()
        );
        assert!(fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX)
            .is_empty());
    }

    #[test]
    fn offline_worker_cleanup_racing_rejoin_keeps_cache_and_locations_consistent() {
        for iteration in 0..8 {
            let (fs, header) = offline_worker_fs();
            let path = "/concurrent-rejoin-cache";
            let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
            offline_heartbeat(&fs, &header, HeartbeatStatus::End);
            let mut pending = fs
                .worker_manager
                .write()
                .take_expired_offline_workers(u64::MAX);
            assert_eq!(pending.len(), 1);

            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let cleanup_barrier = barrier.clone();
            let cleanup_fs = fs.clone();
            let expired = pending.remove(0);
            let (cleanup_tx, cleanup_rx) = std::sync::mpsc::channel();
            let cleanup_thread = std::thread::spawn(move || {
                cleanup_barrier.wait();
                let result = cleanup_fs
                    .delete_lost_worker_locations(&expired)
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let _ = cleanup_tx.send(result);
            });

            let rejoin_fs = fs.clone();
            let mut restarted = header.clone();
            restarted.worker_session_id = Some(format!("concurrent-session-{iteration}"));
            restarted.fs_ctime += 1;
            let block_id = block.block.id;
            let block_size = before.block_size;
            let (rejoin_tx, rejoin_rx) = std::sync::mpsc::channel();
            let rejoin_thread = std::thread::spawn(move || {
                barrier.wait();
                offline_heartbeat(&rejoin_fs, &restarted, HeartbeatStatus::Start);
                let mut deleted =
                    offline_report(&rejoin_fs, &restarted, true, &[(block_id, block_size)]);
                offline_wait_for_full_report(&rejoin_fs, &restarted);
                restarted.status = HeartbeatStatus::Running.into();
                deleted.extend(
                    MasterHandler::process_worker_heartbeat(rejoin_fs, restarted)
                        .unwrap()
                        .into_iter()
                        .flat_map(|command| match command {
                            WorkerCommand::DeleteBlock(command) => command.blocks,
                        }),
                );
                let _ = rejoin_tx.send(deleted);
            });

            // Receive before joining so a lock-order regression fails within a
            // bounded interval instead of hanging the entire test process.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let cleanup_result = cleanup_rx
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .expect("concurrent cleanup did not finish within ten seconds");
            let deleted = rejoin_rx
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .expect("concurrent rejoin did not finish within ten seconds");
            cleanup_thread.join().unwrap();
            rejoin_thread.join().unwrap();
            cleanup_result.unwrap();

            assert_eq!(
                fs.worker_manager
                    .read()
                    .get_worker(header.worker_id)
                    .unwrap()
                    .worker_session_id,
                format!("concurrent-session-{iteration}")
            );
            let after = fs.get_block_locations(path).unwrap();
            let worker_blocks = fs
                .fs_dir
                .read()
                .get_worker_block_ids(header.worker_id)
                .unwrap();
            assert_eq!(after.status.len, before.len);
            assert_eq!(
                after.status.storage_policy.ufs_mtime,
                before.storage_policy.ufs_mtime
            );
            assert!(after.status.ufs_exists());
            if after.status.cv_valid(None) {
                assert!(deleted.is_empty(), "retained cache must not be deleted");
                assert_eq!(after.block_locs.len(), 1);
                assert_eq!(after.block_locs[0].block.id, block_id);
                assert_eq!(after.block_locs[0].locs.len(), 1);
                assert_eq!(after.block_locs[0].locs[0].worker_id, header.worker_id);
                assert_eq!(worker_blocks, vec![block_id]);
            } else {
                assert!(!after.status.cv_exists());
                assert!(after.block_locs.is_empty());
                assert!(worker_blocks.is_empty());
                assert!(
                    deleted.contains(&block_id),
                    "invalidated cache must be deleted from the returning worker"
                );
            }
        }
    }

    #[test]
    fn offline_worker_restart_empty_full_report_invalidates_missing_cache() {
        let (fs, header) = offline_worker_fs();
        let path = "/lost-on-restart-cache";
        let (before, _) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("empty-storage-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        assert!(offline_report(&fs, &restarted, true, &[]).is_empty());
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);
        offline_wait_for_reconcile(|| {
            !fs.file_status(path).unwrap().cv_valid(None)
                && fs
                    .fs_dir
                    .read()
                    .get_worker_block_ids(header.worker_id)
                    .unwrap()
                    .is_empty()
        });

        let after = fs.file_status(path).unwrap();
        assert!(!after.cv_exists());
        assert!(after.ufs_exists());
        assert_eq!(after.len, before.len);
        assert_eq!(
            after.storage_policy.ufs_mtime,
            before.storage_policy.ufs_mtime
        );
        assert!(fs.get_block_locations(path).unwrap().block_locs.is_empty());
    }

    #[test]
    fn offline_worker_partial_full_report_waits_before_invalidating_missing_cache() {
        let (fs, header) = offline_worker_fs();
        let (first, first_block) =
            offline_cache_file(&fs, "/first-reported-cache", TtlAction::Delete, true);
        let (last, last_block) =
            offline_cache_file(&fs, "/last-reported-cache", TtlAction::Delete, true);
        let (_, missing_block) =
            offline_cache_file(&fs, "/missing-from-report-cache", TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("partial-report-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        let mut first_report = offline_report_request(
            &restarted,
            true,
            &[(first_block.block.id, first.block_size)],
        );
        first_report.total_len = 2;
        assert!(
            MasterHandler::process_block_report(fs.clone(), None, first_report)
                .unwrap()
                .is_empty()
        );
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);
        assert!(fs
            .worker_manager
            .read()
            .get_worker(header.worker_id)
            .is_none());
        assert!(fs
            .file_status("/missing-from-report-cache")
            .unwrap()
            .cv_valid(None));
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .contains(&missing_block.block.id));

        let mut last_report =
            offline_report_request(&restarted, true, &[(last_block.block.id, last.block_size)]);
        last_report.total_len = 2;
        assert!(
            MasterHandler::process_block_report(fs.clone(), None, last_report)
                .unwrap()
                .is_empty()
        );
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);

        let missing = fs
            .get_block_locations("/missing-from-report-cache")
            .unwrap();
        assert!(!missing.status.cv_valid(None));
        assert!(missing.status.ufs_exists());
        assert!(missing.block_locs.is_empty());
        for path in ["/first-reported-cache", "/last-reported-cache"] {
            let kept = fs.get_block_locations(path).unwrap();
            assert!(kept.status.cv_valid(None));
            assert_eq!(kept.block_locs.len(), 1);
            assert_eq!(kept.block_locs[0].locs.len(), 1);
            assert_eq!(kept.block_locs[0].locs[0].worker_id, header.worker_id);
        }
    }

    #[test]
    fn offline_worker_incremental_report_between_full_chunks_preserves_recovery() {
        let (fs, header) = offline_worker_fs();
        let (first, first_block) =
            offline_cache_file(&fs, "/interleaved-first-cache", TtlAction::Delete, true);
        let (last, last_block) =
            offline_cache_file(&fs, "/interleaved-last-cache", TtlAction::Delete, true);
        let (incremental, incremental_block) =
            offline_cache_file(&fs, "/interleaved-current-cache", TtlAction::Delete, true);
        offline_cache_file(&fs, "/interleaved-missing-cache", TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let mut restarted = header.clone();
        restarted.worker_session_id = Some("interleaved-report-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);

        let mut first_report = offline_report_request(
            &restarted,
            true,
            &[(first_block.block.id, first.block_size)],
        );
        first_report.total_len = 2;
        assert!(
            MasterHandler::process_block_report(fs.clone(), None, first_report)
                .unwrap()
                .is_empty()
        );
        assert!(offline_report(
            &fs,
            &restarted,
            false,
            &[(incremental_block.block.id, incremental.block_size)],
        )
        .is_empty());
        // Incremental IDs must supplement the inventory without counting as
        // missing chunks of the startup snapshot or canceling its progress.
        assert!(!fs.worker_manager.read().worker_block_report_complete(
            header.worker_id,
            restarted.worker_session_id.as_deref().unwrap(),
        ));
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);
        assert!(fs
            .worker_manager
            .read()
            .get_worker(header.worker_id)
            .is_none());

        let mut last_report =
            offline_report_request(&restarted, true, &[(last_block.block.id, last.block_size)]);
        last_report.total_len = 2;
        assert!(
            MasterHandler::process_block_report(fs.clone(), None, last_report)
                .unwrap()
                .is_empty()
        );
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);

        for path in [
            "/interleaved-first-cache",
            "/interleaved-last-cache",
            "/interleaved-current-cache",
        ] {
            let kept = fs.get_block_locations(path).unwrap();
            assert!(kept.status.cv_valid(None), "reported cache {path} was lost");
            assert_eq!(kept.block_locs.len(), 1);
            assert_eq!(kept.block_locs[0].locs.len(), 1);
            assert_eq!(kept.block_locs[0].locs[0].worker_id, header.worker_id);
        }
        let missing = fs
            .get_block_locations("/interleaved-missing-cache")
            .unwrap();
        assert!(!missing.status.cv_valid(None));
        assert!(missing.status.ufs_exists());
        assert!(missing.block_locs.is_empty());
    }

    #[test]
    fn offline_worker_incremental_report_preserves_queued_full_reconcile() {
        let (fs, header) = offline_worker_fs();
        let (snapshot, snapshot_block) =
            offline_cache_file(&fs, "/queued-snapshot-cache", TtlAction::Delete, true);
        let (incremental, incremental_block) =
            offline_cache_file(&fs, "/queued-current-cache", TtlAction::Delete, true);
        offline_cache_file(&fs, "/queued-missing-cache", TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let mut restarted = header.clone();
        restarted.worker_session_id = Some("queued-report-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);

        // Occupy this worker's executor lane before its reconcile is queued.
        // Dropping the sender on a test failure also releases the blocker.
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        fs.full_block_reconcile_executor_for_test()
            .fixed_spawn(header.worker_id as i64, move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv_timeout(std::time::Duration::from_secs(10));
            })
            .unwrap();
        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("reconcile executor blocker did not start within five seconds");
        assert!(offline_report(
            &fs,
            &restarted,
            true,
            &[(snapshot_block.block.id, snapshot.block_size)],
        )
        .is_empty());
        assert!(offline_report(
            &fs,
            &restarted,
            false,
            &[(incremental_block.block.id, incremental.block_size)],
        )
        .is_empty());
        assert!(!fs.worker_manager.read().worker_block_report_complete(
            header.worker_id,
            restarted.worker_session_id.as_deref().unwrap(),
        ));
        release_tx.send(()).unwrap();
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);

        for path in ["/queued-snapshot-cache", "/queued-current-cache"] {
            let kept = fs.get_block_locations(path).unwrap();
            assert!(kept.status.cv_valid(None), "reported cache {path} was lost");
            assert_eq!(kept.block_locs.len(), 1);
            assert_eq!(kept.block_locs[0].locs.len(), 1);
            assert_eq!(kept.block_locs[0].locs[0].worker_id, header.worker_id);
        }
        let missing = fs.get_block_locations("/queued-missing-cache").unwrap();
        assert!(!missing.status.cv_valid(None));
        assert!(missing.status.ufs_exists());
        assert!(missing.block_locs.is_empty());
        let mut actual_ids = fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap();
        actual_ids.sort_unstable();
        let mut expected_ids = vec![snapshot_block.block.id, incremental_block.block.id];
        expected_ids.sort_unstable();
        assert_eq!(actual_ids, expected_ids);
    }

    #[test]
    fn offline_worker_stale_start_preserves_full_report_progress_and_live_session() {
        let (fs, header) = offline_worker_fs();
        let (first, first_block) =
            offline_cache_file(&fs, "/stale-start-first-cache", TtlAction::Delete, true);
        let (last, last_block) =
            offline_cache_file(&fs, "/stale-start-last-cache", TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let mut restarted = header.clone();
        restarted.worker_session_id = Some("newer-start-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);

        let mut first_report = offline_report_request(
            &restarted,
            true,
            &[(first_block.block.id, first.block_size)],
        );
        first_report.total_len = 2;
        assert!(
            MasterHandler::process_block_report(fs.clone(), None, first_report)
                .unwrap()
                .is_empty()
        );
        let mut stale_start = header.clone();
        stale_start.status = HeartbeatStatus::Start.into();
        assert!(MasterHandler::process_worker_heartbeat(fs.clone(), stale_start.clone()).is_err());

        let mut last_report =
            offline_report_request(&restarted, true, &[(last_block.block.id, last.block_size)]);
        last_report.total_len = 2;
        assert!(
            MasterHandler::process_block_report(fs.clone(), None, last_report)
                .unwrap()
                .is_empty()
        );
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);
        assert!(MasterHandler::process_worker_heartbeat(fs.clone(), stale_start).is_err());
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(header.worker_id)
                .unwrap()
                .worker_session_id,
            "newer-start-session"
        );
        assert!(fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX)
            .is_empty());
        for path in ["/stale-start-first-cache", "/stale-start-last-cache"] {
            let kept = fs.get_block_locations(path).unwrap();
            assert!(kept.status.cv_valid(None));
            assert_eq!(kept.block_locs.len(), 1);
            assert_eq!(kept.block_locs[0].locs.len(), 1);
            assert_eq!(kept.block_locs[0].locs[0].worker_id, header.worker_id);
        }
    }

    #[test]
    fn offline_worker_empty_full_report_preserves_surviving_replica() {
        let (fs, header) = offline_worker_fs();
        let path = "/replicated-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        let mut survivor = header.clone();
        survivor.worker_id = 8;
        survivor.address.worker_id = 8;
        survivor.address.hostname = "surviving-worker".into();
        offline_heartbeat(&fs, &survivor, HeartbeatStatus::Running);
        fs.fs_dir
            .read()
            .add_block_location(block.block.id, BlockLocation::with_id(survivor.worker_id))
            .unwrap();
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("empty-replica-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        assert!(offline_report(&fs, &restarted, true, &[]).is_empty());
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);

        let kept = fs.get_block_locations(path).unwrap();
        assert!(kept.status.cv_valid(None));
        assert_eq!(
            kept.status.storage_policy.state,
            before.storage_policy.state
        );
        assert_eq!(kept.block_locs.len(), 1);
        assert_eq!(kept.block_locs[0].locs.len(), 1);
        assert_eq!(kept.block_locs[0].locs[0].worker_id, survivor.worker_id);
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn offline_worker_stale_session_full_report_cannot_delete_recovered_cache() {
        let (fs, header) = offline_worker_fs();
        let path = "/current-session-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let mut restarted = header.clone();
        restarted.worker_session_id = Some("current-report-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        assert!(offline_report(
            &fs,
            &restarted,
            true,
            &[(block.block.id, before.block_size)],
        )
        .is_empty());
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);

        let stale_empty_report = offline_report_request(&header, true, &[]);
        assert!(MasterHandler::process_block_report(fs.clone(), None, stale_empty_report).is_err());
        let stale_finalized_report =
            offline_report_request(&header, false, &[(block.block.id, before.block_size)]);
        assert!(
            MasterHandler::process_block_report(fs.clone(), None, stale_finalized_report).is_err()
        );

        let kept = fs.get_block_locations(path).unwrap();
        assert!(kept.status.cv_valid(None));
        assert_eq!(kept.block_locs.len(), 1);
        assert_eq!(kept.block_locs[0].block.id, block.block.id);
        assert_eq!(kept.block_locs[0].locs.len(), 1);
        assert_eq!(kept.block_locs[0].locs[0].worker_id, header.worker_id);
    }

    #[test]
    fn offline_worker_late_finalized_report_cannot_resurrect_cleaned_cache() {
        let (fs, header) = offline_worker_fs();
        let path = "/late-reported-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let pending = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(pending.len(), 1);
        fs.delete_lost_worker_locations(&pending[0]).unwrap();
        assert!(!fs.file_status(path).unwrap().cv_valid(None));

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("late-finalized-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        let deleted = offline_report(
            &fs,
            &restarted,
            true,
            &[(block.block.id, before.block_size)],
        );
        assert_eq!(deleted, vec![block.block.id]);
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .is_empty());
        let after = fs.get_block_locations(path).unwrap();
        assert!(!after.status.cv_valid(None));
        assert!(after.status.ufs_exists());
        assert!(after.block_locs.is_empty());
    }

    #[test]
    fn offline_worker_end_retains_cache_until_grace_expires() {
        let (fs, header) = offline_worker_fs();
        let path = "/grace-period-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        let grace_ms = fs
            .worker_manager
            .read()
            .conf
            .master
            .worker_lost_interval_ms();
        assert!(grace_ms > 0);
        let before_end = curvine_runtime::common::LocalTime::mills();
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let before_expiry = before_end.saturating_add(grace_ms).saturating_sub(1);
        assert!(fs
            .worker_manager
            .write()
            .take_expired_offline_workers(before_expiry)
            .is_empty());
        assert!(fs
            .worker_manager
            .read()
            .get_worker(header.worker_id)
            .is_none());
        assert!(fs.file_status(path).unwrap().cv_valid(None));
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .contains(&block.block.id));
        assert!(fs.get_block_locations(path).is_err());

        let expired = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(expired.len(), 1);
        fs.delete_lost_worker_locations(&expired[0]).unwrap();
        let after = fs.get_block_locations(path).unwrap();
        assert!(!after.status.cv_valid(None));
        assert!(after.status.ufs_exists());
        assert_eq!(after.status.len, before.len);
        assert_eq!(
            after.status.storage_policy.ufs_mtime,
            before.storage_policy.ufs_mtime
        );
        assert!(after.block_locs.is_empty());
    }

    #[test]
    fn offline_worker_cleanup_preserves_storage_policy_guarantees() {
        for (ttl_action, ufs_backed, survivor) in [
            (TtlAction::Delete, true, false),
            (TtlAction::Delete, true, true),
            (TtlAction::Free, true, false),
            (TtlAction::Delete, false, false),
        ] {
            let (fs, header) = offline_worker_fs();
            let path = "/offline-cache";
            let (before, block) = offline_cache_file(&fs, path, ttl_action, ufs_backed);
            if survivor {
                let mut replica = header.clone();
                replica.worker_id = 8;
                replica.address.worker_id = 8;
                replica.address.hostname = "surviving-worker".into();
                offline_heartbeat(&fs, &replica, HeartbeatStatus::Running);
                fs.fs_dir
                    .read()
                    .add_block_location(block.block.id, BlockLocation::with_id(8))
                    .unwrap();
            }

            offline_heartbeat(&fs, &header, HeartbeatStatus::End);
            offline_heartbeat(&fs, &header, HeartbeatStatus::End);
            let pending = fs
                .worker_manager
                .write()
                .take_expired_offline_workers(u64::MAX);
            assert_eq!(pending.len(), 1, "duplicate End must not duplicate cleanup");
            let cleanup = fs.delete_lost_worker_locations(&pending[0]).unwrap();
            assert_eq!(cleanup.removed_block_ids, vec![block.block.id]);

            let invalidated = ttl_action == TtlAction::Delete && ufs_backed && !survivor;
            let after = fs.file_status(path).unwrap();
            assert_eq!(after.cv_exists(), !invalidated);
            assert_eq!(after.ufs_exists(), ufs_backed);
            assert_eq!(after.len, before.len);
            assert_eq!(
                after.storage_policy.ufs_mtime,
                before.storage_policy.ufs_mtime
            );
            assert_eq!(
                after.storage_policy.ttl_action,
                before.storage_policy.ttl_action
            );
            if invalidated {
                assert!(cleanup.replication_block_ids.is_empty());
                assert!(!after.cv_valid(None));
                assert!(fs.get_block_locations(path).unwrap().block_locs.is_empty());
            } else {
                assert_eq!(cleanup.replication_block_ids, vec![block.block.id]);
                assert_eq!(after.storage_policy.state, before.storage_policy.state);
            }
            if survivor {
                let blocks = fs.get_block_locations(path).unwrap();
                assert!(after.cv_valid(None));
                assert_eq!(blocks.block_locs[0].locs.len(), 1);
                assert_eq!(blocks.block_locs[0].locs[0].worker_id, 8);
            }
        }
    }

    #[test]
    fn offline_worker_cleanup_skips_same_id_restart() {
        let (fs, header) = offline_worker_fs();
        let path = "/restart-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let pending = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(pending.len(), 1);

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("restarted-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        assert!(offline_report(
            &fs,
            &restarted,
            true,
            &[(block.block.id, before.block_size)],
        )
        .is_empty());
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);
        assert!(fs
            .delete_lost_worker_locations(&pending[0])
            .unwrap()
            .removed_block_ids
            .is_empty());
        fs.worker_manager
            .write()
            .queue_offline_worker(pending[0].clone());
        assert!(fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX)
            .is_empty());
        let blocks = fs.get_block_locations(path).unwrap();
        assert!(blocks.status.cv_valid(None));
        assert_eq!(blocks.block_locs[0].block.id, block.block.id);
    }

    #[test]
    fn offline_worker_failed_restart_after_cleanup_gets_new_expiry() {
        let (fs, header) = offline_worker_fs();
        let path = "/restart-after-cleanup-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        let mut survivor = header.clone();
        survivor.worker_id = 8;
        survivor.address.worker_id = 8;
        survivor.address.hostname = "restart-cleanup-survivor".into();
        survivor.address.rpc_port += 1;
        offline_heartbeat(&fs, &survivor, HeartbeatStatus::Running);
        fs.fs_dir
            .read()
            .add_block_location(block.block.id, BlockLocation::with_id(survivor.worker_id))
            .unwrap();

        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let first_expiry = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(first_expiry.len(), 1);
        fs.delete_lost_worker_locations(&first_expiry[0]).unwrap();
        assert!(fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX)
            .is_empty());
        let retained = fs.get_block_locations(path).unwrap();
        assert!(retained.status.cv_valid(None));
        assert_eq!(retained.block_locs[0].locs.len(), 1);
        assert_eq!(retained.block_locs[0].locs[0].worker_id, survivor.worker_id);

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("restart-after-completed-cleanup".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        let mut partial =
            offline_report_request(&restarted, true, &[(block.block.id, before.block_size)]);
        partial.total_len = 2;
        assert!(
            MasterHandler::process_block_report(fs.clone(), None, partial)
                .unwrap()
                .is_empty()
        );
        assert!(fs
            .worker_manager
            .read()
            .get_worker(header.worker_id)
            .is_none());
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .contains(&block.block.id));

        // This startup never completes its inventory or sends Running. The
        // previous cleanup finished, so Start must have armed a fresh deadline.
        let second_expiry = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(second_expiry.len(), 1, "failed restart needs a new cleanup");
        let cleanup = fs.delete_lost_worker_locations(&second_expiry[0]).unwrap();
        assert_eq!(cleanup.removed_block_ids, vec![block.block.id]);
        assert!(fs.file_status(path).unwrap().cv_valid(None));
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .is_empty());

        // Losing the remaining replica must now invalidate the cache, without
        // the failed startup's partial report masquerading as a surviving copy.
        let last_worker = fs
            .worker_manager
            .write()
            .remove_expired_worker(survivor.worker_id)
            .unwrap();
        let cleanup = fs.delete_lost_worker_locations(&last_worker).unwrap();
        assert!(cleanup.replication_block_ids.is_empty());
        let after = fs.get_block_locations(path).unwrap();
        assert!(!after.status.cv_valid(None));
        assert!(after.status.ufs_exists());
        assert_eq!(after.status.len, before.len);
        assert_eq!(
            after.status.storage_policy.ufs_mtime,
            before.storage_policy.ufs_mtime
        );
        assert!(after.block_locs.is_empty());
    }

    #[test]
    fn offline_worker_partial_report_after_expiry_gets_new_cleanup() {
        let (fs, header) = offline_worker_fs();
        let path = "/late-partial-report-cache";
        let (before, late_block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        let (early, early_block) =
            offline_cache_file(&fs, "/early-partial-report-cache", TtlAction::Delete, true);
        let mut survivor = header.clone();
        survivor.worker_id = 8;
        survivor.address.worker_id = 8;
        survivor.address.hostname = "late-report-survivor".into();
        survivor.address.rpc_port += 1;
        offline_heartbeat(&fs, &survivor, HeartbeatStatus::Running);
        for block_id in [early_block.block.id, late_block.block.id] {
            fs.fs_dir
                .read()
                .add_block_location(block_id, BlockLocation::with_id(survivor.worker_id))
                .unwrap();
        }

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("partial-report-spans-expiry".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        let mut first = offline_report_request(
            &restarted,
            true,
            &[(early_block.block.id, early.block_size)],
        );
        first.total_len = 3;
        assert!(MasterHandler::process_block_report(fs.clone(), None, first)
            .unwrap()
            .is_empty());
        let first_expiry = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(first_expiry.len(), 1);
        fs.delete_lost_worker_locations(&first_expiry[0]).unwrap();
        assert!(fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX)
            .is_empty());
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .is_empty());
        assert!(fs.file_status(path).unwrap().cv_valid(None));

        // The worker continues its original report after cleanup has finished,
        // but never sends the third chunk or reaches Running. Accepting this
        // late location must create another bounded cleanup opportunity.
        let mut late = offline_report_request(
            &restarted,
            true,
            &[(late_block.block.id, before.block_size)],
        );
        late.total_len = 3;
        assert!(MasterHandler::process_block_report(fs.clone(), None, late)
            .unwrap()
            .is_empty());
        assert!(!fs.worker_manager.read().worker_block_report_complete(
            header.worker_id,
            restarted.worker_session_id.as_deref().unwrap(),
        ));
        assert!(fs
            .worker_manager
            .read()
            .get_worker(header.worker_id)
            .is_none());
        assert_eq!(
            fs.fs_dir
                .read()
                .get_worker_block_ids(header.worker_id)
                .unwrap(),
            vec![late_block.block.id]
        );
        let second_expiry = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(second_expiry.len(), 1, "late report needs a new cleanup");
        let cleanup = fs.delete_lost_worker_locations(&second_expiry[0]).unwrap();
        assert_eq!(cleanup.removed_block_ids, vec![late_block.block.id]);
        assert!(fs
            .fs_dir
            .read()
            .get_worker_block_ids(header.worker_id)
            .unwrap()
            .is_empty());

        let last_worker = fs
            .worker_manager
            .write()
            .remove_expired_worker(survivor.worker_id)
            .unwrap();
        let cleanup = fs.delete_lost_worker_locations(&last_worker).unwrap();
        assert!(cleanup.replication_block_ids.is_empty());
        for path in [path, "/early-partial-report-cache"] {
            let after = fs.get_block_locations(path).unwrap();
            assert!(!after.status.cv_valid(None));
            assert!(after.status.ufs_exists());
            assert!(after.block_locs.is_empty());
        }
    }

    #[test]
    fn offline_worker_cleanup_survives_failed_restart() {
        let (fs, header) = offline_worker_fs();
        let path = "/failed-restart-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);

        let mut restarted = header.clone();
        restarted.worker_session_id = Some("failed-restart-session".into());
        restarted.fs_ctime += 1;
        // A worker can announce Start and fail before sending Running. It has
        // not recovered its data, so the original loss must still be cleaned.
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        assert!(fs.worker_manager.read().get_worker(7).is_none());
        let pending = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(pending.len(), 1);

        let cleanup = fs.delete_lost_worker_locations(&pending[0]).unwrap();
        assert_eq!(cleanup.removed_block_ids, vec![block.block.id]);
        assert!(cleanup.replication_block_ids.is_empty());
        let after = fs.file_status(path).unwrap();
        assert!(!after.cv_valid(None));
        assert!(after.ufs_exists());
        assert_eq!(after.len, before.len);
        assert_eq!(
            after.storage_policy.ufs_mtime,
            before.storage_policy.ufs_mtime
        );
        assert!(fs.get_block_locations(path).unwrap().block_locs.is_empty());
    }

    #[test]
    fn offline_worker_ignores_end_from_previous_session() {
        let (fs, header) = offline_worker_fs();
        let path = "/new-session-cache";
        let (before, block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        let mut restarted = header.clone();
        restarted.worker_session_id = Some("restarted-session".into());
        restarted.fs_ctime += 1;
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Start);
        assert!(offline_report(
            &fs,
            &restarted,
            true,
            &[(block.block.id, before.block_size)],
        )
        .is_empty());
        offline_wait_for_full_report(&fs, &restarted);
        offline_heartbeat(&fs, &restarted, HeartbeatStatus::Running);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);

        let mut wrong_endpoint = restarted.clone();
        wrong_endpoint.address.hostname = "different-worker".into();
        offline_heartbeat(&fs, &wrong_endpoint, HeartbeatStatus::End);

        assert!(fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX)
            .is_empty());
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(7)
                .unwrap()
                .worker_session_id,
            "restarted-session"
        );
        assert!(fs.get_block_locations(path).unwrap().status.cv_valid(None));
    }

    #[test]
    fn offline_worker_cleanup_keeps_different_id_replacement() {
        let (fs, header) = offline_worker_fs();
        let path = "/lost-cache";
        let (_, lost_block) = offline_cache_file(&fs, path, TtlAction::Delete, true);
        offline_heartbeat(&fs, &header, HeartbeatStatus::End);
        let pending = fs
            .worker_manager
            .write()
            .take_expired_offline_workers(u64::MAX);
        assert_eq!(pending.len(), 1);

        let mut replacement = header.clone();
        replacement.worker_id = 8;
        replacement.address.worker_id = 8;
        replacement.worker_session_id = Some("fresh-storage".into());
        replacement.fs_ctime += 1;
        offline_heartbeat(&fs, &replacement, HeartbeatStatus::Start);
        assert!(offline_report(&fs, &replacement, true, &[]).is_empty());
        offline_wait_for_full_report(&fs, &replacement);
        offline_heartbeat(&fs, &replacement, HeartbeatStatus::Running);
        let (_, new_block) = offline_cache_file(&fs, "/new-cache", TtlAction::Delete, true);

        let cleanup = fs.delete_lost_worker_locations(&pending[0]).unwrap();
        assert_eq!(cleanup.removed_block_ids, vec![lost_block.block.id]);
        assert!(cleanup.replication_block_ids.is_empty());
        assert!(!fs.file_status(path).unwrap().cv_valid(None));
        let blocks = fs.get_block_locations("/new-cache").unwrap();
        assert!(blocks.status.cv_valid(None));
        assert_eq!(blocks.block_locs[0].block.id, new_block.block.id);
        assert_eq!(blocks.block_locs[0].locs[0].worker_id, 8);
    }

    #[test]
    fn process_worker_heartbeat_stores_worker_report_fields() {
        Master::init_test_metrics();
        let test_name = Utils::rand_str(6);
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-handler-test/meta-{}", test_name));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("master-handler-test/journal-{}", test_name));

        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        let address = WorkerAddress {
            worker_id: 7,
            hostname: "worker-host".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 1234,
            web_port: 5678,
        };
        let component_info = curvine_proto::ComponentInfoProto {
            component: Some("worker".to_string()),
            release_version: Some("0.4.0-alpha".to_string()),
            git_commit: Some("24c848719b5b4fea74519d91cbe462bb49761b36".to_string()),
            git_tag: Some("v0.4.0-alpha".to_string()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: vec!["transfer".to_string()],
        };
        let header = WorkerHeartbeatRequest {
            status: HeartbeatStatus::Running.into(),
            cluster_id: conf.cluster_id.clone(),
            address: ProtoUtils::worker_address_to_pb(&address),
            software_version: "0.1.0-test".to_string(),
            fs_ctime: 123_456,
            component_info: Some(component_info.clone()),
            ..Default::default()
        };

        MasterHandler::process_worker_heartbeat(fs.clone(), header).unwrap();

        let info = fs.filesystem_info().unwrap();
        let worker = info
            .live_workers
            .iter()
            .find(|worker| worker.address.worker_id == address.worker_id)
            .unwrap();
        assert_eq!(worker.software_version, "0.1.0-test");
        assert_eq!(worker.startup_time_ms, 123_456);
        // Structured version metadata survives heartbeat -> WorkerInfo ->
        // WorkerInfoProto (filesystem_info) -> WorkerInfo round trip.
        assert_eq!(worker.component_info, Some(component_info));
    }

    #[test]
    fn build_filesystem_info_response_attaches_master_compatibility() {
        let info = FilesystemInfo {
            active_master: "master-0".to_string(),
            inode_dir_num: 3,
            inode_file_num: 5,
            block_num: 7,
            capacity: 1000,
            available: 500,
            fs_used: 300,
            non_fs_used: 200,
            ..Default::default()
        };

        // Derive expectations from component_version("master") so the test stays
        // stable across BUILD_VERSION overrides and future protocol bumps.
        let master_version = curvine_sys::version::component_version("master");
        let master_compatibility = ProtoUtils::default_master_compatibility_to_pb(&master_version);

        let rep = MasterHandler::build_filesystem_info_response(info, &master_compatibility);

        assert_eq!(rep.active_master, "master-0");
        assert_eq!(rep.inode_file_num, 5);
        let compat = rep
            .compatibility
            .expect("master must advertise compatibility");
        assert_eq!(compat.server.component.as_deref(), Some("master"));
        assert_eq!(
            compat.server.release_version.as_deref(),
            Some(master_version.release_version.as_str())
        );
        assert_eq!(
            compat.server.protocol_version,
            Some(master_version.protocol_version)
        );
        assert_eq!(
            compat.compatibility_mode,
            CompatibilityModeProto::Diagnose as i32
        );
        assert!(compat.blocked_versions.is_empty());
    }

    #[test]
    fn diagnose_warnings_are_deduped_per_peer() {
        // A persistently incompatible worker must not re-log the same warning
        // on every heartbeat: only a verdict change emits a new warning.
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Diagnose,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            ..Default::default()
        };
        let warned = DashMap::new();
        let verdict = policy.check_worker(Some(&ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_component_info()
        }));

        // First occurrence warns and records the verdict.
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict.clone(),
            test_metrics()
        )
        .is_ok());
        assert!(warned.contains_key("worker:7"));

        // Identical verdict on a later heartbeat does not warn again but is
        // still allowed.
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict.clone(),
            test_metrics()
        )
        .is_ok());

        // A different incompatible verdict for the same peer warns again.
        let different = CompatibilityVerdict::ProtocolMismatch {
            peer: 2,
            min: 1,
            max: 1,
        };
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            different.clone(),
            test_metrics()
        )
        .is_ok());
        assert_eq!(warned.get("worker:7").as_deref(), Some(&different));

        // A separate peer is tracked independently.
        assert!(!warned.contains_key("worker:8"));
    }

    #[test]
    fn diagnose_mode_allows_incompatible_worker_heartbeat() {
        // Configure a minimum worker version so the peer is genuinely
        // incompatible (below the bound): diagnose mode must still allow the
        // request (logging a warning) instead of rejecting it. A legacy worker
        // without component_info is also allowed.
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Diagnose,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            ..Default::default()
        };
        let incompatible = ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_component_info()
        };
        let verdict = policy.check_worker(Some(&incompatible));
        assert_eq!(
            verdict,
            CompatibilityVerdict::VersionTooOld {
                peer: "0.1.0".to_string(),
                min: "0.2.0".to_string()
            }
        );
        let warned = DashMap::new();
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict,
            test_metrics()
        )
        .is_ok());

        // Legacy worker (no component_info): MissingInfo, allowed in diagnose.
        let verdict = policy.check_worker(None);
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict,
            test_metrics()
        )
        .is_ok());
    }

    #[test]
    fn enforce_mode_rejects_incompatible_worker_heartbeat() {
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            ..Default::default()
        };
        let incompatible = ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_component_info()
        };
        let verdict = policy.check_worker(Some(&incompatible));
        let warned = DashMap::new();
        let err = MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict,
            test_metrics(),
        )
        .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("rejected by compatibility policy"), "{msg}");
        assert!(msg.contains("0.1.0"), "{msg}");
    }

    #[test]
    fn enforce_mode_rejects_legacy_client_without_component_info() {
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            ..Default::default()
        };
        let verdict = policy.check_client(None);
        let warned = DashMap::new();
        let err = MasterHandler::check_peer_compatibility(
            "client",
            "client:127.0.0.1",
            &warned,
            policy.mode,
            verdict,
            test_metrics(),
        )
        .unwrap_err();
        assert!(format!("{}", err).contains("client rejected"));
    }

    #[test]
    fn compatibility_metrics_record_verdicts_and_rejections() {
        // Worker verdicts are recorded per peer in compat_worker_verdict and
        // enforce-mode rejections bump compat_enforce_rejected_total.
        let metrics = test_metrics();

        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            ..Default::default()
        };
        let incompatible = ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_component_info()
        };
        let verdict = policy.check_worker(Some(&incompatible));
        let warned = DashMap::new();

        // A compatible worker records the compatible verdict. Unique peer ids
        // keep this test isolated: metrics are process-global and other tests
        // also exercise worker:7 / worker:8 concurrently.
        MasterHandler::check_peer_compatibility(
            "worker",
            "worker:700",
            &warned,
            CompatibilityMode::Diagnose,
            CompatibilityVerdict::Compatible,
            metrics,
        )
        .unwrap();
        assert_eq!(
            metrics
                .compat_worker_verdict
                .with_label_values(&["700", "compatible"])
                .get(),
            1
        );

        // When a peer's verdict changes, the previous label must be cleared:
        // only one label per peer is active at a time.
        MasterHandler::check_peer_compatibility(
            "worker",
            "worker:700",
            &warned,
            CompatibilityMode::Diagnose,
            CompatibilityVerdict::VersionTooOld {
                peer: "0.1.0".to_string(),
                min: "0.2.0".to_string(),
            },
            metrics,
        )
        .unwrap();
        assert_eq!(
            metrics
                .compat_worker_verdict
                .with_label_values(&["700", "version_too_old"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .compat_worker_verdict
                .with_label_values(&["700", "compatible"])
                .get(),
            0
        );

        // An enforce-mode rejection (too-old worker) bumps the counter and
        // records the verdict gauge. A unique component label keeps this test
        // isolated from other tests that also land in the version_too_old
        // series (metrics are process-global and tests run in parallel).
        let before = metrics
            .compat_enforce_rejected_total
            .with_label_values(&["worker-test", "version_too_old"])
            .get();
        let err = MasterHandler::check_peer_compatibility(
            "worker-test",
            "worker:701",
            &warned,
            policy.mode,
            verdict,
            metrics,
        )
        .unwrap_err();
        assert!(format!("{}", err).contains("rejected by compatibility policy"));
        assert_eq!(
            metrics
                .compat_enforce_rejected_total
                .with_label_values(&["worker-test", "version_too_old"])
                .get(),
            before + 1
        );
        assert_eq!(
            metrics
                .compat_worker_verdict
                .with_label_values(&["701", "version_too_old"])
                .get(),
            1
        );

        // Client verdicts use the client gauge.
        MasterHandler::check_peer_compatibility(
            "client",
            "client:10.0.0.1",
            &warned,
            CompatibilityMode::Diagnose,
            CompatibilityVerdict::MissingInfo,
            metrics,
        )
        .unwrap();
        assert_eq!(
            metrics
                .compat_client_verdict
                .with_label_values(&["client:10.0.0.1", "missing_info"])
                .get(),
            1
        );
    }

    #[test]
    fn compatibility_to_pb_reflects_policy() {
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            blocked_versions: vec!["0.2.5".parse().unwrap()],
            ..Default::default()
        };
        let master_version = curvine_sys::version::component_version("master");
        let pb = ProtoUtils::compatibility_to_pb(&master_version, &policy);
        assert_eq!(
            pb.compatibility_mode,
            CompatibilityModeProto::Enforce as i32
        );
        assert_eq!(pb.min_worker_version.as_deref(), Some("0.2.0"));
        assert_eq!(pb.blocked_versions, vec!["0.2.5".to_string()]);
    }

    fn sample_component_info() -> ComponentInfoProto {
        ComponentInfoProto {
            component: Some("worker".to_string()),
            release_version: Some("0.4.0-alpha".to_string()),
            git_commit: Some("24c848719b5b4fea74519d91cbe462bb49761b36".to_string()),
            git_tag: Some("v0.4.0-alpha".to_string()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: vec!["transfer".to_string()],
        }
    }

    /// Shared metrics instance for compatibility metric tests.
    fn test_metrics() -> &'static MasterMetrics {
        Master::init_test_metrics();
        Master::get_metrics().unwrap()
    }
}
