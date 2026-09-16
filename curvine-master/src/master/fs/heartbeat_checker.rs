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

use crate::master::fs::MasterFilesystem;
use crate::master::quota::QuotaManager;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::MasterMonitor;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::runtime::{GroupExecutor, LoopTask};
use log::{error, info, warn};
use std::sync::Arc;

pub struct HeartbeatChecker {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    executor: Arc<GroupExecutor>,
    worker_blacklist_ms: u64,
    worker_lost_ms: u64,
    replication_manager: Arc<MasterReplicationManager>,
    quota_manager: Arc<QuotaManager>,
}

impl HeartbeatChecker {
    pub fn new(
        fs: MasterFilesystem,
        monitor: MasterMonitor,
        executor: Arc<GroupExecutor>,
        replication_manager: Arc<MasterReplicationManager>,
        quota_manager: Arc<QuotaManager>,
    ) -> Self {
        let worker_blacklist_ms = fs.conf.worker_blacklist_interval_ms();
        let worker_lost_ms = fs.conf.worker_lost_interval_ms();
        Self {
            fs,
            monitor,
            executor,
            worker_blacklist_ms,
            worker_lost_ms,
            replication_manager,
            quota_manager,
        }
    }
}

impl LoopTask for HeartbeatChecker {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        if !self.monitor.is_active() {
            return Ok(());
        }

        let mut blacklisted_workers = Vec::new();
        let mut removed_workers = Vec::new();
        let now = LocalTime::mills();
        self.fs.retry_full_block_reconciles(now);
        let candidates = {
            let wm = self.fs.worker_manager.read();
            wm.get_last_heartbeat()
                .into_iter()
                .filter(|(_, last_update)| {
                    now > last_update.saturating_add(self.worker_blacklist_ms)
                        || now > last_update.saturating_add(self.worker_lost_ms)
                })
                .filter_map(|(id, _)| wm.get_worker(id).cloned())
                .collect::<Vec<_>>()
        };
        for candidate in candidates {
            let id = candidate.worker_id();
            let lifecycle = self.fs.worker_lifecycle_lock(id);
            let _guard = lifecycle.lock();
            let mut wm = self.fs.worker_manager.write();
            let Some(current) = wm.get_worker(id) else {
                continue;
            };
            if current.last_update != candidate.last_update
                || current.address != candidate.address
                || current.worker_session_id != candidate.worker_session_id
                || current.startup_time_ms != candidate.startup_time_ms
            {
                continue;
            }
            let last_update = current.last_update;
            if now > last_update.saturating_add(self.worker_blacklist_ms) {
                if let Some(worker) = wm.add_blacklist_worker(id) {
                    blacklisted_workers.push((id, worker.address, worker.last_update));
                }
            }
            if now > last_update.saturating_add(self.worker_lost_ms) {
                if let Some(worker) = wm.remove_expired_worker(id) {
                    removed_workers.push(worker);
                }
            }
        }
        let offline_workers = self
            .fs
            .worker_manager
            .write()
            .take_expired_offline_workers(now);

        for (id, address, last_update) in blacklisted_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded blacklist timeout {} ms",
                id, address, last_update, self.worker_blacklist_ms
            );
        }

        for worker in &removed_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded lost timeout {} ms and will be removed",
                worker.worker_id(), worker.address, worker.last_update, self.worker_lost_ms
            );
        }
        removed_workers.extend(offline_workers);

        for worker in removed_workers {
            // Asynchronously delete all block location data.
            let id = worker.worker_id();
            let retry_worker = worker.clone();
            let fs = self.fs.clone();
            let rm = self.replication_manager.clone();
            let res = self.executor.spawn(move || {
                let spend = TimeSpent::new();
                let cleanup = match fs.delete_lost_worker_locations(&worker) {
                    Err(e) => {
                        warn!("{}", curvine_core_error::err_msg!(e));
                        fs.worker_manager.write().queue_offline_worker(worker);
                        return;
                    }
                    Ok(res) => res,
                };
                let replication_block_num = cleanup.replication_block_ids.len();
                if let Err(e) = rm.report_under_replicated_blocks(id, cleanup.replication_block_ids)
                {
                    error!(
                        "Errors on reporting under-replicated {} blocks. err: {:?}",
                        replication_block_num, e
                    );
                }
                info!(
                    "Delete worker {} all locations used {} ms",
                    id,
                    spend.used_ms()
                );
            });
            if let Err(e) = &res {
                warn!("{}", e);
                self.fs
                    .worker_manager
                    .write()
                    .queue_offline_worker(retry_worker);
            }
        }

        if let Ok(info) = self.fs.filesystem_info() {
            self.quota_manager.detector(Some(info));
        };

        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}
