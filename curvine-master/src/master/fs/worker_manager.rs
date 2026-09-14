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

use crate::master::fs::policy::{ChooseContext, WorkerPolicyAdapter};
use crate::master::fs::state::{BlockMap, WorkerMap};
use crate::master::fs::DeleteResult;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsResult;
use curvine_model::{
    BlockLocation, ExtendedBlock, HeartbeatStatus, LocatedBlock, StorageInfo, StorageType,
    TransferWorkerCapabilities, WorkerAddress, WorkerCommand, WorkerInfo, WorkerStatus,
};
use curvine_proto::ComponentInfoProto;
use curvine_runtime::common::{ByteUnit, LocalTime};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

pub struct WorkerManager {
    pub(crate) worker_map: WorkerMap,
    pub(crate) block_map: BlockMap,
    pub(crate) worker_policy: WorkerPolicyAdapter,
    pub(crate) cluster_id: String,
    pub(crate) conf: ClusterConf,
    offline_workers: HashMap<u32, OfflineWorker>,
    recovering_workers: HashMap<u32, WorkerRecovery>,
    retired_worker_sessions: HashMap<u32, HashSet<String>>,
}

struct OfflineWorker {
    worker: WorkerInfo,
    deadline_ms: u64,
    in_flight: bool,
}

struct WorkerRecovery {
    address: WorkerAddress,
    session_id: String,
    startup_time_ms: u64,
    report_complete: bool,
    ended: bool,
}

impl WorkerRecovery {
    fn matches(&self, address: &WorkerAddress, session: &str, startup_time_ms: u64) -> bool {
        self.address.same_endpoint(address)
            && self.session_id == session
            && self.startup_time_ms == startup_time_ms
    }

    fn accepts_report(&self, session: &str) -> bool {
        !self.ended && (session.is_empty() || self.session_id == session)
    }
}

impl WorkerManager {
    pub fn new(conf: &ClusterConf) -> FsResult<Self> {
        let worker_policy = WorkerPolicyAdapter::from_conf(conf)?;

        Ok(Self {
            worker_map: WorkerMap::new(),
            block_map: BlockMap::new(),
            worker_policy,
            cluster_id: conf.cluster_id.to_string(),
            conf: conf.clone(),
            offline_workers: HashMap::new(),
            recovering_workers: HashMap::new(),
            retired_worker_sessions: HashMap::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn heartbeat(
        &mut self,
        cluster_id: &str,
        status: HeartbeatStatus,
        addr: WorkerAddress,
        weight: u32,
        worker_session_id: String,
        transfer_capabilities: TransferWorkerCapabilities,
        software_version: String,
        startup_time_ms: u64,
        storages: Vec<StorageInfo>,
        component_info: Option<ComponentInfoProto>,
    ) -> FsResult<Vec<WorkerCommand>> {
        // The cluster id must match to prevent misregistration.
        if cluster_id != self.cluster_id {
            return err_box!(
                "Registered cluster_id mismatch, expected {}, actual: {}",
                self.cluster_id,
                cluster_id
            );
        }

        let deadline_ms =
            LocalTime::mills().saturating_add(self.conf.master.worker_lost_interval_ms());
        let cmds = match status {
            HeartbeatStatus::Start => {
                info!("Worker register: {}", addr);
                // Enforce the same worker_id ↔ address rule as insert() before remove(): a Start
                // from a conflicting address must not evict the live registration.
                self.validate_worker_start(cluster_id, &addr, &worker_session_id, startup_time_ms)?;
                if self.is_duplicate_worker_start(&addr, &worker_session_id, startup_time_ms) {
                    return Ok(vec![]);
                }
                self.retire_superseded_worker_sessions(&addr, &worker_session_id);
                for stale in self.worker_map.remove_same_endpoint(&addr) {
                    warn!(
                        "Remove stale worker {} on restart endpoint {}",
                        stale.simple_debug(),
                        addr
                    );
                    self.remember_ended_worker(&stale);
                    self.schedule_offline_worker(stale, deadline_ms, false);
                }
                if let Some(worker) = self
                    .worker_map
                    .remove_for_restart(&addr)
                    .or_else(|| self.worker_map.lost_workers.get(&addr.worker_id).cloned())
                {
                    // A previous cleanup may have finished before this attempt.
                    // Re-arm it, while preserving any still-pending deadline.
                    self.schedule_offline_worker(worker, deadline_ms, false);
                } else {
                    // After master restart, persisted locations can exist before
                    // any worker registration. A failed first report still needs
                    // a bounded cleanup deadline for those locations.
                    let mut worker = WorkerInfo::new(addr.clone(), weight);
                    worker.worker_session_id = worker_session_id.clone();
                    worker.startup_time_ms = startup_time_ms;
                    worker.software_version = software_version.clone();
                    worker.transfer_capabilities = transfer_capabilities.clone();
                    worker.component_info = component_info.clone();
                    for storage in &storages {
                        worker.add_storage(storage.clone());
                    }
                    self.worker_map
                        .lost_workers
                        .insert(addr.worker_id, worker.clone());
                    self.schedule_offline_worker(worker, deadline_ms, false);
                }
                self.recovering_workers.insert(
                    addr.worker_id,
                    WorkerRecovery {
                        address: addr,
                        session_id: worker_session_id,
                        startup_time_ms,
                        report_complete: false,
                        ended: false,
                    },
                );
                return Ok(vec![]);
            }

            HeartbeatStatus::Running => {
                if !self.accepts_running_heartbeat(&addr, &worker_session_id, startup_time_ms) {
                    return Ok(vec![]);
                }
                self.block_map.handle_heartbeat(addr.worker_id)
            }

            HeartbeatStatus::End => {
                let Some(worker) = self.get_worker(addr.worker_id) else {
                    if let Some(recovery) = self.recovering_workers.get_mut(&addr.worker_id) {
                        if recovery.matches(&addr, &worker_session_id, startup_time_ms) {
                            recovery.ended = true;
                            recovery.report_complete = false;
                        }
                    }
                    return Ok(vec![]);
                };
                if !worker.address.same_endpoint(&addr)
                    || worker.worker_session_id != worker_session_id
                    || worker.startup_time_ms != startup_time_ms
                {
                    warn!("Ignore stale worker unregister: {}", addr);
                    return Ok(vec![]);
                }
                info!("Worker unregister: {}", addr);
                if let Some(worker) = self.worker_map.remove_offline(addr.worker_id) {
                    self.remember_ended_worker(&worker);
                    self.schedule_offline_worker(worker, deadline_ms, false);
                }
                return Ok(vec![]);
            }
        };

        self.worker_map.ensure_worker_id_addr(&addr)?;
        if self.get_worker(addr.worker_id).is_none() {
            self.retire_superseded_worker_sessions(&addr, &worker_session_id);
        }
        for stale in self.worker_map.remove_same_endpoint(&addr) {
            self.retire_worker_session(stale.worker_id(), &stale.worker_session_id);
            self.remember_ended_worker(&stale);
            self.schedule_offline_worker(stale, deadline_ms, false);
        }
        let worker_id = addr.worker_id;
        self.worker_map.insert(
            addr,
            weight,
            worker_session_id,
            transfer_capabilities,
            software_version,
            startup_time_ms,
            storages,
            component_info,
        )?;
        self.offline_workers.remove(&worker_id);
        self.recovering_workers.remove(&worker_id);
        Ok(cmds)
    }

    pub fn choose_worker(&self, ctx: ChooseContext) -> CommonResult<Vec<WorkerAddress>> {
        let replicas = ctx.replicas;
        let workers = self.worker_policy.choose(self.worker_map.workers(), ctx)?;

        if workers.is_empty() {
            err_box!("No available worker found")
        } else if workers.len() > replicas as usize {
            err_box!("The number of workers exceeds the number of replicas")
        } else {
            Ok(workers)
        }
    }

    /// Select the specified number of workers, do not rely on block information
    pub fn choose_workers(
        &self,
        count: usize,
        exclude_workers: Vec<u32>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        let workers =
            self.worker_policy
                .choose_workers(self.worker_map.workers(), count, exclude_workers)?;

        if workers.is_empty() {
            err_box!("No available worker found")
        } else if workers.len() > count {
            err_box!("The number of workers exceeds the requested count")
        } else {
            Ok(workers)
        }
    }

    pub fn get_last_heartbeat(&self) -> Vec<(u32, u64)> {
        let mut res = vec![];
        for worker in self.worker_map.workers() {
            res.push((*worker.0, worker.1.last_update));
        }
        res
    }

    pub fn available_bytes(&self) -> i64 {
        self.worker_map
            .workers()
            .values()
            .map(|worker| worker.available.max(0))
            .fold(0, i64::saturating_add)
    }

    pub fn remove_expired_worker(&mut self, id: u32) -> Option<WorkerInfo> {
        let worker = self.worker_map.remove_expired(id)?;
        // The heartbeat timeout has already elapsed. Its first cleanup is
        // dispatched by the checker directly, and retries stay immediately due.
        self.schedule_offline_worker(worker.clone(), 0, true);
        Some(worker)
    }

    pub(crate) fn take_expired_offline_workers(&mut self, now_ms: u64) -> Vec<WorkerInfo> {
        self.offline_workers
            .values_mut()
            .filter_map(|pending| {
                if !pending.in_flight && now_ms >= pending.deadline_ms {
                    pending.in_flight = true;
                    Some(pending.worker.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn queue_offline_worker(&mut self, worker: WorkerInfo) {
        if !self.is_lost_worker(&worker) {
            return;
        }
        if let Some(pending) = self.offline_workers.get_mut(&worker.worker_id()) {
            if Self::same_lost_worker(&pending.worker, &worker) {
                pending.in_flight = false;
                return;
            }
        }
        self.schedule_offline_worker(worker, 0, false);
    }

    pub(crate) fn ensure_report_cleanup(&mut self, worker_id: u32) {
        if self.get_worker(worker_id).is_some() {
            return;
        }
        if let Some(worker) = self.worker_map.lost_workers.get(&worker_id).cloned() {
            // Reports after an earlier cleanup can install locations again.
            // Bound that new attempt without extending an existing grace.
            let deadline =
                LocalTime::mills().saturating_add(self.conf.master.worker_lost_interval_ms());
            self.schedule_offline_worker(worker, deadline, false);
        }
    }

    pub(crate) fn finish_offline_worker_cleanup(&mut self, worker: &WorkerInfo) {
        if self
            .offline_workers
            .get(&worker.worker_id())
            .is_some_and(|pending| Self::same_lost_worker(&pending.worker, worker))
        {
            self.offline_workers.remove(&worker.worker_id());
        }
    }

    fn schedule_offline_worker(&mut self, worker: WorkerInfo, deadline_ms: u64, in_flight: bool) {
        if !self.is_lost_worker(&worker) {
            return;
        }
        if self
            .offline_workers
            .get(&worker.worker_id())
            .is_some_and(|pending| Self::same_lost_worker(&pending.worker, &worker))
        {
            return;
        }
        self.offline_workers.insert(
            worker.worker_id(),
            OfflineWorker {
                worker,
                deadline_ms,
                in_flight,
            },
        );
    }

    pub(crate) fn accepts_running_heartbeat(
        &self,
        address: &WorkerAddress,
        session: &str,
        startup_time_ms: u64,
    ) -> bool {
        if !session.is_empty()
            && self
                .retired_worker_sessions
                .get(&address.worker_id)
                .is_some_and(|retired| retired.contains(session))
        {
            return false;
        }
        if let Some(recovery) = self.recovering_workers.get(&address.worker_id) {
            return !recovery.ended
                && recovery.report_complete
                && recovery.matches(address, session, startup_time_ms);
        }
        match self
            .get_worker(address.worker_id)
            .or_else(|| self.worker_map.lost_workers().get(&address.worker_id))
        {
            Some(worker) => {
                worker.address.same_endpoint(address)
                    && worker.worker_session_id == session
                    && worker.startup_time_ms == startup_time_ms
            }
            // A master restart may first observe an already-running worker.
            None => true,
        }
    }

    pub(crate) fn validate_worker_start(
        &self,
        cluster_id: &str,
        address: &WorkerAddress,
        session: &str,
        _startup_time_ms: u64,
    ) -> FsResult<()> {
        if cluster_id != self.cluster_id {
            return err_box!(
                "Registered cluster_id mismatch, expected {}, actual: {}",
                self.cluster_id,
                cluster_id
            );
        }
        self.worker_map.ensure_worker_id_addr(address)?;
        if !session.is_empty()
            && self
                .retired_worker_sessions
                .get(&address.worker_id)
                .is_some_and(|retired| retired.contains(session))
        {
            return err_box!("Worker {} Start uses a retired process session", address);
        }
        Ok(())
    }

    fn retire_worker_session(&mut self, worker_id: u32, session: &str) {
        // Legacy workers without process IDs remain compatible, but their
        // delayed messages cannot be distinguished from a new process.
        if !session.is_empty() {
            self.retired_worker_sessions
                .entry(worker_id)
                .or_default()
                .insert(session.to_string());
        }
    }

    fn retire_superseded_worker_sessions(&mut self, address: &WorkerAddress, session: &str) {
        let superseded = self
            .worker_map
            .workers()
            .values()
            .chain(self.worker_map.lost_workers().values())
            .map(|worker| (&worker.address, worker.worker_session_id.as_str()))
            .chain(
                self.recovering_workers
                    .values()
                    .map(|worker| (&worker.address, worker.session_id.as_str())),
            )
            .filter(|(known_address, known_session)| {
                !known_session.is_empty()
                    && if known_address.worker_id == address.worker_id {
                        *known_session != session
                    } else {
                        known_address.same_endpoint(address)
                    }
            })
            .map(|(address, session)| (address.worker_id, session.to_string()))
            .collect::<Vec<_>>();
        for (worker_id, session) in superseded {
            self.retire_worker_session(worker_id, &session);
        }
    }

    pub(crate) fn is_duplicate_worker_start(
        &self,
        address: &WorkerAddress,
        session: &str,
        startup_time_ms: u64,
    ) -> bool {
        if let Some(recovery) = self.recovering_workers.get(&address.worker_id) {
            return !recovery.ended && recovery.matches(address, session, startup_time_ms);
        }
        self.get_worker(address.worker_id).is_some_and(|worker| {
            worker.address.same_endpoint(address)
                && worker.worker_session_id == session
                && worker.startup_time_ms == startup_time_ms
        })
    }

    fn remember_ended_worker(&mut self, worker: &WorkerInfo) {
        self.recovering_workers.insert(
            worker.worker_id(),
            WorkerRecovery {
                address: worker.address.clone(),
                session_id: worker.worker_session_id.clone(),
                startup_time_ms: worker.startup_time_ms,
                report_complete: false,
                ended: true,
            },
        );
    }

    pub(crate) fn block_report_session_matches(&self, worker_id: u32, session: &str) -> bool {
        if let Some(recovery) = self.recovering_workers.get(&worker_id) {
            return recovery.accepts_report(session);
        }
        if session.is_empty() {
            return true;
        }
        self.get_worker(worker_id)
            .is_some_and(|worker| worker.worker_session_id == session)
    }

    pub(crate) fn complete_worker_block_report(&mut self, worker_id: u32, session: &str) {
        if let Some(recovery) = self.recovering_workers.get_mut(&worker_id) {
            if recovery.accepts_report(session) {
                recovery.report_complete = true;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn worker_block_report_complete(&self, worker_id: u32, session: &str) -> bool {
        self.recovering_workers
            .get(&worker_id)
            .is_some_and(|recovery| recovery.accepts_report(session) && recovery.report_complete)
    }

    fn same_lost_worker(left: &WorkerInfo, right: &WorkerInfo) -> bool {
        left.address.same_endpoint(&right.address)
            && left.worker_session_id == right.worker_session_id
            && left.startup_time_ms == right.startup_time_ms
            && left.last_update == right.last_update
    }

    pub(crate) fn is_lost_worker(&self, worker: &WorkerInfo) -> bool {
        self.get_worker(worker.worker_id()).is_none()
            && self
                .worker_map
                .lost_workers()
                .get(&worker.worker_id())
                .is_some_and(|lost| Self::same_lost_worker(lost, worker))
    }

    pub fn add_blacklist_worker(&mut self, id: u32) -> Option<WorkerInfo> {
        match self.worker_map.workers.get_mut(&id) {
            Some(v) if v.status != WorkerStatus::Blacklist => {
                v.status = WorkerStatus::Blacklist;
                Some(v.clone())
            }

            _ => None,
        }
    }

    pub fn remove_block(&mut self, worker_id: u32, block_id: i64) {
        self.block_map.remove_block(worker_id, block_id)
    }

    // Indicates the block that needs to be deleted.
    pub fn remove_blocks(&mut self, del_res: &DeleteResult) {
        self.block_map.remove_blocks(del_res)
    }

    pub fn deleted_block(&mut self, worker_id: u32, block_id: i64) {
        self.block_map.deleted_block(worker_id, block_id)
    }

    pub fn get_worker(&self, id: u32) -> Option<&WorkerInfo> {
        self.worker_map.workers.get(&id)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        let mut addrs = Vec::with_capacity(locs.len());
        let mut live_storage_types = Vec::with_capacity(locs.len());
        for loc in locs {
            if let Some(info) = self.get_worker(loc.worker_id) {
                addrs.push(info.address.clone());
                live_storage_types.push(loc.storage_type);
            } else {
                warn!(
                    "File {} block {}, worker {} replicas has been lost",
                    path.as_ref(),
                    block.id,
                    loc.worker_id
                );
            }
        }

        if addrs.is_empty() && !locs.is_empty() {
            return err_box!(
                "File {} block {}, all replicas has been lost",
                path.as_ref(),
                block.id
            );
        }

        let has_spdk = live_storage_types.contains(&StorageType::SpdkDisk);
        let lb = LocatedBlock {
            block,
            locs: addrs,
            has_spdk,
        };

        Ok(lb)
    }

    pub fn workers_have_spdk(&self, addrs: &[WorkerAddress]) -> bool {
        for addr in addrs {
            if let Some(info) = self.get_worker(addr.worker_id) {
                if info
                    .storage_map
                    .values()
                    .any(|s| s.storage_type == StorageType::SpdkDisk)
                {
                    return true;
                }
            }
        }
        false
    }

    pub fn add_test_worker(&mut self, worker: WorkerInfo) {
        self.worker_map.workers.insert(worker.worker_id(), worker);
    }

    pub fn add_dcm(&mut self, list: Vec<String>) -> Vec<String> {
        let mut set = HashSet::new();
        for addr in list {
            set.insert(addr);
        }

        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter_mut() {
            if set.contains(&worker.address.hostname) {
                worker.status = WorkerStatus::Decommission;
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn get_dcm(&self) -> Vec<String> {
        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter() {
            if worker.status == WorkerStatus::Decommission {
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn remove_dcm(&mut self, list: Vec<String>) -> Vec<String> {
        let mut set = HashSet::new();
        for addr in list {
            set.insert(addr);
        }

        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter_mut() {
            if set.contains(&worker.address.hostname) {
                worker.status = WorkerStatus::Live;
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn worker_list(&self) -> Vec<String> {
        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter() {
            res.push(worker.simple_string())
        }
        res
    }
}

impl Display for WorkerManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut str = String::new();
        for (_, item) in self.worker_map.workers() {
            let s = format!(
                "worker_id={}, address={}, capacity={}, available={}\n",
                item.worker_id(),
                item.address,
                ByteUnit::byte_to_string(item.capacity as u64),
                ByteUnit::byte_to_string(item.available as u64),
            );
            str.push_str(&s)
        }

        write!(f, "{}", str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lifecycle_manager() -> WorkerManager {
        let mut conf = ClusterConf::default();
        conf.master.worker_lost_interval_unit =
            curvine_runtime::common::DurationUnit::from_str("10m").unwrap();
        WorkerManager::new(&conf).unwrap()
    }

    fn lifecycle_heartbeat(
        manager: &mut WorkerManager,
        status: HeartbeatStatus,
        session: &str,
        startup: u64,
    ) {
        try_lifecycle_heartbeat(manager, status, session, startup).unwrap();
    }

    fn try_lifecycle_heartbeat(
        manager: &mut WorkerManager,
        status: HeartbeatStatus,
        session: &str,
        startup: u64,
    ) -> FsResult<()> {
        manager
            .heartbeat(
                &manager.cluster_id.clone(),
                status,
                WorkerAddress {
                    worker_id: 7,
                    hostname: "worker-7".into(),
                    ip_addr: "127.0.0.1".into(),
                    rpc_port: 8997,
                    web_port: 9001,
                },
                1,
                session.into(),
                TransferWorkerCapabilities::default(),
                "test".into(),
                startup,
                vec![],
                None,
            )
            .map(|_| ())
    }

    fn ended_manager() -> WorkerManager {
        let mut manager = lifecycle_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::End, "old", 1);
        manager
    }

    #[test]
    fn offline_grace_dispatches_at_deadline_once() {
        let mut manager = ended_manager();
        let deadline = manager.offline_workers[&7].deadline_ms;
        assert!(manager
            .take_expired_offline_workers(deadline - 1)
            .is_empty());
        let due = manager.take_expired_offline_workers(deadline);
        assert_eq!(due.len(), 1);
        assert!(manager.take_expired_offline_workers(u64::MAX).is_empty());
        manager.finish_offline_worker_cleanup(&due[0]);
        assert!(manager.offline_workers.is_empty());
    }

    #[test]
    fn duplicate_end_and_cleanup_retry_preserve_original_deadline() {
        let mut manager = ended_manager();
        let deadline = manager.offline_workers[&7].deadline_ms;
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::End, "old", 1);
        assert_eq!(manager.offline_workers[&7].deadline_ms, deadline);

        let worker = manager.take_expired_offline_workers(deadline).remove(0);
        manager.queue_offline_worker(worker);
        assert_eq!(manager.offline_workers[&7].deadline_ms, deadline);
        assert_eq!(manager.take_expired_offline_workers(deadline).len(), 1);
    }

    #[test]
    fn failed_restart_and_early_running_do_not_cancel_grace() {
        let mut manager = ended_manager();
        let deadline = manager.offline_workers[&7].deadline_ms;
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 2);
        assert!(manager.get_worker(7).is_none());
        assert_eq!(manager.offline_workers[&7].deadline_ms, deadline);
        assert_eq!(manager.take_expired_offline_workers(deadline).len(), 1);
    }

    #[test]
    fn completed_current_report_and_running_cancel_grace() {
        let mut manager = ended_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        assert!(!manager.block_report_session_matches(7, "old"));
        assert!(manager.block_report_session_matches(7, "new"));
        assert!(manager.block_report_session_matches(7, ""));
        manager.complete_worker_block_report(7, "old");
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 2);
        assert!(manager.get_worker(7).is_none());

        manager.complete_worker_block_report(7, "new");
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        assert!(manager.get_worker(7).is_none());
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 2);
        assert_eq!(manager.get_worker(7).unwrap().worker_session_id, "new");
        assert!(manager.take_expired_offline_workers(u64::MAX).is_empty());
    }

    #[test]
    fn end_requires_start_even_after_cleanup_finishes() {
        let mut manager = ended_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        assert!(manager.get_worker(7).is_none());
        assert!(!manager.block_report_session_matches(7, "old"));

        let worker = manager.take_expired_offline_workers(u64::MAX).remove(0);
        manager.finish_offline_worker_cleanup(&worker);
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        assert!(manager.get_worker(7).is_none());
    }

    #[test]
    fn ended_worker_rejects_reports_without_session() {
        let mut manager = ended_manager();
        assert!(!manager.block_report_session_matches(7, ""));
        let worker = manager.take_expired_offline_workers(u64::MAX).remove(0);
        manager.finish_offline_worker_cleanup(&worker);
        assert!(!manager.block_report_session_matches(7, ""));
    }

    #[test]
    fn legacy_report_can_complete_start_and_running_registration() {
        for session in ["", "current-session"] {
            let mut manager = lifecycle_manager();
            lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, session, 1);
            assert!(manager.block_report_session_matches(7, ""));
            manager.complete_worker_block_report(7, "");
            lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, session, 1);
            assert!(manager.get_worker(7).is_some());
            assert!(manager.block_report_session_matches(7, ""));
        }
    }

    #[test]
    fn start_of_live_worker_keeps_failed_restart_cleanup() {
        let mut manager = lifecycle_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        assert!(manager.get_worker(7).is_none());
        assert_eq!(manager.take_expired_offline_workers(u64::MAX).len(), 1);
    }

    #[test]
    fn fresh_start_failure_has_cleanup_deadline() {
        let mut manager = lifecycle_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        let worker = &manager.offline_workers[&7].worker;
        assert_eq!(worker.worker_session_id, "new");
        assert_eq!(worker.startup_time_ms, 2);
        assert_eq!(worker.software_version, "test");
        let deadline = manager.offline_workers[&7].deadline_ms;
        assert!(manager
            .take_expired_offline_workers(deadline - 1)
            .is_empty());
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::End, "new", 2);
        assert_eq!(manager.take_expired_offline_workers(deadline).len(), 1);
    }

    #[test]
    fn duplicate_start_preserves_recovery_progress_and_live_registration() {
        let mut manager = lifecycle_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        let deadline = manager.offline_workers[&7].deadline_ms;
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        assert_eq!(manager.offline_workers[&7].deadline_ms, deadline);
        assert!(!manager.worker_block_report_complete(7, "new"));

        manager.complete_worker_block_report(7, "new");
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        assert!(manager.worker_block_report_complete(7, "new"));
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 2);
        assert!(manager.get_worker(7).is_some());

        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        assert!(manager.get_worker(7).is_some());
        assert!(manager.offline_workers.is_empty());
    }

    #[test]
    fn ended_process_can_explicitly_register_again_without_extending_grace() {
        let mut manager = ended_manager();
        let deadline = manager.offline_workers[&7].deadline_ms;
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "old", 1);
        assert_eq!(manager.offline_workers[&7].deadline_ms, deadline);
        assert!(manager.block_report_session_matches(7, "old"));
        manager.complete_worker_block_report(7, "old");
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        assert!(manager.get_worker(7).is_some());
    }

    #[test]
    fn superseded_start_cannot_reset_recovery_or_evict_current_worker() {
        let mut manager = ended_manager();
        let deadline = manager.offline_workers[&7].deadline_ms;
        // Startup timestamps need not be monotonic across process restarts.
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 0);
        manager.complete_worker_block_report(7, "new");
        assert!(try_lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "old", 1).is_err());
        assert_eq!(manager.offline_workers[&7].deadline_ms, deadline);
        assert!(manager.worker_block_report_complete(7, "new"));

        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 0);
        let last_update = manager.get_worker(7).unwrap().last_update;
        assert!(try_lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "old", 1).is_err());
        let current = manager.get_worker(7).unwrap();
        assert_eq!(current.worker_session_id, "new");
        assert_eq!(current.last_update, last_update);
        assert!(manager.offline_workers.is_empty());
    }

    #[test]
    fn legacy_empty_session_is_not_retired() {
        let mut manager = lifecycle_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "", 1);
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        manager.complete_worker_block_report(7, "new");
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 2);
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "", 3);
        manager.complete_worker_block_report(7, "");
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "", 3);
        assert_eq!(manager.get_worker(7).unwrap().worker_session_id, "");
    }

    #[test]
    fn replaced_worker_id_cannot_start_its_old_session_again() {
        let mut manager = ended_manager();
        let mut replacement = manager.worker_map.lost_workers()[&7].address.clone();
        replacement.worker_id = 8;
        for status in [HeartbeatStatus::Start, HeartbeatStatus::Running] {
            manager
                .heartbeat(
                    &manager.cluster_id.clone(),
                    status,
                    replacement.clone(),
                    1,
                    "replacement".into(),
                    TransferWorkerCapabilities::default(),
                    "test".into(),
                    2,
                    vec![],
                    None,
                )
                .unwrap();
            manager.complete_worker_block_report(8, "replacement");
        }
        assert!(try_lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "old", 1).is_err());
        assert_eq!(
            manager.get_worker(8).unwrap().worker_session_id,
            "replacement"
        );
        assert!(manager.get_worker(7).is_none());
    }

    #[test]
    fn direct_running_replacement_retires_known_lost_session() {
        let mut manager = ended_manager();
        let mut replacement = manager.worker_map.lost_workers()[&7].address.clone();
        replacement.worker_id = 8;
        manager
            .heartbeat(
                &manager.cluster_id.clone(),
                HeartbeatStatus::Running,
                replacement,
                1,
                "replacement".into(),
                TransferWorkerCapabilities::default(),
                "test".into(),
                2,
                vec![],
                None,
            )
            .unwrap();
        assert!(try_lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "old", 1).is_err());
        assert_eq!(
            manager.get_worker(8).unwrap().worker_session_id,
            "replacement"
        );
    }

    #[test]
    fn heartbeat_timeout_retries_are_due_without_another_grace() {
        let mut manager = lifecycle_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        let worker = manager.remove_expired_worker(7).unwrap();
        manager.queue_offline_worker(worker);
        assert_eq!(manager.take_expired_offline_workers(0).len(), 1);
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        assert!(manager.get_worker(7).is_some());
        assert!(manager.take_expired_offline_workers(u64::MAX).is_empty());
    }

    #[test]
    fn retired_running_cannot_replace_the_current_timed_out_process() {
        let mut manager = ended_manager();
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Start, "new", 2);
        manager.complete_worker_block_report(7, "new");
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 2);
        let lost = manager.remove_expired_worker(7).unwrap();

        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "old", 1);
        assert!(manager.get_worker(7).is_none());
        assert_eq!(manager.offline_workers[&7].worker.worker_session_id, "new");
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 99);
        assert!(manager.get_worker(7).is_none());
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "unknown", 2);
        assert!(manager.get_worker(7).is_none());

        let mut wrong_endpoint = lost.address.clone();
        wrong_endpoint.rpc_port += 1;
        assert!(!manager.accepts_running_heartbeat(&wrong_endpoint, "new", 2));
        assert!(manager.accepts_running_heartbeat(&lost.address, "new", 2));
        lifecycle_heartbeat(&mut manager, HeartbeatStatus::Running, "new", 2);
        assert_eq!(manager.get_worker(7).unwrap().worker_session_id, "new");
        assert!(manager.offline_workers.is_empty());
    }

    fn worker_with_available(worker_id: u32, available: i64) -> WorkerInfo {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        worker.available = available;
        worker
    }

    #[test]
    fn available_bytes_clamps_negative_values_and_saturates() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        manager.add_test_worker(worker_with_available(1, -10));
        manager.add_test_worker(worker_with_available(2, 20));
        assert_eq!(manager.available_bytes(), 20);

        manager.add_test_worker(worker_with_available(3, i64::MAX));
        assert_eq!(manager.available_bytes(), i64::MAX);
    }
}
