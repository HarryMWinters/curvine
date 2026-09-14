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

use super::*;
use curvine_fault::{FaultRuleBuilder, FaultRuntime};
use curvine_runtime::common::Utils;
use std::time::{Duration, Instant};

struct ReconcileCompletionHook {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

fn reconcile_completion_hooks() -> &'static Mutex<HashMap<u32, ReconcileCompletionHook>> {
    static HOOKS: std::sync::OnceLock<Mutex<HashMap<u32, ReconcileCompletionHook>>> =
        std::sync::OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn pause_completed_reconcile_for_test(worker_id: u32) {
    // Remove the hook before waiting, so other workers and test teardown never
    // need this global mutex while the executor is paused.
    let hook = reconcile_completion_hooks().lock().remove(&worker_id);
    let Some(hook) = hook else {
        return;
    };
    if hook.entered.send(()).is_ok() {
        let _ = hook.release.recv_timeout(Duration::from_secs(10));
    }
}

struct ReconcileCompletionPause {
    worker_id: u32,
    release: std::sync::mpsc::Sender<()>,
}

impl ReconcileCompletionPause {
    fn install(worker_id: u32) -> (Self, std::sync::mpsc::Receiver<()>) {
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        assert!(reconcile_completion_hooks()
            .lock()
            .insert(
                worker_id,
                ReconcileCompletionHook {
                    entered,
                    release: release_rx
                }
            )
            .is_none());
        (Self { worker_id, release }, entered_rx)
    }
}

impl Drop for ReconcileCompletionPause {
    fn drop(&mut self) {
        let _ = self.release.send(());
        reconcile_completion_hooks().lock().remove(&self.worker_id);
    }
}

fn test_fs(worker_id: u32) -> (MasterFilesystem, WorkerInfo) {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    let name = Utils::rand_str(10);
    conf.master.meta_dir = Utils::test_sub_dir(format!("partial-report/meta-{name}"));
    conf.journal.journal_dir = Utils::test_sub_dir(format!("partial-report/journal-{name}"));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = worker_id;
    worker.worker_session_id = "partial-report-original".into();
    fs.add_test_worker(worker.clone());
    (fs, worker)
}

fn cache_block(fs: &MasterFilesystem, path: &str, worker_id: u32) -> i64 {
    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let client = ClientAddress::default();
    let block = fs
        .add_block(path, None, client.clone(), vec![], vec![], 0, None)
        .unwrap();
    fs.complete_file(
        path,
        None,
        128,
        vec![CommitBlock {
            block_id: block.block.id,
            block_len: 128,
            locations: vec![BlockLocation::with_id(worker_id)],
        }],
        &client.client_name,
        false,
        None,
    )
    .unwrap();
    fs.set_attr(path, SetAttrOptsBuilder::new().ufs_mtime(12_345).build())
        .unwrap();
    block.block.id
}

fn report(worker: &WorkerInfo, full_report: bool, blocks: Vec<BlockReportInfo>) -> BlockReportList {
    BlockReportList {
        cluster_id: "curvine".into(),
        worker_id: worker.worker_id(),
        worker_session_id: worker.worker_session_id.clone(),
        full_report,
        total_len: blocks.len() as u64,
        blocks,
    }
}

fn finalized(id: i64) -> BlockReportInfo {
    BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, 128)
}

#[test]
fn partial_incremental_report_preserves_successfully_applied_chunk_in_queued_reconcile() {
    struct RuleGuard(&'static str);
    impl Drop for RuleGuard {
        fn drop(&mut self) {
            let _ = FaultRuntime::process().remove(self.0);
        }
    }

    let (fs, mut worker) = test_fs(92_101);
    let snapshot_id = cache_block(&fs, "/snapshot-cache", worker.worker_id());
    let accepted_id = cache_block(&fs, "/accepted-cache", worker.worker_id());
    let failed_id = cache_block(&fs, "/failed-cache", worker.worker_id());
    // The incremental report will install this valid location for the first
    // time. Its file metadata is already complete and backed by UFS.
    fs.fs_dir
        .write()
        .block_report(vec![(
            false,
            accepted_id,
            BlockLocation::with_id(worker.worker_id()),
        )])
        .unwrap();
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(accepted_id)
        .unwrap()
        .is_empty());

    worker.worker_session_id = "partial-report-restarted".into();
    worker.startup_time_ms += 1;
    fs.worker_manager
        .write()
        .heartbeat(
            "curvine",
            HeartbeatStatus::Start,
            worker.address.clone(),
            worker.weight,
            worker.worker_session_id.clone(),
            worker.transfer_capabilities.clone(),
            worker.software_version.clone(),
            worker.startup_time_ms,
            worker.storage_map.values().cloned().collect(),
            None,
        )
        .unwrap();

    // Keep reconciliation queued until both report chunks have been attempted.
    // Dropping release_tx on a failed assertion also unblocks the executor.
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    fs.full_block_reconcile_executor_for_test()
        .fixed_spawn(worker.worker_id() as i64, move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(Duration::from_secs(10));
        })
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("executor blocker did not start");
    fs.block_report(report(&worker, true, vec![finalized(snapshot_id)]), None)
        .unwrap();

    let rule_id = "partial-incremental-second-chunk";
    let rule = FaultRuleBuilder::named("master.block_report.before_apply_chunk")
        .matches("worker_id", worker.worker_id())
        .unwrap()
        .matches("chunk_first_block_id", failed_id)
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("second chunk rejected")
        .unwrap();
    FaultRuntime::process().configure(rule_id, rule).unwrap();
    let rule_guard = RuleGuard(rule_id);
    // Duplicate IDs exercise the production chunk boundary without creating
    // thousands of files or block records.
    let mut incremental: Vec<_> = (0..MasterFilesystem::BLOCK_REPORT_WRITE_CHUNK)
        .map(|_| finalized(accepted_id))
        .collect();
    incremental.push(finalized(failed_id));
    let error = fs
        .block_report(report(&worker, false, incremental), None)
        .err()
        .expect("the second incremental chunk must fail");
    assert!(error.to_string().contains("second chunk rejected"));
    assert_eq!(
        fs.fs_dir
            .read()
            .get_block_locations(accepted_id)
            .unwrap()
            .len(),
        1
    );
    assert!(!fs
        .worker_manager
        .read()
        .worker_block_report_complete(worker.worker_id(), &worker.worker_session_id));
    drop(rule_guard);
    release_tx.send(()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while !fs
        .worker_manager
        .read()
        .worker_block_report_complete(worker.worker_id(), &worker.worker_session_id)
    {
        assert!(
            Instant::now() < deadline,
            "full reconciliation did not finish"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    for (path, id) in [
        ("/snapshot-cache", snapshot_id),
        ("/accepted-cache", accepted_id),
    ] {
        assert!(
            fs.file_status(path).unwrap().cv_valid(None),
            "accepted cache {path} was discarded"
        );
        let locations = fs.fs_dir.read().get_block_locations(id).unwrap();
        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].worker_id, worker.worker_id());
    }
    assert!(!fs.file_status("/failed-cache").unwrap().cv_valid(None));
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(failed_id)
        .unwrap()
        .is_empty());
}

#[test]
fn incremental_report_after_reconcile_completion_does_not_orphan_cleanup() {
    let (fs, mut worker) = test_fs(92_102);
    let path = "/completed-reconcile-cache";
    let block_id = cache_block(&fs, path, worker.worker_id());
    worker.worker_session_id = "completion-boundary-restarted".into();
    worker.startup_time_ms += 1;
    fs.worker_manager
        .write()
        .heartbeat(
            "curvine",
            HeartbeatStatus::Start,
            worker.address.clone(),
            worker.weight,
            worker.worker_session_id.clone(),
            worker.transfer_capabilities.clone(),
            worker.software_version.clone(),
            worker.startup_time_ms,
            worker.storage_map.values().cloned().collect(),
            None,
        )
        .unwrap();

    let (pause, entered) = ReconcileCompletionPause::install(worker.worker_id());
    fs.block_report(report(&worker, true, vec![finalized(block_id)]), None)
        .unwrap();
    entered
        .recv_timeout(Duration::from_secs(5))
        .expect("reconciliation did not reach the completion boundary");
    assert!(fs
        .worker_manager
        .read()
        .worker_block_report_complete(worker.worker_id(), &worker.worker_session_id));
    assert!(!fs.has_pending_worker_cleanup(worker.worker_id()));

    // The worker lifecycle lock has been released, but the reconciliation
    // executor is still paused. This incremental report must not mistake the
    // completed job's bookkeeping for another inventory that needs cleanup.
    fs.block_report(report(&worker, false, vec![finalized(block_id)]), None)
        .unwrap();
    assert!(
        !fs.has_pending_worker_cleanup(worker.worker_id()),
        "incremental report created cleanup with no queued reconciliation to finish it"
    );
    assert!(fs.worker_manager.read().accepts_running_heartbeat(
        &worker.address,
        &worker.worker_session_id,
        worker.startup_time_ms
    ));
    assert!(fs.file_status(path).unwrap().cv_valid(None));
    assert_eq!(
        fs.fs_dir
            .read()
            .get_block_locations(block_id)
            .unwrap()
            .len(),
        1
    );
    drop(pause);

    // The same executor lane reaches this sentinel only after the reconciler
    // returns; the test leaves no blocked background task behind.
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    fs.full_block_reconcile_executor_for_test()
        .fixed_spawn(worker.worker_id() as i64, move || {
            let _ = finished_tx.send(());
        })
        .unwrap();
    finished_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reconciler did not return");
    assert!(!fs.has_pending_worker_cleanup(worker.worker_id()));
}
