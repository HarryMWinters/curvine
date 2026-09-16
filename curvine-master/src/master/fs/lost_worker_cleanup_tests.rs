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

fn fixture(name: &str, worker_id: u32) -> (MasterFilesystem, WorkerInfo, i64) {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = true;
    conf.master.meta_dir = Utils::test_sub_dir(format!(
        "lost-worker-retry/{name}/meta-{}",
        Utils::rand_str(6)
    ));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "lost-worker-retry/{name}/journal-{}",
        Utils::rand_str(6)
    ));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = worker_id;
    worker.worker_session_id = name.to_string();
    fs.add_test_worker(worker.clone());
    fs.create_with_opts(
        "/cache",
        CreateFileOptsBuilder::new()
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let client = ClientAddress::default();
    let block = fs
        .add_block("/cache", None, client.clone(), vec![], vec![], 0, None)
        .unwrap();
    fs.complete_file(
        "/cache",
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
    fs.set_attr(
        "/cache",
        SetAttrOptsBuilder::new().ufs_mtime(12_345).build(),
    )
    .unwrap();
    (fs, worker, block.block.id)
}

fn fail_once(worker_id: u32) -> String {
    let rule_id = format!("lost-worker-retry-{worker_id}");
    let rule = FaultRuleBuilder::named("master.cache.before_invalidate_lost_chunk")
        .matches("worker_id", worker_id)
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected cache invalidation failure")
        .unwrap();
    FaultRuntime::process().configure(&rule_id, rule).unwrap();
    rule_id
}

fn heartbeat(
    fs: &MasterFilesystem,
    worker: &WorkerInfo,
    status: HeartbeatStatus,
) -> Vec<WorkerCommand> {
    fs.worker_manager
        .write()
        .heartbeat(
            "curvine",
            status,
            worker.address.clone(),
            worker.weight,
            worker.worker_session_id.clone(),
            worker.transfer_capabilities.clone(),
            worker.software_version.clone(),
            worker.startup_time_ms,
            worker.storage_map.values().cloned().collect(),
            None,
        )
        .unwrap()
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "cache invalidation retry did not finish"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn lost_cache_invalidation_retries_after_worker_index_is_empty() {
    let (fs, worker, block_id) = fixture("lost-location-retry", 91_001);
    let rule = fail_once(worker.worker_id());
    assert!(fs.delete_locations(worker.worker_id()).is_err());
    assert!(fs
        .fs_dir
        .read()
        .get_worker_block_ids(worker.worker_id())
        .unwrap()
        .is_empty());
    assert!(fs.file_status("/cache").unwrap().cv_valid(None));

    let cleanup = fs.delete_locations(worker.worker_id()).unwrap();
    assert_eq!(cleanup.removed_block_ids, vec![block_id]);
    assert!(cleanup.replication_block_ids.is_empty());
    assert!(!fs.file_status("/cache").unwrap().cv_valid(None));
    let commands = heartbeat(&fs, &worker, HeartbeatStatus::Running);
    assert!(
        commands.iter().any(|command| match command {
            WorkerCommand::DeleteBlock(command) => command.blocks.contains(&block_id),
        }),
        "the removed worker must receive deletion of its discarded cache block"
    );
    FaultRuntime::process().remove(rule).unwrap();
}

#[test]
fn failed_full_reconcile_is_retried_before_worker_becomes_ready() {
    let (fs, mut worker, block_id) = fixture("full-report-retry", 91_002);
    worker.worker_session_id = "restarted-full-report-retry".into();
    worker.startup_time_ms += 1;
    heartbeat(&fs, &worker, HeartbeatStatus::Start);
    let rule = fail_once(worker.worker_id());
    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: worker.worker_id(),
            worker_session_id: worker.worker_session_id.clone(),
            full_report: true,
            total_len: 0,
            blocks: vec![],
        },
        None,
    )
    .unwrap();
    wait_until(|| {
        !fs.full_block_reconciles
            .lock()
            .get(&worker.worker_id())
            .map(|state| state.running)
            .unwrap_or(false)
    });
    assert!(!fs
        .worker_manager
        .read()
        .worker_block_report_complete(worker.worker_id(), &worker.worker_session_id));
    assert!(fs
        .fs_dir
        .read()
        .get_worker_block_ids(worker.worker_id())
        .unwrap()
        .is_empty());
    assert!(fs.file_status("/cache").unwrap().cv_valid(None));
    assert!(
        fs.full_block_reconciles
            .lock()
            .get(&worker.worker_id())
            .is_some_and(|state| state.pending.is_some()),
        "failed reconciliation must remain pending"
    );

    fs.retry_full_block_reconciles(u64::MAX);
    wait_until(|| {
        fs.worker_manager
            .read()
            .worker_block_report_complete(worker.worker_id(), &worker.worker_session_id)
    });
    assert!(!fs.file_status("/cache").unwrap().cv_valid(None));
    let commands = heartbeat(&fs, &worker, HeartbeatStatus::Running);
    assert!(commands.iter().any(|command| match command {
        WorkerCommand::DeleteBlock(command) => command.blocks.contains(&block_id),
    }));
    FaultRuntime::process().remove(rule).unwrap();
}

#[test]
fn invalidation_retry_journals_applied_inode_and_deletes_surviving_blocks() {
    use crate::master::journal::JournalEntry;

    const BLOCK_LEN: i64 = 1 << 20;

    let (fs, worker, fixture_block_id) = fixture("post-apply-retry", 91_003);
    let mut survivor = worker.clone();
    survivor.address.worker_id = 91_004;
    survivor.address.rpc_port += 1;
    survivor.worker_session_id = "survivor-post-apply-retry".into();
    fs.add_test_worker(survivor.clone());
    fs.create_with_opts(
        "/multi-cache",
        CreateFileOptsBuilder::new()
            .block_size(BLOCK_LEN)
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let client = ClientAddress::default();
    let first = fs
        .add_block(
            "/multi-cache",
            None,
            client.clone(),
            vec![],
            vec![survivor.worker_id()],
            0,
            None,
        )
        .unwrap();
    let first_commit = CommitBlock {
        block_id: first.block.id,
        block_len: BLOCK_LEN,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    let second = fs
        .add_block(
            "/multi-cache",
            None,
            client.clone(),
            vec![first_commit.clone()],
            vec![worker.worker_id()],
            BLOCK_LEN,
            Some(first.block.clone()),
        )
        .unwrap();
    fs.complete_file(
        "/multi-cache",
        None,
        BLOCK_LEN * 2,
        vec![
            first_commit,
            CommitBlock {
                block_id: second.block.id,
                block_len: BLOCK_LEN,
                locations: vec![BlockLocation::with_id(survivor.worker_id())],
            },
        ],
        &client.client_name,
        false,
        None,
    )
    .unwrap();
    fs.set_attr(
        "/multi-cache",
        SetAttrOptsBuilder::new().ufs_mtime(12_345).build(),
    )
    .unwrap();
    let before = fs.file_status("/multi-cache").unwrap();
    assert_eq!(before.len, BLOCK_LEN * 2);
    assert_eq!(first.locs[0].worker_id, worker.worker_id());
    assert_eq!(second.locs[0].worker_id, survivor.worker_id());
    fs.fs_dir.read().take_entries();
    let rule_id = "post-apply-lost-cache-retry";
    let rule = FaultRuleBuilder::named("master.cache.after_apply_lost_chunk")
        .matches("worker_id", worker.worker_id())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("journal append unavailable")
        .unwrap();
    FaultRuntime::process().configure(rule_id, rule).unwrap();

    assert!(fs.delete_locations(worker.worker_id()).is_err());
    assert!(!fs.file_status("/multi-cache").unwrap().cv_valid(None));
    assert!(
        fs.fs_dir.read().take_entries().is_empty(),
        "fault precedes journal append"
    );
    let cleanup = fs.delete_locations(worker.worker_id()).unwrap();
    let mut expected_removed = vec![fixture_block_id, first.block.id];
    expected_removed.sort_unstable();
    assert_eq!(cleanup.removed_block_ids, expected_removed);
    assert!(cleanup.replication_block_ids.is_empty());
    assert!(fs.fs_dir.read().take_entries().iter().any(|entry| {
        matches!(entry, JournalEntry::CacheInvalidation(entry) if entry.inodes.iter().any(|inode| {
            matches!(inode, InodeView::File(file) if file.id == before.id && file.len == before.len && file.storage_policy.ufs_only() && file.blocks.is_empty())
        }))
    }), "retry must journal the current invalidated inode");
    for (target, block_id) in [(&worker, first.block.id), (&survivor, second.block.id)] {
        let commands = heartbeat(&fs, target, HeartbeatStatus::Running);
        assert!(
            commands.iter().any(|command| match command {
                WorkerCommand::DeleteBlock(command) => command.blocks.contains(&block_id),
            }),
            "retry must reclaim block {block_id} on worker {}",
            target.worker_id()
        );
    }
    FaultRuntime::process().remove(rule_id).unwrap();
}
