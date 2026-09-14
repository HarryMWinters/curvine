use curvine_model::ProtoUtils;
use curvine_proto::BlockReportListRequest;

#[test]
fn legacy_block_report_defaults_session_to_empty() {
    let report = ProtoUtils::block_report_list_from_pb(BlockReportListRequest::default());
    assert!(report.worker_session_id.is_empty());
}

#[test]
fn block_report_preserves_worker_session() {
    let report = ProtoUtils::block_report_list_from_pb(BlockReportListRequest {
        worker_session_id: Some("worker-process-session".to_string()),
        ..Default::default()
    });
    assert_eq!(report.worker_session_id, "worker-process-session");
}
