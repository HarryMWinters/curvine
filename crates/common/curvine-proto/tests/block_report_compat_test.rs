use curvine_proto::{
    BlockReportInfoProto, BlockReportListRequest, BlockReportStatusProto, StorageTypeProto,
};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct LegacyBlockReportListRequest {
    #[prost(string, required, tag = "1")]
    cluster_id: String,
    #[prost(uint32, required, tag = "2")]
    worker_id: u32,
    #[prost(bool, required, tag = "3")]
    full_report: bool,
    #[prost(uint64, required, tag = "4")]
    total_len: u64,
    #[prost(message, repeated, tag = "5")]
    blocks: Vec<BlockReportInfoProto>,
}

#[derive(Clone, PartialEq, Message)]
struct BlockReportSession {
    #[prost(string, optional, tag = "6")]
    worker_session_id: Option<String>,
}

fn legacy_report(full_report: bool) -> LegacyBlockReportListRequest {
    LegacyBlockReportListRequest {
        cluster_id: "test-cluster".to_string(),
        worker_id: 7,
        full_report,
        total_len: 1,
        blocks: vec![BlockReportInfoProto {
            id: 42,
            status: BlockReportStatusProto::Finalized as i32,
            block_size: 4096,
            storage_type: StorageTypeProto::Disk as i32,
        }],
    }
}

#[test]
fn legacy_block_report_decodes_without_session() {
    let legacy = legacy_report(true);
    let decoded = BlockReportListRequest::decode(legacy.encode_to_vec().as_slice()).unwrap();
    let encoded = decoded.encode_to_vec();

    assert_eq!(
        LegacyBlockReportListRequest::decode(encoded.as_slice()).unwrap(),
        legacy
    );
    assert!(BlockReportSession::decode(encoded.as_slice())
        .unwrap()
        .worker_session_id
        .is_none());
}

#[test]
fn full_and_incremental_reports_preserve_session_on_wire() {
    for full_report in [true, false] {
        let mut encoded = legacy_report(full_report).encode_to_vec();
        let session = BlockReportSession {
            worker_session_id: Some("worker-process-session".to_string()),
        };
        encoded.extend(session.encode_to_vec());

        let decoded = BlockReportListRequest::decode(encoded.as_slice()).unwrap();
        let round_trip = decoded.encode_to_vec();
        assert_eq!(
            BlockReportSession::decode(round_trip.as_slice()).unwrap(),
            session
        );
        assert_eq!(
            LegacyBlockReportListRequest::decode(round_trip.as_slice()).unwrap(),
            legacy_report(full_report)
        );
    }
}

#[test]
fn legacy_master_ignores_block_report_session() {
    let legacy = legacy_report(false);
    let mut encoded = legacy.encode_to_vec();
    encoded.extend(
        BlockReportSession {
            worker_session_id: Some("worker-process-session".to_string()),
        }
        .encode_to_vec(),
    );

    assert_eq!(
        LegacyBlockReportListRequest::decode(encoded.as_slice()).unwrap(),
        legacy
    );
}
