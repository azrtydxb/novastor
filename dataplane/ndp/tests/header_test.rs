use ndp::header::{volume_hash, NDP_HEADER_SIZE};
use ndp::{NdpError, NdpHeader, NdpOp};

#[test]
fn test_encode_decode_roundtrip() {
    let header = NdpHeader::request(NdpOp::Write, 42, 0xDEADBEEF, 4096, 4096);
    let mut buf = [0u8; NDP_HEADER_SIZE];
    header.encode(&mut buf);
    let decoded = NdpHeader::decode(&buf).unwrap();
    assert_eq!(decoded.op, NdpOp::Write);
    assert_eq!(decoded.request_id, 42);
    assert_eq!(decoded.volume_hash, 0xDEADBEEF);
    assert_eq!(decoded.offset, 4096);
    assert_eq!(decoded.data_length, 4096);
}

#[test]
fn test_invalid_magic() {
    let mut buf = [0u8; NDP_HEADER_SIZE];
    buf[0..4].copy_from_slice(&0x12345678u32.to_le_bytes());
    // Write a valid CRC for the corrupted header so we hit magic check first.
    let crc = crc32c::crc32c(&buf[0..48]);
    buf[48..52].copy_from_slice(&crc.to_le_bytes());
    let err = NdpHeader::decode(&buf).unwrap_err();
    assert!(matches!(err, NdpError::InvalidMagic(0x12345678)));
}

#[test]
fn test_crc_mismatch() {
    let header = NdpHeader::request(NdpOp::Read, 1, 0, 0, 0);
    let mut buf = [0u8; NDP_HEADER_SIZE];
    header.encode(&mut buf);
    buf[10] ^= 0xFF; // corrupt a byte in the header body
    let err = NdpHeader::decode(&buf).unwrap_err();
    assert!(matches!(err, NdpError::HeaderCrcMismatch { .. }));
}

#[test]
fn test_all_ops_roundtrip() {
    let ops = [
        NdpOp::Write,
        NdpOp::Read,
        NdpOp::WriteResp,
        NdpOp::ReadResp,
        NdpOp::WriteZeroes,
        NdpOp::Unmap,
        NdpOp::Replicate,
        NdpOp::ReplicateResp,
        NdpOp::EcShard,
        NdpOp::Ping,
        NdpOp::Pong,
    ];
    for op in ops {
        let h = NdpHeader::request(op, 99, 0, 0, 0);
        let mut buf = [0u8; NDP_HEADER_SIZE];
        h.encode(&mut buf);
        let d = NdpHeader::decode(&buf).unwrap();
        assert_eq!(d.op, op);
    }
}

#[test]
fn test_response_preserves_request_id() {
    let req = NdpHeader::request(NdpOp::Read, 777, 0xABCD, 8192, 0);
    let resp = NdpHeader::response(&req, NdpOp::ReadResp, 0, 4096);
    assert_eq!(resp.request_id, 777);
    assert_eq!(resp.volume_hash, 0xABCD);
    assert_eq!(resp.offset, 8192);
    assert_eq!(resp.data_length, 4096);
}

#[test]
fn test_volume_hash_deterministic() {
    let h1 = volume_hash("test-volume-uuid-123");
    let h2 = volume_hash("test-volume-uuid-123");
    assert_eq!(h1, h2);
    let h3 = volume_hash("different-volume");
    assert_ne!(h1, h3);
}

#[test]
fn test_header_size_is_64() {
    assert_eq!(NDP_HEADER_SIZE, 64);
}
