use ndp::{NdpHeader, NdpMessage, NdpOp};
use tokio::io::duplex;

#[tokio::test]
async fn test_write_read_roundtrip_no_data() {
    let (mut client, mut server) = duplex(1024);
    let header = NdpHeader::request(NdpOp::Ping, 1, 0, 0, 0);
    let msg = NdpMessage::new(header, None);
    msg.write_to(&mut client).await.unwrap();

    let received = NdpMessage::read_from(&mut server).await.unwrap();
    assert_eq!(received.header.op, NdpOp::Ping);
    assert_eq!(received.header.request_id, 1);
    assert!(received.data.is_none());
}

#[tokio::test]
async fn test_write_read_roundtrip_with_data() {
    let (mut client, mut server) = duplex(8192);
    let header = NdpHeader::request(NdpOp::Write, 42, 0xBEEF, 4096, 0);
    let data = vec![0xAB; 4096];
    let msg = NdpMessage::new(header, Some(data.clone()));
    msg.write_to(&mut client).await.unwrap();

    let received = NdpMessage::read_from(&mut server).await.unwrap();
    assert_eq!(received.header.op, NdpOp::Write);
    assert_eq!(received.header.request_id, 42);
    assert_eq!(received.header.data_length, 4096);
    assert_eq!(received.data.unwrap(), data);
}

#[tokio::test]
async fn test_data_crc_is_set() {
    let (mut client, mut server) = duplex(8192);
    let header = NdpHeader::request(NdpOp::Write, 1, 0, 0, 0);
    let data = vec![0xFF; 128];
    let msg = NdpMessage::new(header, Some(data));
    msg.write_to(&mut client).await.unwrap();

    let received = NdpMessage::read_from(&mut server).await.unwrap();
    assert!(received.header.data_crc != 0);
}

#[tokio::test]
async fn test_multiple_messages() {
    let (mut client, mut server) = duplex(65536);
    for i in 0..10u64 {
        let h = NdpHeader::request(NdpOp::Read, i, 0, i * 4096, 0);
        NdpMessage::new(h, None)
            .write_to(&mut client)
            .await
            .unwrap();
    }
    for i in 0..10u64 {
        let msg = NdpMessage::read_from(&mut server).await.unwrap();
        assert_eq!(msg.header.request_id, i);
        assert_eq!(msg.header.offset, i * 4096);
    }
}
