use ndp::{NdpConnection, NdpHeader, NdpMessage, NdpOp};
use tokio::net::TcpListener;

async fn echo_server(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(stream);
    loop {
        match NdpMessage::read_from(&mut reader).await {
            Ok(msg) => {
                let resp = match msg.header.op {
                    NdpOp::Ping => {
                        NdpMessage::new(NdpHeader::response(&msg.header, NdpOp::Pong, 0, 0), None)
                    }
                    NdpOp::Read => NdpMessage::new(
                        NdpHeader::response(&msg.header, NdpOp::ReadResp, 0, 4),
                        Some(vec![0xAB; 4]),
                    ),
                    _ => NdpMessage::new(
                        NdpHeader::response(&msg.header, NdpOp::WriteResp, 0, 0),
                        None,
                    ),
                };
                if resp.write_to(&mut writer).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

#[tokio::test]
async fn test_connection_ping_pong() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(echo_server(listener));

    let conn = NdpConnection::connect(&addr).await.unwrap();
    let header = NdpHeader::request(NdpOp::Ping, 0, 0, 0, 0);
    let resp = conn.request(header, None).await.unwrap();
    assert_eq!(resp.header.op, NdpOp::Pong);
}

#[tokio::test]
async fn test_connection_read_with_data() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(echo_server(listener));

    let conn = NdpConnection::connect(&addr).await.unwrap();
    let header = NdpHeader::request(NdpOp::Read, 0, 0, 0, 0);
    let resp = conn.request(header, None).await.unwrap();
    assert_eq!(resp.header.op, NdpOp::ReadResp);
    assert_eq!(resp.data.unwrap(), vec![0xAB; 4]);
}

#[tokio::test]
async fn test_connection_write() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(echo_server(listener));

    let conn = NdpConnection::connect(&addr).await.unwrap();
    let header = NdpHeader::request(NdpOp::Write, 0, 0xBEEF, 4096, 0);
    let data = vec![0xFF; 4096];
    let resp = conn.request(header, Some(data)).await.unwrap();
    assert_eq!(resp.header.op, NdpOp::WriteResp);
    assert_eq!(resp.header.status, 0);
}

#[tokio::test]
async fn test_connection_sequential_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(echo_server(listener));

    let conn = NdpConnection::connect(&addr).await.unwrap();
    for _ in 0..20 {
        let h = NdpHeader::request(NdpOp::Ping, 0, 0, 0, 0);
        let resp = conn.request(h, None).await.unwrap();
        assert_eq!(resp.header.op, NdpOp::Pong);
    }
}
