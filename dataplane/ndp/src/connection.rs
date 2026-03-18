use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};

use crate::codec::NdpMessage;
use crate::error::{NdpError, Result};
use crate::header::NdpHeader;

/// A multiplexed NDP connection. Multiple requests can be in-flight
/// simultaneously. Responses are matched to requests by request_id.
pub struct NdpConnection {
    writer: Arc<Mutex<WriteHalf<TcpStream>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<NdpMessage>>>>,
    next_id: AtomicU64,
    _reader_task: tokio::task::JoinHandle<()>,
}

impl NdpConnection {
    /// Connect to a remote NDP peer.
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self::from_stream(stream))
    }

    /// Wrap an existing TCP stream.
    pub fn from_stream(stream: TcpStream) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        let writer = Arc::new(Mutex::new(writer));
        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<NdpMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_clone = pending.clone();
        let reader_task = tokio::spawn(async move {
            Self::read_loop(reader, pending_clone).await;
        });

        Self {
            writer,
            pending,
            next_id: AtomicU64::new(1),
            _reader_task: reader_task,
        }
    }

    /// Send a request and wait for the response.
    pub async fn request(
        &self,
        mut header: NdpHeader,
        data: Option<Vec<u8>>,
    ) -> Result<NdpMessage> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        header.request_id = id;

        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let msg = NdpMessage::new(header, data);
        {
            let mut writer = self.writer.lock().await;
            msg.write_to(&mut *writer).await?;
        }

        rx.await.map_err(|_| NdpError::ConnectionClosed)
    }

    /// Send a message without waiting for a response (fire-and-forget).
    pub async fn send(&self, header: NdpHeader, data: Option<Vec<u8>>) -> Result<()> {
        let msg = NdpMessage::new(header, data);
        let mut writer = self.writer.lock().await;
        msg.write_to(&mut *writer).await
    }

    async fn read_loop(
        mut reader: ReadHalf<TcpStream>,
        pending: Arc<Mutex<HashMap<u64, oneshot::Sender<NdpMessage>>>>,
    ) {
        loop {
            match NdpMessage::read_from(&mut reader).await {
                Ok(msg) => {
                    let id = msg.header.request_id;
                    if let Some(tx) = pending.lock().await.remove(&id) {
                        let _ = tx.send(msg);
                    }
                }
                Err(_) => break,
            }
        }
        // Connection lost — clear all pending requests.
        pending.lock().await.clear();
    }
}
