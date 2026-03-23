use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};

use crate::codec::NdpMessage;
use crate::error::{NdpError, Result};
use crate::header::NdpHeader;

/// A multiplexed NDP connection. Multiple requests can be in-flight
/// simultaneously. Responses are matched to requests by request_id.
/// Works over TCP or Unix sockets.
pub struct NdpConnection {
    writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<NdpMessage>>>>,
    next_id: AtomicU64,
    _reader_task: tokio::task::JoinHandle<()>,
}

impl NdpConnection {
    /// Connect to a remote NDP peer via TCP.
    pub async fn connect(addr: &str) -> Result<Self> {
        // Try Unix socket first if addr looks like a path
        if addr.starts_with('/') || addr.starts_with("unix:") {
            let path = addr.strip_prefix("unix:").unwrap_or(addr);
            return Self::connect_unix(path).await;
        }
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self::from_async(stream))
    }

    /// Connect via Unix domain socket.
    pub async fn connect_unix(path: &str) -> Result<Self> {
        let stream = tokio::net::UnixStream::connect(path).await?;
        Ok(Self::from_async(stream))
    }

    /// Wrap any async stream (TCP or Unix).
    pub fn from_async<S: AsyncRead + AsyncWrite + Send + 'static>(stream: S) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        let writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>> =
            Arc::new(Mutex::new(Box::new(writer)));
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

    /// Wrap an existing TCP stream (backward compat).
    pub fn from_stream(stream: TcpStream) -> Self {
        Self::from_async(stream)
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
            writer.flush().await?;
        }

        rx.await.map_err(|_| NdpError::ConnectionClosed)
    }

    /// Send multiple requests and wait for all responses.
    /// Returns responses in the same order as requests.
    pub async fn batch_request(
        &self,
        requests: Vec<(NdpHeader, Option<Vec<u8>>)>,
    ) -> Result<Vec<NdpMessage>> {
        let count = requests.len();
        let mut receivers = Vec::with_capacity(count);

        // Send all requests under one writer lock (single syscall batch).
        {
            let mut writer = self.writer.lock().await;
            for (mut header, data) in requests {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                header.request_id = id;

                let (tx, rx) = oneshot::channel();
                self.pending.lock().await.insert(id, tx);
                receivers.push(rx);

                let msg = NdpMessage::new(header, data);
                msg.write_to(&mut *writer).await?;
            }
            writer.flush().await?;
        }

        // Wait for all responses.
        let mut results = Vec::with_capacity(count);
        for rx in receivers {
            results.push(rx.await.map_err(|_| NdpError::ConnectionClosed)?);
        }
        Ok(results)
    }

    /// Send a message without waiting for a response (fire-and-forget).
    pub async fn send(&self, header: NdpHeader, data: Option<Vec<u8>>) -> Result<()> {
        let msg = NdpMessage::new(header, data);
        let mut writer = self.writer.lock().await;
        msg.write_to(&mut *writer).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn read_loop<R: tokio::io::AsyncReadExt + Unpin>(
        mut reader: R,
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
