use async_trait::async_trait;
use nbd_server::{BlockDevice, NbdError, NbdServer, Result};
use std::sync::{Arc, Mutex};

/// Simple in-memory block device for testing.
struct MemoryDevice {
    data: Arc<Mutex<Vec<u8>>>,
    size: u64,
}

impl MemoryDevice {
    fn new(size: u64) -> Self {
        Self {
            data: Arc::new(Mutex::new(vec![0u8; size as usize])),
            size,
        }
    }
}

#[async_trait]
impl BlockDevice for MemoryDevice {
    fn size(&self) -> u64 {
        self.size
    }

    async fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>> {
        let data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + length as usize;
        if end > data.len() {
            return Err(NbdError::Backend("read past end".into()));
        }
        Ok(data[start..end].to_vec())
    }

    async fn write(&self, offset: u64, write_data: &[u8]) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + write_data.len();
        if end > data.len() {
            return Err(NbdError::Backend("write past end".into()));
        }
        data[start..end].copy_from_slice(write_data);
        Ok(())
    }
}

#[test]
fn test_memory_device() {
    let dev = MemoryDevice::new(1024 * 1024);
    assert_eq!(dev.size(), 1024 * 1024);
}

#[tokio::test]
async fn test_memory_device_read_write() {
    let dev = MemoryDevice::new(4096);

    // Write some data.
    let data = vec![0xAB; 512];
    dev.write(0, &data).await.unwrap();

    // Read it back.
    let read = dev.read(0, 512).await.unwrap();
    assert_eq!(read, data);

    // Read unwritten area — should be zeros.
    let zeros = dev.read(1024, 512).await.unwrap();
    assert_eq!(zeros, vec![0u8; 512]);
}

#[tokio::test]
async fn test_nbd_server_handshake() {
    use nbd_server::protocol::*;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    let dev = MemoryDevice::new(10 * 1024 * 1024); // 10MB
    let server = NbdServer::new(dev);

    let (mut client, server_stream) = duplex(65536);

    // Run server in background.
    let server_handle = tokio::spawn(async move {
        server.serve_connection(server_stream).await
    });

    // Client side: read server hello.
    let magic = client.read_u64().await.unwrap();
    assert_eq!(magic, NBD_MAGIC);

    let opts_magic = client.read_u64().await.unwrap();
    assert_eq!(opts_magic, NBD_OPTS_MAGIC);

    let flags = client.read_u16().await.unwrap();
    assert!(flags & NBD_FLAG_FIXED_NEWSTYLE != 0);

    // Client sends flags.
    client
        .write_all(&(NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES).to_be_bytes())
        .await
        .unwrap();

    // Client sends OPT_EXPORT_NAME.
    client.write_all(&NBD_OPTS_MAGIC.to_be_bytes()).await.unwrap();
    client.write_all(&NBD_OPT_EXPORT_NAME.to_be_bytes()).await.unwrap();
    client.write_all(&0u32.to_be_bytes()).await.unwrap(); // no export name data
    client.flush().await.unwrap();

    // Server replies with size + transmission flags (no reply header for EXPORT_NAME).
    let size = client.read_u64().await.unwrap();
    assert_eq!(size, 10 * 1024 * 1024);

    let tx_flags = client.read_u16().await.unwrap();
    assert!(tx_flags & NBD_FLAG_HAS_FLAGS != 0);
    assert!(tx_flags & NBD_FLAG_SEND_FLUSH != 0);
    assert!(tx_flags & NBD_FLAG_SEND_TRIM != 0);

    // Now in transmission phase — send a read request.
    client.write_all(&NBD_REQUEST_MAGIC.to_be_bytes()).await.unwrap();
    client.write_all(&0u16.to_be_bytes()).await.unwrap(); // flags
    client.write_all(&0u16.to_be_bytes()).await.unwrap(); // cmd = READ
    client.write_all(&42u64.to_be_bytes()).await.unwrap(); // handle
    client.write_all(&0u64.to_be_bytes()).await.unwrap(); // offset
    client.write_all(&512u32.to_be_bytes()).await.unwrap(); // length
    client.flush().await.unwrap();

    // Read reply.
    let reply_magic = client.read_u32().await.unwrap();
    assert_eq!(reply_magic, NBD_REPLY_MAGIC);
    let error = client.read_u32().await.unwrap();
    assert_eq!(error, 0); // NBD_OK
    let handle = client.read_u64().await.unwrap();
    assert_eq!(handle, 42);
    let mut data = vec![0u8; 512];
    client.read_exact(&mut data).await.unwrap();
    assert_eq!(data, vec![0u8; 512]); // unwritten = zeros

    // Send disconnect.
    client.write_all(&NBD_REQUEST_MAGIC.to_be_bytes()).await.unwrap();
    client.write_all(&0u16.to_be_bytes()).await.unwrap();
    client.write_all(&2u16.to_be_bytes()).await.unwrap(); // cmd = DISC
    client.write_all(&0u64.to_be_bytes()).await.unwrap();
    client.write_all(&0u64.to_be_bytes()).await.unwrap();
    client.write_all(&0u32.to_be_bytes()).await.unwrap();
    client.flush().await.unwrap();

    // Server should exit cleanly.
    server_handle.await.unwrap().unwrap();
}
