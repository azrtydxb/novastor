//! NBD server implementation.
//!
//! Handles the newstyle fixed handshake and transmission phases.
//! The caller provides a [`BlockDevice`] implementation for I/O.

use async_trait::async_trait;
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::protocol::*;
use crate::{NbdError, Result};

/// Trait for block device backends. Implement this to provide storage.
#[async_trait]
pub trait BlockDevice: Send + Sync + 'static {
    /// Size of the device in bytes.
    fn size(&self) -> u64;

    /// Read `length` bytes at `offset`.
    async fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>>;

    /// Write `data` at `offset`.
    async fn write(&self, offset: u64, data: &[u8]) -> Result<()>;

    /// Flush all pending writes.
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Trim (discard) a range. Default: no-op.
    async fn trim(&self, _offset: u64, _length: u32) -> Result<()> {
        Ok(())
    }

    /// Write zeroes to a range. Default: no-op (thin provisioning).
    async fn write_zeroes(&self, _offset: u64, _length: u32) -> Result<()> {
        Ok(())
    }
}

/// NBD server that serves a single block device.
pub struct NbdServer<B: BlockDevice> {
    backend: B,
}

impl<B: BlockDevice> NbdServer<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// Serve a single NBD client connection.
    pub async fn serve_connection<S>(&self, mut stream: S) -> Result<()>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        self.handshake(&mut stream).await?;
        self.transmission(&mut stream).await?;
        Ok(())
    }

    /// Newstyle fixed handshake.
    async fn handshake<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        &self,
        stream: &mut S,
    ) -> Result<()> {
        // 1. Server sends: NBDMAGIC + IHAVEOPT + handshake flags
        stream.write_all(&NBD_MAGIC.to_be_bytes()).await?;
        stream.write_all(&NBD_OPTS_MAGIC.to_be_bytes()).await?;
        let flags: u16 = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES;
        stream.write_all(&flags.to_be_bytes()).await?;
        stream.flush().await?;

        // 2. Client sends: client flags (4 bytes)
        let client_flags = stream.read_u32().await?;
        debug!("NBD: client flags = 0x{:08X}", client_flags);

        // 3. Options negotiation loop
        loop {
            // Client sends: IHAVEOPT + option type + length + data
            let opt_magic = stream.read_u64().await?;
            if opt_magic != NBD_OPTS_MAGIC {
                return Err(NbdError::Protocol(format!(
                    "bad option magic: 0x{:016X}",
                    opt_magic
                )));
            }

            let opt_type = stream.read_u32().await?;
            let opt_len = stream.read_u32().await?;

            // Read option data (if any).
            let mut opt_data = vec![0u8; opt_len as usize];
            if opt_len > 0 {
                stream.read_exact(&mut opt_data).await?;
            }

            match opt_type {
                NBD_OPT_EXPORT_NAME => {
                    // Client wants to use the default export.
                    // Send export info directly (no reply header for EXPORT_NAME).
                    let size = self.backend.size();
                    let tx_flags: u16 = NBD_FLAG_HAS_FLAGS
                        | NBD_FLAG_SEND_FLUSH
                        | NBD_FLAG_SEND_TRIM
                        | NBD_FLAG_SEND_WRITE_ZEROES;

                    stream.write_all(&size.to_be_bytes()).await?;
                    stream.write_all(&tx_flags.to_be_bytes()).await?;
                    // No zeroes padding (NBD_FLAG_NO_ZEROES).
                    stream.flush().await?;

                    info!("NBD: export negotiated, size={} bytes", size);
                    return Ok(());
                }
                NBD_OPT_GO => {
                    // Modern option: reply with export info.
                    let export_name = String::from_utf8_lossy(&opt_data[4..]).to_string();
                    debug!("NBD: OPT_GO for export '{}'", export_name);

                    let size = self.backend.size();
                    let tx_flags: u16 = NBD_FLAG_HAS_FLAGS
                        | NBD_FLAG_SEND_FLUSH
                        | NBD_FLAG_SEND_TRIM
                        | NBD_FLAG_SEND_WRITE_ZEROES;

                    // Send NBD_REP_INFO with NBD_INFO_EXPORT
                    let info_len: u32 = 12; // 2 (info type) + 8 (size) + 2 (flags)
                    self.send_option_reply(stream, opt_type, NBD_REP_INFO, info_len)
                        .await?;
                    stream.write_all(&NBD_INFO_EXPORT.to_be_bytes()).await?;
                    stream.write_all(&size.to_be_bytes()).await?;
                    stream.write_all(&tx_flags.to_be_bytes()).await?;

                    // Send NBD_REP_ACK to finish
                    self.send_option_reply(stream, opt_type, NBD_REP_ACK, 0)
                        .await?;
                    stream.flush().await?;

                    info!("NBD: OPT_GO negotiated, size={} bytes", size);
                    return Ok(());
                }
                NBD_OPT_ABORT => {
                    self.send_option_reply(stream, opt_type, NBD_REP_ACK, 0)
                        .await?;
                    return Err(NbdError::Protocol("client aborted".into()));
                }
                NBD_OPT_LIST => {
                    // List exports — we have one default export.
                    let name = b"default";
                    let data_len = 4 + name.len() as u32;
                    self.send_option_reply(stream, opt_type, NBD_REP_SERVER, data_len)
                        .await?;
                    stream
                        .write_all(&(name.len() as u32).to_be_bytes())
                        .await?;
                    stream.write_all(name).await?;
                    // Final ACK
                    self.send_option_reply(stream, opt_type, NBD_REP_ACK, 0)
                        .await?;
                    stream.flush().await?;
                }
                _ => {
                    // Unknown option — reply with ERR_UNSUP
                    self.send_option_reply(stream, opt_type, NBD_REP_ERR_UNSUP, 0)
                        .await?;
                    stream.flush().await?;
                }
            }
        }
    }

    async fn send_option_reply<S: AsyncWriteExt + Unpin>(
        &self,
        stream: &mut S,
        opt: u32,
        reply_type: u32,
        data_len: u32,
    ) -> Result<()> {
        stream.write_all(&NBD_REP_MAGIC.to_be_bytes()).await?;
        stream.write_all(&opt.to_be_bytes()).await?;
        stream.write_all(&reply_type.to_be_bytes()).await?;
        stream.write_all(&data_len.to_be_bytes()).await?;
        Ok(())
    }

    /// Transmission phase — handle read/write/trim/flush requests.
    async fn transmission<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        &self,
        stream: &mut S,
    ) -> Result<()> {
        loop {
            // Read request header (28 bytes).
            let magic = stream.read_u32().await?;
            if magic != NBD_REQUEST_MAGIC {
                return Err(NbdError::Protocol(format!(
                    "bad request magic: 0x{:08X}",
                    magic
                )));
            }

            let flags = stream.read_u16().await?;
            let cmd_type = stream.read_u16().await?;
            let handle = stream.read_u64().await?;
            let offset = stream.read_u64().await?;
            let length = stream.read_u32().await?;

            let command = match NbdCommand::from_u16(cmd_type) {
                Some(c) => c,
                None => {
                    warn!("NBD: unknown command type {}", cmd_type);
                    self.send_reply(stream, handle, NBD_EINVAL, None).await?;
                    continue;
                }
            };

            match command {
                NbdCommand::Read => {
                    match self.backend.read(offset, length).await {
                        Ok(data) => {
                            self.send_reply(stream, handle, NBD_OK, Some(&data)).await?;
                        }
                        Err(e) => {
                            warn!("NBD: read error at offset {}: {}", offset, e);
                            self.send_reply(stream, handle, NBD_EIO, None).await?;
                        }
                    }
                }
                NbdCommand::Write => {
                    // Read write data from client.
                    let mut data = vec![0u8; length as usize];
                    stream.read_exact(&mut data).await?;

                    match self.backend.write(offset, &data).await {
                        Ok(()) => {
                            self.send_reply(stream, handle, NBD_OK, None).await?;
                        }
                        Err(e) => {
                            warn!("NBD: write error at offset {}: {}", offset, e);
                            self.send_reply(stream, handle, NBD_EIO, None).await?;
                        }
                    }
                }
                NbdCommand::Flush => {
                    match self.backend.flush().await {
                        Ok(()) => self.send_reply(stream, handle, NBD_OK, None).await?,
                        Err(_) => self.send_reply(stream, handle, NBD_EIO, None).await?,
                    }
                }
                NbdCommand::Trim => {
                    match self.backend.trim(offset, length).await {
                        Ok(()) => self.send_reply(stream, handle, NBD_OK, None).await?,
                        Err(_) => self.send_reply(stream, handle, NBD_EIO, None).await?,
                    }
                }
                NbdCommand::WriteZeroes => {
                    match self.backend.write_zeroes(offset, length).await {
                        Ok(()) => self.send_reply(stream, handle, NBD_OK, None).await?,
                        Err(_) => self.send_reply(stream, handle, NBD_EIO, None).await?,
                    }
                }
                NbdCommand::Disc => {
                    info!("NBD: client disconnected");
                    return Ok(());
                }
            }
        }
    }

    async fn send_reply<S: AsyncWriteExt + Unpin>(
        &self,
        stream: &mut S,
        handle: u64,
        error: u32,
        data: Option<&[u8]>,
    ) -> Result<()> {
        stream.write_all(&NBD_REPLY_MAGIC.to_be_bytes()).await?;
        stream.write_all(&error.to_be_bytes()).await?;
        stream.write_all(&handle.to_be_bytes()).await?;
        if let Some(d) = data {
            stream.write_all(d).await?;
        }
        Ok(())
    }
}
