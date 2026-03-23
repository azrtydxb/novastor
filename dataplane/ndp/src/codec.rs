use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{NdpError, Result};
use crate::header::{NdpHeader, NDP_HEADER_SIZE};

/// An NDP message: header + optional data payload.
#[derive(Debug)]
pub struct NdpMessage {
    pub header: NdpHeader,
    pub data: Option<Vec<u8>>,
}

impl NdpMessage {
    pub fn new(header: NdpHeader, data: Option<Vec<u8>>) -> Self {
        Self { header, data }
    }

    /// Write this message to an async writer.
    pub async fn write_to<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        let mut hdr_buf = [0u8; NDP_HEADER_SIZE];
        let mut header = self.header;

        if let Some(ref data) = self.data {
            header.data_length = data.len() as u32;
            header.data_crc = crc32c::crc32c(data);
        }

        header.encode(&mut hdr_buf);
        writer.write_all(&hdr_buf).await?;

        if let Some(ref data) = self.data {
            writer.write_all(data).await?;
        }

        Ok(())
    }

    /// Read a message from an async reader.
    pub async fn read_from<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let mut hdr_buf = [0u8; NDP_HEADER_SIZE];
        match reader.read_exact(&mut hdr_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(NdpError::ConnectionClosed);
            }
            Err(e) => return Err(e.into()),
        }

        let header = NdpHeader::decode(&hdr_buf)?;

        // Only read inline data for ops that carry a payload (Write, Replicate, EcShard,
        // ReadResp). For Read requests, data_length is the requested read SIZE, not payload.
        let has_payload = header.op.has_request_data() || header.op.has_response_data();
        let data = if header.data_length > 0 && has_payload {
            let mut buf = vec![0u8; header.data_length as usize];
            reader.read_exact(&mut buf).await?;

            let actual_crc = crc32c::crc32c(&buf);
            if header.data_crc != 0 && actual_crc != header.data_crc {
                return Err(NdpError::DataCrcMismatch {
                    expected: header.data_crc,
                    actual: actual_crc,
                });
            }

            Some(buf)
        } else {
            None
        };

        Ok(Self { header, data })
    }
}
