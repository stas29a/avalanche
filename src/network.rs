use crate::command_executor::{CommandExecutor, CommandResult};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{error, info};
use std::io::{self, IoSlice};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub struct NetworkListener<C>
where
    C: CommandExecutor,
{
    command_executor: Arc<C>,
}

#[derive(Debug)]
pub struct NetworkWriteConnection {
    writer: Mutex<OwnedWriteHalf>,
}

impl NetworkWriteConnection {
    pub fn new(writer: OwnedWriteHalf) -> NetworkWriteConnection {
        Self {
            writer: Mutex::new(writer),
        }
    }

    pub async fn write(
        &self,
        request_id: i32,
        command_result: CommandResult,
    ) -> anyhow::Result<()> {
        let network_frame = NetworkFrame::from_command_result(request_id, command_result)?;
        let mut guard = self.writer.lock().await;
        network_frame.write(&mut *guard).await
    }
}
#[derive(Debug)]
pub struct NetworkFrame<B> {
    pub len: i32,
    pub request_id: i32,
    pub body: B,
}

pub struct NetworkResponseBody {
    pub status_code: i32,
    pub bytes: Bytes,
}

impl NetworkResponseBody {
    pub fn new(status_code: i32, bytes: Bytes) -> NetworkResponseBody {
        Self { status_code, bytes }
    }
}

enum NetworkFrameParseResult {
    Ok(NetworkFrame<Bytes>),
    NotEnoughBytes,
}

impl NetworkFrame<NetworkResponseBody> {
    fn from_command_result(
        request_id: i32,
        command_result: CommandResult,
    ) -> anyhow::Result<NetworkFrame<NetworkResponseBody>> {
        match command_result {
            CommandResult::Success => Ok(NetworkFrame {
                len: 0,
                request_id,
                body: NetworkResponseBody {
                    status_code: 1,
                    bytes: Bytes::new(),
                },
            }),
            CommandResult::SuccessEmptyResult => Ok(NetworkFrame {
                len: 0,
                request_id,
                body: NetworkResponseBody {
                    status_code: 2,
                    bytes: Bytes::new(),
                },
            }),
            CommandResult::SuccessWithResult(result) => Ok(NetworkFrame {
                len: result.len() as i32,
                request_id,
                body: NetworkResponseBody {
                    status_code: 3,
                    bytes: result,
                },
            }),
            CommandResult::Failure(failure) => {
                let bytes = Bytes::from(failure.into_bytes());

                Ok(NetworkFrame {
                    len: bytes.len() as i32,
                    request_id,
                    body: NetworkResponseBody {
                        status_code: 0,
                        bytes,
                    },
                })
            }
        }
    }

    async fn write(&self, writer: &mut (impl AsyncWrite + Unpin)) -> anyhow::Result<()> {
        let mut header = [0u8; 12];
        header[..4].copy_from_slice(&self.len.to_be_bytes());
        header[4..8].copy_from_slice(&self.request_id.to_be_bytes());
        header[8..12].copy_from_slice(&self.body.status_code.to_be_bytes());

        let mut buffers = [IoSlice::new(&header), IoSlice::new(&self.body.bytes)];
        let mut remaining = &mut buffers[..];
        while !remaining.is_empty() {
            match writer.write_vectored(remaining).await {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
                Ok(written) => IoSlice::advance_slices(&mut remaining, written),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

impl NetworkFrame<Bytes> {
    fn new(request_id: i32, body: Bytes) -> NetworkFrame<Bytes> {
        NetworkFrame {
            len: body.len() as i32,
            request_id,
            body,
        }
    }

    fn parse(bytes: &mut BytesMut) -> anyhow::Result<NetworkFrameParseResult> {
        if bytes.len() < 8 {
            return Ok(NetworkFrameParseResult::NotEnoughBytes);
        }

        let len = i32::from_be_bytes(bytes[0..4].try_into()?);
        anyhow::ensure!(len >= 0, "Negative network frame body length: {len}");
        let frame_len = 8 + len as usize;

        if bytes.len() < frame_len {
            return Ok(NetworkFrameParseResult::NotEnoughBytes);
        }

        let request_id = i32::from_be_bytes(bytes[4..8].try_into()?);
        bytes.advance(8);

        Ok(NetworkFrameParseResult::Ok(NetworkFrame {
            len,
            request_id,
            body: bytes.split_to(len as usize).freeze(),
        }))
    }

    async fn write(&self, mut writer: impl AsyncWriteExt + Unpin) -> anyhow::Result<()> {
        writer.write_i32(self.len).await?;
        writer.write_i32(self.request_id).await?;
        writer.write(&self.body).await?;

        Ok(())
    }
}

impl<C: CommandExecutor + Send + 'static + Sync> NetworkListener<C> {
    pub fn new(command_executor: C) -> Self {
        Self {
            command_executor: Arc::new(command_executor),
        }
    }

    pub async fn listen(
        &self,
        addr: String,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;

        loop {
            let (socket, peer) = select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                connection = listener.accept() => connection?,
            };

            let child_token = cancellation_token.child_token();
            let command_executor = self.command_executor.clone();

            tokio::spawn(async move {
                if let Err(error) = Self::read_frames(child_token, socket, command_executor).await {
                    error!("Failed to read frames from {peer}: {error:#}");
                }
            });
        }
    }

    pub async fn read_frames(
        cancel: CancellationToken,
        mut tcp_stream: TcpStream,
        command_executor: Arc<impl CommandExecutor>,
    ) -> anyhow::Result<()> {
        let mut read_buf = BytesMut::with_capacity(8096);

        loop {
            let n = select! {
                _ = cancel.cancelled() => return Ok(()),
                result = tcp_stream.read_buf(&mut read_buf) => result?,
            };

            if n == 0 {
                anyhow::ensure!(
                    read_buf.is_empty(),
                    "Connection closed with an incomplete network frame"
                );
                return Ok(());
            }

            while let NetworkFrameParseResult::Ok(frame) = NetworkFrame::parse(&mut read_buf)? {
                match command_executor.execute(&frame) {
                    Ok(command_result) => {
                        let network_frame =
                            NetworkFrame::from_command_result(frame.request_id, command_result)?;
                        network_frame.write(&mut tcp_stream).await?;
                    }
                    Err(error) => error!("Error while executing command: {error:#}"),
                }

                if cancel.is_cancelled() {
                    return Ok(());
                }
            }
        }
    }
}
#[cfg(test)]
mod tests;
