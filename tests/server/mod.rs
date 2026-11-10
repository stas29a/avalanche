use crate::command_executor::DefaultCommandExecutor;
use crate::network::NetworkListener;
use crate::server::{Config, Server};
use crate::storage::Storage;
use bytes::Bytes;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct TestServer {
    addr: SocketAddr,
    cancel: CancellationToken,
    task: JoinHandle<anyhow::Result<()>>,
}

impl TestServer {
    async fn start(storage: Storage) -> (Self, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let storage = Arc::new(storage);
        let command_executor = DefaultCommandExecutor::new(storage.clone());
        let network_listener = NetworkListener::new(command_executor);
        let server = Server::new(
            network_listener,
            storage,
            Config {
                addr: addr.to_string(),
            },
        );

        // Server::run binds its own listener, so release the reserved test port.
        drop(listener);
        let task = tokio::spawn(server.run(cancel.clone()));
        let server = Self { addr, cancel, task };
        let client = server.connect().await;
        (server, client)
    }

    async fn connect(&self) -> TcpStream {
        timeout(TEST_TIMEOUT, async {
            loop {
                assert!(
                    !self.task.is_finished(),
                    "server stopped before accepting a connection"
                );
                match TcpStream::connect(self.addr).await {
                    Ok(client) => return client,
                    Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                        sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("failed to connect to the server: {error}"),
                }
            }
        })
        .await
        .expect("server did not start listening")
    }

    async fn stop(mut self) {
        self.cancel.cancel();
        timeout(TEST_TIMEOUT, &mut self.task)
            .await
            .expect("server did not stop after cancellation")
            .expect("server task panicked")
            .expect("server returned an error");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

fn get_command(key: &[u8]) -> Vec<u8> {
    let mut command = 1_i32.to_be_bytes().to_vec();
    command.extend_from_slice(key);
    command
}

fn set_command(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut command = 2_i32.to_be_bytes().to_vec();
    command.extend_from_slice(&i32::try_from(key.len()).unwrap().to_be_bytes());
    command.extend_from_slice(key);
    command.extend_from_slice(value);
    command
}

async fn send_command(client: &mut TcpStream, request_id: i32, command: &[u8]) {
    let mut frame = Vec::with_capacity(8 + command.len());
    frame.extend_from_slice(&i32::try_from(command.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(&request_id.to_be_bytes());
    frame.extend_from_slice(command);

    timeout(TEST_TIMEOUT, client.write_all(&frame))
        .await
        .expect("sending a command timed out")
        .expect("failed to send a command");
}

async fn assert_response(client: &mut TcpStream, request_id: i32, status: i32, expected: &[u8]) {
    timeout(TEST_TIMEOUT, async {
        let mut header = [0_u8; 12];
        client
            .read_exact(&mut header)
            .await
            .expect("failed to read response header");
        assert_eq!(
            i32::from_be_bytes(header[..4].try_into().unwrap()),
            i32::try_from(expected.len()).unwrap(),
            "unexpected response body length for request {request_id}",
        );
        assert_eq!(
            i32::from_be_bytes(header[4..8].try_into().unwrap()),
            request_id,
            "response belongs to another request",
        );
        assert_eq!(
            i32::from_be_bytes(header[8..12].try_into().unwrap()),
            status
        );

        let mut body = vec![0; expected.len()];
        client
            .read_exact(&mut body)
            .await
            .expect("failed to read response body");
        assert_eq!(
            body, expected,
            "unexpected response for request {request_id}"
        );
    })
    .await
    .unwrap_or_else(|_| {
        panic!("server did not respond to request {request_id} within {TEST_TIMEOUT:?}")
    });
}

#[tokio::test]
async fn server_reads_binary_values_and_handles_missing_keys_over_tcp() {
    let key = b"binary\0key\xff";
    let value: Vec<u8> = (0..(512 * 1024 + 17)).map(|n| (n % 256) as u8).collect();
    let storage = Storage::new();
    storage
        .set(Bytes::copy_from_slice(key), Bytes::copy_from_slice(&value))
        .unwrap();
    let (server, mut client) = TestServer::start(storage).await;

    send_command(&mut client, 1, &get_command(b"missing")).await;
    assert_response(&mut client, 1, 2, &[]).await;
    send_command(&mut client, -42, &get_command(key)).await;
    assert_response(&mut client, -42, 3, &value).await;

    let mut other_client = server.connect().await;
    send_command(&mut other_client, i32::MAX, &get_command(key)).await;
    assert_response(&mut other_client, i32::MAX, 3, &value).await;

    drop(client);
    drop(other_client);
    server.stop().await;
}

#[tokio::test]
async fn server_sets_reads_and_overwrites_values_over_tcp() {
    let (server, mut client) = TestServer::start(Storage::new()).await;
    let mut other_client = server.connect().await;
    let key = b"socket-key";

    for (request_id, value) in [
        (10, b"first\0\xff".as_slice()),
        (20, b"updated".as_slice()),
        (30, b"".as_slice()),
    ] {
        send_command(&mut client, request_id, &set_command(key, value)).await;
        assert_response(&mut client, request_id, 2, &[]).await;

        send_command(&mut client, request_id + 1, &get_command(key)).await;
        assert_response(&mut client, request_id + 1, 3, value).await;

        send_command(&mut other_client, request_id + 2, &get_command(key)).await;
        assert_response(&mut other_client, request_id + 2, 3, value).await;
    }

    drop(client);
    drop(other_client);
    server.stop().await;
}
