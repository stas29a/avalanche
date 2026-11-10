use super::{NetworkFrame, NetworkFrameParseResult, NetworkListener};
use crate::command_executor::{CommandExecutor, CommandResult};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const NO_FRAME_TIMEOUT: Duration = Duration::from_millis(20);

fn encode_frame(request_id: i32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + body.len());
    bytes.extend_from_slice(&i32::try_from(body.len()).unwrap().to_be_bytes());
    bytes.extend_from_slice(&request_id.to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

fn encode_response(request_id: i32, status: i32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = encode_frame(request_id, payload);
    bytes.splice(8..8, status.to_be_bytes());
    bytes
}

fn assert_frame(frame: &NetworkFrame<Bytes>, request_id: i32, body: &[u8]) {
    assert_eq!(frame.len, i32::try_from(body.len()).unwrap());
    assert_eq!(frame.request_id, request_id);
    assert_eq!(frame.body.as_ref(), body);
}

fn parse_complete_frame(bytes: &[u8]) -> NetworkFrame<Bytes> {
    let mut bytes = BytesMut::from(bytes);
    match NetworkFrame::parse(&mut bytes).expect("valid frame should parse") {
        NetworkFrameParseResult::Ok(frame) => {
            assert!(
                bytes.is_empty(),
                "parsed frame left bytes in the input buffer"
            );
            frame
        }
        NetworkFrameParseResult::NotEnoughBytes => panic!("complete frame was not parsed"),
    }
}

struct RecordingExecutor {
    requests: UnboundedSender<NetworkFrame<Bytes>>,
    fail_on: Option<i32>,
    cancel: Option<CancellationToken>,
    response: Option<Bytes>,
}

impl CommandExecutor for RecordingExecutor {
    fn execute(&self, frame: &NetworkFrame<Bytes>) -> anyhow::Result<CommandResult> {
        // This channel only observes synchronous calls from the network reader.
        // Production no longer uses a queue to dispatch commands.
        self.requests
            .send(NetworkFrame::new(frame.request_id, frame.body.clone()))
            .map_err(|_| anyhow::anyhow!("test observer closed"))?;
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        anyhow::ensure!(
            self.fail_on != Some(frame.request_id),
            "injected executor failure"
        );
        Ok(match &self.response {
            Some(value) => CommandResult::SuccessWithResult(value.clone()),
            None => CommandResult::SuccessEmptyResult,
        })
    }
}

fn recording_executor() -> (RecordingExecutor, UnboundedReceiver<NetworkFrame<Bytes>>) {
    let (requests, receiver) = mpsc::unbounded_channel();
    (
        RecordingExecutor {
            requests,
            fail_on: None,
            cancel: None,
            response: None,
        },
        receiver,
    )
}

struct TestConnection {
    client: TcpStream,
    requests: UnboundedReceiver<NetworkFrame<Bytes>>,
    cancel: CancellationToken,
    reader: JoinHandle<anyhow::Result<()>>,
}

impl TestConnection {
    async fn new() -> Self {
        let (executor, requests) = recording_executor();
        Self::with_executor(executor, requests).await
    }

    async fn with_executor(
        executor: RecordingExecutor,
        requests: UnboundedReceiver<NetworkFrame<Bytes>>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, (server, _)) = timeout(TEST_TIMEOUT, async {
            tokio::try_join!(TcpStream::connect(addr), listener.accept())
        })
        .await
        .expect("TCP connection timed out")
        .expect("TCP connection failed");
        let cancel = executor.cancel.clone().unwrap_or_default();
        let reader = tokio::spawn(NetworkListener::<RecordingExecutor>::read_frames(
            cancel.clone(),
            server,
            Arc::new(executor),
        ));

        Self {
            client,
            requests,
            cancel,
            reader,
        }
    }

    async fn send(&mut self, bytes: &[u8]) {
        timeout(TEST_TIMEOUT, self.client.write_all(bytes))
            .await
            .expect("TCP write timed out")
            .expect("TCP write failed");
    }

    async fn receive(&mut self) -> NetworkFrame<Bytes> {
        timeout(TEST_TIMEOUT, self.requests.recv())
            .await
            .expect("request was not forwarded")
            .expect("request channel closed unexpectedly")
    }

    async fn assert_response(&mut self, request_id: i32, status: i32, payload: &[u8]) {
        let expected = encode_response(request_id, status, payload);
        let mut received = vec![0; expected.len()];
        timeout(TEST_TIMEOUT, self.client.read_exact(&mut received))
            .await
            .expect("response frame timed out")
            .expect("failed to read response frame");
        assert_eq!(received, expected);
    }

    async fn assert_no_request(&mut self) {
        let result = timeout(NO_FRAME_TIMEOUT, self.requests.recv()).await;
        assert!(
            result.is_err(),
            "incomplete frame produced a request: {result:?}"
        );
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        timeout(TEST_TIMEOUT, &mut self.reader)
            .await
            .expect("reader did not stop")
            .expect("reader task panicked")
    }
}

impl Drop for TestConnection {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.reader.abort();
    }
}

#[test]
fn parse_decodes_big_endian_header_and_binary_body() {
    // Keep the fixture independent of encode_frame to check the wire format.
    let bytes = [0, 0, 0, 4, 1, 2, 3, 4, 0, 0xff, 0x80, 42];
    let frame = parse_complete_frame(&bytes);

    assert_frame(&frame, 0x01020304, &[0, 0xff, 0x80, 42]);
}

#[test]
fn parse_accepts_empty_bodies_and_signed_request_ids() {
    for request_id in [0, -42, i32::MIN, i32::MAX] {
        let frame = parse_complete_frame(&encode_frame(request_id, b""));
        assert_frame(&frame, request_id, b"");
    }
}

#[test]
fn parse_waits_for_every_incomplete_header_and_body_prefix() {
    let bytes = encode_frame(7, b"hello");

    for len in 0..bytes.len() {
        let mut prefix = BytesMut::from(&bytes[..len]);
        let pointer = prefix.as_ptr();
        let capacity = prefix.capacity();
        assert!(
            matches!(
                NetworkFrame::parse(&mut prefix).unwrap(),
                NetworkFrameParseResult::NotEnoughBytes
            ),
            "prefix of {len} bytes should be incomplete"
        );
        assert_eq!(prefix.as_ref(), &bytes[..len]);
        assert_eq!(prefix.as_ptr(), pointer);
        assert_eq!(prefix.capacity(), capacity);
    }
}

#[test]
fn parse_rejects_negative_body_lengths() {
    for len in [-1_i32, i32::MIN] {
        let bytes = [len.to_be_bytes(), 7_i32.to_be_bytes()].concat();
        let mut buffer = BytesMut::from(bytes.as_slice());
        assert!(NetworkFrame::parse(&mut buffer).is_err());
        assert_eq!(buffer.as_ref(), bytes);
    }
}

#[test]
fn parse_reads_the_first_frame_when_more_bytes_follow() {
    let first = encode_frame(1, b"first");
    let second = encode_frame(2, b"second");
    let mut bytes = BytesMut::from([first.as_slice(), second.as_slice()].concat().as_slice());
    let NetworkFrameParseResult::Ok(first_frame) = NetworkFrame::parse(&mut bytes).unwrap() else {
        panic!("first frame was not parsed");
    };
    assert_frame(&first_frame, 1, b"first");
    assert_eq!(bytes.as_ref(), second);
    let NetworkFrameParseResult::Ok(second_frame) = NetworkFrame::parse(&mut bytes).unwrap() else {
        panic!("second frame was not parsed");
    };
    assert_frame(&second_frame, 2, b"second");
    assert_frame(&first_frame, 1, b"first");
    assert!(bytes.is_empty());
}

#[test]
fn parse_shares_the_body_storage_with_the_input_buffer() {
    let encoded = encode_frame(42, b"body without copying");
    let mut buffer = BytesMut::from(encoded.as_slice());
    let body_pointer = buffer.as_ptr().wrapping_add(8);
    let NetworkFrameParseResult::Ok(frame) = NetworkFrame::parse(&mut buffer).unwrap() else {
        panic!("complete frame was not parsed");
    };
    assert_eq!(frame.body.as_ptr(), body_pointer);
    assert_frame(&frame, 42, b"body without copying");
    assert!(buffer.is_empty());
}

#[test]
fn parse_waits_for_a_large_body_without_allocating_it() {
    let bytes = [i32::MAX.to_be_bytes(), 7_i32.to_be_bytes()].concat();
    let mut buffer = BytesMut::from(bytes.as_slice());
    let capacity = buffer.capacity();

    assert!(matches!(
        NetworkFrame::parse(&mut buffer).unwrap(),
        NetworkFrameParseResult::NotEnoughBytes
    ));
    assert_eq!(buffer.as_ref(), bytes);
    assert_eq!(buffer.capacity(), capacity);
}

#[tokio::test]
async fn response_write_encodes_status32_and_keeps_payload_length_separate() {
    for (result, status, payload) in [
        (CommandResult::Success, 1, b"".as_slice()),
        (CommandResult::SuccessEmptyResult, 2, b"".as_slice()),
        (
            CommandResult::SuccessWithResult(Bytes::from_static(b"a\0\xff")),
            3,
            b"a\0\xff".as_slice(),
        ),
        (
            CommandResult::Failure("failed".into()),
            0,
            b"failed".as_slice(),
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (mut client, (mut server, _)) =
            tokio::try_join!(TcpStream::connect(addr), listener.accept()).unwrap();
        let frame = NetworkFrame::from_command_result(-42, result).unwrap();
        let expected = encode_response(-42, status, payload);
        let mut received = vec![0; expected.len()];
        timeout(TEST_TIMEOUT, async {
            let (written, read) =
                tokio::join!(frame.write(&mut server), client.read_exact(&mut received));
            written.unwrap();
            read.unwrap();
        })
        .await
        .expect("response frame timed out");
        assert_eq!(received, expected);
    }
}

#[tokio::test]
async fn response_write_handles_partial_writes_and_backpressure() {
    for capacity in [1, 4, 8, 11, 12, 13, 20] {
        for payload in [b"".as_slice(), b"a\0\xff payload across buffers".as_slice()] {
            let frame = NetworkFrame::from_command_result(
                -42,
                CommandResult::SuccessWithResult(Bytes::copy_from_slice(payload)),
            )
            .unwrap();
            let expected = encode_response(-42, 3, payload);
            let mut received = vec![0; expected.len()];
            let (mut writer, mut reader) = tokio::io::duplex(capacity);

            timeout(TEST_TIMEOUT, async {
                let (written, read) =
                    tokio::join!(frame.write(&mut writer), reader.read_exact(&mut received));
                written.unwrap();
                read.unwrap();
            })
            .await
            .expect("partial response write timed out");
            assert_eq!(received, expected, "duplex capacity: {capacity}");
        }
    }
}

#[tokio::test]
async fn response_write_reports_zero_progress_after_a_partial_header() {
    let frame = NetworkFrame::from_command_result(-42, CommandResult::SuccessEmptyResult).unwrap();
    let mut output = [0u8; 5];
    let mut writer = std::io::Cursor::new(output.as_mut_slice());

    let error = frame.write(&mut writer).await.unwrap_err();

    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::WriteZero
    );
    assert_eq!(writer.position(), 5);
    assert_eq!(output, encode_response(-42, 2, b"")[..5]);
}

#[tokio::test]
async fn response_write_propagates_a_closed_connection() {
    let frame = NetworkFrame::from_command_result(7, CommandResult::Success).unwrap();
    let (mut writer, reader) = tokio::io::duplex(1);
    drop(reader);

    let error = frame.write(&mut writer).await.unwrap_err();

    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn read_frames_forwards_a_complete_frame() {
    let mut connection = TestConnection::new().await;
    connection.send(&encode_frame(42, &[0, 0xff, 0x80])).await;

    let request = connection.receive().await;
    assert_frame(&request, 42, &[0, 0xff, 0x80]);

    connection.cancel.cancel();
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_sends_the_injected_executor_result_with_the_request_id() {
    let (mut executor, requests) = recording_executor();
    executor.response = Some(Bytes::from_static(&[0, 0xff, 42]));
    let mut connection = TestConnection::with_executor(executor, requests).await;
    connection.send(&encode_frame(-42, b"custom command")).await;

    connection.assert_response(-42, 3, &[0, 0xff, 42]).await;

    connection.cancel.cancel();
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_reassembles_fragmented_headers_and_bodies() {
    let mut connection = TestConnection::new().await;
    let bytes = encode_frame(7, b"abcdef");
    let mut start = 0;

    for end in [2, 6, 10, 12] {
        connection.send(&bytes[start..end]).await;
        connection.assert_no_request().await;
        start = end;
    }
    connection.send(&bytes[start..]).await;

    let request = connection.receive().await;
    assert_frame(&request, 7, b"abcdef");

    connection.cancel.cancel();
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_reassembles_a_frame_larger_than_the_read_buffer() {
    let mut connection = TestConnection::new().await;
    let body: Vec<u8> = (0..(3 * 8192 + 123)).map(|n| (n % 256) as u8).collect();
    connection.send(&encode_frame(9, &body)).await;

    let request = connection.receive().await;
    assert_frame(&request, 9, &body);

    connection.cancel.cancel();
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_forwards_concatenated_frames_in_order() {
    let mut connection = TestConnection::new().await;
    let bytes = [
        encode_frame(1, b"first"),
        encode_frame(2, b""),
        encode_frame(3, b"third"),
    ]
    .concat();
    connection.send(&bytes).await;

    for (request_id, body) in [(1, b"first".as_slice()), (2, b""), (3, b"third")] {
        let request = connection.receive().await;
        assert_frame(&request, request_id, body);
    }

    connection.cancel.cancel();
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_keeps_a_partial_frame_after_a_complete_frame() {
    let mut connection = TestConnection::new().await;
    let first = encode_frame(1, b"first");
    let second = encode_frame(2, b"second");
    let split = 10;
    connection
        .send(&[first.as_slice(), &second[..split]].concat())
        .await;

    let request = connection.receive().await;
    assert_frame(&request, 1, b"first");
    connection.assert_no_request().await;

    connection.send(&second[split..]).await;
    let request = connection.receive().await;
    assert_frame(&request, 2, b"second");

    connection.cancel.cancel();
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_handles_mixed_fragmented_and_complete_frames() {
    let mut connection = TestConnection::new().await;
    let first = encode_frame(1, b"first");
    let second = encode_frame(2, b"second");
    let third = encode_frame(3, b"third");
    let fourth = encode_frame(4, b"fourth");

    connection.send(&first[..10]).await;
    connection.assert_no_request().await;
    connection
        .send(&[&first[10..], second.as_slice(), &third[..6]].concat())
        .await;

    for (request_id, body) in [(1, b"first".as_slice()), (2, b"second")] {
        let request = connection.receive().await;
        assert_frame(&request, request_id, body);
    }
    connection.assert_no_request().await;

    connection
        .send(&[&third[6..], fourth.as_slice()].concat())
        .await;
    for (request_id, body) in [(3, b"third".as_slice()), (4, b"fourth")] {
        let request = connection.receive().await;
        assert_frame(&request, request_id, body);
    }

    connection.send(&encode_frame(5, b"fifth")).await;
    let request = connection.receive().await;
    assert_frame(&request, 5, b"fifth");

    connection.cancel.cancel();
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_sends_responses_to_their_original_connection() {
    let mut first = TestConnection::new().await;
    let mut second = TestConnection::new().await;

    first.send(&encode_frame(1, b"one")).await;
    let first_request = first.receive().await;
    first.send(&encode_frame(2, b"two")).await;
    let next_request = first.receive().await;
    second.send(&encode_frame(3, b"three")).await;
    let other_request = second.receive().await;

    assert_frame(&first_request, 1, b"one");
    assert_frame(&next_request, 2, b"two");
    assert_frame(&other_request, 3, b"three");
    second.assert_response(3, 2, &[]).await;
    first.assert_response(1, 2, &[]).await;
    first.assert_response(2, 2, &[]).await;

    first.cancel.cancel();
    second.cancel.cancel();
    first.finish().await.unwrap();
    second.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_stops_on_clean_eof() {
    let mut connection = TestConnection::new().await;
    connection.client.shutdown().await.unwrap();

    assert!(
        timeout(TEST_TIMEOUT, connection.requests.recv())
            .await
            .unwrap()
            .is_none()
    );
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_reports_eof_in_an_incomplete_frame() {
    let mut connection = TestConnection::new().await;
    let bytes = encode_frame(7, b"incomplete");
    connection.send(&bytes[..bytes.len() - 1]).await;
    connection.client.shutdown().await.unwrap();

    assert!(connection.finish().await.is_err());
}

#[tokio::test]
async fn read_frames_reports_invalid_lengths() {
    let mut connection = TestConnection::new().await;
    let bytes = [(-1_i32).to_be_bytes(), 7_i32.to_be_bytes()].concat();
    connection.send(&bytes).await;
    connection.client.shutdown().await.unwrap();

    assert!(connection.finish().await.is_err());
}

#[tokio::test]
async fn read_frames_cancels_while_waiting_for_socket_data() {
    let mut connection = TestConnection::new().await;
    connection.cancel.cancel();

    connection.finish().await.unwrap();
}

#[tokio::test]
async fn read_frames_stops_between_commands_when_executor_cancels() {
    let (mut executor, requests) = recording_executor();
    executor.cancel = Some(CancellationToken::new());
    let mut connection = TestConnection::with_executor(executor, requests).await;
    connection
        .send(&[encode_frame(1, b"one"), encode_frame(2, b"two")].concat())
        .await;

    let request = connection.receive().await;
    assert_frame(&request, 1, b"one");
    connection.finish().await.unwrap();
    assert!(
        connection.requests.recv().await.is_none(),
        "second command ran after cancellation"
    );
}

#[tokio::test]
async fn read_frames_continues_after_executor_error() {
    let (mut executor, requests) = recording_executor();
    executor.fail_on = Some(1);
    let mut connection = TestConnection::with_executor(executor, requests).await;
    connection.send(&encode_frame(1, b"fails")).await;
    assert_frame(&connection.receive().await, 1, b"fails");
    connection.send(&encode_frame(2, b"succeeds")).await;
    assert_frame(&connection.receive().await, 2, b"succeeds");
    connection.assert_response(2, 2, &[]).await;
    connection.cancel.cancel();
    connection.finish().await.unwrap();
}

#[tokio::test]
async fn listen_stops_when_cancelled() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (executor, _requests) = recording_executor();

    timeout(
        TEST_TIMEOUT,
        NetworkListener::new(executor).listen("127.0.0.1:0".to_owned(), cancel),
    )
    .await
    .expect("listener did not stop")
    .unwrap();
}

#[tokio::test]
async fn listen_reports_an_address_already_in_use() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = occupied.local_addr().unwrap();
    let (executor, _requests) = recording_executor();

    let result = timeout(
        TEST_TIMEOUT,
        NetworkListener::new(executor).listen(addr.to_string(), CancellationToken::new()),
    )
    .await
    .expect("binding to an occupied address timed out");

    assert!(result.is_err());
}
