use avalanche::{command_executor, network, server, storage};

use anyhow::{Context, ensure};
use bytes::Bytes;
use command_executor::DefaultCommandExecutor;
use network::NetworkListener;
use server::{Config, Server};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use storage::Storage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

const IO_TIMEOUT: Duration = Duration::from_secs(5);
// Serialize load tests even if the caller omits --test-threads=1.
static LOAD_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy)]
enum Mode {
    Get,
    Set,
    Mixed,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Set => "SET",
            Self::Mixed => "MIXED(50% GET / 50% SET)",
        }
    }

    fn is_get(self, sequence: u64) -> bool {
        match self {
            Self::Get => true,
            Self::Set => false,
            Self::Mixed => sequence % 2 == 0,
        }
    }
}

struct LoadConfig {
    clients: Vec<usize>,
    duration: Duration,
    warmup: Duration,
    value_bytes: usize,
    keys: usize,
    workers: usize,
    threads: usize,
}

impl LoadConfig {
    fn from_env() -> anyhow::Result<Self> {
        fn number(name: &str, default: usize) -> anyhow::Result<usize> {
            std::env::var(name)
                .unwrap_or_else(|_| default.to_string())
                .parse()
                .with_context(|| format!("{name} must be a nonnegative integer"))
        }

        let clients = std::env::var("AVALANCHE_LOAD_CLIENTS")
            .unwrap_or_else(|_| "1,8,32,128".to_owned())
            .split(',')
            .map(|value| value.trim().parse::<usize>())
            .collect::<Result<Vec<_>, _>>()
            .context("AVALANCHE_LOAD_CLIENTS must be comma-separated positive integers")?;
        let config = Self {
            clients,
            duration: Duration::from_secs(number("AVALANCHE_LOAD_SECONDS", 3)? as u64),
            warmup: Duration::from_secs(number("AVALANCHE_LOAD_WARMUP_SECONDS", 1)? as u64),
            value_bytes: number("AVALANCHE_LOAD_VALUE_BYTES", 256)?,
            keys: number("AVALANCHE_LOAD_KEYS", 1024)?,
            workers: number("AVALANCHE_LOAD_WORKERS", 30)?,
            threads: number(
                "AVALANCHE_LOAD_THREADS",
                std::thread::available_parallelism().map_or(1, |count| count.get()),
            )?,
        };
        ensure!(
            config
                .clients
                .iter()
                .all(|&n| n > 0 && n <= i32::MAX as usize),
            "client counts must be between 1 and i32::MAX"
        );
        ensure!(
            !config.duration.is_zero(),
            "measurement duration must be positive"
        );
        ensure!(config.keys > 0, "key count must be positive");
        ensure!(config.workers > 0, "server worker count must be positive");
        ensure!(config.threads > 0, "runtime thread count must be positive");
        ensure!(
            config.value_bytes <= i32::MAX as usize - 16,
            "value is too large for the wire format"
        );
        Ok(config)
    }
}

struct TestServer {
    addr: SocketAddr,
    cancel: CancellationToken,
    task: JoinHandle<anyhow::Result<()>>,
}

impl TestServer {
    async fn start(config: &LoadConfig, value: &[u8]) -> anyhow::Result<Self> {
        let storage = Storage::new();
        for key in 0..config.keys {
            storage.set(
                Bytes::copy_from_slice(&(key as u64).to_be_bytes()),
                Bytes::copy_from_slice(value),
            )?;
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let storage = Arc::new(storage);
        let executor = DefaultCommandExecutor::new(storage.clone());
        let server = Server::new(
            NetworkListener::new(executor),
            storage,
            Config {
                addr: addr.to_string(),
            },
        );
        // The current Server API binds its own socket.
        drop(listener);
        let task = tokio::spawn(server.run(cancel.clone()));
        let server = Self { addr, cancel, task };
        drop(server.connect().await?);
        Ok(server)
    }

    async fn connect(&self) -> anyhow::Result<TcpStream> {
        timeout(IO_TIMEOUT, async {
            loop {
                ensure!(
                    !self.task.is_finished(),
                    "server stopped before accepting a connection"
                );
                match TcpStream::connect(self.addr).await {
                    Ok(client) => {
                        client.set_nodelay(true)?;
                        return Ok(client);
                    }
                    Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                        sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await
        .context("server did not start listening")?
    }

    async fn stop(mut self) -> anyhow::Result<()> {
        self.cancel.cancel();
        timeout(IO_TIMEOUT, &mut self.task)
            .await
            .context("server shutdown timed out")?
            .context("server task panicked")?
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

#[derive(Default)]
struct Stats {
    get: u64,
    set: u64,
    errors: u64,
    drained: u64,
    first_error: Option<String>,
}

impl Stats {
    fn completed(&self) -> u64 {
        self.get + self.set
    }

    fn check(&self) -> anyhow::Result<()> {
        ensure!(
            self.errors == 0,
            "{} requests failed; first error: {}",
            self.errors,
            self.first_error.as_deref().unwrap_or("unknown")
        );
        ensure!(
            self.completed() > 0,
            "no requests completed during the measurement window"
        );
        Ok(())
    }
}

fn frame(request_id: i32, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(8 + body.len());
    frame.extend_from_slice(&(body.len() as i32).to_be_bytes());
    frame.extend_from_slice(&request_id.to_be_bytes());
    frame.extend_from_slice(body);
    frame
}

async fn exchange(
    client: &mut TcpStream,
    request: &[u8],
    request_id: i32,
    expected_status: i32,
    expected: &[u8],
    response: &mut [u8],
) -> anyhow::Result<()> {
    client
        .write_all(request)
        .await
        .context("socket write failed")?;
    let mut header = [0_u8; 12];
    client
        .read_exact(&mut header)
        .await
        .context("response header read failed")?;
    let len = i32::from_be_bytes(header[..4].try_into()?);
    let response_id = i32::from_be_bytes(header[4..8].try_into()?);
    let status = i32::from_be_bytes(header[8..12].try_into()?);
    ensure!(response_id == request_id, "response request_id mismatch");
    ensure!(
        status == expected_status,
        "unexpected response status: {status}"
    );
    ensure!(
        len == expected.len() as i32,
        "unexpected response length: {len}, expected {}",
        expected.len()
    );
    let body = &mut response[..expected.len()];
    client
        .read_exact(body)
        .await
        .context("response body read failed")?;
    ensure!(body == expected, "unexpected response status or value");
    Ok(())
}

async fn phase(
    server: &TestServer,
    config: &LoadConfig,
    clients: usize,
    mode: Mode,
    duration: Duration,
    expected_get: Arc<Vec<u8>>,
) -> anyhow::Result<Stats> {
    let (start, ready) = watch::channel(None::<Instant>);
    let mut tasks = JoinSet::new();
    for client_id in 0..clients {
        let mut client = server.connect().await?;
        let request_id = client_id as i32;
        let mut get = frame(
            request_id,
            &[1_i32.to_be_bytes().as_slice(), &[0; 8]].concat(),
        );
        let mut set = frame(
            request_id,
            &[
                2_i32.to_be_bytes().as_slice(),
                8_i32.to_be_bytes().as_slice(),
                &[0; 8],
                &expected_get,
            ]
            .concat(),
        );
        let mut response = vec![0; expected_get.len()];
        let expected_get = expected_get.clone();
        let mut ready = ready.clone();
        let keys = config.keys;
        tasks.spawn(async move {
            ready
                .changed()
                .await
                .context("benchmark start signal was dropped")?;
            let deadline = ready.borrow().context("missing start time")? + duration;
            let mut stats = Stats::default();
            let mut sequence = client_id as u64;
            let mut key = client_id % keys;

            while Instant::now() < deadline {
                let is_get = mode.is_get(sequence);
                let request = if is_get {
                    get[12..20].copy_from_slice(&(key as u64).to_be_bytes());
                    &get
                } else {
                    set[16..24].copy_from_slice(&(key as u64).to_be_bytes());
                    &set
                };
                let expected = if is_get { expected_get.as_slice() } else { &[] };
                // Finish an outstanding request even if the measurement window
                // ends. Late responses are drained and excluded from RPS.
                match timeout(
                    IO_TIMEOUT,
                    exchange(
                        &mut client,
                        request,
                        request_id,
                        if is_get { 3 } else { 2 },
                        expected,
                        &mut response,
                    ),
                )
                .await
                {
                    Ok(Ok(())) if Instant::now() <= deadline => {
                        if is_get {
                            stats.get += 1;
                        } else {
                            stats.set += 1;
                        }
                    }
                    Ok(Ok(())) => stats.drained += 1,
                    error => {
                        stats.errors += 1;
                        stats.first_error = Some(match error {
                            Ok(Err(error)) => format!("client {client_id}: {error:#}"),
                            Err(_) => format!(
                                "client {client_id}: request timed out after {IO_TIMEOUT:?}"
                            ),
                            Ok(Ok(())) => unreachable!(),
                        });
                        break;
                    }
                }
                sequence = sequence.wrapping_add(1);
                key = (key + 1) % keys;
            }
            Ok::<_, anyhow::Error>(stats)
        });
    }

    // Connection setup, frame allocation and storage seeding are not timed.
    start
        .send(Some(Instant::now()))
        .context("no load clients are ready")?;
    let mut total = Stats::default();
    while let Some(result) = tasks.join_next().await {
        let stats = result.context("load client panicked")??;
        total.get += stats.get;
        total.set += stats.set;
        total.errors += stats.errors;
        total.drained += stats.drained;
        if total.first_error.is_none() {
            total.first_error = stats.first_error;
        }
    }
    Ok(total)
}

fn benchmark(mode: Mode) -> anyhow::Result<()> {
    let _guard = LOAD_TEST_LOCK
        .lock()
        .map_err(|error| anyhow::anyhow!("load test lock poisoned: {error}"))?;
    let config = LoadConfig::from_env()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.threads)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        println!("\n{} | profile={} | window={:.1}s | warmup={:.1}s | value={}B | keys={} | workers={} | threads={} | storage=DashMap | shards=1024",
            mode.label(), if cfg!(debug_assertions) { "debug" } else { "release" },
            config.duration.as_secs_f64(), config.warmup.as_secs_f64(), config.value_bytes,
            config.keys, config.workers, config.threads);
        println!("Loopback TCP; server and clients share this process; logging disabled; one outstanding request per client.");
        println!("Commands execute inline in each connection task; the configured worker count is unused.");
        let expected_get: Vec<u8> = (0..config.value_bytes).map(|n| (n % 256) as u8).collect();
        let expected_get = Arc::new(expected_get);
        let mut best = (0.0_f64, 0);

        for &clients in &config.clients {
            let server = TestServer::start(&config, &expected_get).await?;
            if !config.warmup.is_zero() {
                phase(&server, &config, clients, mode, config.warmup, expected_get.clone()).await?.check()?;
            }
            let stats = phase(&server, &config, clients, mode, config.duration, expected_get.clone()).await?;
            let rps = stats.completed() as f64 / config.duration.as_secs_f64();
            println!("{} clients={clients:>4} | RPS={rps:>12.0} | completed={} | GET={} | SET={} | errors={} | drained={}",
                mode.label(), stats.completed(), stats.get, stats.set, stats.errors, stats.drained);
            server.stop().await?;
            stats.check()?;
            if rps > best.0 { best = (rps, clients); }
        }
        println!("{} BEST measured RPS={:.0} at {} clients\n", mode.label(), best.0, best.1);
        Ok(())
    })
}

#[test]
#[ignore = "load test: run with --release --ignored --nocapture"]
fn get_rps() -> anyhow::Result<()> {
    benchmark(Mode::Get)
}

#[test]
#[ignore = "load test: run with --release --ignored --nocapture"]
fn set_rps() -> anyhow::Result<()> {
    benchmark(Mode::Set)
}

#[test]
#[ignore = "load test: run with --release --ignored --nocapture"]
fn mixed_rps() -> anyhow::Result<()> {
    benchmark(Mode::Mixed)
}
