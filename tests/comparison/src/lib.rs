use anyhow::{Context, bail, ensure};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_ID: i32 = 7;

#[derive(Clone, Copy, Debug)]
pub enum Database {
    Avalanche,
    Redis,
    Valkey,
    Memcached,
    Dragonfly,
    Keydb,
}

impl Database {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        Ok(match value {
            "avalanche" => Self::Avalanche,
            "redis" => Self::Redis,
            "valkey" => Self::Valkey,
            "memcached" => Self::Memcached,
            "dragonfly" => Self::Dragonfly,
            "keydb" => Self::Keydb,
            _ => bail!("unknown database: {value}"),
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Avalanche => "avalanche",
            Self::Redis => "redis",
            Self::Valkey => "valkey",
            Self::Memcached => "memcached",
            Self::Dragonfly => "dragonfly",
            Self::Keydb => "keydb",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Mode {
    Get,
    Set,
    Mixed,
}

impl Mode {
    fn parse(value: &str) -> anyhow::Result<Self> {
        Ok(match value {
            "get" => Self::Get,
            "set" => Self::Set,
            "mixed" => Self::Mixed,
            _ => bail!("unknown mode: {value}"),
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Set => "set",
            Self::Mixed => "mixed",
        }
    }
    fn is_get(self, sequence: usize) -> bool {
        match self {
            Self::Get => true,
            Self::Set => false,
            Self::Mixed => sequence % 2 == 0,
        }
    }
}

#[derive(Clone)]
pub struct Settings {
    pub database: Database,
    pub mode: Mode,
    pub address: String,
    pub clients: usize,
    pub seconds: f64,
    pub warmup_seconds: f64,
    pub repeats: usize,
    pub keys: usize,
    pub value_bytes: usize,
    pub threads: usize,
}

impl Settings {
    pub fn parse(args: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        let mut config = Self {
            database: Database::Avalanche,
            mode: Mode::Get,
            address: std::env::var("BENCH_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into()),
            clients: 32,
            seconds: 3.0,
            warmup_seconds: 1.0,
            repeats: 3,
            keys: 1024,
            value_bytes: 256,
            threads: 4,
        };
        let mut args = args;
        while let Some(flag) = args.next() {
            let value = args
                .next()
                .with_context(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--database" => config.database = Database::parse(&value)?,
                "--mode" => config.mode = Mode::parse(&value)?,
                "--address" => config.address = value,
                "--clients" => config.clients = value.parse()?,
                "--seconds" => config.seconds = value.parse()?,
                "--warmup-seconds" => config.warmup_seconds = value.parse()?,
                "--repeats" => config.repeats = value.parse()?,
                "--keys" => config.keys = value.parse()?,
                "--value-bytes" => config.value_bytes = value.parse()?,
                "--threads" => config.threads = value.parse()?,
                _ => bail!("unknown argument: {flag}"),
            }
        }
        ensure!(
            config.clients > 0 && config.threads > 0 && config.keys > 0 && config.repeats > 0,
            "clients, threads, keys and repeats must be positive"
        );
        ensure!(
            config.seconds.is_finite() && config.seconds > 0.0 && config.seconds <= 3600.0,
            "seconds must be between 0 and 3600"
        );
        ensure!(
            config.warmup_seconds.is_finite() && (0.0..=3600.0).contains(&config.warmup_seconds),
            "warmup must be between 0 and 3600 seconds"
        );
        ensure!(
            config.keys <= u32::MAX as usize,
            "key count exceeds 8-character key space"
        );
        ensure!(
            config.value_bytes <= 16 * 1024 * 1024,
            "value exceeds 16 MiB"
        );
        Ok(config)
    }
}

struct Template {
    get: Vec<u8>,
    set: Vec<u8>,
    got: Vec<u8>,
    stored: Vec<u8>,
}

fn avalanche_frame(body: &[u8]) -> Vec<u8> {
    [
        (body.len() as i32).to_be_bytes().as_slice(),
        REQUEST_ID.to_be_bytes().as_slice(),
        body,
    ]
    .concat()
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum AvalancheResponseFormat {
    Legacy,
    Status32,
}

impl AvalancheResponseFormat {
    fn name(self) -> &'static str {
        match self {
            Self::Legacy => "legacy-status8",
            Self::Status32 => "payload-length-status32",
        }
    }
}

fn avalanche_response(format: AvalancheResponseFormat, status: u8, value: &[u8]) -> Vec<u8> {
    match format {
        AvalancheResponseFormat::Legacy => avalanche_frame(&[&[status], value].concat()),
        AvalancheResponseFormat::Status32 => [
            (value.len() as i32).to_be_bytes().as_slice(),
            REQUEST_ID.to_be_bytes().as_slice(),
            i32::from(status).to_be_bytes().as_slice(),
            value,
        ]
        .concat(),
    }
}

async fn detect_avalanche_response(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    request: &[u8],
) -> anyhow::Result<AvalancheResponseFormat> {
    timeout(REQUEST_TIMEOUT, async {
        stream.write_all(request).await?;
        let mut header = [0_u8; 8];
        stream.read_exact(&mut header).await?;
        ensure!(
            i32::from_be_bytes(header[4..8].try_into()?) == REQUEST_ID,
            "missing-key response request_id mismatch"
        );
        let format = match i32::from_be_bytes(header[..4].try_into()?) {
            1 => AvalancheResponseFormat::Legacy,
            0 => AvalancheResponseFormat::Status32,
            len => bail!("invalid missing-key response length: {len}"),
        };
        let width = if format == AvalancheResponseFormat::Legacy {
            1
        } else {
            4
        };
        let mut status = [0_u8; 4];
        stream.read_exact(&mut status[..width]).await?;
        let status = if width == 1 {
            i32::from(status[0])
        } else {
            i32::from_be_bytes(status)
        };
        ensure!(
            status == 2,
            "unexpected missing-key response status: {status}"
        );
        Ok(format)
    })
    .await
    .context("Avalanche response-format check timed out")?
}

fn resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut result = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        result.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        result.extend_from_slice(part);
        result.extend_from_slice(b"\r\n");
    }
    result
}

fn template(
    database: Database,
    key: &[u8],
    value: &[u8],
    format: AvalancheResponseFormat,
) -> Template {
    match database {
        Database::Avalanche => Template {
            get: avalanche_frame(&[1_i32.to_be_bytes().as_slice(), key].concat()),
            set: avalanche_frame(
                &[
                    2_i32.to_be_bytes().as_slice(),
                    (key.len() as i32).to_be_bytes().as_slice(),
                    key,
                    value,
                ]
                .concat(),
            ),
            got: avalanche_response(format, 3, value),
            stored: avalanche_response(format, 2, &[]),
        },
        Database::Memcached => {
            let key = std::str::from_utf8(key).expect("benchmark keys are ASCII");
            Template {
                get: format!("get {key}\r\n").into_bytes(),
                set: [
                    format!("set {key} 0 0 {}\r\n", value.len()).as_bytes(),
                    value,
                    b"\r\n",
                ]
                .concat(),
                got: [
                    format!("VALUE {key} 0 {}\r\n", value.len()).as_bytes(),
                    value,
                    b"\r\nEND\r\n",
                ]
                .concat(),
                stored: b"STORED\r\n".to_vec(),
            }
        }
        _ => Template {
            get: resp(&[b"GET", key]),
            set: resp(&[b"SET", key, value]),
            got: [format!("${}\r\n", value.len()).as_bytes(), value, b"\r\n"].concat(),
            stored: b"+OK\r\n".to_vec(),
        },
    }
}

async fn connect(address: &str) -> anyhow::Result<TcpStream> {
    let stream = timeout(REQUEST_TIMEOUT, TcpStream::connect(address))
        .await
        .context("connect timed out")?
        .with_context(|| format!("connect to {address}"))?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn exchange(
    stream: &mut TcpStream,
    request: &[u8],
    expected: &[u8],
    buffer: &mut Vec<u8>,
) -> anyhow::Result<()> {
    stream.write_all(request).await.context("write request")?;
    buffer.resize(expected.len(), 0);
    stream.read_exact(buffer).await.context("read response")?;
    ensure!(
        buffer.as_slice() == expected,
        "response differs from expected wire bytes"
    );
    Ok(())
}

async fn checked_exchange(
    stream: &mut TcpStream,
    request: &[u8],
    expected: &[u8],
    buffer: &mut Vec<u8>,
) -> anyhow::Result<()> {
    timeout(REQUEST_TIMEOUT, exchange(stream, request, expected, buffer))
        .await
        .context("request timed out")?
}

async fn correctness_check(config: &Settings) -> anyhow::Result<AvalancheResponseFormat> {
    let mut stream = connect(&config.address).await?;
    let mut buffer = Vec::new();
    let missing_request = template(
        config.database,
        b"absent-x",
        b"",
        AvalancheResponseFormat::Legacy,
    );
    let format = if matches!(config.database, Database::Avalanche) {
        let format = detect_avalanche_response(&mut stream, &missing_request.get).await?;
        eprintln!("AVALANCHE_RESPONSE_FORMAT={}", format.name());
        format
    } else {
        let missing = if matches!(config.database, Database::Memcached) {
            b"END\r\n".as_slice()
        } else {
            b"$-1\r\n"
        };
        checked_exchange(&mut stream, &missing_request.get, missing, &mut buffer).await?;
        AvalancheResponseFormat::Legacy
    };
    for value in [b"binary\0\xff\r\n".as_slice(), b"overwritten".as_slice()] {
        let request = template(config.database, b"check-00", value, format);
        checked_exchange(&mut stream, &request.set, &request.stored, &mut buffer).await?;
        checked_exchange(&mut stream, &request.get, &request.got, &mut buffer).await?;
    }
    Ok(format)
}

// 32 sub-buckets per power of two, measured in microseconds. Quantiles
// report bucket upper bounds: <= 3.125% relative bucket width above 32 us.
#[derive(Clone)]
struct Histogram {
    bins: Vec<u64>,
    count: u64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            bins: vec![0; 64 * 32],
            count: 0,
        }
    }
    fn record(&mut self, duration: Duration) {
        let micros = duration.as_nanos().div_ceil(1000).max(1) as u64;
        let exponent = (63 - micros.leading_zeros()) as usize;
        let base = 1_u64 << exponent;
        let sub = ((micros - base) * 32 / base) as usize;
        self.bins[exponent * 32 + sub] += 1;
        self.count += 1;
    }
    fn merge(&mut self, other: &Self) {
        for (dest, source) in self.bins.iter_mut().zip(&other.bins) {
            *dest += source;
        }
        self.count += other.count;
    }
    fn percentile(&self, fraction: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let rank = (self.count as f64 * fraction).ceil() as u64;
        let mut seen = 0;
        for (index, count) in self.bins.iter().enumerate() {
            seen += count;
            if seen >= rank {
                let base = 1_u64 << (index / 32);
                return base + ((index % 32 + 1) as u64 * base).div_ceil(32);
            }
        }
        0
    }
}

struct Stats {
    get: u64,
    set: u64,
    errors: u64,
    drained: u64,
    first_error: Option<String>,
    latency: Histogram,
}

impl Stats {
    fn new() -> Self {
        Self {
            get: 0,
            set: 0,
            errors: 0,
            drained: 0,
            first_error: None,
            latency: Histogram::new(),
        }
    }
    fn check(&self) -> anyhow::Result<()> {
        ensure!(
            self.errors == 0,
            "{} failed requests; first error: {}",
            self.errors,
            self.first_error.as_deref().unwrap_or("unknown")
        );
        ensure!(
            self.get + self.set > 0,
            "no successful responses in measurement window"
        );
        Ok(())
    }
    fn merge(&mut self, other: Self) {
        self.get += other.get;
        self.set += other.set;
        self.errors += other.errors;
        self.drained += other.drained;
        self.latency.merge(&other.latency);
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }
}

async fn phase(
    config: &Settings,
    templates: Arc<Vec<Template>>,
    seconds: f64,
    repeat: usize,
) -> anyhow::Result<(Stats, u128)> {
    let (start, ready) = watch::channel(None::<Instant>);
    let mut tasks = JoinSet::new();
    for client_id in 0..config.clients {
        let mut stream = connect(&config.address).await?;
        let templates = templates.clone();
        let mut ready = ready.clone();
        let mode = config.mode;
        let mut buffer = Vec::with_capacity(templates[0].got.len().max(templates[0].stored.len()));
        tasks.spawn(async move {
            let mut stats = Stats::new();
            let mut sequence = client_id;
            let mut rng =
                (client_id as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15) ^ (repeat as u64 + 42);
            ready.changed().await.context("start signal dropped")?;
            let deadline =
                ready.borrow().context("missing start time")? + Duration::from_secs_f64(seconds);
            while Instant::now() < deadline {
                // Independent from operation parity; mixed reads and writes
                // sample the same key space, without GET-even / SET-odd bias.
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let template = &templates[rng as usize % templates.len()];
                let get = mode.is_get(sequence);
                let (request, expected) = if get {
                    (&template.get, &template.got)
                } else {
                    (&template.set, &template.stored)
                };
                let began = Instant::now();
                match checked_exchange(&mut stream, request, expected, &mut buffer).await {
                    Ok(()) => {
                        let ended = Instant::now();
                        if ended <= deadline {
                            if get {
                                stats.get += 1;
                            } else {
                                stats.set += 1;
                            }
                            stats.latency.record(ended - began);
                        } else {
                            stats.drained += 1;
                        }
                    }
                    Err(error) => {
                        stats.errors += 1;
                        stats.first_error = Some(format!("client {client_id}: {error:#}"));
                        break;
                    }
                }
                sequence = sequence.wrapping_add(1);
            }
            Ok::<_, anyhow::Error>(stats)
        });
    }
    // Docker stats and the load client share the Docker host's wall clock.
    // Capture the window only after all connections are ready, excluding warmup
    // and drained responses from the corresponding server-memory samples.
    let started_unix_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    start
        .send(Some(Instant::now()))
        .context("no clients ready")?;
    let mut total = Stats::new();
    while let Some(stats) = tasks.join_next().await {
        total.merge(stats.context("client task panicked")??);
    }
    total.check()?;
    Ok((total, started_unix_ns))
}

pub struct Measurement {
    database: Database,
    mode: Mode,
    clients: usize,
    repeat: usize,
    seconds: f64,
    keys: usize,
    value_bytes: usize,
    started_unix_ns: u128,
    stats: Stats,
}

impl Measurement {
    pub fn json(&self) -> String {
        format!(
            concat!(
                "{{\"database\":\"{}\",\"mode\":\"{}\",\"clients\":{},\"repeat\":{},",
                "\"seconds\":{},\"keys\":{},\"value_bytes\":{},\"rps\":{:.3},",
                "\"completed\":{},\"get\":{},\"set\":{},\"errors\":{},\"drained\":{},",
                "\"p50_us\":{},\"p95_us\":{},\"p99_us\":{},",
                "\"started_unix_ns\":{},\"finished_unix_ns\":{}}}"
            ),
            self.database.name(),
            self.mode.name(),
            self.clients,
            self.repeat,
            self.seconds,
            self.keys,
            self.value_bytes,
            (self.stats.get + self.stats.set) as f64 / self.seconds,
            self.stats.get + self.stats.set,
            self.stats.get,
            self.stats.set,
            self.stats.errors,
            self.stats.drained,
            self.stats.latency.percentile(0.5),
            self.stats.latency.percentile(0.95),
            self.stats.latency.percentile(0.99),
            self.started_unix_ns,
            self.started_unix_ns + Duration::from_secs_f64(self.seconds).as_nanos()
        )
    }
}

pub fn execute(config: Settings) -> anyhow::Result<Vec<Measurement>> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.threads)
        .enable_all()
        .build()?
        .block_on(async {
            let response_format = correctness_check(&config)
                .await
                .context("GET/SET/overwrite correctness check failed")?;
            let value: Vec<u8> = (0..config.value_bytes).map(|n| (n % 256) as u8).collect();
            let templates = Arc::new(
                (0..config.keys)
                    .map(|key| {
                        template(
                            config.database,
                            format!("{key:08x}").as_bytes(),
                            &value,
                            response_format,
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            let mut stream = connect(&config.address).await?;
            let mut buffer = Vec::new();
            for template in templates.iter() {
                checked_exchange(&mut stream, &template.set, &template.stored, &mut buffer)
                    .await
                    .context("seed key space")?;
            }
            drop(stream);
            let mut measurements = Vec::new();
            for repeat in 1..=config.repeats {
                if config.warmup_seconds > 0.0 {
                    phase(&config, templates.clone(), config.warmup_seconds, repeat)
                        .await
                        .context("warmup")?;
                }
                let (stats, started_unix_ns) =
                    phase(&config, templates.clone(), config.seconds, repeat)
                        .await
                        .context("measurement")?;
                measurements.push(Measurement {
                    database: config.database,
                    mode: config.mode,
                    clients: config.clients,
                    repeat,
                    seconds: config.seconds,
                    keys: config.keys,
                    value_bytes: config.value_bytes,
                    started_unix_ns,
                    stats,
                });
            }
            Ok(measurements)
        })
}

pub fn database_test(database: Database, mode: Mode) -> anyhow::Result<()> {
    let mut args = Vec::new();
    for (name, flag) in [
        ("BENCH_TEST_CLIENTS", "--clients"),
        ("BENCH_TEST_SECONDS", "--seconds"),
        ("BENCH_TEST_WARMUP_SECONDS", "--warmup-seconds"),
        ("BENCH_TEST_REPEATS", "--repeats"),
        ("BENCH_TEST_THREADS", "--threads"),
    ] {
        if let Ok(value) = std::env::var(name) {
            args.extend([flag.to_owned(), value]);
        }
    }
    let mut settings = Settings::parse(args.into_iter())?;
    settings.database = database;
    settings.mode = mode;
    for measurement in execute(settings)? {
        println!("{}", measurement.json());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avalanche_status32_responses_separate_status_from_payload_length() {
        let format = AvalancheResponseFormat::Status32;
        let fixture = template(Database::Avalanche, b"key", b"a\0\xff", format);
        assert_eq!(
            fixture.got,
            b"\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00\x03a\0\xff"
        );
        assert_eq!(
            fixture.stored,
            b"\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x02"
        );
        assert_eq!(
            avalanche_response(format, 3, b""),
            b"\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x03"
        );
    }

    #[test]
    fn avalanche_legacy_and_status32_use_identical_requests() {
        let legacy = template(
            Database::Avalanche,
            b"key",
            b"a\0\xff",
            AvalancheResponseFormat::Legacy,
        );
        let current = template(
            Database::Avalanche,
            b"key",
            b"a\0\xff",
            AvalancheResponseFormat::Status32,
        );
        assert_eq!(legacy.get, current.get);
        assert_eq!(legacy.set, current.set);
        assert_eq!(legacy.got, b"\x00\x00\x00\x04\x00\x00\x00\x07\x03a\0\xff");
        assert_eq!(legacy.stored, b"\x00\x00\x00\x01\x00\x00\x00\x07\x02");
    }

    async fn probe_response(reply: Vec<u8>) -> anyhow::Result<AvalancheResponseFormat> {
        let (mut client, mut server) = tokio::io::duplex(128);
        let request = b"missing-key request";
        let peer = async move {
            let mut received = vec![0; request.len()];
            server.read_exact(&mut received).await.unwrap();
            assert_eq!(received, request);
            for byte in reply {
                server.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
        };
        let (result, ()) = tokio::join!(detect_avalanche_response(&mut client, request), peer);
        result
    }

    #[tokio::test]
    async fn detects_both_avalanche_formats_with_fragmented_responses() {
        for format in [
            AvalancheResponseFormat::Legacy,
            AvalancheResponseFormat::Status32,
        ] {
            assert_eq!(
                probe_response(avalanche_response(format, 2, b""))
                    .await
                    .unwrap(),
                format
            );
        }
    }

    #[tokio::test]
    async fn response_probe_rejects_wrong_ids_lengths_statuses_and_truncation() {
        let valid = avalanche_response(AvalancheResponseFormat::Status32, 2, b"");
        let mut wrong_id = valid.clone();
        wrong_id[7] = 9;
        let mut wrong_length = valid.clone();
        wrong_length[..4].copy_from_slice(&(-1_i32).to_be_bytes());
        let mut wrong_status = valid.clone();
        wrong_status[11] = 3;
        for reply in [wrong_id, wrong_length, wrong_status, valid[..10].to_vec()] {
            assert!(probe_response(reply).await.is_err());
        }
    }

    #[test]
    fn latency_quantiles_merge_counts_and_bound_known_values() {
        let mut first = Histogram::new();
        let mut second = Histogram::new();
        for _ in 0..95 {
            first.record(Duration::from_micros(100));
        }
        for _ in 0..5 {
            second.record(Duration::from_micros(1000));
        }
        first.merge(&second);
        assert_eq!(first.count, 100);
        assert!((100..=104).contains(&first.percentile(0.95)));
        assert!((1000..=1032).contains(&first.percentile(0.99)));
    }
    #[test]
    fn resp_binary_bulk_string_fixture() {
        assert_eq!(
            resp(&[b"SET", b"k", b"a\0b"]),
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\na\0b\r\n"
        );
    }
}
