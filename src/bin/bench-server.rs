use avalanche::{command_executor, network, server, storage};

use std::sync::Arc;

fn main() -> anyhow::Result<()> {
    let available_threads = std::thread::available_parallelism().map_or(1, |count| count.get());
    let threads = std::env::var("BENCH_SERVER_THREADS")
        .unwrap_or_else(|_| available_threads.to_string())
        .parse()?;
    anyhow::ensure!(threads > 0, "BENCH_SERVER_THREADS must be positive");
    let addr = std::env::var("BENCH_SERVER_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let storage = Arc::new(storage::Storage::new());
    let executor = command_executor::DefaultCommandExecutor::new(storage.clone());
    eprintln!("AVALANCHE_SERVER_THREADS={threads}");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?
        .block_on(
            server::Server::new(
                network::NetworkListener::new(executor),
                storage,
                server::Config { addr },
            )
            .run(tokio_util::sync::CancellationToken::new()),
        )
}
