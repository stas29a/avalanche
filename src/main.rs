use asynclog::{LogOptions, parse_level};
use avalanche::command_executor::DefaultCommandExecutor;
use avalanche::network::NetworkListener;
use avalanche::server::{Config, Server};
use avalanche::storage::Storage;
use log::{info, log};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Build log configuration
    let opts = LogOptions::new()
        .level(parse_level("info").map_err(anyhow::Error::from_boxed)?) // Set log level to info
        .use_console(true) // Enable console output
        .use_async(true); // Enable asynchronous writing

    // Initialize the logging system
    opts.builder().map_err(anyhow::Error::from_boxed)?;
    let storage = Arc::new(Storage::new());
    let command_executor = DefaultCommandExecutor::new(storage.clone());
    let network_listener = NetworkListener::new(command_executor);
    let config = Config {
        addr: "0.0.0.0:8080".to_string(),
    };
    let server = Server::new(network_listener, storage.clone(), config);
    let cancellation_token = CancellationToken::new();
    let child_cancellation_token = cancellation_token.clone();

    let server_task = tokio::spawn(async move {
        server
            .run(child_cancellation_token)
            .await
            .expect("Error running server");
    });

    tokio::signal::ctrl_c().await?;

    info!("Stopping server");
    cancellation_token.cancel();

    server_task.await?;
    println!("Server stopped");

    anyhow::Ok(())
}
