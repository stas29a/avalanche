use crate::command_executor::CommandExecutor;
use crate::network::NetworkListener;
use crate::storage::Storage;
use log::{error, info};
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::channel;
use tokio_util::sync::CancellationToken;

pub struct Server<C: CommandExecutor> {
    network_listener: NetworkListener<C>,
    storage: Arc<Storage>,
    config: Config,
}

pub struct Config {
    pub addr: String,
}

impl<C: CommandExecutor + Sync + Send + 'static> Server<C> {
    pub fn new(
        network_listener: NetworkListener<C>,
        storage: Arc<Storage>,
        config: Config,
    ) -> Self {
        Self {
            network_listener,
            storage,
            config,
        }
    }

    pub async fn run(self, cancellation_token: CancellationToken) -> Result<(), anyhow::Error> {
        select! {
            _ = self.network_listener.listen(self.config.addr, cancellation_token.clone()) => {
                info!("Network listener accepted");
            },
            _ = cancellation_token.cancelled() => (),
        }

        anyhow::Ok(())
    }
}
