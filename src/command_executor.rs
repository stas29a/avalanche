use crate::network::NetworkFrame;
use crate::storage::Storage;
use bytes::Bytes;
use std::sync::Arc;

#[derive(Debug)]
#[repr(i32)]
pub enum Command {
    GET(Bytes),
    SET(Bytes, Bytes),
}

impl Command {
    pub fn decode(body: &Bytes) -> Option<Command> {
        if body.len() < 4 {
            return None;
        }

        let command_id = i32::from_be_bytes(body[0..4].try_into().ok()?);

        match command_id {
            1 => Some(Command::GET(body.slice(4..))),
            2 => {
                let key_len = i32::from_be_bytes(body[4..8].try_into().ok()?);
                Some(Command::SET(
                    Bytes::copy_from_slice(&body[8..8 + key_len as usize]),
                    Bytes::copy_from_slice(&body[8 + key_len as usize..]),
                ))
            }
            _ => None,
        }
    }
}

pub enum CommandResult {
    Success,
    SuccessEmptyResult,
    SuccessWithResult(Bytes),
    Failure(String),
}

pub struct DefaultCommandExecutor {
    storage: Arc<Storage>,
}

pub trait CommandExecutor {
    fn execute(&self, client_request: &NetworkFrame<Bytes>) -> anyhow::Result<CommandResult>;
}

impl DefaultCommandExecutor {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
}

impl CommandExecutor for DefaultCommandExecutor {
    fn execute(&self, network_frame: &NetworkFrame<Bytes>) -> anyhow::Result<CommandResult> {
        let command = Command::decode(&network_frame.body)
            .ok_or_else(|| anyhow::anyhow!("invalid command"))?;

        match command {
            Command::GET(key) => match self.storage.get(key) {
                Some(value) => Ok(CommandResult::SuccessWithResult(value.clone())),
                None => Ok(CommandResult::SuccessEmptyResult),
            },
            Command::SET(key, value) => {
                self.storage.set(key, value)?;
                Ok(CommandResult::SuccessEmptyResult)
            }
        }
    }
}
