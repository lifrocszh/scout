mod coordinator;
pub(crate) mod frontier;
mod protocol;
mod worker;

pub(crate) mod proto {
    tonic::include_proto!("scout.distributed.v1");
}

const PROTOCOL_VERSION: u32 = 1;
const MAX_BODY_TRANSFER_BYTES: usize = 16 * 1024 * 1024;
const MAX_RPC_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
const MAX_URL_BYTES: usize = 8192;
const EMPTY_POLL_MS: u64 = 250;

pub(crate) use coordinator::run_coordinator;
pub(crate) use worker::run_worker;
