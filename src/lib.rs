//! Async client library for talking to an EventDBX control server over Cap'n Proto.

mod client;
mod config;
mod control_capnp;
mod error;
mod noise;
mod types;

#[doc(hidden)]
pub mod internal {
    pub use crate::noise::*;
}

pub use crate::client::EventDbxClient;
pub use crate::config::ClientConfig;
pub use crate::error::{Error, Result, ServerError};
pub use crate::types::{
    AggregateSort, AggregateSortField, AppendEventRequest, AppendEventResult,
    CreateAggregateRequest, CreateAggregateResult, GetAggregateResult, ListAggregatesOptions,
    ListAggregatesResult, ListEventsOptions, ListEventsResult, PatchEventRequest, PatchEventResult,
    PublishTarget, SelectAggregateRequest, SelectAggregateResult, SetAggregateArchiveRequest,
    SetAggregateArchiveResult, VerifyAggregateResult,
};
