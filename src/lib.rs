//! Async client library for talking to an EventDBX control server over Cap'n Proto.

mod client;
mod config;
mod control_capnp;
mod error;
mod noise;
mod schema_capnp;
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
    CreateAggregateRequest, CreateAggregateResult, CreateSnapshotRequest, CreateSnapshotResult,
    GetAggregateResult, GetSnapshotRequest, GetSnapshotResult, ListAggregatesOptions,
    ListAggregatesResult,
    ListEventsOptions, ListEventsResult, ListSchemasOptions, ListSchemasResult,
    ListSnapshotsOptions, ListSnapshotsResult, PatchEventRequest, PatchEventResult, PublishTarget,
    ReplaceSchemasRequest, ReplaceSchemasResult, SelectAggregateRequest, SelectAggregateResult,
    SetAggregateArchiveRequest, SetAggregateArchiveResult, TenantAssignRequest, TenantAssignResult,
    TenantQuotaClearRequest, TenantQuotaClearResult, TenantQuotaRecalcRequest,
    TenantQuotaRecalcResult, TenantQuotaSetRequest, TenantQuotaSetResult, TenantReloadRequest,
    TenantReloadResult, TenantSchemaPublishRequest, TenantSchemaPublishResult,
    TenantUnassignRequest, TenantUnassignResult, VerifyAggregateResult,
};
