use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct ListAggregatesOptions {
    pub cursor: Option<String>,
    pub take: Option<u64>,
    pub filter: Option<String>,
    pub sort: Vec<AggregateSort>,
    pub sort_text: Option<String>,
    pub include_archived: bool,
    pub archived_only: bool,
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AggregateSort {
    pub field: AggregateSortField,
    pub descending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateSortField {
    AggregateType,
    AggregateId,
    Archived,
    CreatedAt,
    UpdatedAt,
}

#[derive(Debug, Clone)]
pub struct ListAggregatesResult {
    pub aggregates: Value,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetAggregateResult {
    pub found: bool,
    pub aggregate: Option<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct ListEventsOptions {
    pub cursor: Option<String>,
    pub take: Option<u64>,
    pub filter: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListEventsResult {
    pub events: Value,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppendEventRequest {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: Value,
    pub note: Option<String>,
    pub metadata: Option<Value>,
    pub token: Option<String>,
    pub publish_targets: Vec<PublishTarget>,
}

impl AppendEventRequest {
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        event_type: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            event_type: event_type.into(),
            payload,
            note: None,
            metadata: None,
            token: None,
            publish_targets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendEventResult {
    pub event: Value,
}

#[derive(Debug, Clone)]
pub struct PatchEventRequest {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub patch: Value,
    pub note: Option<String>,
    pub metadata: Option<Value>,
    pub token: Option<String>,
    pub publish_targets: Vec<PublishTarget>,
}

impl PatchEventRequest {
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        event_type: impl Into<String>,
        patch: Value,
    ) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            event_type: event_type.into(),
            patch,
            note: None,
            metadata: None,
            token: None,
            publish_targets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PatchEventResult {
    pub event: Value,
}

#[derive(Debug, Clone)]
pub struct CreateAggregateRequest {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: Value,
    pub note: Option<String>,
    pub metadata: Option<Value>,
    pub token: Option<String>,
    pub publish_targets: Vec<PublishTarget>,
}

impl CreateAggregateRequest {
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        event_type: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            event_type: event_type.into(),
            payload,
            note: None,
            metadata: None,
            token: None,
            publish_targets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateAggregateResult {
    pub aggregate: Value,
}

#[derive(Debug, Clone)]
pub struct SelectAggregateRequest {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub fields: Vec<String>,
    pub token: Option<String>,
}

impl SelectAggregateRequest {
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        fields: Vec<String>,
    ) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            fields,
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SelectAggregateResult {
    pub found: bool,
    pub selection: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct SetAggregateArchiveRequest {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub archived: bool,
    pub note: Option<String>,
    pub token: Option<String>,
}

impl SetAggregateArchiveRequest {
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        archived: bool,
    ) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            archived,
            note: None,
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SetAggregateArchiveResult {
    pub aggregate: Value,
}

#[derive(Debug, Clone)]
pub struct VerifyAggregateResult {
    pub merkle_root: String,
}

#[derive(Debug, Clone)]
pub struct CreateSnapshotRequest {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub comment: Option<String>,
    pub token: Option<String>,
}

impl CreateSnapshotRequest {
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
    ) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            comment: None,
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateSnapshotResult {
    pub snapshot: Value,
}

#[derive(Debug, Clone, Default)]
pub struct ListSnapshotsOptions {
    pub aggregate_type: Option<String>,
    pub aggregate_id: Option<String>,
    pub version: Option<u64>,
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListSnapshotsResult {
    pub snapshots: Value,
}

#[derive(Debug, Clone)]
pub struct GetSnapshotRequest {
    pub snapshot_id: u64,
    pub token: Option<String>,
}

impl GetSnapshotRequest {
    pub fn new(snapshot_id: u64) -> Self {
        Self {
            snapshot_id,
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GetSnapshotResult {
    pub found: bool,
    pub snapshot: Option<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct ListSchemasOptions {
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListSchemasResult {
    pub schemas: Value,
}

#[derive(Debug, Clone)]
pub struct ReplaceSchemasRequest {
    pub schemas: Value,
    pub token: Option<String>,
}

impl ReplaceSchemasRequest {
    pub fn new(schemas: Value) -> Self {
        Self {
            schemas,
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplaceSchemasResult {
    pub replaced: u32,
}

#[derive(Debug, Clone)]
pub struct TenantAssignRequest {
    pub tenant_id: String,
    pub shard_id: String,
    pub token: Option<String>,
}

impl TenantAssignRequest {
    pub fn new(tenant_id: impl Into<String>, shard_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            shard_id: shard_id.into(),
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TenantAssignResult {
    pub changed: bool,
    pub shard_id: String,
}

#[derive(Debug, Clone)]
pub struct TenantUnassignRequest {
    pub tenant_id: String,
    pub token: Option<String>,
}

impl TenantUnassignRequest {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TenantUnassignResult {
    pub changed: bool,
}

#[derive(Debug, Clone)]
pub struct TenantQuotaSetRequest {
    pub tenant_id: String,
    pub max_storage_mb: u64,
    pub token: Option<String>,
}

impl TenantQuotaSetRequest {
    pub fn new(tenant_id: impl Into<String>, max_storage_mb: u64) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            max_storage_mb,
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TenantQuotaSetResult {
    pub changed: bool,
    pub quota_mb: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TenantQuotaClearRequest {
    pub tenant_id: String,
    pub token: Option<String>,
}

impl TenantQuotaClearRequest {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TenantQuotaClearResult {
    pub changed: bool,
}

#[derive(Debug, Clone)]
pub struct TenantQuotaRecalcRequest {
    pub tenant_id: String,
    pub token: Option<String>,
}

impl TenantQuotaRecalcRequest {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TenantQuotaRecalcResult {
    pub storage_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct TenantReloadRequest {
    pub tenant_id: String,
    pub token: Option<String>,
}

impl TenantReloadRequest {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TenantReloadResult {
    pub reloaded: bool,
}

#[derive(Debug, Clone)]
pub struct TenantSchemaPublishRequest {
    pub tenant_id: String,
    pub reason: Option<String>,
    pub actor: Option<String>,
    pub labels: Vec<String>,
    pub activate: bool,
    pub force: bool,
    pub reload: bool,
    pub token: Option<String>,
}

impl TenantSchemaPublishRequest {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            reason: None,
            actor: None,
            labels: Vec::new(),
            activate: false,
            force: false,
            reload: false,
            token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TenantSchemaPublishResult {
    pub version_id: String,
    pub activated: bool,
    pub skipped: bool,
}

#[derive(Debug, Clone)]
pub struct PublishTarget {
    pub plugin: String,
    pub mode: Option<String>,
    pub priority: Option<String>,
}

impl PublishTarget {
    pub fn new(plugin: impl Into<String>) -> Self {
        Self {
            plugin: plugin.into(),
            mode: None,
            priority: None,
        }
    }

    pub fn with_mode(mut self, mode: impl Into<String>) -> Self {
        self.mode = Some(mode.into());
        self
    }

    pub fn with_priority(mut self, priority: impl Into<String>) -> Self {
        self.priority = Some(priority.into());
        self
    }
}
