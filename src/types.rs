use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct ListAggregatesOptions {
    pub cursor: Option<String>,
    pub take: Option<u64>,
    pub filter: Option<String>,
    pub sort: Vec<AggregateSort>,
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
    Version,
    MerkleRoot,
    Archived,
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
    pub comment: Option<String>,
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
            comment: None,
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
