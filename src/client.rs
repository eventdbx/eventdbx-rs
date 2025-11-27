use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use capnp::message::ReaderOptions;
use capnp::serialize::{self, write_message_to_words};
use capnp_futures::serialize as capnp_async;
use futures::io::{AsyncReadExt as FuturesAsyncReadExt, AsyncWriteExt as FuturesAsyncWriteExt};
use std::io::{Cursor, ErrorKind};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;
use tokio::time;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::config::ClientConfig;
use crate::control_capnp;
use crate::error::{Error, Result, ServerError};
use crate::noise::{
    TransportState, perform_client_handshake, read_encrypted_frame, write_encrypted_frame,
};
use crate::types::{
    AggregateSortField, AppendEventRequest, AppendEventResult, CreateAggregateRequest,
    CreateAggregateResult, CreateSnapshotRequest, CreateSnapshotResult, GetAggregateResult,
    GetSnapshotRequest, GetSnapshotResult, ListAggregatesOptions, ListAggregatesResult,
    ListEventsOptions, ListEventsResult, ListSchemasOptions, ListSchemasResult,
    ListSnapshotsOptions, ListSnapshotsResult, PatchEventRequest, PatchEventResult,
    ReplaceSchemasRequest, ReplaceSchemasResult, SelectAggregateRequest, SelectAggregateResult,
    SetAggregateArchiveRequest, SetAggregateArchiveResult, TenantAssignRequest, TenantAssignResult,
    TenantQuotaClearRequest, TenantQuotaClearResult, TenantQuotaRecalcRequest,
    TenantQuotaRecalcResult, TenantQuotaSetRequest, TenantQuotaSetResult, TenantReloadRequest,
    TenantReloadResult, TenantSchemaPublishRequest, TenantSchemaPublishResult,
    TenantUnassignRequest, TenantUnassignResult, VerifyAggregateResult,
};

/// Async client that can issue control-plane calls against an EventDBX server.
pub struct EventDbxClient {
    config: Arc<ClientConfig>,
    transport: Arc<Mutex<ClientTransport>>,
    request_counter: AtomicU64,
}

impl EventDbxClient {
    /// Connects to the remote server, performs the hello handshake, and returns a ready client.
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        let config = Arc::new(config);
        let address = config.address();
        let connect_future = TcpStream::connect(address.clone());
        let stream = if config.connect_timeout.is_zero() {
            connect_future.await?
        } else {
            match time::timeout(config.connect_timeout, connect_future).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(err)) => return Err(Error::Io(err)),
                Err(_) => return Err(Error::Timeout),
            }
        };

        stream.set_nodelay(true)?;
        let transport = perform_handshake(&config, stream).await?;

        Ok(Self {
            config,
            transport: Arc::new(Mutex::new(transport)),
            request_counter: AtomicU64::new(1),
        })
    }

    /// Lists aggregates using the provided options.
    pub async fn list_aggregates(
        &self,
        options: ListAggregatesOptions,
    ) -> Result<ListAggregatesResult> {
        let sort_text = self.encode_sort_options(&options);
        self.invoke(
            |request| {
                let mut payload = request.reborrow().init_payload();
                let mut list = payload.reborrow().init_list_aggregates();

                if let Some(cursor) = options.cursor.as_deref() {
                    list.set_cursor(cursor);
                    list.set_has_cursor(true);
                } else {
                    list.set_has_cursor(false);
                }

                if let Some(take) = options.take {
                    list.set_take(take);
                    list.set_has_take(true);
                } else {
                    list.set_has_take(false);
                }

                if let Some(filter) = options.filter.as_deref() {
                    list.set_filter(filter);
                    list.set_has_filter(true);
                } else {
                    list.set_has_filter(false);
                }

                if let Some(sort) = sort_text.as_deref() {
                    list.set_has_sort(true);
                    list.set_sort(sort);
                } else {
                    list.set_has_sort(false);
                }

                list.set_include_archived(options.include_archived);
                list.set_archived_only(options.archived_only);
                list.set_token(self.token(options.token.as_deref()));
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::ListAggregates(resp) => {
                    let resp = resp?;
                    let aggregates_json = capnp_text_to_str(resp.get_aggregates_json())?;
                    let aggregates = serde_json::from_str::<serde_json::Value>(aggregates_json)?;
                    let next_cursor = if resp.get_has_next_cursor() {
                        Some(capnp_text_to_str(resp.get_next_cursor())?.to_string())
                    } else {
                        None
                    };

                    Ok(ListAggregatesResult {
                        aggregates,
                        next_cursor,
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "listAggregates: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Selects a subset of fields from an aggregate.
    pub async fn select_aggregate(
        &self,
        request: SelectAggregateRequest,
    ) -> Result<SelectAggregateResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut select = payload.reborrow().init_select_aggregate();
                select.set_aggregate_type(&request.aggregate_type);
                select.set_aggregate_id(&request.aggregate_id);
                select.set_token(self.token(request.token.as_deref()));

                let mut fields = select.reborrow().init_fields(request.fields.len() as u32);
                for (idx, field) in request.fields.iter().enumerate() {
                    fields.set(idx as u32, field);
                }
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::SelectAggregate(resp) => {
                    let resp = resp?;
                    let found = resp.get_found();
                    let selection = if found {
                        let json = capnp_text_to_str(resp.get_selection_json())?;
                        Some(serde_json::from_str::<serde_json::Value>(json)?)
                    } else {
                        None
                    };
                    Ok(SelectAggregateResult { found, selection })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "selectAggregate: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Fetches the canonical aggregate JSON for a given type and id.
    pub async fn get_aggregate(
        &self,
        aggregate_type: impl AsRef<str>,
        aggregate_id: impl AsRef<str>,
    ) -> Result<GetAggregateResult> {
        let aggregate_type = aggregate_type.as_ref().to_owned();
        let aggregate_id = aggregate_id.as_ref().to_owned();

        self.invoke(
            |request| {
                let mut payload = request.reborrow().init_payload();
                let mut get = payload.reborrow().init_get_aggregate();
                get.set_aggregate_type(&aggregate_type);
                get.set_aggregate_id(&aggregate_id);
                get.set_token(self.token(None));
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::GetAggregate(resp) => {
                    let resp = resp?;
                    let found = resp.get_found();
                    let aggregate = if found {
                        let json = capnp_text_to_str(resp.get_aggregate_json())?;
                        Some(serde_json::from_str::<serde_json::Value>(json)?)
                    } else {
                        None
                    };
                    Ok(GetAggregateResult { found, aggregate })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "getAggregate: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Lists events for an aggregate.
    pub async fn list_events(
        &self,
        aggregate_type: impl AsRef<str>,
        aggregate_id: impl AsRef<str>,
        options: ListEventsOptions,
    ) -> Result<ListEventsResult> {
        let aggregate_type = aggregate_type.as_ref().to_owned();
        let aggregate_id = aggregate_id.as_ref().to_owned();

        self.invoke(
            |request| {
                let mut payload = request.reborrow().init_payload();
                let mut list = payload.reborrow().init_list_events();
                list.set_aggregate_type(&aggregate_type);
                list.set_aggregate_id(&aggregate_id);
                list.set_token(self.token(options.token.as_deref()));

                if let Some(cursor) = options.cursor.as_deref() {
                    list.set_cursor(cursor);
                    list.set_has_cursor(true);
                } else {
                    list.set_has_cursor(false);
                }

                if let Some(take) = options.take {
                    list.set_take(take);
                    list.set_has_take(true);
                } else {
                    list.set_has_take(false);
                }

                if let Some(filter) = options.filter.as_deref() {
                    list.set_filter(filter);
                    list.set_has_filter(true);
                } else {
                    list.set_has_filter(false);
                }

                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::ListEvents(resp) => {
                    let resp = resp?;
                    let events_json = capnp_text_to_str(resp.get_events_json())?;
                    let events = serde_json::from_str::<serde_json::Value>(events_json)?;
                    let next_cursor = if resp.get_has_next_cursor() {
                        Some(capnp_text_to_str(resp.get_next_cursor())?.to_string())
                    } else {
                        None
                    };
                    Ok(ListEventsResult {
                        events,
                        next_cursor,
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "listEvents: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Appends an event to an aggregate.
    pub async fn append_event(&self, request: AppendEventRequest) -> Result<AppendEventResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut append = payload.reborrow().init_append_event();
                append.set_aggregate_type(&request.aggregate_type);
                append.set_aggregate_id(&request.aggregate_id);
                append.set_event_type(&request.event_type);
                append.set_payload_json(&serde_json::to_string(&request.payload)?);
                append.set_token(self.token(request.token.as_deref()));

                if let Some(note) = request.note.as_deref() {
                    append.set_note(note);
                    append.set_has_note(true);
                } else {
                    append.set_has_note(false);
                }

                if let Some(metadata) = request.metadata.as_ref() {
                    append.set_metadata_json(&serde_json::to_string(metadata)?);
                    append.set_has_metadata(true);
                } else {
                    append.set_has_metadata(false);
                }

                if request.publish_targets.is_empty() {
                    append.set_has_publish_targets(false);
                } else {
                    append.set_has_publish_targets(true);
                    let mut targets = append
                        .reborrow()
                        .init_publish_targets(request.publish_targets.len() as u32);
                    for (idx, target) in request.publish_targets.iter().enumerate() {
                        let mut dest = targets.reborrow().get(idx as u32);
                        dest.set_plugin(&target.plugin);
                        if let Some(mode) = target.mode.as_deref() {
                            dest.set_mode(mode);
                            dest.set_has_mode(true);
                        } else {
                            dest.set_has_mode(false);
                        }
                        if let Some(priority) = target.priority.as_deref() {
                            dest.set_priority(priority);
                            dest.set_has_priority(true);
                        } else {
                            dest.set_has_priority(false);
                        }
                    }
                }
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::AppendEvent(resp) => {
                    let resp = resp?;
                    let event_json = capnp_text_to_str(resp.get_event_json())?;
                    let event = serde_json::from_str::<serde_json::Value>(event_json)?;
                    Ok(AppendEventResult { event })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "appendEvent: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Applies a JSON patch to an existing event.
    pub async fn patch_event(&self, request: PatchEventRequest) -> Result<PatchEventResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut patch = payload.reborrow().init_patch_event();
                patch.set_aggregate_type(&request.aggregate_type);
                patch.set_aggregate_id(&request.aggregate_id);
                patch.set_event_type(&request.event_type);
                patch.set_patch_json(&serde_json::to_string(&request.patch)?);
                patch.set_token(self.token(request.token.as_deref()));

                if let Some(note) = request.note.as_deref() {
                    patch.set_note(note);
                    patch.set_has_note(true);
                } else {
                    patch.set_has_note(false);
                }

                if let Some(metadata) = request.metadata.as_ref() {
                    patch.set_metadata_json(&serde_json::to_string(metadata)?);
                    patch.set_has_metadata(true);
                } else {
                    patch.set_has_metadata(false);
                }

                if request.publish_targets.is_empty() {
                    patch.set_has_publish_targets(false);
                } else {
                    patch.set_has_publish_targets(true);
                    let mut targets = patch
                        .reborrow()
                        .init_publish_targets(request.publish_targets.len() as u32);
                    for (idx, target) in request.publish_targets.iter().enumerate() {
                        let mut dest = targets.reborrow().get(idx as u32);
                        dest.set_plugin(&target.plugin);
                        if let Some(mode) = target.mode.as_deref() {
                            dest.set_mode(mode);
                            dest.set_has_mode(true);
                        } else {
                            dest.set_has_mode(false);
                        }
                        if let Some(priority) = target.priority.as_deref() {
                            dest.set_priority(priority);
                            dest.set_has_priority(true);
                        } else {
                            dest.set_has_priority(false);
                        }
                    }
                }
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::AppendEvent(resp) => {
                    let resp = resp?;
                    let event_json = capnp_text_to_str(resp.get_event_json())?;
                    let event = serde_json::from_str::<serde_json::Value>(event_json)?;
                    Ok(PatchEventResult { event })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "patchEvent: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Creates a new aggregate by appending its first event atomically.
    pub async fn create_aggregate(
        &self,
        request: CreateAggregateRequest,
    ) -> Result<CreateAggregateResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut create = payload.reborrow().init_create_aggregate();
                create.set_aggregate_type(&request.aggregate_type);
                create.set_aggregate_id(&request.aggregate_id);
                create.set_event_type(&request.event_type);
                create.set_payload_json(&serde_json::to_string(&request.payload)?);
                create.set_token(self.token(request.token.as_deref()));

                if let Some(note) = request.note.as_deref() {
                    create.set_note(note);
                    create.set_has_note(true);
                } else {
                    create.set_has_note(false);
                }

                if let Some(metadata) = request.metadata.as_ref() {
                    create.set_metadata_json(&serde_json::to_string(metadata)?);
                    create.set_has_metadata(true);
                } else {
                    create.set_has_metadata(false);
                }

                if request.publish_targets.is_empty() {
                    create.set_has_publish_targets(false);
                } else {
                    create.set_has_publish_targets(true);
                    let mut targets = create
                        .reborrow()
                        .init_publish_targets(request.publish_targets.len() as u32);
                    for (idx, target) in request.publish_targets.iter().enumerate() {
                        let mut dest = targets.reborrow().get(idx as u32);
                        dest.set_plugin(&target.plugin);
                        if let Some(mode) = target.mode.as_deref() {
                            dest.set_mode(mode);
                            dest.set_has_mode(true);
                        } else {
                            dest.set_has_mode(false);
                        }
                        if let Some(priority) = target.priority.as_deref() {
                            dest.set_priority(priority);
                            dest.set_has_priority(true);
                        } else {
                            dest.set_has_priority(false);
                        }
                    }
                }
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::CreateAggregate(resp) => {
                    let resp = resp?;
                    let aggregate_json = capnp_text_to_str(resp.get_aggregate_json())?;
                    let aggregate = serde_json::from_str::<serde_json::Value>(aggregate_json)?;
                    Ok(CreateAggregateResult { aggregate })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "createAggregate: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Toggles whether the aggregate is archived and optionally leaves a note.
    pub async fn set_aggregate_archive(
        &self,
        request: SetAggregateArchiveRequest,
    ) -> Result<SetAggregateArchiveResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut set_archive = payload.reborrow().init_set_aggregate_archive();
                set_archive.set_aggregate_type(&request.aggregate_type);
                set_archive.set_aggregate_id(&request.aggregate_id);
                set_archive.set_archived(request.archived);
                set_archive.set_token(self.token(request.token.as_deref()));

                if let Some(note) = request.note.as_deref() {
                    set_archive.set_note(note);
                    set_archive.set_has_note(true);
                } else {
                    set_archive.set_has_note(false);
                }
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::SetAggregateArchive(resp) => {
                    let resp = resp?;
                    let aggregate_json = capnp_text_to_str(resp.get_aggregate_json())?;
                    let aggregate = serde_json::from_str::<serde_json::Value>(aggregate_json)?;
                    Ok(SetAggregateArchiveResult { aggregate })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "setAggregateArchive: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Verifies the Merkle root for an aggregate.
    pub async fn verify_aggregate(
        &self,
        aggregate_type: impl AsRef<str>,
        aggregate_id: impl AsRef<str>,
    ) -> Result<VerifyAggregateResult> {
        let aggregate_type = aggregate_type.as_ref().to_owned();
        let aggregate_id = aggregate_id.as_ref().to_owned();

        self.invoke(
            |request| {
                let mut payload = request.reborrow().init_payload();
                let mut verify = payload.reborrow().init_verify_aggregate();
                verify.set_aggregate_type(&aggregate_type);
                verify.set_aggregate_id(&aggregate_id);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::VerifyAggregate(resp) => {
                    let resp = resp?;
                    Ok(VerifyAggregateResult {
                        merkle_root: capnp_text_to_string(resp.get_merkle_root())?,
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "verifyAggregate: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Creates a new snapshot for an aggregate.
    pub async fn create_snapshot(
        &self,
        request: CreateSnapshotRequest,
    ) -> Result<CreateSnapshotResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut create = payload.reborrow().init_create_snapshot();
                create.set_aggregate_type(&request.aggregate_type);
                create.set_aggregate_id(&request.aggregate_id);
                create.set_token(self.token(request.token.as_deref()));
                if let Some(comment) = request.comment.as_deref() {
                    create.set_comment(comment);
                    create.set_has_comment(true);
                } else {
                    create.set_has_comment(false);
                }
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::CreateSnapshot(resp) => {
                    let resp = resp?;
                    let snapshot_json = capnp_text_to_str(resp.get_snapshot_json())?;
                    let snapshot = serde_json::from_str::<serde_json::Value>(snapshot_json)?;
                    Ok(CreateSnapshotResult { snapshot })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "createSnapshot: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Lists snapshots optionally filtered by aggregate or version.
    pub async fn list_snapshots(
        &self,
        options: ListSnapshotsOptions,
    ) -> Result<ListSnapshotsResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut list = payload.reborrow().init_list_snapshots();
                list.set_token(self.token(options.token.as_deref()));

                if let Some(aggregate_type) = options.aggregate_type.as_deref() {
                    list.set_aggregate_type(aggregate_type);
                    list.set_has_aggregate_type(true);
                } else {
                    list.set_has_aggregate_type(false);
                }

                if let Some(aggregate_id) = options.aggregate_id.as_deref() {
                    list.set_aggregate_id(aggregate_id);
                    list.set_has_aggregate_id(true);
                } else {
                    list.set_has_aggregate_id(false);
                }

                if let Some(version) = options.version {
                    list.set_version(version);
                    list.set_has_version(true);
                } else {
                    list.set_has_version(false);
                }
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::ListSnapshots(resp) => {
                    let resp = resp?;
                    let snapshots_json = capnp_text_to_str(resp.get_snapshots_json())?;
                    let snapshots = serde_json::from_str::<serde_json::Value>(snapshots_json)?;
                    Ok(ListSnapshotsResult { snapshots })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "listSnapshots: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Fetches a single snapshot by id.
    pub async fn get_snapshot(&self, request: GetSnapshotRequest) -> Result<GetSnapshotResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut get = payload.reborrow().init_get_snapshot();
                get.set_snapshot_id(request.snapshot_id);
                get.set_token(self.token(request.token.as_deref()));
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::GetSnapshot(resp) => {
                    let resp = resp?;
                    let found = resp.get_found();
                    let snapshot = if found {
                        let json = capnp_text_to_str(resp.get_snapshot_json())?;
                        Some(serde_json::from_str::<serde_json::Value>(json)?)
                    } else {
                        None
                    };
                    Ok(GetSnapshotResult { found, snapshot })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "getSnapshot: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Lists all schemas known to the server.
    pub async fn list_schemas(&self, options: ListSchemasOptions) -> Result<ListSchemasResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut list = payload.reborrow().init_list_schemas();
                list.set_token(self.token(options.token.as_deref()));
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::ListSchemas(resp) => {
                    let resp = resp?;
                    let schemas_json = capnp_text_to_str(resp.get_schemas_json())?;
                    let schemas = serde_json::from_str::<serde_json::Value>(schemas_json)?;
                    Ok(ListSchemasResult { schemas })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "listSchemas: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Replaces schemas with the provided JSON blob.
    pub async fn replace_schemas(
        &self,
        request: ReplaceSchemasRequest,
    ) -> Result<ReplaceSchemasResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut replace = payload.reborrow().init_replace_schemas();
                replace.set_token(self.token(request.token.as_deref()));
                replace.set_schemas_json(&serde_json::to_string(&request.schemas)?);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::ReplaceSchemas(resp) => {
                    let resp = resp?;
                    Ok(ReplaceSchemasResult {
                        replaced: resp.get_replaced(),
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "replaceSchemas: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Assigns a tenant to a shard.
    pub async fn tenant_assign(&self, request: TenantAssignRequest) -> Result<TenantAssignResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut assign = payload.reborrow().init_tenant_assign();
                assign.set_token(self.token(request.token.as_deref()));
                assign.set_tenant_id(&request.tenant_id);
                assign.set_shard_id(&request.shard_id);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::TenantAssign(resp) => {
                    let resp = resp?;
                    Ok(TenantAssignResult {
                        changed: resp.get_changed(),
                        shard_id: capnp_text_to_string(resp.get_shard_id())?,
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "tenantAssign: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Unassigns a tenant from any shard.
    pub async fn tenant_unassign(
        &self,
        request: TenantUnassignRequest,
    ) -> Result<TenantUnassignResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut unassign = payload.reborrow().init_tenant_unassign();
                unassign.set_token(self.token(request.token.as_deref()));
                unassign.set_tenant_id(&request.tenant_id);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::TenantUnassign(resp) => {
                    let resp = resp?;
                    Ok(TenantUnassignResult {
                        changed: resp.get_changed(),
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "tenantUnassign: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Sets or updates a tenant storage quota in megabytes.
    pub async fn tenant_quota_set(
        &self,
        request: TenantQuotaSetRequest,
    ) -> Result<TenantQuotaSetResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut set = payload.reborrow().init_tenant_quota_set();
                set.set_token(self.token(request.token.as_deref()));
                set.set_tenant_id(&request.tenant_id);
                set.set_max_storage_mb(request.max_storage_mb);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::TenantQuotaSet(resp) => {
                    let resp = resp?;
                    let quota_mb = if resp.get_has_quota() {
                        Some(resp.get_quota_mb())
                    } else {
                        None
                    };
                    Ok(TenantQuotaSetResult {
                        changed: resp.get_changed(),
                        quota_mb,
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "tenantQuotaSet: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Clears a tenant storage quota.
    pub async fn tenant_quota_clear(
        &self,
        request: TenantQuotaClearRequest,
    ) -> Result<TenantQuotaClearResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut clear = payload.reborrow().init_tenant_quota_clear();
                clear.set_token(self.token(request.token.as_deref()));
                clear.set_tenant_id(&request.tenant_id);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::TenantQuotaClear(resp) => {
                    let resp = resp?;
                    Ok(TenantQuotaClearResult {
                        changed: resp.get_changed(),
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "tenantQuotaClear: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Recalculates storage usage for a tenant.
    pub async fn tenant_quota_recalc(
        &self,
        request: TenantQuotaRecalcRequest,
    ) -> Result<TenantQuotaRecalcResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut recalc = payload.reborrow().init_tenant_quota_recalc();
                recalc.set_token(self.token(request.token.as_deref()));
                recalc.set_tenant_id(&request.tenant_id);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::TenantQuotaRecalc(resp) => {
                    let resp = resp?;
                    Ok(TenantQuotaRecalcResult {
                        storage_bytes: resp.get_storage_bytes(),
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "tenantQuotaRecalc: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Reloads a tenant's schemas without redeploying them.
    pub async fn tenant_reload(&self, request: TenantReloadRequest) -> Result<TenantReloadResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut reload = payload.reborrow().init_tenant_reload();
                reload.set_token(self.token(request.token.as_deref()));
                reload.set_tenant_id(&request.tenant_id);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::TenantReload(resp) => {
                    let resp = resp?;
                    Ok(TenantReloadResult {
                        reloaded: resp.get_reloaded(),
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "tenantReload: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Publishes schemas to a tenant with optional activation/reload flags.
    pub async fn tenant_schema_publish(
        &self,
        request: TenantSchemaPublishRequest,
    ) -> Result<TenantSchemaPublishResult> {
        self.invoke(
            |raw| {
                let mut payload = raw.reborrow().init_payload();
                let mut publish = payload.reborrow().init_tenant_schema_publish();
                publish.set_token(self.token(request.token.as_deref()));
                publish.set_tenant_id(&request.tenant_id);
                if let Some(reason) = request.reason.as_deref() {
                    publish.set_reason(reason);
                    publish.set_has_reason(true);
                } else {
                    publish.set_has_reason(false);
                }
                if let Some(actor) = request.actor.as_deref() {
                    publish.set_actor(actor);
                    publish.set_has_actor(true);
                } else {
                    publish.set_has_actor(false);
                }
                let mut labels = publish.reborrow().init_labels(request.labels.len() as u32);
                for (idx, label) in request.labels.iter().enumerate() {
                    labels.set(idx as u32, label);
                }
                publish.set_activate(request.activate);
                publish.set_force(request.force);
                publish.set_reload(request.reload);
                Ok(())
            },
            |response| match response.get_payload().which()? {
                control_capnp::control_response::payload::TenantSchemaPublish(resp) => {
                    let resp = resp?;
                    Ok(TenantSchemaPublishResult {
                        version_id: capnp_text_to_string(resp.get_version_id())?,
                        activated: resp.get_activated(),
                        skipped: resp.get_skipped(),
                    })
                }
                control_capnp::control_response::payload::Error(err) => {
                    Err(Error::Server(read_server_error(err?)?))
                }
                _ => Err(Error::Protocol(
                    "tenantSchemaPublish: unexpected payload".to_string(),
                )),
            },
        )
        .await
    }

    /// Gives callers read-only access to the configuration used to create this client.
    pub fn config(&self) -> &ClientConfig {
        self.config.as_ref()
    }

    fn next_request_id(&self) -> u64 {
        self.request_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn token<'a>(&'a self, override_token: Option<&'a str>) -> &'a str {
        override_token.unwrap_or(self.config.token.as_str())
    }

    fn encode_sort_options(&self, options: &ListAggregatesOptions) -> Option<String> {
        if let Some(text) = options.sort_text.as_ref() {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }

        if options.sort.is_empty() {
            return None;
        }

        let mut parts = Vec::with_capacity(options.sort.len());
        for sort in &options.sort {
            let field = match sort.field {
                AggregateSortField::AggregateType => "aggregate_type",
                AggregateSortField::AggregateId => "aggregate_id",
                AggregateSortField::Archived => "archived",
                AggregateSortField::CreatedAt => "created_at",
                AggregateSortField::UpdatedAt => "updated_at",
            };
            let order = if sort.descending { "desc" } else { "asc" };
            parts.push(format!("{field}:{order}"));
        }

        Some(parts.join(", "))
    }

    async fn invoke<F, R>(
        &self,
        build_payload: F,
        parse_response: impl FnOnce(control_capnp::control_response::Reader<'_>) -> Result<R>,
    ) -> Result<R>
    where
        F: FnOnce(&mut control_capnp::control_request::Builder<'_>) -> Result<()>,
    {
        let request_id = self.next_request_id();
        let mut message = capnp::message::Builder::new_default();

        {
            let mut request = message.init_root::<control_capnp::control_request::Builder>();
            request.set_id(request_id);
            build_payload(&mut request)?;
        }

        let payload = write_message_to_words(&message);

        let response_bytes = {
            let mut transport = self.transport.lock().await;
            transport.write_frame(&payload).await?;
            if let Some(timeout) = self.config.request_timeout {
                time::timeout(timeout, transport.read_frame())
                    .await
                    .map_err(|_| Error::Timeout)??
            } else {
                transport.read_frame().await?
            }
        };

        let mut cursor = Cursor::new(&response_bytes);
        let response_message = serialize::read_message(&mut cursor, ReaderOptions::new())?;
        let response = response_message.get_root::<control_capnp::control_response::Reader>()?;
        if response.get_id() != request_id {
            return Err(Error::Protocol(format!(
                "mismatched response: wanted {}, received {}",
                request_id,
                response.get_id()
            )));
        }

        parse_response(response)
    }
}

async fn perform_handshake(config: &ClientConfig, stream: TcpStream) -> Result<ClientTransport> {
    let (reader_half, writer_half) = stream.into_split();
    let mut reader = reader_half.compat();
    let mut writer = writer_half.compat_write();

    let mut message = capnp::message::Builder::new_default();
    {
        let mut hello = message.init_root::<control_capnp::control_hello::Builder>();
        hello.set_protocol_version(config.protocol_version);
        hello.set_token(&config.token);
        hello.set_tenant_id(&config.tenant_id);
        hello.set_no_noise(!config.use_noise);
    }

    let buffer = write_message_to_words(&message);
    FuturesAsyncWriteExt::write_all(&mut writer, &buffer).await?;
    FuturesAsyncWriteExt::flush(&mut writer).await?;

    let response_message = match config.request_timeout {
        Some(timeout) => time::timeout(timeout, async {
            capnp_async::read_message(&mut reader, ReaderOptions::new()).await
        })
        .await
        .map_err(|_| Error::Timeout)??,
        None => capnp_async::read_message(&mut reader, ReaderOptions::new()).await?,
    };

    let response = response_message.get_root::<control_capnp::control_hello_response::Reader>()?;
    if !response.get_accepted() {
        let message = capnp_text_to_string(response.get_message())?;
        return Err(Error::Authentication(message));
    }

    let use_noise = config.use_noise && !response.get_no_noise();
    let noise = if use_noise {
        Some(perform_client_handshake(&mut reader, &mut writer, config.token.as_bytes()).await?)
    } else {
        None
    };
    Ok(ClientTransport {
        reader,
        writer,
        noise,
    })
}

fn read_server_error(reader: control_capnp::control_error::Reader<'_>) -> Result<ServerError> {
    let code = capnp_text_to_string(reader.get_code())?;
    let message = capnp_text_to_string(reader.get_message())?;
    Ok(ServerError::new(code, message))
}

fn capnp_text_to_str<'a>(reader: capnp::Result<capnp::text::Reader<'a>>) -> Result<&'a str> {
    Ok(reader?.to_str()?)
}

fn capnp_text_to_string(reader: capnp::Result<capnp::text::Reader<'_>>) -> Result<String> {
    Ok(capnp_text_to_str(reader)?.to_string())
}

struct ClientTransport {
    reader: Compat<OwnedReadHalf>,
    writer: Compat<OwnedWriteHalf>,
    noise: Option<TransportState>,
}

impl ClientTransport {
    async fn write_frame(&mut self, payload: &[u8]) -> Result<()> {
        if let Some(noise) = self.noise.as_mut() {
            write_encrypted_frame(&mut self.writer, noise, payload).await?;
        } else {
            write_plain_frame(&mut self.writer, payload).await?;
        }
        Ok(())
    }

    async fn read_frame(&mut self) -> Result<Vec<u8>> {
        let maybe_bytes = if let Some(noise) = self.noise.as_mut() {
            read_encrypted_frame(&mut self.reader, noise).await?
        } else {
            read_plain_frame(&mut self.reader).await?
        };

        match maybe_bytes {
            Some(bytes) => Ok(bytes),
            None => Err(Error::Protocol(
                "control connection closed unexpectedly".to_string(),
            )),
        }
    }
}

async fn write_plain_frame<W>(writer: &mut W, payload: &[u8]) -> Result<()>
where
    W: futures::io::AsyncWrite + Unpin,
{
    if payload.len() > u32::MAX as usize {
        return Err(Error::Protocol(
            "frame payload exceeds u32 length".to_string(),
        ));
    }
    let mut header = [0u8; 4];
    header.copy_from_slice(&(payload.len() as u32).to_be_bytes());
    FuturesAsyncWriteExt::write_all(writer, &header).await?;
    FuturesAsyncWriteExt::write_all(writer, payload).await?;
    FuturesAsyncWriteExt::flush(writer).await?;
    Ok(())
}

async fn read_plain_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>>
where
    R: futures::io::AsyncRead + Unpin,
{
    let mut header = [0u8; 4];
    match FuturesAsyncReadExt::read_exact(reader, &mut header).await {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(Error::Io(err)),
    }
    let len = u32::from_be_bytes(header) as usize;
    let mut buffer = vec![0u8; len];
    FuturesAsyncReadExt::read_exact(reader, &mut buffer).await?;
    Ok(Some(buffer))
}
