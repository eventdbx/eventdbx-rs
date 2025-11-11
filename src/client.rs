use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use capnp::message::ReaderOptions;
use capnp::serialize::{self, write_message_to_words};
use capnp_futures::serialize as capnp_async;
use futures::io::AsyncWriteExt as FuturesAsyncWriteExt;
use std::io::Cursor;
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
    CreateAggregateResult, GetAggregateResult, ListAggregatesOptions, ListAggregatesResult,
    ListEventsOptions, ListEventsResult, PatchEventRequest, PatchEventResult,
    SelectAggregateRequest, SelectAggregateResult, SetAggregateArchiveRequest,
    SetAggregateArchiveResult, VerifyAggregateResult,
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

                if options.sort.is_empty() {
                    list.set_has_sort(false);
                } else {
                    list.set_has_sort(true);
                    let mut sort = list.reborrow().init_sort(options.sort.len() as u32);
                    for (idx, sort_entry) in options.sort.iter().enumerate() {
                        let mut entry = sort.reborrow().get(idx as u32);
                        entry.set_descending(sort_entry.descending);
                        entry.set_field(match sort_entry.field {
                            AggregateSortField::AggregateType => {
                                control_capnp::AggregateSortField::AggregateType
                            }
                            AggregateSortField::AggregateId => {
                                control_capnp::AggregateSortField::AggregateId
                            }
                            AggregateSortField::Version => {
                                control_capnp::AggregateSortField::Version
                            }
                            AggregateSortField::MerkleRoot => {
                                control_capnp::AggregateSortField::MerkleRoot
                            }
                            AggregateSortField::Archived => {
                                control_capnp::AggregateSortField::Archived
                            }
                        });
                    }
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

    /// Toggles whether the aggregate is archived and optionally leaves a comment.
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

                if let Some(comment) = request.comment.as_deref() {
                    set_archive.set_comment(comment);
                    set_archive.set_has_comment(true);
                } else {
                    set_archive.set_has_comment(false);
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
            transport.write_encrypted(&payload).await?;
            if let Some(timeout) = self.config.request_timeout {
                time::timeout(timeout, transport.read_encrypted())
                    .await
                    .map_err(|_| Error::Timeout)??
            } else {
                transport.read_encrypted().await?
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

    let noise = perform_client_handshake(&mut reader, &mut writer, config.token.as_bytes()).await?;
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
    noise: TransportState,
}

impl ClientTransport {
    async fn write_encrypted(&mut self, payload: &[u8]) -> Result<()> {
        write_encrypted_frame(&mut self.writer, &mut self.noise, payload).await?;
        Ok(())
    }

    async fn read_encrypted(&mut self) -> Result<Vec<u8>> {
        match read_encrypted_frame(&mut self.reader, &mut self.noise).await? {
            Some(bytes) => Ok(bytes),
            None => Err(Error::Protocol(
                "control connection closed unexpectedly".to_string(),
            )),
        }
    }
}
