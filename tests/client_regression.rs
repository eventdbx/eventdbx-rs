use std::error::Error;
use std::future::Future;
use std::io::{Cursor, ErrorKind};
use std::time::Duration;

use capnp::message::ReaderOptions;
use capnp_futures::serialize as capnp_async;
use eventdbx_client::internal::{
    NoiseError, TransportState, perform_server_handshake as noise_perform_server_handshake,
    read_encrypted_frame as noise_read_encrypted_frame,
    write_encrypted_frame as noise_write_encrypted_frame,
};
use eventdbx_client::{
    AggregateSort, AggregateSortField, AppendEventRequest, CreateAggregateRequest, EventDbxClient,
    ListAggregatesOptions, ListEventsOptions, PatchEventRequest, SelectAggregateRequest,
    SetAggregateArchiveRequest,
};
use futures::io::{AsyncWrite, AsyncWriteExt as FuturesAsyncWriteExt};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const TEST_HOST: &str = "127.0.0.1";
const TEST_TOKEN: &str = "secret-token";
const TEST_TENANT: &str = "tenant";

mod control_capnp {
    include!(concat!(env!("OUT_DIR"), "/control_capnp.rs"));
}

struct ServerTransport {
    reader: Compat<OwnedReadHalf>,
    writer: Compat<OwnedWriteHalf>,
    noise: TransportState,
}

fn spawn_mock_server<F, Fut>(
    listener: TcpListener,
    handler: F,
) -> tokio::task::JoinHandle<TestResult<()>>
where
    F: FnOnce(ServerTransport) -> Fut + Send + 'static,
    Fut: Future<Output = TestResult<()>> + Send + 'static,
{
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let (reader_half, writer_half) = socket.into_split();
        let mut reader = reader_half.compat();
        let mut writer = writer_half.compat_write();
        perform_control_handshake(&mut reader, &mut writer).await?;
        let noise = noise_perform_server_handshake(&mut reader, &mut writer, TEST_TOKEN.as_bytes())
            .await
            .map_err(noise_to_test_error)?;
        let transport = ServerTransport {
            reader,
            writer,
            noise,
        };
        handler(transport).await
    })
}

fn noise_to_test_error(err: NoiseError) -> Box<dyn Error + Send + Sync> {
    Box::new(err)
}

async fn perform_control_handshake(
    reader: &mut Compat<OwnedReadHalf>,
    writer: &mut Compat<OwnedWriteHalf>,
) -> TestResult<()> {
    let message = capnp_async::read_message(reader, ReaderOptions::new()).await?;
    let hello = message
        .get_root::<control_capnp::control_hello::Reader>()
        .expect("hello reader");
    assert_eq!(read_text(hello.get_token()), TEST_TOKEN);
    assert_eq!(read_text(hello.get_tenant_id()), TEST_TENANT);

    write_message(writer, |message| {
        let mut payload = message.init_root::<control_capnp::control_hello_response::Builder>();
        payload.set_accepted(true);
    })
    .await?;
    Ok(())
}

async fn read_control_request(
    transport: &mut ServerTransport,
) -> TestResult<capnp::message::Reader<capnp::serialize::OwnedSegments>> {
    let bytes = match noise_read_encrypted_frame(&mut transport.reader, &mut transport.noise)
        .await
        .map_err(noise_to_test_error)?
    {
        Some(bytes) => bytes,
        None => {
            return Err("client closed connection unexpectedly".into());
        }
    };
    let mut cursor = Cursor::new(&bytes);
    Ok(capnp::serialize::read_message(
        &mut cursor,
        ReaderOptions::new(),
    )?)
}

async fn send_control_response<F>(
    transport: &mut ServerTransport,
    id: u64,
    build_payload: F,
) -> TestResult<()>
where
    F: FnOnce(control_capnp::control_response::payload::Builder<'_>),
{
    let mut message = capnp::message::Builder::new_default();
    {
        let mut response = message.init_root::<control_capnp::control_response::Builder>();
        response.set_id(id);
        let payload = response.reborrow().init_payload();
        build_payload(payload);
    }
    let mut buffer = Vec::new();
    capnp::serialize::write_message(&mut buffer, &message)?;
    noise_write_encrypted_frame(&mut transport.writer, &mut transport.noise, &buffer)
        .await
        .map_err(noise_to_test_error)?;
    Ok(())
}

async fn test_client(port: u16) -> eventdbx_client::Result<EventDbxClient> {
    let config = eventdbx_client::ClientConfig::new(TEST_HOST, TEST_TOKEN)
        .with_tenant(TEST_TENANT)
        .with_port(port)
        .with_request_timeout(Some(Duration::from_secs(1)));
    EventDbxClient::connect(config).await
}

fn read_text(reader: capnp::Result<capnp::text::Reader<'_>>) -> String {
    reader.unwrap().to_str().unwrap().to_string()
}

async fn bind_test_listener() -> TestResult<Option<(TcpListener, u16)>> {
    match TcpListener::bind((TEST_HOST, 0)).await {
        Ok(listener) => {
            let port = listener.local_addr()?.port();
            Ok(Some((listener, port)))
        }
        Err(err) if err.kind() == ErrorKind::PermissionDenied => {
            eprintln!("Skipping regression test: {err}");
            Ok(None)
        }
        Err(err) => Err(err.into()),
    }
}

async fn write_message<W, F>(writer: &mut W, build: F) -> TestResult<()>
where
    W: AsyncWrite + Unpin,
    F: FnOnce(&mut capnp::message::Builder<capnp::message::HeapAllocator>),
{
    let mut message = capnp::message::Builder::new_default();
    build(&mut message);
    let mut buffer = Vec::new();
    capnp::serialize::write_message(&mut buffer, &message)?;
    FuturesAsyncWriteExt::write_all(writer, &buffer).await?;
    FuturesAsyncWriteExt::flush(writer).await?;
    Ok(())
}

#[tokio::test]
async fn list_aggregates_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::ListAggregates(req) => {
                let req = req.unwrap();
                assert!(req.get_has_cursor());
                assert_eq!(read_text(req.get_cursor()), "cursor-1");
                assert!(req.get_has_take());
                assert_eq!(req.get_take(), 25);
                assert!(req.get_has_filter());
                assert_eq!(read_text(req.get_filter()), "status = 'active'");
                assert!(req.get_has_sort());
                let sort = req.get_sort().unwrap();
                assert_eq!(sort.len(), 2);
                assert_eq!(
                    sort.get(0).get_field().unwrap(),
                    control_capnp::AggregateSortField::AggregateType
                );
                assert!(!sort.get(0).get_descending());
                assert_eq!(
                    sort.get(1).get_field().unwrap(),
                    control_capnp::AggregateSortField::Version
                );
                assert!(sort.get(1).get_descending());
                assert!(req.get_include_archived());
                assert!(req.get_archived_only());
                assert_eq!(read_text(req.get_token()), "override-token");
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_list_aggregates();
            resp.set_aggregates_json(r#"{"items":[{"id":"agg-1"}]}"#);
            resp.set_next_cursor("cursor-2");
            resp.set_has_next_cursor(true);
        })
        .await
    });

    let client = test_client(port).await?;
    let mut options = ListAggregatesOptions::default();
    options.cursor = Some("cursor-1".into());
    options.take = Some(25);
    options.filter = Some("status = 'active'".into());
    options.sort = vec![
        AggregateSort {
            field: AggregateSortField::AggregateType,
            descending: false,
        },
        AggregateSort {
            field: AggregateSortField::Version,
            descending: true,
        },
    ];
    options.include_archived = true;
    options.archived_only = true;
    options.token = Some("override-token".into());

    let result = client.list_aggregates(options).await?;
    assert_eq!(result.next_cursor.as_deref(), Some("cursor-2"));
    assert_eq!(result.aggregates["items"][0]["id"], json!("agg-1"));
    drop(client);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn get_aggregate_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::GetAggregate(req) => {
                let req = req.unwrap();
                assert_eq!(read_text(req.get_aggregate_type()), "person");
                assert_eq!(read_text(req.get_aggregate_id()), "p-1");
                assert_eq!(read_text(req.get_token()), TEST_TOKEN);
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_get_aggregate();
            resp.set_found(true);
            resp.set_aggregate_json(r#"{"id":"p-1"}"#);
        })
        .await
    });

    let client = test_client(port).await?;
    let result = client.get_aggregate("person", "p-1").await?;
    assert!(result.found);
    assert_eq!(result.aggregate.unwrap()["id"], json!("p-1"));
    drop(client);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn list_events_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::ListEvents(req) => {
                let req = req.unwrap();
                assert_eq!(read_text(req.get_aggregate_type()), "person");
                assert_eq!(read_text(req.get_aggregate_id()), "p-1");
                assert!(req.get_has_cursor());
                assert_eq!(read_text(req.get_cursor()), "cursor-10");
                assert!(req.get_has_take());
                assert_eq!(req.get_take(), 50);
                assert!(req.get_has_filter());
                assert_eq!(read_text(req.get_filter()), "eventType = 'status'");
                assert_eq!(read_text(req.get_token()), "events-token");
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_list_events();
            resp.set_events_json(r#"{"events":[{"version":1}]}"#);
            resp.set_next_cursor("cursor-11");
            resp.set_has_next_cursor(true);
        })
        .await
    });

    let client = test_client(port).await?;
    let mut options = ListEventsOptions::default();
    options.cursor = Some("cursor-10".into());
    options.take = Some(50);
    options.filter = Some("eventType = 'status'".into());
    options.token = Some("events-token".into());
    let result = client.list_events("person", "p-1", options).await?;

    assert_eq!(result.next_cursor.as_deref(), Some("cursor-11"));
    assert_eq!(result.events["events"][0]["version"], json!(1));
    drop(client);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn append_event_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::AppendEvent(req) => {
                let req = req.unwrap();
                assert_eq!(read_text(req.get_aggregate_type()), "person");
                assert_eq!(read_text(req.get_aggregate_id()), "p-2");
                assert_eq!(read_text(req.get_event_type()), "person_created");
                assert_eq!(read_text(req.get_payload_json()), r#"{"status":"active"}"#);
                assert!(req.get_has_note());
                assert_eq!(read_text(req.get_note()), "initial import");
                assert!(req.get_has_metadata());
                assert_eq!(read_text(req.get_metadata_json()), r#"{"trace_id":"abc"}"#);
                assert_eq!(read_text(req.get_token()), "append-token");
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_append_event();
            resp.set_event_json(r#"{"id":"evt-1"}"#);
        })
        .await
    });

    let client = test_client(port).await?;
    let mut request = AppendEventRequest::new(
        "person",
        "p-2",
        "person_created",
        json!({ "status": "active" }),
    );
    request.note = Some("initial import".into());
    request.metadata = Some(json!({ "trace_id": "abc" }));
    request.token = Some("append-token".into());

    let result = client.append_event(request).await?;
    assert_eq!(result.event["id"], json!("evt-1"));
    drop(client);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn create_aggregate_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::CreateAggregate(req) => {
                let req = req.unwrap();
                assert_eq!(read_text(req.get_aggregate_type()), "order");
                assert_eq!(read_text(req.get_aggregate_id()), "o-1");
                assert_eq!(read_text(req.get_event_type()), "order_created");
                assert_eq!(read_text(req.get_payload_json()), r#"{"status":"draft"}"#);
                assert!(req.get_has_note());
                assert_eq!(read_text(req.get_note()), "via api");
                assert!(req.get_has_metadata());
                assert_eq!(
                    read_text(req.get_metadata_json()),
                    r#"{"request_id":"req-1"}"#
                );
                assert_eq!(read_text(req.get_token()), "create-token");
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_create_aggregate();
            resp.set_aggregate_json(r#"{"id":"o-1"}"#);
        })
        .await
    });

    let client = test_client(port).await?;
    let mut request = CreateAggregateRequest::new(
        "order",
        "o-1",
        "order_created",
        json!({ "status": "draft" }),
    );
    request.note = Some("via api".into());
    request.metadata = Some(json!({ "request_id": "req-1" }));
    request.token = Some("create-token".into());

    let result = client.create_aggregate(request).await?;
    assert_eq!(result.aggregate["id"], json!("o-1"));
    drop(client);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn patch_event_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::PatchEvent(req) => {
                let req = req.unwrap();
                assert_eq!(read_text(req.get_aggregate_type()), "person");
                assert_eq!(read_text(req.get_aggregate_id()), "p-3");
                assert_eq!(read_text(req.get_event_type()), "person_status_updated");
                assert_eq!(
                    read_text(req.get_patch_json()),
                    r#"[{"op":"replace","path":"/status","value":"inactive"}]"#
                );
                assert!(req.get_has_note());
                assert_eq!(read_text(req.get_note()), "correct status");
                assert!(req.get_has_metadata());
                assert_eq!(
                    read_text(req.get_metadata_json()),
                    r#"{"trace_id":"patch"}"#
                );
                assert_eq!(read_text(req.get_token()), "patch-token");
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_append_event();
            resp.set_event_json(r#"{"id":"evt-2"}"#);
        })
        .await
    });

    let client = test_client(port).await?;
    let mut request = PatchEventRequest::new(
        "person",
        "p-3",
        "person_status_updated",
        json!([{ "op": "replace", "path": "/status", "value": "inactive" }]),
    );
    request.note = Some("correct status".into());
    request.metadata = Some(json!({ "trace_id": "patch" }));
    request.token = Some("patch-token".into());

    let result = client.patch_event(request).await?;
    assert_eq!(result.event["id"], json!("evt-2"));
    drop(client);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn select_aggregate_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::SelectAggregate(req) => {
                let req = req.unwrap();
                assert_eq!(read_text(req.get_aggregate_type()), "person");
                assert_eq!(read_text(req.get_aggregate_id()), "p-4");
                let fields = req.get_fields().unwrap();
                assert_eq!(fields.len(), 2);
                assert_eq!(fields.get(0).unwrap().to_str().unwrap(), "status");
                assert_eq!(fields.get(1).unwrap().to_str().unwrap(), "email");
                assert_eq!(read_text(req.get_token()), "select-token");
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_select_aggregate();
            resp.set_found(true);
            resp.set_selection_json(r#"{"status":"active"}"#);
        })
        .await
    });

    let client = test_client(port).await?;
    let request = SelectAggregateRequest {
        aggregate_type: "person".into(),
        aggregate_id: "p-4".into(),
        fields: vec!["status".into(), "email".into()],
        token: Some("select-token".into()),
    };
    let result = client.select_aggregate(request).await?;
    assert!(result.found);
    assert_eq!(result.selection.unwrap()["status"], json!("active"));
    drop(client);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn set_aggregate_archive_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::SetAggregateArchive(req) => {
                let req = req.unwrap();
                assert_eq!(read_text(req.get_aggregate_type()), "person");
                assert_eq!(read_text(req.get_aggregate_id()), "p-5");
                assert!(req.get_archived());
                assert!(req.get_has_comment());
                assert_eq!(read_text(req.get_comment()), "cleanup");
                assert_eq!(read_text(req.get_token()), "archive-token");
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_set_aggregate_archive();
            resp.set_aggregate_json(r#"{"id":"p-5","archived":true}"#);
        })
        .await
    });

    let client = test_client(port).await?;
    let mut request = SetAggregateArchiveRequest::new("person", "p-5", true);
    request.comment = Some("cleanup".into());
    request.token = Some("archive-token".into());

    let result = client.set_aggregate_archive(request).await?;
    assert_eq!(result.aggregate["archived"], json!(true));
    drop(client);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn verify_aggregate_regression() -> TestResult<()> {
    let Some((listener, port)) = bind_test_listener().await? else {
        return Ok(());
    };
    let server = spawn_mock_server(listener, |mut transport| async move {
        let message = read_control_request(&mut transport).await?;
        let request = message
            .get_root::<control_capnp::control_request::Reader>()
            .unwrap();
        match request.get_payload().which().unwrap() {
            control_capnp::control_request::payload::VerifyAggregate(req) => {
                let req = req.unwrap();
                assert_eq!(read_text(req.get_aggregate_type()), "order");
                assert_eq!(read_text(req.get_aggregate_id()), "o-99");
            }
            _ => panic!("unexpected payload"),
        }

        send_control_response(&mut transport, request.get_id(), |mut payload| {
            let mut resp = payload.reborrow().init_verify_aggregate();
            resp.set_merkle_root("abc123");
        })
        .await
    });

    let client = test_client(port).await?;
    let result = client.verify_aggregate("order", "o-99").await?;
    assert_eq!(result.merkle_root, "abc123");
    drop(client);
    server.await??;
    Ok(())
}
