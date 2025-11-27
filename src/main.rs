use std::env;

use clap::{Parser, Subcommand};
use eventdbx_client::{
    AppendEventRequest, ClientConfig, CreateAggregateRequest, CreateSnapshotRequest,
    EventDbxClient, GetSnapshotRequest, ListAggregatesOptions, ListEventsOptions,
    ListSchemasOptions, ListSnapshotsOptions, PatchEventRequest, ReplaceSchemasRequest,
    SelectAggregateRequest, SetAggregateArchiveRequest, TenantAssignRequest,
    TenantQuotaClearRequest, TenantQuotaRecalcRequest, TenantQuotaSetRequest, TenantReloadRequest,
    TenantSchemaPublishRequest, TenantUnassignRequest,
};
use fake::Fake;
use fake::faker::lorem::en::{Sentence, Word};
use fake::faker::number::en::NumberWithFormat;
use serde_json::{Value, json};

#[derive(Parser, Debug)]
#[command(
    name = "eventdbx-cli",
    version,
    about = "Ad-hoc EventDBX client for exercising API calls"
)]
struct Cli {
    /// Override EVENTDBX_HOST
    #[arg(long)]
    host: Option<String>,
    /// Override EVENTDBX_PORT
    #[arg(long)]
    port: Option<u16>,
    /// Override EVENTDBX_TOKEN
    #[arg(long)]
    token: Option<String>,
    /// Override EVENTDBX_TENANT
    #[arg(long)]
    tenant: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// List aggregates with optional filters.
    List {
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        take: Option<u64>,
        #[arg(long)]
        include_archived: bool,
        #[arg(long)]
        archived_only: bool,
        #[arg(long)]
        filter: Option<String>,
    },
    /// Select specific fields from an aggregate.
    Select {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
        #[arg(long = "field", value_name = "FIELD", num_args(0..), value_delimiter = ',')]
        fields: Vec<String>,
    },
    /// Fetch a single aggregate.
    Get {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
    },
    /// List events for an aggregate.
    Events {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        take: Option<u64>,
        #[arg(long)]
        filter: Option<String>,
    },
    /// Append a new event with optional fake payload.
    Append {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
        #[arg(long = "event-type", short = 'e')]
        event_type: Option<String>,
        #[arg(long)]
        payload: Option<String>,
        #[arg(long)]
        metadata: Option<String>,
        #[arg(long)]
        note: Option<String>,
    },
    /// Apply a JSON Patch to an event.
    Patch {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
        #[arg(long = "event-type", short = 'e')]
        event_type: Option<String>,
        #[arg(long)]
        patch: Option<String>,
        #[arg(long)]
        metadata: Option<String>,
        #[arg(long)]
        note: Option<String>,
    },
    /// Create a new aggregate by appending its first event.
    Create {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
        #[arg(long = "event-type", short = 'e')]
        event_type: Option<String>,
        #[arg(long)]
        payload: Option<String>,
        #[arg(long)]
        metadata: Option<String>,
        #[arg(long)]
        note: Option<String>,
    },
    /// Toggle the archive flag on an aggregate.
    Archive {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
        #[arg(long, default_value_t = true)]
        archived: bool,
        #[arg(long, alias = "comment")]
        note: Option<String>,
    },
    /// Verify an aggregate's Merkle root.
    Verify {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
    },
    /// Create a snapshot for an aggregate.
    CreateSnapshot {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
        #[arg(long)]
        comment: Option<String>,
    },
    /// List snapshots, optionally filtered by aggregate or version.
    ListSnapshots {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
        #[arg(long)]
        version: Option<u64>,
    },
    /// Fetch a snapshot by id.
    GetSnapshot {
        #[arg(long = "snapshot-id")]
        snapshot_id: u64,
    },
    /// List the registered schemas.
    ListSchemas,
    /// Replace schemas with a JSON document.
    ReplaceSchemas {
        #[arg(long)]
        schemas: Option<String>,
    },
    /// Assign a tenant to a shard.
    TenantAssign {
        #[arg(long)]
        tenant_id: Option<String>,
        #[arg(long)]
        shard_id: Option<String>,
    },
    /// Unassign a tenant.
    TenantUnassign {
        #[arg(long)]
        tenant_id: Option<String>,
    },
    /// Set a tenant storage quota (MB).
    TenantQuotaSet {
        #[arg(long)]
        tenant_id: Option<String>,
        #[arg(long)]
        max_storage_mb: Option<u64>,
    },
    /// Clear a tenant storage quota.
    TenantQuotaClear {
        #[arg(long)]
        tenant_id: Option<String>,
    },
    /// Recalculate a tenant's storage usage.
    TenantQuotaRecalc {
        #[arg(long)]
        tenant_id: Option<String>,
    },
    /// Reload schemas for a tenant.
    TenantReload {
        #[arg(long)]
        tenant_id: Option<String>,
    },
    /// Publish schemas for a tenant.
    TenantSchemaPublish {
        #[arg(long)]
        tenant_id: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        actor: Option<String>,
        #[arg(long = "label", value_name = "LABEL", num_args(0..), value_delimiter = ',')]
        labels: Vec<String>,
        #[arg(long)]
        activate: bool,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        reload: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let (host, port, token, tenant) = resolve_connection(&cli)?;

    let config = ClientConfig::new(host, token)
        .with_tenant(tenant)
        .with_port(port);
    let client = EventDbxClient::connect(config).await?;

    match cli.command {
        Commands::List {
            cursor,
            take,
            include_archived,
            archived_only,
            filter,
        } => {
            let mut options = ListAggregatesOptions::default();
            options.cursor = cursor;
            options.take = take;
            options.include_archived = include_archived;
            options.archived_only = archived_only;
            options.filter = filter;
            let result = client.list_aggregates(options).await?;
            println!("{}", serde_json::to_string_pretty(&result.aggregates)?);
            if let Some(cursor) = result.next_cursor {
                println!("Next cursor: {cursor}");
            }
        }
        Commands::Select {
            aggregate_type,
            aggregate_id,
            mut fields,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            if fields.is_empty() {
                fields = random_fields(2);
            }
            let request = SelectAggregateRequest {
                aggregate_type,
                aggregate_id,
                fields,
                token: None,
            };
            let result = client.select_aggregate(request).await?;
            if let Some(selection) = result.selection {
                println!("{}", serde_json::to_string_pretty(&selection)?);
            } else {
                println!("Aggregate not found.");
            }
        }
        Commands::Get {
            aggregate_type,
            aggregate_id,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let result = client.get_aggregate(&aggregate_type, &aggregate_id).await?;
            if let Some(value) = result.aggregate {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                println!("Aggregate {aggregate_type}/{aggregate_id} not found.");
            }
        }
        Commands::Events {
            aggregate_type,
            aggregate_id,
            cursor,
            take,
            filter,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let mut options = ListEventsOptions::default();
            options.cursor = cursor;
            options.take = take;
            options.filter = filter;
            let result = client
                .list_events(&aggregate_type, &aggregate_id, options)
                .await?;
            println!("{}", serde_json::to_string_pretty(&result.events)?);
            if let Some(cursor) = result.next_cursor {
                println!("Next cursor: {cursor}");
            }
        }
        Commands::Append {
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            metadata,
            note,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let event_type = event_type.unwrap_or_else(random_event_type);
            let payload_value = parse_json_arg(payload.as_ref(), random_payload)?;
            let metadata_value = parse_optional_json(metadata.as_ref())?;

            let mut request = AppendEventRequest::new(
                aggregate_type.clone(),
                aggregate_id.clone(),
                event_type.clone(),
                payload_value,
            );
            request.metadata = metadata_value;
            request.note = note;

            let result = client.append_event(request).await?;
            println!("{}", serde_json::to_string_pretty(&result.event)?);
        }
        Commands::Patch {
            aggregate_type,
            aggregate_id,
            event_type,
            patch,
            metadata,
            note,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let event_type = event_type.unwrap_or_else(random_event_type);
            let patch_value = parse_json_arg(patch.as_ref(), random_patch)?;
            let metadata_value = parse_optional_json(metadata.as_ref())?;

            let mut request = PatchEventRequest::new(
                aggregate_type.clone(),
                aggregate_id.clone(),
                event_type.clone(),
                patch_value,
            );
            request.metadata = metadata_value;
            request.note = note;

            let result = client.patch_event(request).await?;
            println!("{}", serde_json::to_string_pretty(&result.event)?);
        }
        Commands::Create {
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            metadata,
            note,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let event_type = event_type.unwrap_or_else(random_event_type);
            let payload_value = parse_json_arg(payload.as_ref(), random_payload)?;
            let metadata_value = parse_optional_json(metadata.as_ref())?;

            let mut request = CreateAggregateRequest::new(
                aggregate_type.clone(),
                aggregate_id.clone(),
                event_type.clone(),
                payload_value,
            );
            request.metadata = metadata_value;
            request.note = note;

            let result = client.create_aggregate(request).await?;
            println!("{}", serde_json::to_string_pretty(&result.aggregate)?);
        }
        Commands::Archive {
            aggregate_type,
            aggregate_id,
            archived,
            note,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let mut request = SetAggregateArchiveRequest::new(
                aggregate_type.clone(),
                aggregate_id.clone(),
                archived,
            );
            request.note = note;

            let result = client.set_aggregate_archive(request).await?;
            println!("{}", serde_json::to_string_pretty(&result.aggregate)?);
        }
        Commands::Verify {
            aggregate_type,
            aggregate_id,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let result = client
                .verify_aggregate(&aggregate_type, &aggregate_id)
                .await?;
            println!(
                "Merkle root for {aggregate_type}/{aggregate_id}: {}",
                result.merkle_root
            );
        }
        Commands::CreateSnapshot {
            aggregate_type,
            aggregate_id,
            comment,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let mut request = CreateSnapshotRequest::new(
                aggregate_type.clone(),
                aggregate_id.clone(),
            );
            request.comment = comment;
            let result = client.create_snapshot(request).await?;
            println!("{}", serde_json::to_string_pretty(&result.snapshot)?);
        }
        Commands::ListSnapshots {
            aggregate_type,
            aggregate_id,
            version,
        } => {
            let mut options = ListSnapshotsOptions::default();
            options.aggregate_type = aggregate_type;
            options.aggregate_id = aggregate_id;
            options.version = version;
            let result = client.list_snapshots(options).await?;
            println!("{}", serde_json::to_string_pretty(&result.snapshots)?);
        }
        Commands::GetSnapshot { snapshot_id } => {
            let request = GetSnapshotRequest {
                snapshot_id,
                token: None,
            };
            let result = client.get_snapshot(request).await?;
            if let Some(snapshot) = result.snapshot {
                println!("{}", serde_json::to_string_pretty(&snapshot)?);
            } else {
                println!("Snapshot {snapshot_id} not found.");
            }
        }
        Commands::ListSchemas => {
            let result = client.list_schemas(ListSchemasOptions::default()).await?;
            println!("{}", serde_json::to_string_pretty(&result.schemas)?);
        }
        Commands::ReplaceSchemas { schemas } => {
            let schemas_value = parse_json_arg(schemas.as_ref(), || json!({}))?;
            let request = ReplaceSchemasRequest::new(schemas_value);
            let result = client.replace_schemas(request).await?;
            println!("Replaced {} schemas.", result.replaced);
        }
        Commands::TenantAssign { tenant_id, shard_id } => {
            let tenant_id = tenant_id.unwrap_or_else(random_tenant_id);
            let shard_id = shard_id.unwrap_or_else(random_shard_id);
            let request = TenantAssignRequest::new(tenant_id.clone(), shard_id.clone());
            let result = client.tenant_assign(request).await?;
            println!(
                "Tenant {tenant_id} assigned to shard {} (changed: {}).",
                result.shard_id, result.changed
            );
        }
        Commands::TenantUnassign { tenant_id } => {
            let tenant_id = tenant_id.unwrap_or_else(random_tenant_id);
            let request = TenantUnassignRequest::new(tenant_id.clone());
            let result = client.tenant_unassign(request).await?;
            println!(
                "Tenant {tenant_id} unassigned (changed: {}).",
                result.changed
            );
        }
        Commands::TenantQuotaSet {
            tenant_id,
            max_storage_mb,
        } => {
            let tenant_id = tenant_id.unwrap_or_else(random_tenant_id);
            let max_storage_mb = max_storage_mb.unwrap_or(1024);
            let request = TenantQuotaSetRequest::new(tenant_id.clone(), max_storage_mb);
            let result = client.tenant_quota_set(request).await?;
            if let Some(quota) = result.quota_mb {
                println!(
                    "Quota for {tenant_id}: {quota} MB (changed: {}).",
                    result.changed
                );
            } else {
                println!(
                    "Quota for {tenant_id} cleared (changed: {}).",
                    result.changed
                );
            }
        }
        Commands::TenantQuotaClear { tenant_id } => {
            let tenant_id = tenant_id.unwrap_or_else(random_tenant_id);
            let request = TenantQuotaClearRequest::new(tenant_id.clone());
            let result = client.tenant_quota_clear(request).await?;
            println!("Quota cleared for {tenant_id} (changed: {}).", result.changed);
        }
        Commands::TenantQuotaRecalc { tenant_id } => {
            let tenant_id = tenant_id.unwrap_or_else(random_tenant_id);
            let request = TenantQuotaRecalcRequest::new(tenant_id.clone());
            let result = client.tenant_quota_recalc(request).await?;
            println!(
                "Storage for {tenant_id}: {} bytes.",
                result.storage_bytes
            );
        }
        Commands::TenantReload { tenant_id } => {
            let tenant_id = tenant_id.unwrap_or_else(random_tenant_id);
            let request = TenantReloadRequest::new(tenant_id.clone());
            let result = client.tenant_reload(request).await?;
            println!("Tenant {tenant_id} reloaded: {}.", result.reloaded);
        }
        Commands::TenantSchemaPublish {
            tenant_id,
            reason,
            actor,
            labels,
            activate,
            force,
            reload,
        } => {
            let tenant_id = tenant_id.unwrap_or_else(random_tenant_id);
            let mut request = TenantSchemaPublishRequest::new(tenant_id.clone());
            request.reason = reason;
            request.actor = actor;
            request.labels = labels;
            request.activate = activate;
            request.force = force;
            request.reload = reload;
            let result = client.tenant_schema_publish(request).await?;
            println!(
                "Published version {} for {tenant_id} (activated: {}, skipped: {}).",
                result.version_id, result.activated, result.skipped
            );
        }
    }

    Ok(())
}

fn resolve_connection(cli: &Cli) -> Result<(String, u16, String, String), String> {
    let host = cli
        .host
        .clone()
        .or_else(|| env::var("EVENTDBX_HOST").ok())
        .ok_or_else(|| "EVENTDBX_HOST missing (set env or use --host)".to_string())?;

    let token = cli
        .token
        .clone()
        .or_else(|| env::var("EVENTDBX_TOKEN").ok())
        .ok_or_else(|| "EVENTDBX_TOKEN missing (set env or use --token)".to_string())?;

    let port = cli
        .port
        .or_else(|| {
            env::var("EVENTDBX_PORT")
                .ok()
                .and_then(|v| v.parse::<u16>().ok())
        })
        .unwrap_or(6363);

    let tenant = cli
        .tenant
        .clone()
        .or_else(|| env::var("EVENTDBX_TENANT").ok())
        .unwrap_or_else(|| "default".to_string());

    Ok((host, port, token, tenant))
}

fn parse_json_arg(
    arg: Option<&String>,
    default_fn: impl FnOnce() -> Value,
) -> Result<Value, serde_json::Error> {
    match arg {
        Some(raw) => serde_json::from_str(raw),
        None => Ok(default_fn()),
    }
}

fn parse_optional_json(arg: Option<&String>) -> Result<Option<Value>, serde_json::Error> {
    match arg {
        Some(raw) => Ok(Some(serde_json::from_str(raw)?)),
        None => Ok(None),
    }
}

fn random_aggregate_type() -> String {
    format!("{}Type", capitalize(&Word().fake::<String>()))
}

fn random_aggregate_id() -> String {
    format!(
        "{}-{}",
        Word().fake::<String>(),
        NumberWithFormat("#####").fake::<String>()
    )
}

fn random_event_type() -> String {
    format!("{}Event", capitalize(&Word().fake::<String>()))
}

fn random_tenant_id() -> String {
    format!("tenant-{}", NumberWithFormat("#####").fake::<String>())
}

fn random_shard_id() -> String {
    format!("shard-{}", NumberWithFormat("##").fake::<String>())
}

fn random_fields(count: usize) -> Vec<String> {
    (0..count).map(|_| Word().fake::<String>()).collect()
}

fn random_payload() -> Value {
    json!({
        "status": Word().fake::<String>(),
        "note": Sentence(3..6).fake::<String>(),
    })
}

fn random_patch() -> Value {
    json!([{
        "op": "replace",
        "path": format!("/{}", Word().fake::<String>()),
        "value": Word().fake::<String>()
    }])
}

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}
