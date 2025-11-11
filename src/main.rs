use std::env;

use clap::{Parser, Subcommand};
use eventdbx_client::{
    AppendEventRequest, ClientConfig, CreateAggregateRequest, EventDbxClient,
    ListAggregatesOptions, ListEventsOptions, PatchEventRequest, SelectAggregateRequest,
    SetAggregateArchiveRequest,
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
        #[arg(long)]
        comment: Option<String>,
    },
    /// Verify an aggregate's Merkle root.
    Verify {
        #[arg(long = "aggregate-type", short = 'a')]
        aggregate_type: Option<String>,
        #[arg(long = "aggregate-id", short = 'i')]
        aggregate_id: Option<String>,
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
            comment,
        } => {
            let aggregate_type = aggregate_type.unwrap_or_else(random_aggregate_type);
            let aggregate_id = aggregate_id.unwrap_or_else(random_aggregate_id);
            let mut request = SetAggregateArchiveRequest::new(
                aggregate_type.clone(),
                aggregate_id.clone(),
                archived,
            );
            request.comment = comment;

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
