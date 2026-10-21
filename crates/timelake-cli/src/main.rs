//! `timelake`: create, load, query, and maintain Timelake tables.

mod bench;
mod info;
mod load;
mod settings;

use std::io::{Read, Write};
use std::time::Instant;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use timelake::datafusion::prelude::DataFrame;
use timelake::{Lake, MaintainOptions, RollupRefresh, Session, TableName, UpgradeOptions};

use crate::info::TableInfo;
use crate::load::{LoadArgs, TableDef};

#[derive(Parser)]
#[expect(clippy::doc_markdown, reason = "doc comments are clap help text, shown verbatim")]
#[command(name = "timelake", version, about = "Timelake: the open time-series lakehouse")]
struct Cli {
    /// Warehouse location: s3://bucket/prefix, gs://…, az://…, file:///… or a local path.
    #[arg(short, long, env = "TIMELAKE_WAREHOUSE", global = true, default_value = "./warehouse")]
    warehouse: String,
    /// Storage option, e.g. -o aws_endpoint=http://localhost:9000 -o aws_allow_http=true.
    #[arg(short = 'o', long = "option", value_parser = parse_kv, global = true)]
    options: Vec<(String, String)>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create a table. The schema comes from a Parquet/CSV file (--from) or --schema.
    Create {
        table: String,
        #[command(flatten)]
        def: TableDef,
    },
    /// Append Parquet or CSV files (one commit per --rows-per-commit rows, or per file).
    Append {
        table: String,
        files: Vec<String>,
        #[command(flatten)]
        load: LoadArgs,
    },
    /// Create the table if needed (schema inferred from the first file), then append the files.
    Import {
        table: String,
        files: Vec<String>,
        #[command(flatten)]
        def: TableDef,
        #[command(flatten)]
        load: LoadArgs,
    },
    /// Run SQL (argument or stdin). Tables are `namespace.table`.
    Sql(SqlArgs),
    /// Newest row of every series.
    Latest {
        table: String,
        #[arg(long, value_enum, default_value_t = Format::Table)]
        format: Format,
    },
    /// Compact, apply retention, refresh rollups, expire snapshots. All tables if none given.
    Maintain {
        tables: Vec<String>,
        /// Don't compact small or unsorted files.
        #[arg(long)]
        no_compact: bool,
        /// Don't drop partitions older than the table's retention.
        #[arg(long)]
        no_retention: bool,
        /// Don't expire old snapshots (nor delete the files only they use).
        #[arg(long)]
        no_expire: bool,
        /// Don't refresh the tables' rollups.
        #[arg(long)]
        no_rollups: bool,
    },
    /// Bring tables to the current format: format version, last-value index, sorted and compacted
    /// files, head object. Each step is a commit; no snapshot expires for --keep-snapshots, so
    /// `rollback` undoes it. All tables if none given.
    Upgrade {
        tables: Vec<String>,
        /// Only show what would change.
        #[arg(long)]
        dry_run: bool,
        /// Don't rewrite unsorted or small files.
        #[arg(long)]
        no_compact: bool,
        /// How long every snapshot is kept, so the upgrade can be rolled back.
        #[arg(long, default_value = "7d", value_parser = parse_duration)]
        keep_snapshots: std::time::Duration,
    },
    /// Change a table's settings: --set read-dedup=false, --unset retention (back to the default).
    Alter {
        table: String,
        /// Setting or Iceberg property to set, name=value.
        #[arg(long = "set", value_parser = parse_kv)]
        set: Vec<(String, String)>,
        /// Setting or Iceberg property to remove.
        #[arg(long)]
        unset: Vec<String>,
    },
    /// List the settings with their defaults and where they can be set, or a table's values.
    Settings {
        table: Option<String>,
        /// Machine-readable output.
        #[arg(long)]
        json: bool,
    },
    /// Make an earlier snapshot current again (snapshot ids are in `info`).
    Rollback { table: String, snapshot: i64 },
    /// Table details: contract, snapshots, files, partitions.
    Info {
        table: String,
        /// Machine-readable output.
        #[arg(long)]
        json: bool,
    },
    /// List tables.
    Ls {
        /// Machine-readable output.
        #[arg(long)]
        json: bool,
    },
    /// Print the DuckDB SQL that reads a table without Timelake.
    Duckdb { table: String },
    /// Delete a table and all its files.
    Drop { table: String },
    /// Rewrite timelake-tables.json, the table index for readers that cannot list (HTTPS, browsers).
    Index,
}

#[derive(clap::Args)]
#[expect(clippy::doc_markdown, reason = "doc comments are clap help text, shown verbatim")]
struct SqlArgs {
    query: Option<String>,
    #[arg(long, value_enum, default_value_t = Format::Table)]
    format: Format,
    /// Default namespace for unqualified table names.
    #[arg(long, default_value = "default")]
    namespace: String,
    /// Session setting, e.g. --set timelake.read_dedup=false or
    /// --set datafusion.execution.target_partitions=32.
    #[arg(long = "set", value_parser = parse_kv)]
    settings: Vec<(String, String)>,
    /// Benchmark mode: run 1 + N times, print {"times": [...], "rows": n} as JSON.
    #[arg(long, hide = true)]
    bench_runs: Option<usize>,
    /// Benchmark mode: s3proxy admin address for request/byte counts.
    #[arg(long, hide = true)]
    proxy_admin: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Table,
    Csv,
    Json,
    None,
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected key=value, got {s:?}"))
}

#[tokio::main]
async fn main() {
    let result = run(Cli::parse()).await;
    // Version hints are updated in the background: let them land before exiting.
    timelake::flush().await;
    if let Err(e) = result {
        eprintln!("Error: {}", report(&e));
        std::process::exit(1);
    }
}

/// An error and its causes, joined by ": ", leaving out a cause the message before it ends with: DataFusion's errors
/// repeat their source in their own message.
fn report(e: &anyhow::Error) -> String {
    let mut out = String::new();
    let mut previous = String::new();
    for cause in e.chain() {
        let text = cause.to_string();
        if !previous.ends_with(&text) {
            if !out.is_empty() {
                out.push_str(": ");
            }
            out.push_str(&text);
        }
        previous = text;
    }
    out
}

async fn run(cli: Cli) -> Result<()> {
    let started = Instant::now();
    let lake = Lake::open(&cli.warehouse, cli.options.into_iter().collect())?;
    match cli.cmd {
        Cmd::Create { table, def } => create(&lake, &table, &def).await,
        Cmd::Append { table, files, load } => load::load_files(lake.table(&table).await?, &files, &load).await,
        Cmd::Import { table, files, def, load } => import(&lake, &table, &files, &def, &load).await,
        Cmd::Sql(args) => sql(lake, args, started).await,
        Cmd::Latest { table, format } => print_df(Session::new(lake).latest(&table).await?, format).await,
        Cmd::Maintain { tables, no_compact, no_retention, no_expire, no_rollups } => {
            let opts = MaintainOptions {
                compact: !no_compact,
                retention: !no_retention,
                expire: !no_expire,
                rollups: if no_rollups { RollupRefresh::Never } else { RollupRefresh::Always },
                ..Default::default()
            };
            maintain(&lake, tables, opts).await
        }
        Cmd::Upgrade { tables, dry_run, no_compact, keep_snapshots } => {
            let opts = UpgradeOptions { dry_run, compact: !no_compact, keep_snapshots };
            upgrade(&lake, tables, &opts).await
        }
        Cmd::Alter { table, set, unset } => {
            let changes: Vec<(&str, Option<&str>)> = set
                .iter()
                .map(|(k, v)| (k.as_str(), Some(v.as_str())))
                .chain(unset.iter().map(|k| (k.as_str(), None)))
                .collect();
            anyhow::ensure!(!changes.is_empty(), "nothing to change: pass --set name=value or --unset name");
            let t = lake.table(&table).await?.set_properties(&changes).await?;
            println!("{table}: settings changed → v{}", t.version());
            Ok(())
        }
        Cmd::Settings { table, json } => {
            let table = match table {
                Some(name) => Some(lake.table(&name).await?),
                None => None,
            };
            settings::print(table.as_ref(), json);
            Ok(())
        }
        Cmd::Rollback { table, snapshot } => {
            let t = lake.table(&table).await?.rollback(snapshot).await?;
            println!("{table}: snapshot {snapshot} is current again → v{}", t.version());
            Ok(())
        }
        Cmd::Info { table, json } => {
            let info = TableInfo::load(&lake.table(&table).await?).await?;
            if json {
                println!("{}", info.to_json());
            } else {
                print!("{info}");
            }
            Ok(())
        }
        Cmd::Ls { json } => ls(&lake, json).await,
        Cmd::Duckdb { table } => duckdb(&lake, &table).await,
        Cmd::Index => {
            let names = lake.write_tables_index().await?;
            println!("indexed {} tables: {}", names.len(), names.join(", "));
            Ok(())
        }
        Cmd::Drop { table } => {
            lake.drop_table(&table).await?;
            println!("dropped {table}");
            Ok(())
        }
    }
}

async fn create(lake: &Lake, table: &str, def: &TableDef) -> Result<()> {
    let schema = def.schema(None).await?;
    let t = lake.create_table(table, &schema, def.options()).await?;
    println!("created {} at {}", t.name(), t.location());
    Ok(())
}

async fn import(lake: &Lake, table: &str, files: &[String], def: &TableDef, load: &LoadArgs) -> Result<()> {
    let t = match lake.table(table).await {
        Ok(t) => t,
        Err(timelake::Error::NotFound(_)) => {
            let schema = def.schema(files.first().map(String::as_str)).await?;
            let t = lake.create_table(table, &schema, def.options()).await?;
            eprintln!("created {}", t.name());
            t
        }
        Err(e) => return Err(e.into()),
    };
    load::load_files(t, files, load).await
}

async fn sql(lake: Lake, args: SqlArgs, started: Instant) -> Result<()> {
    let sql = if let Some(q) = args.query {
        q
    } else {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        s
    };
    let mut config = Session::config().with_default_catalog_and_schema("timelake", &args.namespace);
    for (k, v) in &args.settings {
        config.options_mut().set(&timelake::session_key(k), v)?;
    }
    let session = Session::with_config(lake, config);
    if let Some(runs) = args.bench_runs {
        return bench::run(&session, &sql, runs, started.elapsed().as_secs_f64(), args.proxy_admin.as_deref()).await;
    }
    for statement in session.parse(&sql)? {
        print_df(session.execute(statement).await?, args.format).await?;
    }
    Ok(())
}

/// A duration argument: `90d`, `12h`, `30m`.
fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    timelake::parse_duration(s).map_err(|e| e.to_string())
}

/// Each table with the name to print: as the user wrote it, or qualified when listing them all.
async fn targets(lake: &Lake, tables: Vec<String>) -> Result<Vec<(TableName, String)>> {
    if tables.is_empty() {
        return Ok(lake.all_tables().await?.into_iter().map(|name| (name.clone(), name.to_string())).collect());
    }
    Ok(tables.into_iter().map(|t| Ok((t.parse()?, t))).collect::<timelake::Result<_>>()?)
}

async fn upgrade(lake: &Lake, tables: Vec<String>, opts: &UpgradeOptions) -> Result<()> {
    for (name, label) in targets(lake, tables).await? {
        let (t, r) = lake.table(name).await?.upgrade(opts).await?;
        if r.steps.is_empty() {
            println!("{label}: already current (v{})", t.version());
            continue;
        }
        let verb = if opts.dry_run { "would" } else { "did" };
        println!("{label}: {verb}:");
        for step in &r.steps {
            println!("  - {step}");
        }
        if !opts.dry_run {
            let undo = r.snapshot_before.map_or_else(String::new, |s| format!("; undo: timelake rollback {label} {s}"));
            println!("  v{} → v{}{undo}", r.version_before, t.version());
        }
    }
    Ok(())
}

async fn maintain(lake: &Lake, tables: Vec<String>, opts: MaintainOptions) -> Result<()> {
    for (name, label) in targets(lake, tables).await? {
        let (t, r) = lake.table(name).await?.maintain(opts.clone()).await?;
        println!(
            "{label}: compacted {} partitions ({} → {} files, {} rows, {} duplicates removed), dropped {} partitions, refreshed {} rollups ({} rows), expired {} snapshots, deleted {} objects in {:.2}s → v{}",
            r.partitions_compacted,
            r.files_rewritten,
            r.files_written,
            r.rows_rewritten,
            r.duplicates_removed,
            r.partitions_dropped,
            r.rollups_refreshed,
            r.rollup_rows,
            r.snapshots_expired,
            r.objects_deleted,
            r.duration.as_secs_f64(),
            t.version()
        );
    }
    Ok(())
}

async fn ls(lake: &Lake, json: bool) -> Result<()> {
    let names: Vec<String> = lake.all_tables().await?.iter().map(ToString::to_string).collect();
    if json {
        println!("{}", serde_json::to_string(&names)?);
    } else {
        for name in names {
            println!("{name}");
        }
    }
    Ok(())
}

async fn duckdb(lake: &Lake, table: &str) -> Result<()> {
    let t = lake.table(table).await?;
    println!("INSTALL iceberg; LOAD iceberg;");
    if t.location().starts_with("s3://") {
        println!(
            "-- CREATE SECRET (TYPE s3, KEY_ID '…', SECRET '…', REGION '…' [, ENDPOINT '…', URL_STYLE 'path', USE_SSL false]);"
        );
    }
    println!("SELECT * FROM iceberg_scan('{}');", t.location());
    if let Some(lvi) = t.last_value_index() {
        println!("-- newest row per series (Timelake last-value index for v{}):", t.version());
        println!("SELECT * FROM read_parquet('{lvi}');");
    }
    Ok(())
}

/// Prints a result. A result without rows keeps its columns (a header, an empty table); a statement without columns
/// (`SET`) prints nothing.
async fn print_df(df: DataFrame, format: Format) -> Result<()> {
    let schema = std::sync::Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;
    let mut out = std::io::stdout().lock();
    if schema.fields().is_empty() && !matches!(format, Format::None) {
        return Ok(());
    }
    match format {
        Format::Table => {
            let table = timelake::arrow::util::pretty::pretty_format_batches_with_schema(schema, &batches)?;
            writeln!(out, "{table}")?;
        }
        Format::Csv => {
            let mut w = timelake::arrow::csv::Writer::new(out);
            if batches.is_empty() {
                w.write(&timelake::arrow::array::RecordBatch::new_empty(schema))?;
            }
            for b in &batches {
                w.write(b)?;
            }
        }
        Format::Json => {
            let mut w = timelake::arrow::json::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, timelake::arrow::json::writer::LineDelimited>(out);
            for b in &batches {
                w.write(b)?;
            }
            w.finish()?;
        }
        Format::None => {
            writeln!(out, "{} rows", batches.iter().map(timelake::arrow::array::RecordBatch::num_rows).sum::<usize>())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cause the message before it ends with is left out; one that only appears inside it is kept.
    #[test]
    fn report_leaves_out_repeated_causes() {
        let repeated = anyhow::anyhow!("No field named nope").context("Schema error: No field named nope");
        assert_eq!(report(&repeated), "Schema error: No field named nope");
        let inside = anyhow::anyhow!("iot.r").context("loading iot.r failed");
        assert_eq!(report(&inside), "loading iot.r failed: iot.r");
    }
}
