//! Loading Parquet and CSV files: table definitions from flags or files, and appends in commits.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use timelake::arrow::array::RecordBatch;
use timelake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use timelake::datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use timelake::datafusion::prelude::{CsvReadOptions, DataFrame, ParquetReadOptions, SessionContext};
use timelake::{AppendOptions, AppendResult, Chunk, IcebergType, Table, TableOptions};

use crate::info::megabytes;
use crate::{parse_duration, parse_kv};

/// How to create a table.
#[derive(clap::Args)]
pub(crate) struct TableDef {
    /// Take the schema from this Parquet or CSV file.
    #[arg(long)]
    from: Option<String>,
    /// Schema as "name:type,…": Iceberg types (int, long, double, string, boolean, date, timestamptz, decimal(10,2), …),
    /// also as bigint, text, bool.
    #[arg(long)]
    schema: Option<String>,
    /// The time column (makes this a time-series table).
    #[arg(long)]
    time_column: Option<String>,
    /// Columns identifying a series, comma-separated.
    #[arg(long, value_delimiter = ',')]
    series_key: Vec<String>,
    /// Time partition size: hour, day or month (default day).
    #[arg(long)]
    chunk: Option<Chunk>,
    /// Drop data older than this, e.g. 90d.
    #[arg(long, value_parser = parse_duration)]
    retention: Option<Duration>,
    /// Disable dedup of identical (series, time) rows during compaction.
    #[arg(long)]
    no_dedup: bool,
    /// Leave out the insertion-time column of a time-series table.
    #[arg(long)]
    no_ingest_time: bool,
    /// Extra table property k=v.
    #[arg(long = "prop", value_parser = parse_kv)]
    props: Vec<(String, String)>,
}

impl TableDef {
    pub(crate) fn options(&self) -> TableOptions {
        TableOptions {
            time_column: self.time_column.clone(),
            series_key: self.series_key.clone(),
            chunk: self.chunk,
            dedup: self.no_dedup.then_some(false),
            ingest_time: self.no_ingest_time.then_some(false),
            retention: self.retention,
            properties: self.props.iter().cloned().collect(),
        }
    }

    /// The schema from `--schema`, else from `--from` or `fallback_file`.
    pub(crate) async fn schema(&self, fallback_file: Option<&str>) -> Result<SchemaRef> {
        if let Some(s) = &self.schema {
            return parse_schema(s);
        }
        let file = self.from.as_deref().or(fallback_file).context("pass --from <file> or --schema")?;
        let df = read_file(&SessionContext::new(), file).await?;
        Ok(df.schema().inner().clone())
    }
}

/// How to append.
#[derive(clap::Args)]
pub(crate) struct LoadArgs {
    /// Rows per commit (0 = one commit per input file).
    #[arg(long, default_value_t = 10_000_000)]
    rows_per_commit: usize,
    /// Skip inline auto-maintenance.
    #[arg(long)]
    no_maintain: bool,
    /// Idempotency key (suffixed with the commit number).
    #[arg(long)]
    batch_id: Option<String>,
}

fn parse_schema(s: &str) -> Result<SchemaRef> {
    let fields = columns(s)
        .into_iter()
        .map(|f| {
            let (name, ty) = f.trim().split_once(':').with_context(|| format!("expected name:type, got {f:?}"))?;
            Ok(Field::new(name.trim(), column_type(ty)?, true))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// `s` split at the commas outside parentheses, so `decimal(10,2)` stays whole.
fn columns(s: &str) -> Vec<&str> {
    let (mut parts, mut start, mut depth) = (vec![], 0, 0_usize);
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// The Arrow type of a `--schema` type: an Iceberg type name (`long`, `timestamptz`, `decimal(10,2)`), or one of a few
/// SQL spellings of them, in any case.
fn column_type(name: &str) -> Result<DataType> {
    let name = name.trim().to_ascii_lowercase();
    let ty = match name.as_str() {
        "integer" | "int32" => IcebergType::Int,
        "bigint" | "int64" => IcebergType::Long,
        "real" | "float32" => IcebergType::Float,
        "float64" => IcebergType::Double,
        "text" | "varchar" => IcebergType::String,
        "bool" => IcebergType::Boolean,
        other => IcebergType::named(other),
    };
    ty.arrow_type().with_context(|| format!("unknown type {name:?}"))
}

fn has_extension(path: &Path, extension: &str) -> bool {
    path.extension().is_some_and(|e| e.eq_ignore_ascii_case(extension))
}

/// The last `parts` extensions of `path` as written, with the dot: `.CSV.gz`. DataFusion checks a
/// file's name against the extension it is given, in its case.
fn suffix(path: &Path, parts: usize) -> String {
    let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let pieces: Vec<&str> = name.rsplitn(parts + 1, '.').collect();
    let mut extensions: Vec<&str> = pieces.into_iter().take(parts).collect();
    extensions.reverse();
    format!(".{}", extensions.join("."))
}

/// Reads a `.parquet` file or directory, or a `.csv`, `.csv.gz` or `.tsv` file with a header, the
/// extension in any case.
async fn read_file(ctx: &SessionContext, path: &str) -> Result<DataFrame> {
    let p = Path::new(path);
    let gzip = has_extension(p, "gz");
    let csv = if gzip {
        p.file_stem().is_some_and(|stem| has_extension(Path::new(stem), "csv"))
    } else {
        has_extension(p, "csv")
    };
    let tsv = !gzip && has_extension(p, "tsv");
    if csv || tsv {
        let mut o = CsvReadOptions::new().has_header(true);
        if tsv {
            o = o.delimiter(b'\t');
        }
        if gzip {
            o = o.file_compression_type(FileCompressionType::GZIP);
        }
        let extension = suffix(p, if gzip { 2 } else { 1 });
        Ok(ctx.read_csv(path, o.file_extension(&extension)).await?)
    } else if p.is_dir() {
        Ok(ctx.read_parquet(path, ParquetReadOptions::default()).await?)
    } else if has_extension(p, "parquet") {
        let extension = suffix(p, 1);
        Ok(ctx.read_parquet(path, ParquetReadOptions::default().file_extension(&extension)).await?)
    } else {
        bail!("unsupported file type: {path} (use .parquet or .csv)")
    }
}

/// Appends files to a table, printing a line per commit and a summary.
pub(crate) async fn load_files(table: Table, files: &[String], args: &LoadArgs) -> Result<()> {
    if files.is_empty() {
        bail!("no input files");
    }
    let ctx = SessionContext::new();
    let mut loader = Loader::new(table, args);
    for file in files {
        let mut stream = read_file(&ctx, file).await?.execute_stream().await?;
        while let Some(batch) = stream.next().await {
            loader.push(batch?);
            if args.rows_per_commit > 0 && loader.pending_rows >= args.rows_per_commit {
                loader.flush().await?;
            }
        }
        if args.rows_per_commit == 0 {
            loader.flush().await?;
        }
    }
    loader.flush().await?;
    loader.summary();
    Ok(())
}

/// Appends record batches to a table, one commit per `--rows-per-commit` rows (or per file).
struct Loader<'a> {
    table: Table,
    args: &'a LoadArgs,
    started: Instant,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    /// Appends attempted so far; numbers the batch ids.
    appends: usize,
    commits: usize,
    duplicates: usize,
    rows: usize,
}

impl<'a> Loader<'a> {
    fn new(table: Table, args: &'a LoadArgs) -> Self {
        let started = Instant::now();
        Self { table, args, started, pending: vec![], pending_rows: 0, appends: 0, commits: 0, duplicates: 0, rows: 0 }
    }

    fn push(&mut self, batch: RecordBatch) {
        self.pending_rows += batch.num_rows();
        self.pending.push(batch);
    }

    /// Appends the pending batches as one commit, if there are any.
    async fn flush(&mut self) -> Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        let opts = AppendOptions {
            batch_id: self.args.batch_id.as_ref().map(|b| format!("{b}-{}", self.appends)),
            maintain: self.args.no_maintain.then_some(false),
            ..Default::default()
        };
        let batches = std::mem::take(&mut self.pending);
        let (table, r) = self.table.append(&batches, opts).await?;
        self.report(&r);
        self.table = table;
        self.appends += 1;
        self.pending_rows = 0;
        if r.commit.duplicate {
            self.duplicates += 1;
        } else {
            self.commits += 1;
            self.rows += r.rows;
        }
        Ok(())
    }

    fn report(&self, r: &AppendResult) {
        let maintenance = match &r.maintenance {
            Some(Ok(m)) => format!(", compacted {} partitions", m.partitions_compacted),
            Some(Err(e)) => format!(", maintenance failed (the append is committed): {e}"),
            None => String::new(),
        };
        let duplicate = if r.commit.duplicate { " (duplicate batch, skipped)" } else { "" };
        eprintln!(
            "  commit {}: {} rows, {} files, {:.1} MB, commit {:.0} ms{maintenance}{duplicate} [{:.1}s]",
            self.appends,
            r.rows,
            r.files,
            megabytes(r.bytes),
            r.commit.duration.as_secs_f64() * 1000.0,
            self.started.elapsed().as_secs_f64()
        );
    }

    fn summary(&self) {
        let secs = self.started.elapsed().as_secs_f64();
        let skipped = match self.duplicates {
            0 => String::new(),
            1 => ", skipped 1 duplicate batch".to_owned(),
            n => format!(", skipped {n} duplicate batches"),
        };
        println!(
            "{}: appended {} rows in {} commits{skipped}, {secs:.2}s ({:.0} rows/s) → v{}",
            self.table.name(),
            self.rows,
            self.commits,
            self.rows as f64 / secs,
            self.table.version()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_schema_strings() {
        let s = parse_schema("time:timestamptz, device:INT, value:double, price:decimal(10, 2), n:bigint").unwrap();
        let types: Vec<_> = s.fields().iter().map(|f| (f.name().as_str(), f.data_type().clone())).collect();
        let expected = [
            ("time", IcebergType::Timestamptz),
            ("device", IcebergType::Int),
            ("value", IcebergType::Double),
            ("price", IcebergType::Decimal { precision: 10, scale: 2 }),
            ("n", IcebergType::Long),
        ];
        assert_eq!(types, expected.map(|(name, ty)| (name, ty.arrow_type().unwrap())));
        assert!(parse_schema("time").is_err());
        assert!(parse_schema("x:decimal").is_err());
        assert!(parse_schema("x:list").is_err());
    }

    #[test]
    fn suffixes_keep_their_case() {
        assert_eq!(suffix(Path::new("/d/a.b.CSV"), 1), ".CSV");
        assert_eq!(suffix(Path::new("/d/a.b.CSV.Gz"), 2), ".CSV.Gz");
    }
}
