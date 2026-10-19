//! Benchmark reader for spec 32: DataFusion over Parquet or Vortex, on local disk or S3 (RustFS via
//! tools/s3proxy). Invoked by run.py (engines `datafusion-rs*`): reads one query from stdin, runs it
//! 1 + `--runs` times in this process, and prints one JSON line in run.py's worker format.
//!
//!   bench-reader --suite tsbs_iot --format vortex --storage s3 --layout sorted --runs 3 < query.sql

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use datafusion::common::GetExt;
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::aws::AmazonS3Builder;
use vortex_datafusion::VortexFormatFactory;

const TPCH: [&str; 8] = ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"];
const TSBS: [&str; 3] = ["tags", "readings", "diagnostics"];
const S3_ENDPOINT: &str = "http://localhost:59100";
const PROXY_ADMIN: &str = "127.0.0.1:59101";

struct Args {
    suite: String,
    format: String,
    storage: String,
    layout: String,
    runs: usize,
    pushdown: bool,
    data: PathBuf,
}

fn parse_args() -> Result<Args> {
    let mut kv: HashMap<String, String> = HashMap::new();
    let mut flags = vec![];
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.strip_prefix("--") {
            Some("pushdown") => flags.push("pushdown"),
            Some(k) => {
                kv.insert(k.to_string(), it.next().context("missing value")?);
            }
            None => bail!("unexpected argument {a}"),
        }
    }
    let get = |k: &str, d: &str| kv.get(k).cloned().unwrap_or_else(|| d.to_string());
    Ok(Args {
        suite: get("suite", "tpch"),
        format: get("format", "parquet"),
        storage: get("storage", "local"),
        layout: get("layout", "as-is"),
        runs: get("runs", "3").parse()?,
        pushdown: flags.contains(&"pushdown"),
        data: PathBuf::from(get("data", concat!(env!("CARGO_MANIFEST_DIR"), "/../data"))).canonicalize()?,
    })
}

/// Table name -> location, following the layout of bench/data (and s3://bench).
fn tables(a: &Args) -> Vec<(String, String)> {
    let base = if a.storage == "s3" { "s3://bench".to_string() } else { a.data.display().to_string() };
    let vortex = a.format == "vortex";
    let tsbs_dir = match (vortex, a.layout.as_str()) {
        (false, "sorted") => "parquet-sorted",
        (true, "sorted") => "vortex-sorted",
        (false, _) => "parquet",
        (true, _) => "vortex",
    };
    match a.suite.as_str() {
        "tpch" => TPCH
            .iter()
            .map(|t| {
                let loc =
                    if vortex { format!("{base}/tpch/vortex/{t}.vortex") } else { format!("{base}/tpch/{t}.parquet") };
                (t.to_string(), loc)
            })
            .collect(),
        "tsbs_iot" | "iot_point" => {
            TSBS.iter().map(|t| (t.to_string(), format!("{base}/tsbs/{tsbs_dir}/{t}.{}", a.format))).collect()
        }
        "clickbench" => {
            let dir = if vortex { format!("{base}/clickbench/vortex/") } else { format!("{base}/clickbench/") };
            vec![("hits_raw".to_string(), dir)]
        }
        s => panic!("unknown suite {s}"),
    }
}

async fn setup(a: &Args) -> Result<SessionContext> {
    let mut config = SessionConfig::new();
    if a.pushdown {
        let opts = config.options_mut();
        opts.execution.parquet.pushdown_filters = true;
        opts.execution.parquet.reorder_filters = true;
    }
    let factory = Arc::new(VortexFormatFactory::new());
    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_table_factory(factory.get_ext().to_uppercase(), Arc::new(DefaultTableFactory::new()));
    if let Some(formats) = builder.file_formats() {
        formats.push(factory.clone() as _);
    }
    let ctx = SessionContext::new_with_state(builder.build());

    if a.storage == "s3" {
        let store = AmazonS3Builder::new()
            .with_bucket_name("bench")
            .with_region("us-east-1")
            .with_access_key_id("benchkey")
            .with_secret_access_key("benchsecret123")
            .with_endpoint(S3_ENDPOINT)
            .with_allow_http(true)
            .build()?;
        ctx.register_object_store(&url::Url::parse("s3://bench")?, Arc::new(store));
    }

    let stored_as = if a.format == "vortex" { "VORTEX" } else { "PARQUET" };
    for (name, loc) in tables(a) {
        let options =
            if a.suite == "clickbench" && a.format == "parquet" { " OPTIONS ('binary_as_string' 'true')" } else { "" };
        ctx.sql(&format!("CREATE EXTERNAL TABLE {name} STORED AS {stored_as} LOCATION '{loc}'{options}"))
            .await?
            .collect()
            .await?;
    }
    if a.suite == "clickbench" {
        ctx.sql(
            r#"CREATE VIEW hits AS SELECT * EXCEPT ("EventDate"), CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw"#,
        )
        .await?
        .collect()
        .await?;
    }
    Ok(ctx)
}

/// (requests, bytes) counters from tools/s3proxy.
fn proxy_stats() -> Result<(i64, i64)> {
    let mut s = TcpStream::connect(PROXY_ADMIN)?;
    s.write_all(b"GET /stats HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
    let mut resp = String::new();
    s.read_to_string(&mut resp)?;
    let body = resp.split("\r\n\r\n").nth(1).context("no body")?;
    let v: serde_json::Value = serde_json::from_str(body.trim())?;
    Ok((v["requests"].as_i64().unwrap_or(0), v["bytes"].as_i64().unwrap_or(0)))
}

#[tokio::main]
async fn main() -> Result<()> {
    let a = parse_args()?;
    let mut sql = String::new();
    std::io::stdin().read_to_string(&mut sql)?;

    let t = Instant::now();
    let ctx = setup(&a).await?;
    let setup_s = t.elapsed().as_secs_f64();

    let (mut times, mut requests, mut bytes, mut rows) = (vec![], vec![], vec![], 0usize);
    for _ in 0..=a.runs {
        let before = if a.storage == "s3" { Some(proxy_stats()?) } else { None };
        let t = Instant::now();
        let batches = ctx.sql(&sql).await?.collect().await?;
        times.push(t.elapsed().as_secs_f64());
        rows = batches.iter().map(|b| b.num_rows()).sum();
        if let Some((r0, b0)) = before {
            let (r1, b1) = proxy_stats()?;
            requests.push(r1 - r0);
            bytes.push(b1 - b0);
        }
    }
    let mut out = serde_json::json!({ "times": times, "rows": rows, "setup_s": setup_s });
    if a.storage == "s3" {
        out["requests"] = requests.into();
        out["bytes"] = bytes.into();
    }
    println!("{out}");
    Ok(())
}
