//! The head object (spec 7, P1): `<table>/metadata/timelake-head`, one object holding what a
//! reader needs to plan a query at the current version: the table metadata, the live data files
//! with their column statistics, and the last-value index and footers when they fit. A cold query
//! reads it instead of following the version hint, metadata, manifest list and manifests one by
//! one. Its layout and sections are `timelake_format::head`'s, shared with the WASM reader and
//! writer.
//!
//! The head is a cache of a commit, not part of it. `vN.metadata.json` stays the commit point, so
//! other Iceberg engines see no change. The head is written in the background after each commit
//! and never replaces a newer one; readers check that `vN+1.metadata.json` doesn't exist before
//! they rely on it, and load the table the slow way when it does.

mod codec;

use std::sync::Arc;

use bytes::Bytes;
use iceberg::spec::TableMetadata;

pub(crate) use self::codec::Codecs;
use crate::TableName;
use crate::background;
use crate::error::Result;
use crate::lake::Lake;
use crate::storage::{Storage, VersionedWrite};
use crate::table::{LiveEntry, Table};
use crate::{footer, lvi};
pub use timelake_format::FormatError;
use timelake_format::head::content::{Contents, HeadBuilder, footer_order};
pub use timelake_format::head::{ArrowCodec, HeadCodec, HeadDoc, JsonCodec};
use timelake_format::head::{Header, MAX_HEADER};
use timelake_format::layout::head_uri;
use timelake_format::rows::align_batch;

/// Attempts to replace a head that other writers keep changing; they write newer ones anyway.
const WRITE_ATTEMPTS: usize = 5;
/// Footers read from the store at once while writing a head.
const FOOTER_READS: usize = 16;

/// Writes `table`'s head in the background. Failures are ignored: readers fall back to the
/// metadata chain, and the next commit writes a new head.
pub(crate) fn update(table: &Table) {
    let table = table.clone();
    background::spawn(async move {
        let _ = write(&table).await;
    });
}

/// The table version the stored head describes, if there is a head this process can parse.
pub(crate) async fn stored_version(table: &Table) -> Result<Option<u64>> {
    let prefix = table.lake().storage().read_prefix_versioned(&head_uri(table.location()), MAX_HEADER).await?;
    Ok(prefix.and_then(|(bytes, _)| Header::parse(&bytes).map(|h| h.version)))
}

/// Writes `table`'s head now (see [`update`] for the background version).
pub(crate) async fn write(table: &Table) -> Result<()> {
    let lake = table.lake();
    if lake.has_newer(table.name(), table.version()) {
        return Ok(());
    }
    let storage = lake.storage();
    let uri = head_uri(table.location());
    let contract = table.contract();
    let deleted = &lake.caches().deleted_heads;
    let Some(codec) = lake.head_codecs().get(&contract.head_format) else {
        // Turned off, or a codec this process doesn't have: leave no head behind that could go
        // stale. Once is enough, not a request on every commit.
        if deleted.get(&uri).is_none() {
            storage.delete(&uri).await?;
            deleted.insert(uri, ());
        }
        return Ok(());
    };
    deleted.remove(&uri);
    let files = table.files().await?.iter().map(LiveEntry::to_shared).collect::<Result<Vec<_>>>()?;
    let json = serde_json::to_string(table.metadata())?;
    let mut head = HeadBuilder::new(table.version(), codec.as_ref(), json, &files, contract.head_inline_bytes)?;
    if let Some(index) = table.last_value_index() {
        head.last_values(&lvi::read(table, &index).await?)?;
    }
    // Footers this process doesn't have yet are read from the store (by this background task).
    'footers: for chunk in footer_order(&files).chunks(FOOTER_READS) {
        let reads = chunk.iter().map(|e| footer::read(lake, &e.file.path, e.file.file_size));
        for (e, bytes) in chunk.iter().zip(futures::future::try_join_all(reads).await?) {
            if !head.footer(&e.file.path, &bytes) {
                break 'footers;
            }
        }
    }
    write_if_newer(storage, &uri, table.version(), Bytes::from(head.finish()?)).await
}

/// Writes a head unless the stored one is for `version` or newer. The write is conditional on the
/// stored head being unchanged since it was read, so two writers can't move it backwards.
async fn write_if_newer(storage: &Storage, uri: &str, version: u64, bytes: Bytes) -> Result<()> {
    for _ in 0..WRITE_ATTEMPTS {
        let expected = match storage.read_prefix_versioned(uri, MAX_HEADER).await? {
            Some((prefix, _)) if Header::parse(&prefix).is_some_and(|h| h.version >= version) => return Ok(()),
            Some((_, stored)) => Some(stored),
            None => None,
        };
        if storage.write_versioned(uri, bytes.clone(), expected).await? == VersionedWrite::Written {
            break;
        }
    }
    Ok(())
}

/// Loads a table from its head, without checking for newer versions. `None` if the table has no
/// head, or one this process can't decode (an unknown codec or layout).
pub(crate) async fn read(lake: &Lake, name: &TableName, location: &str) -> Result<Option<Table>> {
    let Some(bytes) = lake.storage().read_opt(&head_uri(location)).await? else { return Ok(None) };
    Ok(decode(lake, name, location, &bytes).ok())
}

fn decode(lake: &Lake, name: &TableName, location: &str, bytes: &[u8]) -> Result<Table> {
    let head = Contents::decode(bytes, |codec| lake.head_codecs().get(codec))?;
    let metadata: TableMetadata = serde_json::from_str(&head.metadata_json)?;
    let table = Table::new(lake.clone(), name.clone(), location.to_string(), head.version, metadata)?;
    let files = head.files.iter().map(|e| LiveEntry::from_shared(e, table.metadata())).collect::<Result<Vec<_>>>()?;
    if let (Some(uri), Some(index)) = (table.last_value_index(), &head.last_values) {
        lake.caches().last_value_indexes.insert(uri, align_batch(index, &table.schema())?);
    }
    for (path, footer) in head.footers {
        lake.caches().footers.insert(path, Bytes::from(footer));
    }
    Ok(table.with_files(Arc::new(files)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow::array::{Int64Array, RecordBatch};
    use timelake_format::head::encode;

    use super::*;
    use crate::Error;
    use crate::testing::{FaultyStore, Op, T0, batch, faulty_lake, opts, reopen, reopen_hint_lookup, schema};
    use crate::{AppendOptions, Session, TableOptions, property};

    fn no_maintenance() -> AppendOptions {
        AppendOptions { maintain: Some(false), ..Default::default() }
    }

    fn with_properties(kv: &[(&str, &str)]) -> TableOptions {
        let mut o = opts();
        o.properties = kv.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
        o
    }

    async fn stored_header(lake: &Lake, table: &Table) -> Option<(u64, String)> {
        let bytes = lake.storage().read_opt(&head_uri(table.location())).await.unwrap()?;
        let h = Header::parse(&bytes).unwrap();
        Some((h.version, h.codec.to_string()))
    }

    fn paths(files: &[LiveEntry]) -> Vec<String> {
        let mut v: Vec<String> = files.iter().map(|e| e.data_file.file_path().to_string()).collect();
        v.sort();
        v
    }

    /// A table loaded from its head has the same version, files, statistics and last-value index
    /// as one loaded through the manifests.
    #[tokio::test]
    async fn head_describes_the_table() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0), (T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0 + 86_400_000_000, 1, 3.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await, Some((3, "arrow".into())));

        let cold = reopen(&store);
        let from_head = cold.table("iot.r").await.unwrap();
        assert_eq!(from_head.version(), 3);
        let files = from_head.known_files().expect("files from the head").to_vec();
        // The same version read through its manifests.
        let metadata = t.metadata().clone();
        let manifests = Table::new(lake.clone(), t.name().clone(), t.location().to_string(), 3, metadata).unwrap();
        let mut expected = manifests.files().await.unwrap();
        expected.sort_by(|a, b| a.data_file.file_path().cmp(b.data_file.file_path()));
        let mut got = files.clone();
        got.sort_by(|a, b| a.data_file.file_path().cmp(b.data_file.file_path()));
        assert_eq!(paths(&got), paths(&expected));
        for (g, e) in got.iter().zip(&expected) {
            assert_eq!(g.data_file.as_ref(), e.data_file.as_ref());
            assert_eq!((g.snapshot_id, g.sequence_number), (e.snapshot_id, e.sequence_number));
        }
        let index = from_head.last_value_index().unwrap();
        assert!(cold.caches().last_value_indexes.get(&index).is_some(), "index inline");
    }

    /// Round trips a cold query waits for, one after another, on a store where each request
    /// takes 100 ms of (paused) time: `None` runs `latest`, `Some(sql)` a query. The memory store
    /// stands in for object storage, so filter pushdown is off as `auto` would have it there.
    async fn cold_round_trips(head_format: &str, sql: Option<&str>) -> u128 {
        cold_round_trips_with(&[(property::HEAD_FORMAT, head_format)], sql, reopen).await
    }

    async fn cold_round_trips_with(
        properties: &[(&str, &str)],
        sql: Option<&str>,
        open: fn(&Arc<FaultyStore>) -> Lake,
    ) -> u128 {
        const RTT: Duration = Duration::from_millis(100);
        let (lake, store) = faulty_lake();
        let o = with_properties(&[&[(property::FILTER_PUSHDOWN, "false")], properties].concat());
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        t.append(&[batch(&[(T0, 1, 1.0), (T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;

        store.set_latency(RTT);
        let session = Session::new(open(&store));
        let started = tokio::time::Instant::now();
        let df = match sql {
            None => session.latest("iot.r").await.unwrap(),
            Some(sql) => session.sql(sql).await.unwrap(),
        };
        assert_ne!(df.collect().await.unwrap().len(), 0);
        started.elapsed().as_millis() / RTT.as_millis()
    }

    /// With the head, a cold `latest` reads the head and checks for a newer version; a filtered
    /// scan also reads footers (alongside the check) and data. Without it, the version hint,
    /// metadata, manifest list and manifests come first.
    #[tokio::test(start_paused = true)]
    async fn cold_queries_read_the_head_first() {
        let scan = Some("SELECT value FROM iot.r WHERE device = 2");
        assert_eq!(cold_round_trips("arrow", None).await, 1);
        assert_eq!(cold_round_trips("arrow", scan).await, 2);
        // `timelake_version_lookup=hint`: the version hint instead of a listing, then the check.
        assert_eq!(cold_round_trips_with(&[], None, reopen_hint_lookup).await, 2);
        assert_eq!(cold_round_trips_with(&[], scan, reopen_hint_lookup).await, 3);
        assert_eq!(cold_round_trips("none", None).await, 3);
        assert_eq!(cold_round_trips("none", scan).await, 6);
        // Filter pushdown reads the filter column, then the others.
        let pushdown = [(property::FILTER_PUSHDOWN, "true")];
        assert_eq!(cold_round_trips_with(&pushdown, scan, reopen).await, 3);
    }

    /// A head that is behind (its write failed, and so did the version hint's) is noticed, and
    /// the newest version is read.
    #[tokio::test]
    async fn readers_notice_a_stale_head() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        store.fail(Op::Put, "timelake-head");
        store.fail(Op::Put, "version-hint");
        let (t, _) = t.append(&[batch(&[(T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await.unwrap().0, 2);

        assert_eq!(reopen(&store).table("iot.r").await.unwrap().version(), 3);
        let session = Session::new(reopen(&store));
        // Rows only in version 3: one in total, with device 2.
        for (sql, expected) in [("SELECT count(*) FROM iot.r", 2), ("SELECT count(*) FROM iot.r WHERE device = 2", 1)] {
            let rows = session.sql(sql).await.unwrap().collect().await.unwrap();
            let n = rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
            assert_eq!(n, expected, "{sql}");
        }
        let latest = session.latest("iot.r").await.unwrap().collect().await.unwrap();
        assert_eq!(latest.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }

    /// A reader that already has a version confirms it with one request (no head download), and
    /// sees a commit made by another process.
    #[tokio::test(start_paused = true)]
    async fn warm_queries_check_for_newer_versions() {
        const RTT: Duration = Duration::from_millis(100);
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let session = Session::new(reopen(&store));
        let count = || async {
            let rows = session.sql("SELECT count(*) FROM iot.r").await.unwrap().collect().await.unwrap();
            rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
        };
        assert_eq!(count().await, 1);

        // Past the planning cache's second (wall-clock time).
        std::thread::sleep(Duration::from_millis(1100));
        store.set_latency(RTT);
        let started = tokio::time::Instant::now();
        session.latest("iot.r").await.unwrap().collect().await.unwrap();
        assert_eq!(started.elapsed().as_millis() / RTT.as_millis(), 1);

        store.set_latency(Duration::ZERO);
        t.append(&[batch(&[(T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(count().await, 2);
    }

    /// Within `version-check-interval` a reader uses its version without a request, and doesn't
    /// see other processes' commits; a session's value overrides the table's.
    #[tokio::test(start_paused = true)]
    async fn version_check_interval_skips_the_check() {
        const RTT: Duration = Duration::from_millis(100);
        let (lake, store) = faulty_lake();
        let o = with_properties(&[(property::VERSION_CHECK_INTERVAL, "1h")]);
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let session_with = |interval: Option<&str>| {
            let mut config = Session::config();
            if let Some(v) = interval {
                config.options_mut().set("timelake.version_check_interval", v).unwrap();
            }
            Session::with_config(reopen(&store), config)
        };
        let count = |session: &Session| {
            let session = session.clone();
            async move {
                let rows = session.sql("SELECT count(*) FROM iot.r").await.unwrap().collect().await.unwrap();
                rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
            }
        };
        let (trusting, checking) = (session_with(None), session_with(Some("0s")));
        assert_eq!((count(&trusting).await, count(&checking).await), (1, 1));

        // Past the default second (wall-clock time): only the session with `0s` checks.
        std::thread::sleep(Duration::from_millis(1100));
        store.set_latency(RTT);
        let round_trips = |session: Session| async move {
            let started = tokio::time::Instant::now();
            session.latest("iot.r").await.unwrap().collect().await.unwrap();
            started.elapsed().as_millis() / RTT.as_millis()
        };
        assert_eq!(round_trips(trusting.clone()).await, 0);
        assert_eq!(round_trips(checking.clone()).await, 1);

        store.set_latency(Duration::ZERO);
        t.append(&[batch(&[(T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!((count(&trusting).await, count(&checking).await), (1, 2));
    }

    /// Writers that finish out of order never move the head back.
    #[tokio::test]
    async fn head_never_moves_back() {
        let (lake, _) = faulty_lake();
        let storage = lake.storage();
        let uri = "memory://test/wh/h";
        let head = |v: u64| Bytes::from(encode(v, &JsonCodec, &HeadDoc::default()).unwrap());
        write_if_newer(storage, uri, 5, head(5)).await.unwrap();
        write_if_newer(storage, uri, 4, head(4)).await.unwrap();
        assert_eq!(Header::parse(&storage.read(uri).await.unwrap()).unwrap().version, 5);
        write_if_newer(storage, uri, 6, head(6)).await.unwrap();
        assert_eq!(Header::parse(&storage.read(uri).await.unwrap()).unwrap().version, 6);
    }

    #[tokio::test]
    async fn format_is_a_table_property() {
        let (lake, store) = faulty_lake();
        let t =
            lake.create_table("iot.j", &schema(), with_properties(&[(property::HEAD_FORMAT, "json")])).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await, Some((2, "json".into())));
        let cold = reopen(&store).table("iot.j").await.unwrap();
        assert_eq!((cold.version(), cold.known_files().map(|f| f.len())), (2, Some(1)));

        let off = with_properties(&[(property::HEAD_FORMAT, "none")]);
        let t = lake.create_table("iot.off", &schema(), off).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await, None);
        assert_eq!(reopen(&store).table("iot.off").await.unwrap().version(), 2);

        // The head is deleted once, not again on every commit.
        let (t, _) = t.append(&[batch(&[(T0 + 1, 1, 1.0)])], no_maintenance()).await.unwrap();
        t.append(&[batch(&[(T0 + 2, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let head = head_uri(t.location());
        let head = head.trim_start_matches("memory://test/");
        assert!(store.deleted().iter().filter(|p| *p == head).count() <= 1, "{:?}", store.deleted());

        let err = lake.create_table("iot.x", &schema(), with_properties(&[(property::HEAD_FORMAT, "bson")])).await;
        assert!(matches!(err, Err(Error::Invalid(_))), "{err:?}");
    }

    /// A codec registered by the application: here JSON, stored reversed.
    #[derive(Debug)]
    struct Reversed;

    impl HeadCodec for Reversed {
        fn name(&self) -> &'static str {
            "reversed"
        }
        fn encode(&self, doc: &HeadDoc) -> Result<Vec<u8>, FormatError> {
            Ok(JsonCodec.encode(doc)?.into_iter().rev().collect())
        }
        fn decode(&self, bytes: &[u8]) -> Result<HeadDoc, FormatError> {
            JsonCodec.decode(&bytes.iter().rev().copied().collect::<Vec<_>>())
        }
    }

    /// Custom codecs work like the built-in ones; a reader without the codec loads the table
    /// through the metadata chain instead.
    #[tokio::test]
    async fn custom_codecs_can_be_registered() {
        let (lake, store) = faulty_lake();
        lake.register_head_codec(Arc::new(Reversed));
        let o = with_properties(&[(property::HEAD_FORMAT, "reversed")]);
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await, Some((2, "reversed".into())));

        let with_codec = reopen(&store);
        with_codec.register_head_codec(Arc::new(Reversed));
        assert!(with_codec.table("iot.r").await.unwrap().known_files().is_some());
        let without = reopen(&store).table("iot.r").await.unwrap();
        assert_eq!((without.version(), without.known_files().is_some()), (2, false));
    }

    /// The last-value index and footers are inline only while they fit the table's budget.
    #[tokio::test]
    async fn optional_sections_fit_the_budget() {
        let (lake, store) = faulty_lake();
        let o = with_properties(&[(property::HEAD_INLINE_BYTES, "0")]);
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let cold = reopen(&store);
        let table = cold.table("iot.r").await.unwrap();
        assert!(cold.caches().last_value_indexes.get(&table.last_value_index().unwrap()).is_none());
        let file = table.files().await.unwrap()[0].data_file.file_path().to_string();
        assert!(cold.caches().footers.get(&file).is_none());

        // With the default budget, both come with the head.
        let t = lake.create_table("iot.d", &schema(), opts()).await.unwrap();
        t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let cold = reopen(&store);
        let table = cold.table("iot.d").await.unwrap();
        assert!(cold.caches().last_value_indexes.get(&table.last_value_index().unwrap()).is_some());
        let file = table.files().await.unwrap()[0].data_file.file_path().to_string();
        assert!(cold.caches().footers.get(&file).is_some());
    }
}
