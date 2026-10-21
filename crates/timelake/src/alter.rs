//! Changing a table's settings after creation (`timelake alter`, spec 9).

use std::collections::HashMap;

use timelake_format::settings::{self, Level};
use timelake_format::{Contract, FormatError};

use crate::commit::publish_retrying;
use crate::error::Result;
use crate::property;
use crate::table::Table;

impl Table {
    /// Sets table properties, or removes them with `None` (back to the default), in one commit of
    /// new metadata (no new snapshot). Timelake settings may be named in any spelling, with or
    /// without the `timelake.` prefix (`read-dedup`, `timelake.read_dedup`); only those the registry
    /// allows at the table level can change ([`settings::SETTINGS`]). Keys with a dot outside the
    /// `timelake.` prefix are Iceberg properties and pass through.
    ///
    /// ```no_run
    /// # async fn demo(table: timelake::Table) -> timelake::Result<()> {
    /// let table = table.set_properties(&[("read-dedup", Some("false")), ("retention", None)]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn set_properties(&self, changes: &[(&str, Option<&str>)]) -> Result<Table> {
        let mut set = HashMap::new();
        let mut remove = vec![];
        for &(key, value) in changes {
            let key = property_key(key)?;
            match value {
                Some(v) => {
                    set.insert(key, v.to_string());
                }
                None => remove.push(key),
            }
        }
        let set = settings::check_properties(&set, Level::Table)?;
        for key in &remove {
            if let Some(s) =
                settings::find(key).filter(|s| key.starts_with(settings::PREFIX) && !s.allows(Level::Table))
            {
                return Err(settings::not_settable(s, Level::Table).into());
            }
        }
        let mut merged = self.metadata().properties().clone();
        for key in &remove {
            merged.remove(key);
        }
        merged.extend(set.clone());
        let contract = Contract::from_properties(&merged)?;
        self.lake().check_head_format(contract.head_format.as_deref())?;
        let compression = [property::COMPRESSION_CODEC, property::COMPRESSION_LEVEL];
        if set.iter().map(|(k, _)| k).chain(&remove).any(|k| compression.contains(&k.as_str())) {
            contract.compression.map_err(FormatError::from)?;
        }
        write_properties(self, &set, &remove).await
    }
}

/// The property a user's key names: a Timelake setting in its canonical spelling (a bare
/// setting name counts), or any other key as it is.
pub(crate) fn property_key(key: &str) -> Result<String> {
    let trimmed = key.trim();
    if trimmed.to_ascii_lowercase().starts_with(settings::PREFIX) || !trimmed.contains('.') {
        return settings::find(trimmed)
            .map(settings::Setting::property)
            .ok_or_else(|| settings::unknown(trimmed).into());
    }
    Ok(trimmed.to_string())
}

/// Commits new table properties without checking them (no new snapshot): for Timelake's own
/// changes, such as `timelake upgrade` recording the format.
pub(crate) async fn write_properties(table: &Table, set: &[(String, String)], remove: &[String]) -> Result<Table> {
    update_properties(table, |_| (set.to_vec(), remove.to_vec())).await
}

/// Like [`write_properties`], with the change computed from the table version it applies to
/// (again after a concurrent commit), such as adding a name to a list.
pub(crate) async fn update_properties(
    table: &Table,
    change: impl Fn(&Table) -> (Vec<(String, String)>, Vec<String>),
) -> Result<Table> {
    Ok(publish_retrying(table, |base| {
        let (set, remove) = change(base);
        Box::pin(async move {
            let metadata = base
                .next_metadata()
                .set_properties(set.into_iter().collect())?
                .remove_properties(&remove)?
                .build()?
                .metadata;
            Ok(Some((metadata, base.known_files().cloned(), ())))
        })
    })
    .await?
    .0)
}

#[cfg(test)]
mod tests {
    use crate::property;
    use crate::testing::{faulty_lake, no_maintenance, opts, schema};

    #[tokio::test]
    async fn settings_change_after_creation() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        assert!(t.contract().read_dedup);
        let t = t.set_properties(&[("read_dedup", Some("false")), ("timelake.retention", Some("30d"))]).await.unwrap();
        assert!(!t.contract().read_dedup);
        assert_eq!(t.metadata().properties().get(property::RETENTION).map(String::as_str), Some("30d"));
        let t = t.set_properties(&[("retention", None)]).await.unwrap();
        assert_eq!(t.contract().retention, None);
        assert_eq!(lake.table("iot.r").await.unwrap().version(), t.version());

        for (change, msg) in [
            (("chunk-interval", Some("hour")), "fixed when the table is created"),
            (("time-column", None), "fixed when the table is created"),
            (("format-version", Some("9.0.0")), "set by Timelake itself"),
            (("min-snapshots", Some("many")), "non-negative integer"),
            (("head-format", Some("avro")), "unknown head format"),
            (("no-such-setting", Some("1")), "unknown setting"),
            (("series-order", Some("sorted")), "must be auto, segments, or merge"),
            (("write.parquet.compression-codec", Some("lzo")), "use zstd, lz4"),
            (("write.parquet.compression-level", Some("30")), "out of range for zstd (1 to 22)"),
        ] {
            let err = t.set_properties(&[change]).await.unwrap_err().to_string();
            assert!(err.contains(msg), "{change:?}: {err}");
        }
    }

    /// A change that keeps losing the race to publish gives up, as commits do, rather than trying
    /// forever: settings, rollback and expiry.
    #[tokio::test(start_paused = true)]
    async fn publishing_gives_up_after_losing_too_often() {
        use std::time::Duration;

        use crate::testing::{Op, T0, batch};
        use crate::{Error, MaintainOptions};

        let (lake, store) = faulty_lake();
        let mut o = opts();
        o.properties.insert(property::SNAPSHOT_RETENTION.into(), "0s".into());
        o.properties.insert(property::MIN_SNAPSHOTS.into(), "1".into());
        let mut t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        for i in 0..2 {
            t = t.append(&[batch(&[(T0 + i, 1, 1.0)])], no_maintenance()).await.unwrap().0;
        }
        let first = t.metadata().snapshots().map(|s| s.snapshot_id()).min().unwrap();
        // Every version is taken by "another writer", which never shows up.
        store.set_latency(Duration::from_millis(10));
        store.fail(Op::LoseCreate, ".metadata.json");
        let expire = MaintainOptions { compact: false, retention: false, ..MaintainOptions::default() };
        let within = Duration::from_secs(30);
        let set = tokio::time::timeout(within, t.set_properties(&[("retention", Some("1d"))])).await;
        let rollback = tokio::time::timeout(within, t.rollback(first)).await;
        let expired = tokio::time::timeout(within, t.maintain(expire)).await.map(|r| r.map(|_| ()));
        for (what, result) in [
            ("settings", set.map(|r| r.map(|_| ()))),
            ("rollback", rollback.map(|r| r.map(|_| ()))),
            ("expiry", expired),
        ] {
            assert!(matches!(result, Ok(Err(Error::Conflict(_)))), "{what}: {result:?}");
        }
    }

    /// A value outside a setting's set, as another engine could write it, is an error naming the
    /// setting when the table is opened, as a bad boolean is.
    #[tokio::test]
    async fn a_table_with_a_value_outside_a_settings_set_is_refused() {
        use crate::testing::reopen;

        use timelake_format::layout::{head_uri, metadata_uri};

        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let uri = metadata_uri(t.location(), t.version());
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&lake.storage().read(&uri).await.unwrap()).unwrap();
        metadata["properties"][property::SERIES_ORDER] = "sorted".into();
        lake.storage().write(&uri, serde_json::to_vec(&metadata).unwrap().into()).await.unwrap();
        lake.storage().delete(&head_uri(t.location())).await.unwrap();
        let err = reopen(&store).table("iot.r").await.unwrap_err().to_string();
        assert!(err.contains("timelake.series-order must be auto, segments, or merge"), "{err}");
    }

    #[tokio::test]
    async fn settings_at_creation_take_any_spelling() {
        let (lake, _store) = faulty_lake();
        let mut o = opts();
        o.properties = [("retention", "12h"), ("read_dedup", "false"), ("write.parquet.row-group-limit", "100")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        assert_eq!(t.contract().retention, Some(std::time::Duration::from_hours(12)));
        assert!(!t.contract().read_dedup);
        let props = t.metadata().properties();
        assert!(!props.contains_key("retention"), "{props:?}");
        assert_eq!(props.get("write.parquet.row-group-limit").map(String::as_str), Some("100"));

        let mut o = opts();
        o.properties.insert("no-such-setting".into(), "1".into());
        let err = lake.create_table("iot.s", &schema(), o).await.unwrap_err().to_string();
        assert!(err.contains("unknown setting"), "{err}");
    }
}
