//! Timelake's session settings, under the `timelake.` prefix of DataFusion's configuration:
//! `SET timelake.read_dedup = false` in SQL, `--set timelake.read_dedup=false` on the CLI. Every
//! setting the registry allows at the session level can be set; a session setting overrides the
//! table's property for that session (spec 9).

use std::any::Any;
use std::collections::BTreeMap;
use std::time::Duration;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result as DfResult};
use timelake_format::settings::{self, Level, SETTINGS};
use timelake_format::value::SettingValue;
use timelake_format::{AutoBool, Contract, ReadDedupStrategy, SeriesOrder};

use crate::property;

/// A session setting's key as the session's configuration takes it: a name without a prefix
/// (`read_dedup`, `read-dedup`) is Timelake's (`timelake.read_dedup`); prefixed keys
/// (`timelake.…`, `datafusion.…`) stay as they are.
pub fn session_key(name: &str) -> String {
    let name = name.trim();
    if name.contains('.') { name.to_string() } else { format!("{}{}", settings::PREFIX, name.replace('-', "_")) }
}

/// Timelake settings for one session; unset ones follow each table's properties.
#[derive(Clone, Debug, Default)]
pub struct SessionSettings {
    /// Values by setting name (kebab-case), checked when set.
    values: BTreeMap<&'static str, String>,
}

impl SessionSettings {
    /// The session's value of a setting (any spelling), if the session set it.
    pub fn get(&self, name: &str) -> Option<&str> {
        settings::find(name).and_then(|s| self.values.get(s.name)).map(String::as_str)
    }

    /// The session's value of a setting, typed ([`timelake_format::value`]); values are checked
    /// when set, so they parse.
    pub fn value<T: SettingValue>(&self, name: &str) -> Option<T> {
        self.get(name).and_then(|v| T::parse_setting(v).ok())
    }

    /// The session's value of a boolean setting.
    pub fn flag(&self, name: &str) -> Option<bool> {
        self.value(name)
    }

    /// The session's value of a duration setting.
    pub fn duration(&self, name: &str) -> Option<Duration> {
        self.value(name)
    }
}

/// The session's value of a setting, if it sets one.
pub(crate) fn session<T: SettingValue>(options: &ConfigOptions, name: &str) -> Option<T> {
    options.extensions.get::<SessionSettings>()?.value(name)
}

/// A setting a session can override for the tables it reads: its property, and the table's value
/// in the contract.
pub(crate) struct Override<T> {
    property: &'static str,
    table: fn(&Contract) -> T,
}

impl<T: SettingValue> Override<T> {
    /// The session's value, if it sets one.
    pub(crate) fn session(&self, options: &ConfigOptions) -> Option<T> {
        session(options, self.property)
    }

    /// The session's value, else the table's (its property, or the default).
    pub(crate) fn of(&self, options: &ConfigOptions, contract: &Contract) -> T {
        self.session(options).unwrap_or_else(|| (self.table)(contract))
    }
}

/// `read-dedup` (applies only to tables with `dedup` on).
pub(crate) const READ_DEDUP: Override<bool> = Override { property: property::READ_DEDUP, table: |c| c.read_dedup };
/// `read-dedup-strategy`.
pub(crate) const READ_DEDUP_STRATEGY: Override<ReadDedupStrategy> =
    Override { property: property::READ_DEDUP_STRATEGY, table: |c| c.read_dedup_strategy };
/// `filter-pushdown`.
pub(crate) const FILTER_PUSHDOWN: Override<AutoBool> =
    Override { property: property::FILTER_PUSHDOWN, table: |c| c.filter_pushdown };
/// `in-list-pruning`.
pub(crate) const IN_LIST_PRUNING: Override<bool> =
    Override { property: property::IN_LIST_PRUNING, table: |c| c.in_list_pruning };
/// `newest-first`.
pub(crate) const NEWEST_FIRST: Override<bool> =
    Override { property: property::NEWEST_FIRST, table: |c| c.newest_first };
/// `series-order`.
pub(crate) const SERIES_ORDER: Override<SeriesOrder> =
    Override { property: property::SERIES_ORDER, table: |c| c.series_order };
/// `window-series-order`.
pub(crate) const WINDOW_SERIES_ORDER: Override<AutoBool> =
    Override { property: property::WINDOW_SERIES_ORDER, table: |c| c.window_series_order };
/// `rollup-routing`.
pub(crate) const ROLLUP_ROUTING: Override<bool> =
    Override { property: property::ROLLUP_ROUTING, table: |c| c.rollup_routing };
/// `version-check-interval`.
pub(crate) const VERSION_CHECK_INTERVAL: Override<Duration> =
    Override { property: property::VERSION_CHECK_INTERVAL, table: |c| c.version_check_interval };

impl ConfigExtension for SessionSettings {
    const PREFIX: &'static str = "timelake";
}

impl ExtensionOptions for SessionSettings {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DfResult<()> {
        let config_err = |e: timelake_format::FormatError| DataFusionError::Configuration(e.to_string());
        let name = format!("{}{key}", settings::PREFIX);
        let setting = settings::find(key).ok_or_else(|| config_err(settings::unknown(&name)))?;
        if !setting.allows(Level::Session) {
            return Err(config_err(settings::not_settable(setting, Level::Session)));
        }
        setting.check(value).map_err(config_err)?;
        self.values.insert(setting.name, value.trim().to_ascii_lowercase());
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        SETTINGS
            .iter()
            .filter(|s| s.allows(Level::Session))
            .map(|s| ConfigEntry {
                key: s.sql_name(),
                value: self.values.get(s.name).cloned(),
                description: s.description,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionConfig;

    use super::*;

    fn config() -> SessionConfig {
        SessionConfig::new().with_option_extension(SessionSettings::default())
    }

    #[test]
    fn session_settings_are_checked_against_the_registry() {
        let mut config = config();
        let options = config.options_mut();
        options.set("timelake.read_dedup", "FALSE").unwrap();
        options.set("timelake.read-dedup-strategy", "keys-first").unwrap();
        let s = options.extensions.get::<SessionSettings>().unwrap();
        assert_eq!((s.flag("read-dedup"), s.get("timelake.read_dedup_strategy")), (Some(false), Some("keys-first")));
        for (key, msg) in [
            ("timelake.read_dedup", "must be true or false"),
            ("timelake.min_snapshots", "only at: table"),
            ("timelake.chunk_interval", "fixed when the table is created"),
            ("timelake.nope", "unknown setting \"timelake.nope\""),
        ] {
            let err = options.set(key, "maybe").unwrap_err().to_string();
            assert!(err.contains(msg), "{key}: {err}");
        }
    }

    /// A session's value overrides the table's; unset, the table's applies.
    #[test]
    fn overrides_resolve_to_the_session_then_the_table() {
        use std::collections::HashMap;

        let table = Contract::from_properties(&HashMap::from([
            (property::SERIES_ORDER.to_string(), "merge".to_string()),
            (property::NEWEST_FIRST.to_string(), "false".to_string()),
        ]))
        .unwrap();
        let mut config = config();
        config.options_mut().set("timelake.series_order", "Segments").unwrap();
        let options = config.options();
        assert_eq!(SERIES_ORDER.of(options, &table), SeriesOrder::Segments);
        assert!(!NEWEST_FIRST.of(options, &table));
        assert_eq!(NEWEST_FIRST.session(options), None);
        assert_eq!(FILTER_PUSHDOWN.of(options, &table), AutoBool::Auto);
    }

    #[test]
    fn session_keys_get_the_prefix() {
        assert_eq!(session_key("read-dedup"), "timelake.read_dedup");
        assert_eq!(session_key("read_dedup"), "timelake.read_dedup");
        assert_eq!(session_key("timelake.read-dedup"), "timelake.read-dedup");
        assert_eq!(session_key("datafusion.execution.batch_size"), "datafusion.execution.batch_size");
    }

    #[test]
    fn entries_list_the_session_settings() {
        let mut config = config();
        config.options_mut().set("timelake.read_dedup", "false").unwrap();
        let entries = config.options().entries();
        let ours: Vec<_> = entries.iter().filter(|e| e.key.starts_with("timelake.")).collect();
        assert_eq!(ours.len(), SETTINGS.iter().filter(|s| s.allows(Level::Session)).count());
        let dedup = ours.iter().find(|e| e.key == "timelake.read_dedup").unwrap();
        assert_eq!(dedup.value.as_deref(), Some("false"));
    }
}
