//! Timelake's session settings, under the `timelake.` prefix of DataFusion's configuration:
//! `SET timelake.read_dedup = false` in SQL, `--set timelake.read_dedup=false` on the CLI. Every
//! setting the registry allows at the session level can be set; a session setting overrides the
//! table's property for that session (spec 25).

use std::any::Any;
use std::collections::BTreeMap;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::error::{DataFusionError, Result as DfResult};
use timelake_format::settings::{self, Level, SETTINGS};

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

    /// The session's value of a boolean setting.
    pub fn flag(&self, name: &str) -> Option<bool> {
        self.get(name).map(|v| v.eq_ignore_ascii_case("true"))
    }

    /// The session's value of a duration setting.
    pub fn duration(&self, name: &str) -> Option<std::time::Duration> {
        self.get(name).and_then(|v| timelake_format::parse_duration(v).ok())
    }
}

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
