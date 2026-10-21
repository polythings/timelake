//! Typed values of the settings that take one of a fixed set of names (spec 9): each parses the
//! names a table property or `SET` accepts, in any case, and displays the canonical one, as tables
//! store it. The settings registry lists and checks them from here.

use std::fmt;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::time::Duration;

use crate::{FormatError, Result, parse_duration};

/// A setting's value, parsed from the text a table property or a session setting holds.
pub trait SettingValue: Sized {
    /// Parses `s` (trimmed, in any case); errors say `must be …, got …`.
    fn parse_setting(s: &str) -> Result<Self>;
}

impl SettingValue for bool {
    fn parse_setting(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(FormatError::new(format!("must be true or false, got {:?}", s.trim()))),
        }
    }
}

impl SettingValue for Duration {
    fn parse_setting(s: &str) -> Result<Self> {
        parse_duration(s)
    }
}

/// A setting value that is one of a fixed set of names.
pub trait Choice: Copy + Sized + 'static {
    /// Every value, in the order the docs list them.
    const ALL: &'static [Self];

    /// The canonical name, as tables store it.
    fn name(self) -> &'static str;

    /// Other spellings that parse to this value.
    fn aliases(self) -> &'static [&'static str] {
        &[]
    }
}

/// The canonical names of `T`'s values, for the docs: `a or b`, `a, b, or c`.
pub fn describe<T: Choice>() -> String {
    match T::ALL {
        [] => String::new(),
        [one] => one.name().to_string(),
        [a, b] => format!("{} or {}", a.name(), b.name()),
        [rest @ .., last] => {
            let rest: Vec<&str> = rest.iter().map(|c| c.name()).collect();
            format!("{}, or {}", rest.join(", "), last.name())
        }
    }
}

/// The value of `T` named by `s` (trimmed, in any case, or one of its other spellings).
pub(crate) fn find<T: Choice>(s: &str) -> Option<T> {
    let s = s.trim();
    T::ALL
        .iter()
        .copied()
        .find(|c| c.name().eq_ignore_ascii_case(s) || c.aliases().iter().any(|a| a.eq_ignore_ascii_case(s)))
}

fn parse_choice<T: Choice>(s: &str) -> Result<T> {
    find(s).ok_or_else(|| FormatError::new(format!("must be {}, got {:?}", describe::<T>(), s.trim())))
}

/// Defines a [`Choice`] enum with `FromStr` (errors say `must be …, got …`) and `Display` (the
/// canonical name).
macro_rules! choice {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident = $text:literal $(| $alias:literal)*,)+
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name {
            $($(#[$variant_meta])* $variant,)+
        }

        impl Choice for $name {
            const ALL: &'static [Self] = &[$(Self::$variant),+];

            fn name(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }

            fn aliases(self) -> &'static [&'static str] {
                match self {
                    $(Self::$variant => &[$($alias),*],)+
                }
            }
        }

        impl FromStr for $name {
            type Err = FormatError;

            fn from_str(s: &str) -> Result<Self> {
                parse_choice(s)
            }
        }

        impl SettingValue for $name {
            fn parse_setting(s: &str) -> Result<Self> {
                parse_choice(s)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.name())
            }
        }
    };
}

choice! {
    /// A switch that is on, off, or decided by the reader (`filter-pushdown`, `window-series-order`).
    pub enum AutoBool {
        /// The reader decides, as the setting describes.
        Auto = "auto",
        /// Always on.
        True = "true",
        /// Always off.
        False = "false",
    }
}

impl AutoBool {
    /// Whether the switch is on, with `auto` deciding as `auto()` says.
    pub fn is_on(self, auto: impl FnOnce() -> bool) -> bool {
        match self {
            Self::Auto => auto(),
            Self::True => true,
            Self::False => false,
        }
    }
}

choice! {
    /// How the Timelake reader removes copies of a `(series…, time)` (`read-dedup-strategy`, spec 12).
    pub enum ReadDedupStrategy {
        /// Merge the files that may share keys in key order.
        Merge = "merge",
        /// List the older copies from the key columns, then read the files and leave them out.
        KeysFirst = "keys-first" | "keys_first",
    }
}

choice! {
    /// How the signal aggregates get each series' readings in time order (`series-order`).
    pub enum SeriesOrder {
        /// Merge the files that overlap or aren't sorted, with the files in the time they span.
        Auto = "auto",
        /// Merge only copies (with dedup on read); files that overlap are an error.
        Segments = "segments",
        /// Merge every file.
        Merge = "merge",
    }
}

choice! {
    /// How data files encode float columns (`float-encoding`).
    pub enum FloatEncoding {
        /// A dictionary of each column chunk's values, falling back to byte-stream-split once it
        /// outgrows its page.
        Dictionary = "dictionary",
        /// Byte-stream-split only.
        ByteStreamSplit = "byte-stream-split",
    }
}

choice! {
    /// Iceberg's Parquet codec, `write.parquet.compression-codec`.
    pub enum CompressionCodec {
        /// ZSTD (the default).
        Zstd = "zstd",
        /// LZ4, as Parquet's `LZ4_RAW`.
        Lz4 = "lz4" | "lz4_raw",
        /// Snappy.
        Snappy = "snappy",
        /// Gzip.
        Gzip = "gzip",
        /// Brotli.
        Brotli = "brotli",
        /// No compression.
        Uncompressed = "uncompressed" | "none",
    }
}

/// ZSTD level for data files when the table sets none: fast to write, close to the best ratio.
pub const ZSTD_LEVEL: i32 = 3;

impl CompressionCodec {
    /// The levels the codec takes (as Parquet checks them), or `None` if it has none.
    pub fn levels(self) -> Option<RangeInclusive<i32>> {
        match self {
            Self::Zstd => Some(1..=22),
            Self::Gzip => Some(0..=9),
            Self::Brotli => Some(0..=11),
            Self::Lz4 | Self::Snappy | Self::Uncompressed => None,
        }
    }

    /// The level used when the table sets none, for codecs with levels.
    pub fn default_level(self) -> Option<i32> {
        match self {
            Self::Zstd => Some(ZSTD_LEVEL),
            Self::Gzip => Some(6),
            Self::Brotli => Some(1),
            Self::Lz4 | Self::Snappy | Self::Uncompressed => None,
        }
    }
}

/// Iceberg's `write.parquet.compression-codec` and `-level`, checked against each other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Compression {
    codec: CompressionCodec,
    level: Option<i32>,
}

impl Compression {
    /// `codec` at `level` (its default level when `None`); levels are ignored for codecs without
    /// them, as Iceberg does.
    pub fn new(codec: CompressionCodec, level: Option<i32>) -> std::result::Result<Self, CompressionError> {
        let Some(levels) = codec.levels() else { return Ok(Self { codec, level: None }) };
        let level = level.or(codec.default_level());
        match level {
            Some(l) if !levels.contains(&l) => Err(CompressionError::LevelOutOfRange { codec, level: l, levels }),
            _ => Ok(Self { codec, level }),
        }
    }

    /// From the table properties' values (`zstd` when the codec is unset).
    pub fn parse(codec: Option<&str>, level: Option<&str>) -> std::result::Result<Self, CompressionError> {
        let codec = codec.map_or(Ok(CompressionCodec::Zstd), |c| {
            c.parse().map_err(|_| CompressionError::UnknownCodec(c.to_string()))
        })?;
        if codec.levels().is_none() {
            return Self::new(codec, None);
        }
        let level = level.map(|l| l.trim().parse().map_err(|_| CompressionError::LevelNotInteger(l.to_string())));
        Self::new(codec, level.transpose()?)
    }

    /// The codec.
    pub fn codec(self) -> CompressionCodec {
        self.codec
    }

    /// The level, for codecs with levels.
    pub fn level(self) -> Option<i32> {
        self.level
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self { codec: CompressionCodec::Zstd, level: Some(ZSTD_LEVEL) }
    }
}

/// Iceberg compression properties a file can't be written with.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompressionError {
    /// A codec Timelake doesn't write.
    #[error("{key} {0:?}: use {names}", key = crate::property::COMPRESSION_CODEC, names = describe::<CompressionCodec>())]
    UnknownCodec(String),
    /// A level that isn't a number.
    #[error("{key} {0:?} is not an integer", key = crate::property::COMPRESSION_LEVEL)]
    LevelNotInteger(String),
    /// A level the codec doesn't have.
    #[error(
        "{key} {level} is out of range for {codec} ({min} to {max})",
        key = crate::property::COMPRESSION_LEVEL,
        min = levels.start(),
        max = levels.end()
    )]
    LevelOutOfRange {
        /// The codec.
        codec: CompressionCodec,
        /// The level set.
        level: i32,
        /// The levels it has.
        levels: RangeInclusive<i32>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn choices_parse_any_case_and_display_their_canonical_name() {
        assert_eq!(" Keys_First ".parse::<ReadDedupStrategy>().unwrap(), ReadDedupStrategy::KeysFirst);
        assert_eq!(ReadDedupStrategy::KeysFirst.to_string(), "keys-first");
        assert_eq!("LZ4_RAW".parse::<CompressionCodec>().unwrap(), CompressionCodec::Lz4);
        assert_eq!("TRUE".parse::<AutoBool>().unwrap(), AutoBool::True);
        for c in SeriesOrder::ALL {
            assert_eq!(c.to_string().parse::<SeriesOrder>().unwrap(), *c);
        }
        let err = "sorted".parse::<SeriesOrder>().unwrap_err().to_string();
        assert_eq!(err, "must be auto, segments, or merge, got \"sorted\"");
        assert_eq!(describe::<FloatEncoding>(), "dictionary or byte-stream-split");
    }

    #[test]
    fn compression_levels_are_checked_per_codec() {
        let zstd = |l: Option<&str>| Compression::parse(Some("zstd"), l);
        assert_eq!(zstd(None).unwrap(), Compression::default());
        assert_eq!(zstd(Some("5")).unwrap().level(), Some(5));
        assert_eq!(Compression::parse(None, None).unwrap(), Compression::default());
        assert_eq!(Compression::parse(Some("gzip"), None).unwrap().level(), Some(6));
        // Codecs without levels ignore them, as Iceberg does.
        assert_eq!(Compression::parse(Some("snappy"), Some("high")).unwrap().level(), None);
        for (codec, level) in [("zstd", "high"), ("zstd", "99"), ("gzip", "-1"), ("brotli", "12"), ("gzip", "1.5")] {
            assert!(Compression::parse(Some(codec), Some(level)).is_err(), "{codec} {level}");
        }
        let err = Compression::parse(Some("lzo"), None).unwrap_err().to_string();
        assert!(err.contains("\"lzo\": use zstd, lz4, snappy, gzip, brotli, or uncompressed"), "{err}");
    }
}
