//! A materialized rollup's aggregates (spec 27), as its `timelake.rollup-aggregates` property
//! stores them: JSON objects of an output column, a function, and the raw column it reads
//! (`[{"name": "avg_value", "function": "avg", "column": "value"}]`). A function is a name, with
//! the signal aggregates' method (`integral:linear`) or the gap aggregates' threshold
//! (`gap_count:300s`) after a colon.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{FormatError, Result};

/// A signal aggregate over readings `(time, value)`, by what it returns from a group's runs of
/// readings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SignalFunction {
    /// `time_weighted_mean`.
    Mean,
    /// `integral`.
    Integral,
    /// `counter_diff`.
    CounterDiff,
    /// `counter_rate`.
    CounterRate,
    /// `time_weighted_state`: the runs themselves, as a rollup stores them.
    State,
}

impl SignalFunction {
    /// Every signal aggregate.
    pub const ALL: [Self; 5] = [Self::Mean, Self::Integral, Self::CounterDiff, Self::CounterRate, Self::State];

    /// The SQL name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Mean => "time_weighted_mean",
            Self::Integral => "integral",
            Self::CounterDiff => "counter_diff",
            Self::CounterRate => "counter_rate",
            Self::State => "time_weighted_state",
        }
    }

    /// The aggregate called `name`, if it is one of these.
    pub fn of(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|o| o.name() == name)
    }

    /// Whether the result depends on how the signal moves between readings.
    pub fn takes_method(self) -> bool {
        matches!(self, Self::Mean | Self::Integral | Self::State)
    }
}

/// How a signal moves between readings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    /// A reading holds until the next (`forward_fill`, pandas' `ffill`; the default).
    ForwardFill,
    /// A straight line between readings (`linear`).
    Linear,
}

impl FromStr for Method {
    type Err = FormatError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "forward_fill" | "ffill" => Ok(Self::ForwardFill),
            "linear" => Ok(Self::Linear),
            other => Err(FormatError::new(format!("the method must be 'forward_fill' or 'linear', not {other:?}"))),
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ForwardFill => "forward_fill",
            Self::Linear => "linear",
        })
    }
}

/// A gap aggregate over readings' times.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GapFunction {
    /// `gap_count`.
    Count,
    /// `gap_seconds`.
    Seconds,
    /// `gap_state`: the runs themselves, as a rollup stores them.
    State,
}

impl GapFunction {
    /// Every gap aggregate.
    pub const ALL: [Self; 3] = [Self::Count, Self::Seconds, Self::State];

    /// The SQL name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "gap_count",
            Self::Seconds => "gap_seconds",
            Self::State => "gap_state",
        }
    }

    /// The aggregate called `name`, if it is one of these.
    pub fn of(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|o| o.name() == name)
    }
}

/// An aggregate function a rollup stores: a bucket's rows give each exactly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum RollupFunction {
    /// `count`.
    Count,
    /// `sum`.
    Sum,
    /// `min`.
    Min,
    /// `max`.
    Max,
    /// `avg`.
    Avg,
    /// `first_value`, ordered by time.
    FirstValue,
    /// `last_value`, ordered by time.
    LastValue,
    /// `m4`: the first, last, smallest and largest points.
    M4,
    /// A signal aggregate's state, by its method; aggregates that don't depend on the method are
    /// always [`Method::ForwardFill`] (see [`RollupFunction::signal`]).
    Signal(SignalFunction, Method),
    /// A gap aggregate's state for a threshold, in microseconds.
    Gap(GapFunction, i64),
}

impl RollupFunction {
    const PLAIN: [Self; 8] =
        [Self::Count, Self::Sum, Self::Min, Self::Max, Self::Avg, Self::FirstValue, Self::LastValue, Self::M4];

    /// Signal aggregate `function` by `method`, which only the mean, the integral and the state
    /// depend on.
    pub fn signal(function: SignalFunction, method: Method) -> Self {
        Self::Signal(function, if function.takes_method() { method } else { Method::ForwardFill })
    }

    fn plain_name(self) -> Option<&'static str> {
        Some(match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::FirstValue => "first_value",
            Self::LastValue => "last_value",
            Self::M4 => "m4",
            Self::Signal(..) | Self::Gap(..) => return None,
        })
    }
}

impl fmt::Display for RollupFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Signal(function, Method::Linear) => write!(f, "{}:linear", function.name()),
            Self::Signal(function, Method::ForwardFill) => f.write_str(function.name()),
            // Whole seconds where it can, as the default column names show it.
            Self::Gap(function, threshold) if threshold % 1_000_000 == 0 => {
                write!(f, "{}:{}s", function.name(), threshold / 1_000_000)
            }
            Self::Gap(function, threshold) => write!(f, "{}:{threshold}us", function.name()),
            plain => f.write_str(plain.plain_name().unwrap_or_default()),
        }
    }
}

impl FromStr for RollupFunction {
    type Err = FormatError;

    fn from_str(s: &str) -> Result<Self> {
        let unknown = || FormatError::new(format!("a rollup can't store {s}"));
        if let Some(plain) = Self::PLAIN.into_iter().find(|p| p.plain_name() == Some(s)) {
            return Ok(plain);
        }
        let (name, parameter) = match s.split_once(':') {
            Some((name, parameter)) => (name, Some(parameter)),
            None => (s, None),
        };
        if let Some(function) = SignalFunction::of(name) {
            let method = match parameter {
                None => Method::ForwardFill,
                Some("linear") => Method::Linear,
                Some(_) => return Err(unknown()),
            };
            return Ok(Self::signal(function, method));
        }
        let (Some(function), Some(threshold)) = (GapFunction::of(name), parameter) else { return Err(unknown()) };
        let threshold = match threshold.strip_suffix("us") {
            Some(us) => us.parse().ok(),
            None => threshold.strip_suffix('s').and_then(|s| s.parse::<i64>().ok()?.checked_mul(1_000_000)),
        };
        threshold.map(|t| Self::Gap(function, t)).ok_or_else(unknown)
    }
}

impl TryFrom<String> for RollupFunction {
    type Error = FormatError;

    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
}

impl From<RollupFunction> for String {
    fn from(f: RollupFunction) -> Self {
        f.to_string()
    }
}

/// One aggregate of a rollup: its output column, the function, and the raw column it reads
/// (`None` for `count(*)`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Aggregate {
    /// The rollup's column.
    pub name: String,
    /// The function.
    pub function: RollupFunction,
    /// The raw table's column, or `None` for `count(*)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

/// The aggregates of a `timelake.rollup-aggregates` property.
pub fn parse_aggregates(json: &str) -> Result<Vec<Aggregate>> {
    serde_json::from_str(json).map_err(FormatError::from)
}

/// `aggregates` as a `timelake.rollup-aggregates` property.
pub fn aggregates_json(aggregates: &[Aggregate]) -> Result<String> {
    serde_json::to_string(aggregates).map_err(FormatError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn functions_round_trip_as_stored() {
        for (s, f) in [
            ("count", RollupFunction::Count),
            ("first_value", RollupFunction::FirstValue),
            ("m4", RollupFunction::M4),
            ("time_weighted_mean", RollupFunction::Signal(SignalFunction::Mean, Method::ForwardFill)),
            ("integral:linear", RollupFunction::Signal(SignalFunction::Integral, Method::Linear)),
            ("time_weighted_state:linear", RollupFunction::Signal(SignalFunction::State, Method::Linear)),
            ("counter_rate", RollupFunction::Signal(SignalFunction::CounterRate, Method::ForwardFill)),
            ("gap_count:300s", RollupFunction::Gap(GapFunction::Count, 300_000_000)),
            ("gap_seconds:1500us", RollupFunction::Gap(GapFunction::Seconds, 1_500)),
            ("gap_state:0s", RollupFunction::Gap(GapFunction::State, 0)),
        ] {
            assert_eq!(s.parse::<RollupFunction>().unwrap(), f, "{s}");
            assert_eq!(f.to_string(), s);
        }
        // A counter doesn't depend on the method.
        let diff = "counter_diff:linear".parse::<RollupFunction>().unwrap();
        assert_eq!(diff, RollupFunction::Signal(SignalFunction::CounterDiff, Method::ForwardFill));
        for bad in ["median", "integral:spline", "gap_count", "gap_count:5m", "count:linear", "gap_state:xs"] {
            let err = bad.parse::<RollupFunction>().unwrap_err().to_string();
            assert_eq!(err, format!("a rollup can't store {bad}"));
        }
    }

    /// The JSON is the same as before the functions were typed.
    #[test]
    fn aggregates_keep_their_json() {
        let json = r#"[{"name":"n","function":"count"},{"name":"i","function":"integral:linear","column":"v"}]"#;
        let aggregates = parse_aggregates(json).unwrap();
        assert_eq!(aggregates[1].function, RollupFunction::Signal(SignalFunction::Integral, Method::Linear));
        assert_eq!(aggregates_json(&aggregates).unwrap(), json);
        assert!(parse_aggregates(r#"[{"name":"x","function":"median","column":"v"}]"#).is_err());
    }
}
