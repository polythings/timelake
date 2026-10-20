use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::ExprFunctionExt;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext, ident, lit};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::dialect::dialect_from_str;

use super::CATALOG;
use super::catalog::{INFORMATION_SCHEMA, LakeCatalog};
use super::provider::TimelakeProvider;
use super::rewrite::LatestRewrite;
use super::rollup::{definition_of, refresh_session, rollup_frame};
use super::route::{RollupRewrite, RouteMap, prepare};
use super::settings::SessionSettings;
use super::statement::{self, RollupStatement, Statement};
use crate::error::{Error, Result};
use crate::lake::Lake;
use crate::name::{DEFAULT_NAMESPACE, TableName};
use crate::rollup::{Definition, RollupRefresh};
use crate::{Chunk, TableOptions, parse_duration, property};

/// Name of the helper column of [`Session::latest`]'s fallback query.
const ROW_NUMBER: &str = "__timelake_row_number";
/// Scan partitions per core on remote object storage, where scans wait on requests, not CPU.
const REMOTE_PARTITIONS_PER_CORE: usize = 4;
const MIN_REMOTE_PARTITIONS: usize = 32;

/// A SQL session over a lake. Tables are addressed as `namespace.table` (or just `table` in the
/// `default` namespace) and always read at their latest committed version.
///
/// ```no_run
/// # async fn demo(lake: timelake::Lake) -> timelake::Result<()> {
/// let session = timelake::Session::new(lake);
/// let batches = session.sql("SELECT count(*) FROM iot.readings").await?.collect().await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Session {
    lake: Lake,
    ctx: SessionContext,
    catalog: Arc<LakeCatalog>,
    /// What rollup routing may use, worked out before each query is planned.
    pub(super) routes: RouteMap,
}

impl Session {
    /// A session with DataFusion's default configuration.
    pub fn new(lake: Lake) -> Self {
        Self::with_config(lake, Self::config())
    }

    /// DataFusion's default configuration with Timelake's settings registered, so `timelake.*`
    /// keys can be set on it ([`SessionSettings`]).
    pub fn config() -> SessionConfig {
        SessionConfig::new().with_option_extension(SessionSettings::default())
    }

    /// A session with a custom DataFusion configuration. Its default schema, if set, is the
    /// namespace unqualified table names resolve in.
    pub fn with_config(lake: Lake, config: SessionConfig) -> Self {
        let namespace = match config.options().catalog.default_schema.as_str() {
            "public" => DEFAULT_NAMESPACE.to_string(),
            s => s.to_string(),
        };
        // Raise the default (one partition per core) on remote storage, unless the caller changed it.
        let cores = std::thread::available_parallelism().map_or(8, std::num::NonZero::get);
        let config = if lake.storage().is_remote() && config.target_partitions() == cores {
            config.with_target_partitions((cores * REMOTE_PARTITIONS_PER_CORE).max(MIN_REMOTE_PARTITIONS))
        } else {
            config
        };
        let config = if config.options().extensions.get::<SessionSettings>().is_some() {
            config
        } else {
            config.with_option_extension(SessionSettings::default())
        };
        // Rollup routing unions a raw table's rows with a rollup's, whose columns carry other Iceberg
        // field ids: DataFusion's logical union drops metadata its inputs disagree on, the physical
        // union merges it, and its check of an aggregate's input schema rejects the difference.
        let mut config = config;
        config.options_mut().execution.skip_physical_aggregate_schema_check = true;
        let config = config.with_information_schema(true).with_default_catalog_and_schema(CATALOG, namespace);
        let routes = RouteMap::default();
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_optimizer_rule(Arc::new(LatestRewrite))
            .with_optimizer_rule(Arc::new(super::newest::NewestFirst))
            .with_optimizer_rule(Arc::new(super::series_order::SeriesOrder))
            .with_optimizer_rule(Arc::new(RollupRewrite { routes: routes.clone() }))
            .with_optimizer_rule(Arc::new(super::shared_state::SharedState))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_object_store(lake.storage().url(), lake.storage().object_store());
        let catalog = Arc::new(LakeCatalog::new(lake.clone()));
        ctx.register_catalog(CATALOG, catalog.clone());
        // time_bucket(width, ts [, origin]) is the name time-series users know for date_bin.
        let time_bucket = datafusion::functions::datetime::date_bin().as_ref().clone().with_aliases(["time_bucket"]);
        ctx.register_udf(time_bucket);
        ctx.register_udaf(super::m4::m4_udaf());
        for f in super::selection::udafs().into_iter().chain(super::moments::udafs()).chain(super::quantile::udafs()) {
            ctx.register_udaf(f);
        }
        // pandas' names for DataFusion's sample standard deviation and covariance.
        ctx.register_udaf(
            datafusion::functions_aggregate::stddev::stddev_udaf().as_ref().clone().with_aliases(["std"]),
        );
        ctx.register_udaf(
            datafusion::functions_aggregate::covariance::covar_samp_udaf().as_ref().clone().with_aliases(["cov"]),
        );
        ctx.register_udtf("time_buckets", Arc::new(super::gapfill::TimeBuckets));
        for f in super::gapfill::window_udfs()
            .into_iter()
            .chain(super::changes::window_udfs())
            .chain(super::rolling::window_udfs())
        {
            ctx.register_udwf(f);
        }
        for output in super::signal::Output::ALL {
            ctx.register_udaf(super::signal::udaf(output));
        }
        for output in super::gaps::Output::ALL {
            ctx.register_udaf(super::gaps::udaf(output));
        }
        for output in super::flatline::Output::ALL {
            ctx.register_udaf(super::flatline::udaf(output));
        }
        Self { lake, ctx, catalog, routes }
    }

    /// The lake this session reads.
    pub fn lake(&self) -> &Lake {
        &self.lake
    }

    /// The underlying DataFusion context, for registering more tables or functions.
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Plans one SQL statement.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let mut statements = self.parse(sql)?;
        match (statements.pop(), statements.is_empty()) {
            (Some(statement), true) => self.execute(statement).await,
            _ => Err(Error::invalid("expected one SQL statement")),
        }
    }

    /// Splits SQL text into statements (so a `;` inside a string literal or comment doesn't
    /// split), for running a script one statement at a time with [`Self::execute`].
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>> {
        let state = self.ctx.state();
        let options = &state.config().options().sql_parser;
        let dialect = dialect_from_str(options.dialect)
            .ok_or_else(|| Error::config(format!("unsupported SQL dialect {}", options.dialect)))?;
        statement::parse(sql, dialect.as_ref(), options.recursion_limit)
    }

    /// Plans one parsed statement. Statements about rollups run here and return a summary.
    pub async fn execute(&self, statement: Statement) -> Result<DataFrame> {
        match statement {
            Statement::Sql(s) => self.execute_sql(*s).await,
            Statement::Rollup(r) => self.execute_rollup(r).await,
        }
    }

    async fn execute_sql(&self, statement: datafusion::sql::parser::Statement) -> Result<DataFrame> {
        let state = self.ctx.state();
        // DataFusion resolves tables one after another; on object storage each costs several
        // round trips (version hint, metadata, manifest list, manifests). Load every table the
        // query references concurrently first, so planning finds them all in cache.
        if let Ok(references) = state.resolve_table_references(&statement) {
            // DataFusion lists namespaces and tables synchronously; list them up front instead.
            if references.iter().any(|r| r.schema() == Some(INFORMATION_SCHEMA)) {
                self.catalog.refresh_listing().await?;
            }
            let default_namespace = &state.config().options().catalog.default_schema;
            let names: Vec<TableName> = references
                .iter()
                .filter(|r| r.catalog().is_none_or(|c| c == CATALOG))
                .filter_map(|r| TableName::new(r.schema().unwrap_or(default_namespace), r.table()).ok())
                .collect();
            let interval = self.version_check_interval();
            self.catalog.set_interval(interval);
            futures::future::join_all(names.iter().map(|n| self.prefetch(n, interval))).await;
            futures::future::join_all(names.iter().map(|n| prepare(&self.lake, n, interval, &self.routes, None))).await;
        }
        let plan = state.statement_to_plan(statement).await?;
        Ok(self.ctx.execute_logical_plan(plan).await?)
    }

    /// `CREATE`, `DROP` or `REFRESH MATERIALIZED ROLLUP`: one row with the rollup and the rows
    /// it took.
    async fn execute_rollup(&self, statement: RollupStatement) -> Result<DataFrame> {
        let (name, rows) = match statement {
            RollupStatement::Create { name, query, options, if_not_exists } => {
                let name = self.table_name(&name)?;
                match self.lake.table(&name).await {
                    Ok(_) if if_not_exists => (name, 0),
                    Ok(_) => return Err(Error::AlreadyExists(name.to_string())),
                    Err(Error::NotFound(_)) => {
                        let rows = self.create_rollup(&name, &query, options).await?;
                        (name, rows)
                    }
                    Err(e) => return Err(e),
                }
            }
            RollupStatement::Drop { name, if_exists } => {
                let name = self.table_name(&name)?;
                match crate::rollup::drop(&self.lake, &name).await {
                    Ok(()) => {}
                    Err(Error::NotFound(_)) if if_exists => {}
                    Err(e) => return Err(e),
                }
                (name, 0)
            }
            RollupStatement::Refresh { name } => {
                let name = self.table_name(&name)?;
                let rows = crate::rollup::refresh(&self.lake, &name, None, RollupRefresh::Always).await?;
                (name, rows)
            }
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("rollup", DataType::Utf8, false),
            Field::new("rows", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![name.to_string()])), Arc::new(UInt64Array::from(vec![rows]))],
        )?;
        Ok(self.ctx.read_batch(batch)?)
    }

    /// Creates rollup `name` defined by `query` and computes it. Returns the rows written.
    async fn create_rollup(&self, name: &TableName, query: &str, options: Vec<(String, String)>) -> Result<u64> {
        // Plan the definition as written, not routed to another rollup.
        let mut config = self.ctx.copied_config();
        config.options_mut().set("timelake.rollup_routing", "false")?;
        let planner = Session::with_config(self.lake.clone(), config);
        let mut statements = planner.parse(query)?;
        let (Some(Statement::Sql(select)), true) = (statements.pop(), statements.is_empty()) else {
            return Err(Error::invalid(format!("rollup {name}: AS must be followed by one SELECT")));
        };
        let plan = planner.execute_sql(*select).await?.into_optimized_plan()?;
        let (raw, def, order) = definition_of(&plan, name)?;
        let ctx = refresh_session(&self.lake, raw.name(), name).await?;
        let order: Vec<&str> = order.iter().map(String::as_str).collect();
        let frame = rollup_frame(&ctx, &raw, &def, Some(&[]))?.select_columns(&order)?;
        let schema = frame.schema().as_arrow().clone();
        crate::rollup::create(&raw, name, &def, &schema, rollup_options(&def, options)?).await
    }

    /// A table name, in the session's default namespace when it has none.
    fn table_name(&self, name: &str) -> Result<TableName> {
        match name.split_once('.') {
            Some((namespace, table)) => TableName::new(namespace, table),
            None => TableName::new(&self.ctx.state().config().options().catalog.default_schema, name),
        }
    }

    /// The session's `version-check-interval`, if it sets one.
    fn version_check_interval(&self) -> Option<Duration> {
        let state = self.ctx.state();
        state.config().options().extensions.get::<SessionSettings>()?.duration(property::VERSION_CHECK_INTERVAL)
    }

    /// Loads a table's metadata, file list and last-value index into the lake's caches: the
    /// query's first lookup of the table, so it decides whether to check for a newer version.
    /// Errors are left for planning to report.
    async fn prefetch(&self, name: &TableName, interval: Option<Duration>) {
        let Ok(table) = self.lake.table_recent(name, interval, Duration::ZERO).await else { return };
        let index = table.last_value_index();
        let read_index = async {
            if let Some(uri) = index {
                let _ = crate::lvi::read(&table, &uri).await;
            }
        };
        let (_, ()) = tokio::join!(table.files(), read_index);
    }

    /// The newest row of every series (from the last-value index when available).
    pub async fn latest<N>(&self, table: N) -> Result<DataFrame>
    where
        N: TryInto<TableName>,
        Error: From<N::Error>,
    {
        let interval = self.version_check_interval();
        self.catalog.set_interval(interval);
        let table = self.lake.table_recent(&table.try_into()?, interval, Duration::ZERO).await?;
        let Some(time_column) = table.contract().time_column.clone() else {
            return Err(Error::invalid(format!("{} has no time column", table.name())));
        };
        if table.last_value_index().is_some() {
            return Ok(self.ctx.read_table(Arc::new(TimelakeProvider::latest(table)))?);
        }
        // Fallback for tables without an index: the newest row per series with a window function.
        let keys = table.contract().series_key.iter().map(ident).collect();
        let rn = row_number().partition_by(keys).order_by(vec![ident(time_column).sort(false, true)]).build()?;
        let name = table.name();
        let df = self
            .ctx
            .table(TableReference::full(CATALOG, name.namespace(), name.name()))
            .await?
            .window(vec![rn.alias(ROW_NUMBER)])?
            .filter(ident(ROW_NUMBER).eq(lit(1u64)))?
            .drop_columns(&[ROW_NUMBER])?;
        Ok(df)
    }
}

/// The rollup table's options: bucket and keys from the definition, dedup on, no insertion time,
/// and the `WITH` options (`chunk_interval`, `retention`, `refresh_interval`, or any table setting).
fn rollup_options(def: &Definition, options: Vec<(String, String)>) -> Result<TableOptions> {
    const HOUR_MICROS: i64 = 3_600_000_000;
    let mut opts = TableOptions {
        time_column: Some(def.bucket.clone()),
        series_key: def.keys.clone(),
        chunk: Some(if def.width < HOUR_MICROS { Chunk::Day } else { Chunk::Month }),
        dedup: Some(true),
        ingest_time: Some(false),
        ..Default::default()
    };
    for (key, value) in options {
        match key.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "chunk-interval" | "chunk" => opts.chunk = Some(value.parse()?),
            "retention" => opts.retention = Some(parse_duration(&value)?),
            "refresh-interval" => {
                opts.properties.insert(property::ROLLUP_REFRESH_INTERVAL.into(), value);
            }
            _ => {
                opts.properties.insert(crate::alter::property_key(&key)?, value);
            }
        }
    }
    Ok(opts)
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session").field("lake", &self.lake).finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array, RecordBatch, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;
    use crate::TableOptions;
    use crate::testing::{T0, append_without_index, faulty_lake};

    /// Without a last-value index, `latest` runs a window query; odd column and table names must
    /// not break it.
    #[tokio::test]
    async fn latest_without_index_quotes_names() {
        let (lake, _store) = faulty_lake();
        let schema = Arc::new(Schema::new(vec![
            Field::new("the time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("dev\"ice", DataType::Int32, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let opts = TableOptions {
            time_column: Some("the time".into()),
            series_key: vec!["dev\"ice".into()],
            ..Default::default()
        };
        let t = lake.create_table("iot.my-readings", &schema, opts).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![T0, T0 + 1, T0, T0 + 2]).with_timezone("UTC")),
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        )
        .unwrap();
        let t = append_without_index(&t, &[batch]).await;
        assert!(t.last_value_index().is_none());

        let df = Session::new(lake).latest("iot.my-readings").await.unwrap();
        let batches = df.sort(vec![ident("dev\"ice").sort(true, true)]).unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(batch.num_columns(), 4, "time, device, value and the insertion time");
        let values = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.values(), &[2.0, 4.0]);
    }

    #[tokio::test]
    async fn parse_splits_statements_outside_literals() {
        let (lake, _store) = faulty_lake();
        let s = Session::new(lake);
        let statements = s.parse("SELECT 'a;b'; -- c;d\nSELECT 2;;").unwrap();
        assert_eq!(statements.len(), 2);
        assert!(s.parse("SELECT 'unterminated").is_err());
    }
}
