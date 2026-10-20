//! The lake as a DataFusion catalog: namespaces are schemas, tables are tables.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DfResult;

use super::PLANNING_REUSE;
use super::provider::TimelakeProvider;
use crate::error::{Error, Result};
use crate::lake::Lake;
use crate::name::TableName;

pub(super) const INFORMATION_SCHEMA: &str = "information_schema";

/// Namespace → table names, as of the last listing.
type Listing = RwLock<BTreeMap<String, Vec<String>>>;

/// The session's `version-check-interval`, if it sets one, as of the query being planned.
type Interval = Arc<RwLock<Option<Duration>>>;

/// DataFusion asks for schema and table names synchronously (for `information_schema` and `SHOW
/// TABLES`), so listing can't do I/O there: [`Session`](super::Session) lists the lake
/// asynchronously first, and these callbacks answer from that snapshot.
#[derive(Debug)]
pub(super) struct LakeCatalog {
    lake: Lake,
    listing: Arc<Listing>,
    interval: Interval,
}

impl LakeCatalog {
    pub(super) fn new(lake: Lake) -> Self {
        Self { lake, listing: Arc::default(), interval: Interval::default() }
    }

    /// Sets the session's `version-check-interval` for the lookups of the query about to be
    /// planned (DataFusion's lookups don't see the session's settings).
    pub(super) fn set_interval(&self, interval: Option<Duration>) {
        *self.interval.write().unwrap_or_else(PoisonError::into_inner) = interval;
    }

    /// Lists namespaces and tables for the synchronous callbacks.
    pub(super) async fn refresh_listing(&self) -> Result<()> {
        let namespaces = self.lake.namespaces().await?;
        let tables = futures::future::try_join_all(namespaces.iter().map(|ns| self.lake.tables(ns))).await?;
        let listing = namespaces
            .into_iter()
            .zip(tables)
            .map(|(ns, names)| (ns, names.iter().map(|n| n.name().to_string()).collect()))
            .collect();
        *self.listing.write().unwrap_or_else(PoisonError::into_inner) = listing;
        Ok(())
    }
}

impl CatalogProvider for LakeCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.listing.read().unwrap_or_else(PoisonError::into_inner).keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let (listing, interval) = (self.listing.clone(), self.interval.clone());
        Some(Arc::new(NamespaceSchema { lake: self.lake.clone(), namespace: name.to_string(), listing, interval }))
    }
}

/// One namespace as a DataFusion schema.
#[derive(Debug)]
struct NamespaceSchema {
    lake: Lake,
    namespace: String,
    listing: Arc<Listing>,
    interval: Interval,
}

#[async_trait]
impl SchemaProvider for NamespaceSchema {
    fn table_names(&self) -> Vec<String> {
        let listing = self.listing.read().unwrap_or_else(PoisonError::into_inner);
        listing.get(&self.namespace).cloned().unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Ok(name) = TableName::new(&self.namespace, name) else { return Ok(None) };
        let interval = *self.interval.read().unwrap_or_else(PoisonError::into_inner);
        match self.lake.table_recent(&name, interval, PLANNING_REUSE).await {
            Ok(t) => Ok(Some(Arc::new(TimelakeProvider::data(t)))),
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// From the last listing, or the tables this process has loaded since: it can't do I/O.
    fn table_exist(&self, name: &str) -> bool {
        let loaded = TableName::new(&self.namespace, name).is_ok_and(|n| self.lake.caches().tables.get(&n).is_some());
        loaded || self.table_names().iter().any(|t| t == name)
    }
}

#[cfg(test)]
mod tests {
    use crate::Session;
    use crate::testing::{faulty_lake, opts, reopen, schema};

    /// A table counts as existing before any query listed the lake.
    #[tokio::test]
    async fn tables_exist_without_a_listing() {
        let (lake, store) = faulty_lake();
        lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let session = Session::new(lake);
        assert!(session.context().table_exist("iot.r").unwrap());
        assert!(!session.context().table_exist("iot.missing").unwrap());
        // In another process, `CREATE EXTERNAL TABLE IF NOT EXISTS` of an existing table does nothing.
        let session = Session::new(reopen(&store));
        let create = "CREATE EXTERNAL TABLE IF NOT EXISTS iot.r STORED AS PARQUET LOCATION 'memory://test/x/'";
        session.sql(create).await.unwrap().collect().await.unwrap();
    }
}
