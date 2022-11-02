//! An trait to abstract resolving a[`DatabaseName`] to [`NamespaceId`], and a
//! collection of composable implementations.

use std::{ops::DerefMut, sync::Arc};

use async_trait::async_trait;
use data_types::{DatabaseName, NamespaceId};
use iox_catalog::interface::{get_schema_by_name, Catalog};
use observability_deps::tracing::*;
use thiserror::Error;

use crate::namespace_cache::NamespaceCache;

pub mod mock;
mod ns_autocreation;
pub use ns_autocreation::*;

/// Error states encountered during [`NamespaceId`] lookup.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occured when attempting to fetch the namespace ID.
    #[error("failed to resolve namespace ID: {0}")]
    Lookup(iox_catalog::interface::Error),

    /// An error state for errors returned by [`NamespaceAutocreation`].
    #[error(transparent)]
    Create(#[from] NamespaceCreationError),
}

/// An abstract resolver of [`DatabaseName`] to [`NamespaceId`].
#[async_trait]
pub trait NamespaceResolver: std::fmt::Debug + Send + Sync {
    /// Return the [`NamespaceId`] for the given [`DatabaseName`].
    async fn get_namespace_id(
        &self,
        namespace: &DatabaseName<'static>,
    ) -> Result<NamespaceId, Error>;
}

/// An implementation of [`NamespaceResolver`] that queries the [`Catalog`] to
/// resolve a [`NamespaceId`], and populates the [`NamespaceCache`] as a side
/// effect.
#[derive(Debug)]
pub struct NamespaceSchemaResolver<C> {
    catalog: Arc<dyn Catalog>,
    cache: C,
}

impl<C> NamespaceSchemaResolver<C> {
    /// Construct a new [`NamespaceSchemaResolver`] that fetches schemas from
    /// `catalog` and caches them in `cache`.
    pub fn new(catalog: Arc<dyn Catalog>, cache: C) -> Self {
        Self { catalog, cache }
    }
}

#[async_trait]
impl<C> NamespaceResolver for NamespaceSchemaResolver<C>
where
    C: NamespaceCache,
{
    async fn get_namespace_id(
        &self,
        namespace: &DatabaseName<'static>,
    ) -> Result<NamespaceId, Error> {
        // Load the namespace schema from the cache, falling back to pulling it
        // from the global catalog (if it exists).
        match self.cache.get_schema(namespace) {
            Some(v) => Ok(v.id),
            None => {
                let mut repos = self.catalog.repositories().await;

                // Pull the schema from the global catalog or error if it does
                // not exist.
                let schema = get_schema_by_name(namespace, repos.deref_mut())
                    .await
                    .map_err(|e| {
                        warn!(
                            error=%e,
                            %namespace,
                            "failed to retrieve namespace schema"
                        );
                        Error::Lookup(e)
                    })
                    .map(Arc::new)?;

                // Cache population MAY race with other threads and lead to
                // overwrites, but an entry will always exist once inserted, and
                // the schemas will eventually converge.
                self.cache
                    .put_schema(namespace.clone(), Arc::clone(&schema));

                trace!(%namespace, "schema cache populated");
                Ok(schema.id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, NamespaceSchema, QueryPoolId, TopicId};
    use iox_catalog::mem::MemCatalog;

    use super::*;
    use crate::namespace_cache::MemoryNamespaceCache;

    #[tokio::test]
    async fn test_cache_hit() {
        let ns = DatabaseName::try_from("bananas").unwrap();

        // Prep the cache before the test to cause a hit
        let cache = Arc::new(MemoryNamespaceCache::default());
        cache.put_schema(
            ns.clone(),
            NamespaceSchema {
                id: NamespaceId::new(42),
                topic_id: TopicId::new(2),
                query_pool_id: QueryPoolId::new(3),
                tables: Default::default(),
                max_columns_per_table: 4,
            },
        );

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let resolver = NamespaceSchemaResolver::new(Arc::clone(&catalog), Arc::clone(&cache));

        // Drive the code under test
        resolver
            .get_namespace_id(&ns)
            .await
            .expect("lookup should succeed");

        assert!(cache.get_schema(&ns).is_some());

        // The cache hit should mean the catalog SHOULD NOT see a create request
        // for the namespace.
        let mut repos = catalog.repositories().await;
        assert!(
            repos
                .namespaces()
                .get_by_name(ns.as_str())
                .await
                .expect("lookup should not error")
                .is_none(),
            "expected no request to the catalog"
        );
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let ns = DatabaseName::try_from("bananas").unwrap();

        let cache = Arc::new(MemoryNamespaceCache::default());
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        // Create the namespace in the catalog
        {
            let mut repos = catalog.repositories().await;
            let topic = repos.topics().create_or_get("bananas").await.unwrap();
            let query_pool = repos.query_pools().create_or_get("platanos").await.unwrap();
            repos
                .namespaces()
                .create(
                    &ns,
                    iox_catalog::INFINITE_RETENTION_POLICY,
                    topic.id,
                    query_pool.id,
                )
                .await
                .expect("failed to setup catalog state");
        }

        let resolver = NamespaceSchemaResolver::new(Arc::clone(&catalog), Arc::clone(&cache));

        resolver
            .get_namespace_id(&ns)
            .await
            .expect("lookup should succeed");

        // The cache should be populated as a result of the lookup.
        assert!(cache.get_schema(&ns).is_some());
    }

    #[tokio::test]
    async fn test_cache_miss_does_not_exist() {
        let ns = DatabaseName::try_from("bananas").unwrap();

        let cache = Arc::new(MemoryNamespaceCache::default());
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let resolver = NamespaceSchemaResolver::new(Arc::clone(&catalog), Arc::clone(&cache));

        let err = resolver
            .get_namespace_id(&ns)
            .await
            .expect_err("lookup should error");

        assert_matches!(err, Error::Lookup(_));
        assert!(cache.get_schema(&ns).is_none());
    }
}