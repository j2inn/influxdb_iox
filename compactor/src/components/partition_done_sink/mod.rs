use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;

use crate::DynError;

pub mod error_kind;
pub mod logging;
pub mod metrics;
pub mod mock;
pub mod outcome;

/// Records "partition is done" status for given partition.
#[async_trait]
pub trait PartitionDoneSink: Debug + Display + Send + Sync {
    /// Record "partition is done" status for given partition.
    ///
    /// This method should retry.
    async fn record(
        &self,
        partition: PartitionId,
        res: Result<(), DynError>,
    ) -> Result<(), DynError>;
}

#[async_trait]
impl<T> PartitionDoneSink for Arc<T>
where
    T: PartitionDoneSink + ?Sized,
{
    async fn record(
        &self,
        partition: PartitionId,
        res: Result<(), DynError>,
    ) -> Result<(), DynError> {
        self.as_ref().record(partition, res).await
    }
}
