use std::sync::Arc;

use compactor_scheduler::{
    CommitUpdate, CompactionJob, CompactionJobStatus, CompactionJobStatusResponse,
    CompactionJobStatusVariant, Scheduler,
};
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};

#[derive(Debug)]
pub struct CommitToScheduler {
    scheduler: Arc<dyn Scheduler>,
}

impl CommitToScheduler {
    pub fn new(scheduler: Arc<dyn Scheduler>) -> Self {
        Self { scheduler }
    }

    pub async fn commit(
        &self,
        partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>, crate::DynError> {
        match self
            .scheduler
            .update_job_status(CompactionJobStatus {
                job: CompactionJob::new(partition_id),
                status: CompactionJobStatusVariant::Update(CommitUpdate::new(
                    partition_id,
                    delete.into(),
                    upgrade.into(),
                    create.into(),
                    target_level,
                )),
            })
            .await?
        {
            CompactionJobStatusResponse::CreatedParquetFiles(ids) => Ok(ids),
            CompactionJobStatusResponse::Ack => unreachable!("scheduler should not ack"),
        }
    }
}

impl std::fmt::Display for CommitToScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommitToScheduler")
    }
}
