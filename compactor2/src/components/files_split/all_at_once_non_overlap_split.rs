use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use super::FilesSplit;

#[derive(Debug)]
/// In AllAtOnce version, we will compact all files at once and do not split anything
pub struct AllAtOnceNonOverlapSplit {}

impl AllAtOnceNonOverlapSplit {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for AllAtOnceNonOverlapSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Non-overlapping  split for AllAtOnce version")
    }
}

impl FilesSplit for AllAtOnceNonOverlapSplit {
    fn apply(
        &self,
        files: Vec<data_types::ParquetFile>,
        _target_level: CompactionLevel,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        (files, vec![])
    }
}

#[cfg(test)]
mod tests {

    use crate::test_util::create_overlapped_files;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            AllAtOnceNonOverlapSplit::new().to_string(),
            "Non-overlapping  split for AllAtOnce version"
        );
    }

    #[test]
    fn test_apply_empty_files() {
        let files = vec![];
        let split = AllAtOnceNonOverlapSplit::new();

        let (overlap, non_overlap) = split.apply(files, CompactionLevel::FileNonOverlapped);
        assert_eq!(overlap.len(), 0);
        assert_eq!(non_overlap.len(), 0);
    }

    #[test]
    fn test_apply() {
        // Create 8 files with all levels
        let files = create_overlapped_files();
        assert_eq!(files.len(), 8);

        let split = AllAtOnceNonOverlapSplit::new();
        let (overlap, non_overlap) = split.apply(files.clone(), CompactionLevel::Initial);
        assert_eq!(overlap.len(), 8);
        assert_eq!(non_overlap.len(), 0);

        let (overlap, non_overlap) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(overlap.len(), 8);
        assert_eq!(non_overlap.len(), 0);

        let (overlap, non_overlap) = split.apply(files, CompactionLevel::Final);
        assert_eq!(overlap.len(), 8);
        assert_eq!(non_overlap.len(), 0);
    }
}
