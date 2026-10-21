//! Iceberg manifests through lakekit (`lakekit_iceberg`, which reads and writes them without
//! iceberg-rust, as WebAssembly needs), and Timelake's single time partition in a file's partition
//! tuple.

use lakekit_iceberg::Datum;
pub use lakekit_iceberg::{DataFile, Entry, ManifestFile, Status, read_manifest, read_manifest_list};

use crate::Chunk;

/// A file's time partition: hours, days or months since the epoch, by the table's chunk (`None`
/// for unpartitioned tables).
pub fn time_partition(file: &DataFile) -> Option<i32> {
    file.partition.first()?.as_ref()?.as_i64().and_then(|v| i32::try_from(v).ok())
}

/// The partition tuple of a file in time partition `partition` of a table chunked by `chunk`. A day
/// is an Iceberg `date`, hours and months are `int`s, as the partition transforms type them.
pub fn partition_values(chunk: Option<Chunk>, partition: Option<i32>) -> Vec<Option<Datum>> {
    match chunk {
        None => vec![],
        Some(Chunk::Day) => vec![partition.map(Datum::Date)],
        Some(_) => vec![partition.map(Datum::Int)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_round_trip() {
        for chunk in [Chunk::Hour, Chunk::Day, Chunk::Month] {
            let file = DataFile { partition: partition_values(Some(chunk), Some(20_000)), ..DataFile::default() };
            assert_eq!(time_partition(&file), Some(20_000));
        }
        assert_eq!(partition_values(Some(Chunk::Day), Some(1)), [Some(Datum::Date(1))]);
        assert_eq!(partition_values(None, None), Vec::new());
        assert_eq!(time_partition(&DataFile::default()), None);
    }
}
