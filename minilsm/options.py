from dataclasses import dataclass


@dataclass
class Options:
    """DB open options.

    sync_writes: when True, fsync the WAL after every append for durability.
    Set False later to measure how much durability costs in benchmarks.

    memtable_max_bytes: flush the MemTable to an SSTable once its approximate
    size reaches this threshold. Tests set it small to force flushes.

    index_interval: keep one sparse-index entry per this many SSTable records.

    bloom_bits_per_key: Bloom filter density. 0 disables Bloom filters entirely
    (nothing built, nothing checked) for on/off comparisons.

    compaction_min_files: after each flush, if the SSTable count is >= this
    value, run compact(). 0 disables automatic compaction.
    """

    sync_writes: bool = True
    memtable_max_bytes: int = 4 * 1024 * 1024
    index_interval: int = 16
    bloom_bits_per_key: int = 10
    compaction_min_files: int = 0
