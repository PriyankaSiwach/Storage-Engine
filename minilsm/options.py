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
    """

    sync_writes: bool = True
    memtable_max_bytes: int = 4 * 1024 * 1024
    index_interval: int = 16
    bloom_bits_per_key: int = 10
