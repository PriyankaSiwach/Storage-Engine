# minilsm — Stage 4a

A from-scratch LSM-tree key-value storage engine.

**So far:** MemTable + WAL + SSTables with a **sparse index**, per-SSTable **Bloom filters**, and **compaction**. Tombstones are retained during compaction in this stage.

## How it works so far

1. **Writes** go to the WAL first, then the in-memory MemTable (durability before visibility).
2. When the MemTable reaches `memtable_max_bytes`, it is **flushed** to a new `sst_NNNNNN.sst` (write `.tmp` → fsync → rename → fsync directory), then the WAL is rotated.
3. **Reads** check the MemTable first, then SSTables from newest to oldest. Tombstones hide older values.
4. **Open** loads SSTables, then replays `wal.log` (truncating a torn tail if needed).
5. **Compaction** merges all SSTables into one via a k-way merge (newest value per key wins). Set `compaction_min_files > 0` to run this automatically after flushes.

### Sparse index

Each SSTable keeps only every `index_interval`-th key (plus the first) in memory, as a sorted `(key, offset)` list. A lookup binary-searches that list, then reads one on-disk block between two index entries. That uses far less RAM than a full key→offset map, at the cost of scanning a small run of records per hit (and still reading a block for misses that fall inside the key range).

### Bloom filters

Before touching disk, each SSTable can ask a Bloom filter whether a key is **definitely absent**. If so, that SSTable is skipped (no block read). Bloom filters never produce false negatives (every inserted key, including tombstones, is remembered), but they can produce **false positives**: the filter says "maybe" and we still read a block only to find the key is not there. `bloom_bits_per_key` trades memory for a lower false-positive rate; set it to `0` to disable filters for A/B measurements.

### Compaction

Compaction rewrites many overlapping SSTables into one sorted file so reads probe fewer files. This stage **keeps tombstones** in the merged output: dropping them would be unsafe if a crash left the new file beside the old ones, because an older put could resurrect a deleted key. The tradeoff is **write amplification** — data is rewritten on disk beyond the original user puts/deletes (`write_amplification()` = SSTable bytes written / user bytes written).

## Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run tests

```bash
pytest -q
```

## Run the CLI

```bash
python cli.py ./data
```

Commands: `put <k> <v>`, `get <k>`, `delete <k>`, `quit`.

## Benchmarks

Bloom on vs off (reads):

```bash
python bench/bench_reads.py --keys 20000 --lookups 10000 --memtable-bytes 100000 --seed 42
```

Compaction before vs after:

```bash
python bench/bench_compaction.py --keys 20000 --overwrites 20000 --deletes 2000 --lookups 10000 --memtable-bytes 100000 --seed 42
```

Results land in `bench/results/` (`stage3b_bloom.json`, `stage4_compaction.json`, etc.). Older baselines are left alone.

## Crash testing

```bash
# Quick
python bench/crash_test.py --iterations 20 --seed 42

# Full
python bench/crash_test.py --iterations 1000 --seed 42
```

The harness runs the DB in a child process, kills it with `SIGKILL` at a random time, reopens the directory, and checks that every **acknowledged** write is still present (an in-flight op may or may not be). Flushes and compactions are forced often (`memtable_max_bytes=2000`, `compaction_min_files=3`). Results go to `bench/results/crash_test.json`.

**What it proves:** no acknowledged put/delete is lost across random process kills, including while flushes and compactions are happening.

**What it does NOT prove:** `SIGKILL` is a process crash, not a power loss, and it does not check whether the drive actually persisted fsynced data (on macOS, plain `fsync` does not force the drive cache to flush). Random kills also rarely hit the tiny windows inside a flush or compaction; targeted crash points would be a future improvement.
