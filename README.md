# minilsm — Stage 3b

A from-scratch LSM-tree key-value storage engine.

**So far:** MemTable + WAL + SSTables with a **sparse index** and per-SSTable **Bloom filters**. No compaction yet.

## How it works so far

1. **Writes** go to the WAL first, then the in-memory MemTable (durability before visibility).
2. When the MemTable reaches `memtable_max_bytes`, it is **flushed** to a new `sst_NNNNNN.sst` (write `.tmp` → fsync → rename → fsync directory), then the WAL is rotated.
3. **Reads** check the MemTable first, then SSTables from newest to oldest. Tombstones hide older values.
4. **Open** loads SSTables, then replays `wal.log` (truncating a torn tail if needed).

### Sparse index

Each SSTable keeps only every `index_interval`-th key (plus the first) in memory, as a sorted `(key, offset)` list. A lookup binary-searches that list, then reads one on-disk block between two index entries. That uses far less RAM than a full key→offset map, at the cost of scanning a small run of records per hit (and still reading a block for misses that fall inside the key range).

### Bloom filters

Before touching disk, each SSTable can ask a Bloom filter whether a key is **definitely absent**. If so, that SSTable is skipped (no block read). Bloom filters never produce false negatives (every inserted key, including tombstones, is remembered), but they can produce **false positives**: the filter says "maybe" and we still read a block only to find the key is not there. `bloom_bits_per_key` trades memory for a lower false-positive rate; set it to `0` to disable filters for A/B measurements.

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

Same workload with Bloom off then on (results side by side):

```bash
python bench/bench_reads.py --keys 20000 --lookups 10000 --memtable-bytes 100000 --seed 42
```

Results are written to `bench/results/stage3b_bloom.json`. Older baselines (`baseline_stage2.json`, `baseline_stage3a_sparse.json`) are left alone.
