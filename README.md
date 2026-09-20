# minilsm — Stage 3a

A from-scratch LSM-tree key-value storage engine.

**So far:** MemTable + WAL + SSTables with a **sparse index**. No Bloom filters or compaction yet.

## How it works so far

1. **Writes** go to the WAL first, then the in-memory MemTable (durability before visibility).
2. When the MemTable reaches `memtable_max_bytes`, it is **flushed** to a new `sst_NNNNNN.sst` (write `.tmp` → fsync → rename → fsync directory), then the WAL is rotated.
3. **Reads** check the MemTable first, then SSTables from newest to oldest. Tombstones hide older values.
4. **Open** loads SSTables, then replays `wal.log` (truncating a torn tail if needed).

### Sparse index

Each SSTable keeps only every `index_interval`-th key (plus the first) in memory, as a sorted `(key, offset)` list. A lookup binary-searches that list, then reads one on-disk block between two index entries. That uses far less RAM than a full key→offset map, at the cost of scanning a small run of records per hit (and still reading a block for misses that fall inside the key range).

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

Read benchmark (missing keys sorted inside the real key range vs existing keys):

```bash
python bench/bench_reads.py --keys 20000 --lookups 10000 --memtable-bytes 100000 --seed 42
```

Results are written to `bench/results/` (e.g. `baseline_stage3a_sparse.json`). Older baselines such as `baseline_stage2.json` are left alone.
