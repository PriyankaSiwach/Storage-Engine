#!/usr/bin/env python3
"""Compaction before/after read benchmark (Stage 4a)."""

from __future__ import annotations

import argparse
import json
import random
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from minilsm import DB, Options  # noqa: E402


def _percentile_us(latencies_ns: list[int], p: float) -> float:
    if not latencies_ns:
        return 0.0
    ordered = sorted(latencies_ns)
    rank = (p / 100.0) * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    ns = ordered[lo] * (1.0 - frac) + ordered[hi] * frac
    return ns / 1000.0


def _reset_read_stats(db: DB) -> None:
    # Keep write/compaction totals for write_amplification(); only clear reads.
    db.stats.sstables_probed = 0
    db.stats.disk_reads = 0
    db.stats.bloom_skips = 0
    db.stats.bloom_false_positives = 0


def _run_lookups(db: DB, keys: list[str]) -> list[int]:
    latencies_ns: list[int] = []
    for key in keys:
        t0 = time.perf_counter_ns()
        db.get(key)
        latencies_ns.append(time.perf_counter_ns() - t0)
    return latencies_ns


def _summarize(name: str, lookups: int, latencies_ns: list[int], db: DB) -> dict:
    total_ns = sum(latencies_ns)
    elapsed_s = total_ns / 1e9 if total_ns else 0.0
    qps = lookups / elapsed_s if elapsed_s else 0.0
    probed = db.stats.sstables_probed
    disk_reads = db.stats.disk_reads
    return {
        "name": name,
        "lookups": lookups,
        "sstables_probed": probed,
        "sstables_probed_per_lookup": probed / lookups if lookups else 0.0,
        "disk_reads": disk_reads,
        "disk_reads_per_lookup": disk_reads / lookups if lookups else 0.0,
        "bloom_skips": db.stats.bloom_skips,
        "p50_us": _percentile_us(latencies_ns, 50),
        "p99_us": _percentile_us(latencies_ns, 99),
        "lookups_per_sec": qps,
    }


def _sst_bytes(db_dir: Path) -> int:
    return sum(p.stat().st_size for p in db_dir.glob("sst_*.sst"))


def _measure_reads(
    db: DB, missing_keys: list[str], existing_keys: list[str]
) -> list[dict]:
    _reset_read_stats(db)
    missing_lat = _run_lookups(db, missing_keys)
    missing_row = _summarize("missing keys", len(missing_keys), missing_lat, db)

    _reset_read_stats(db)
    existing_lat = _run_lookups(db, existing_keys)
    existing_row = _summarize("existing keys", len(existing_keys), existing_lat, db)
    return [missing_row, existing_row]


def _print_table(title: str, rows: list[dict], *, sst_files: int, sst_bytes: int) -> None:
    print(f"\n=== {title} (sst_files={sst_files}, sst_bytes={sst_bytes}) ===")
    headers = [
        ("benchmark", "name", "s"),
        ("probed/lookup", "sstables_probed_per_lookup", ".2f"),
        ("disk/lookup", "disk_reads_per_lookup", ".2f"),
        ("bloom_skips", "bloom_skips", "d"),
        ("p50_us", "p50_us", ".1f"),
        ("p99_us", "p99_us", ".1f"),
        ("lookups/s", "lookups_per_sec", ".0f"),
    ]
    cells = [
        [
            f"{row[key]:{fmt}}" if fmt != "s" else str(row[key])
            for _, key, fmt in headers
        ]
        for row in rows
    ]
    widths = [
        max(len(h[0]), max(len(r[i]) for r in cells)) for i, h in enumerate(headers)
    ]
    print("  ".join(h[0].ljust(widths[i]) for i, h in enumerate(headers)))
    print("  ".join("-" * widths[i] for i in range(len(headers))))
    for row_cells in cells:
        print("  ".join(row_cells[i].ljust(widths[i]) for i in range(len(headers))))


def main() -> None:
    parser = argparse.ArgumentParser(description="minilsm Stage 4a compaction bench")
    parser.add_argument("--keys", type=int, default=20_000)
    parser.add_argument("--overwrites", type=int, default=20_000)
    parser.add_argument("--deletes", type=int, default=2_000)
    parser.add_argument("--lookups", type=int, default=10_000)
    parser.add_argument("--memtable-bytes", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    value = "x" * 100
    keys = [f"key_{i:05d}" for i in range(args.keys)]
    insert_order = keys[:]
    rng.shuffle(insert_order)

    print(
        f"sync_writes=False bloom=10 compaction disabled during load. "
        f"keys={args.keys} overwrites={args.overwrites} deletes={args.deletes} "
        f"lookups={args.lookups} memtable_max_bytes={args.memtable_bytes} seed={args.seed}"
    )

    with tempfile.TemporaryDirectory(prefix="minilsm_compact_bench_") as tmp:
        db_dir = Path(tmp)
        opts = Options(
            sync_writes=False,
            memtable_max_bytes=args.memtable_bytes,
            bloom_bits_per_key=10,
            compaction_min_files=0,
        )
        db = DB.open(str(db_dir), opts)

        for key in insert_order:
            db.put(key, value)
        for _ in range(args.overwrites):
            db.put(rng.choice(keys), "y" * 100)
        deleted: set[str] = set()
        while len(deleted) < args.deletes:
            key = rng.choice(keys)
            if key not in deleted:
                db.delete(key)
                deleted.add(key)
        db.flush()

        live_keys = [k for k in keys if k not in deleted]
        missing_keys = [f"key_{i:05d}_x" for i in range(args.lookups)]
        existing_keys = [rng.choice(live_keys) for _ in range(args.lookups)]

        before_files = len(list(db_dir.glob("sst_*.sst")))
        before_bytes = _sst_bytes(db_dir)
        before_rows = _measure_reads(db, missing_keys, existing_keys)
        _print_table("BEFORE compact", before_rows, sst_files=before_files, sst_bytes=before_bytes)

        t0 = time.perf_counter()
        db.compact()
        compact_s = time.perf_counter() - t0

        after_files = len(list(db_dir.glob("sst_*.sst")))
        after_bytes = _sst_bytes(db_dir)
        after_rows = _measure_reads(db, missing_keys, existing_keys)
        _print_table("AFTER compact", after_rows, sst_files=after_files, sst_bytes=after_bytes)

        wa = db.write_amplification()
        print(f"\ncompaction_time_s={compact_s:.4f}")
        print(f"sst_bytes_before={before_bytes}  sst_bytes_after={after_bytes}")
        print(f"write_amplification={wa:.3f}")
        print(
            f"user_bytes_written={db.stats.user_bytes_written}  "
            f"sstable_bytes_written={db.stats.sstable_bytes_written}  "
            f"compactions={db.stats.compactions}"
        )

        results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "settings": {
                "keys": args.keys,
                "overwrites": args.overwrites,
                "deletes": args.deletes,
                "lookups": args.lookups,
                "memtable_bytes": args.memtable_bytes,
                "seed": args.seed,
                "sync_writes": False,
                "bloom_bits_per_key": 10,
                "compaction_min_files": 0,
            },
            "before": {
                "sst_files": before_files,
                "sst_bytes": before_bytes,
                "benchmarks": before_rows,
            },
            "after": {
                "sst_files": after_files,
                "sst_bytes": after_bytes,
                "benchmarks": after_rows,
            },
            "compaction_time_s": compact_s,
            "write_amplification": wa,
            "user_bytes_written": db.stats.user_bytes_written,
            "sstable_bytes_written": db.stats.sstable_bytes_written,
            "compactions": db.stats.compactions,
        }

        out_dir = ROOT / "bench" / "results"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "stage4_compaction.json"
        out_path.write_text(json.dumps(results, indent=2) + "\n")
        print(f"\nWrote {out_path}")

        db.close()


if __name__ == "__main__":
    main()
