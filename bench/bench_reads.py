#!/usr/bin/env python3
"""Read benchmark for minilsm (sparse index Stage 3a; no Bloom filters yet)."""

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


def _run_lookups(db: DB, keys: list[str]) -> list[int]:
    latencies_ns: list[int] = []
    for key in keys:
        t0 = time.perf_counter_ns()
        db.get(key)
        latencies_ns.append(time.perf_counter_ns() - t0)
    return latencies_ns


def _summarize(
    name: str,
    lookups: int,
    latencies_ns: list[int],
    sstables_probed: int,
    disk_reads: int,
) -> dict:
    total_ns = sum(latencies_ns)
    elapsed_s = total_ns / 1e9 if total_ns else 0.0
    qps = lookups / elapsed_s if elapsed_s else 0.0
    return {
        "name": name,
        "lookups": lookups,
        "sstables_probed": sstables_probed,
        "sstables_probed_per_lookup": sstables_probed / lookups if lookups else 0.0,
        "disk_reads": disk_reads,
        "disk_reads_per_lookup": disk_reads / lookups if lookups else 0.0,
        "p50_us": _percentile_us(latencies_ns, 50),
        "p99_us": _percentile_us(latencies_ns, 99),
        "lookups_per_sec": qps,
    }


def _print_table(rows: list[dict]) -> None:
    headers = [
        ("benchmark", "name", "s"),
        ("probed", "sstables_probed", "d"),
        ("probed/lookup", "sstables_probed_per_lookup", ".2f"),
        ("disk_reads", "disk_reads", "d"),
        ("disk/lookup", "disk_reads_per_lookup", ".2f"),
        ("p50_us", "p50_us", ".1f"),
        ("p99_us", "p99_us", ".1f"),
        ("lookups/s", "lookups_per_sec", ".0f"),
    ]
    cells: list[list[str]] = []
    for row in rows:
        cells.append(
            [
                f"{row[key]:{fmt}}" if fmt != "s" else str(row[key])
                for _, key, fmt in headers
            ]
        )
    widths = [
        max(len(h[0]), max(len(r[i]) for r in cells)) for i, h in enumerate(headers)
    ]
    header_line = "  ".join(h[0].ljust(widths[i]) for i, h in enumerate(headers))
    sep = "  ".join("-" * widths[i] for i in range(len(headers)))
    print(header_line)
    print(sep)
    for row_cells in cells:
        print("  ".join(row_cells[i].ljust(widths[i]) for i in range(len(headers))))


def main() -> None:
    parser = argparse.ArgumentParser(description="minilsm Stage 3a read benchmark")
    parser.add_argument("--keys", type=int, default=20_000)
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
        f"sync_writes=False (load faster; not measuring durability). "
        f"keys={args.keys} lookups={args.lookups} "
        f"memtable_max_bytes={args.memtable_bytes} seed={args.seed}"
    )
    print(
        "Missing keys use names like key_00000_x so they sort INSIDE the real "
        "key range (forces sparse-index block reads, unlike key_ prefix misses)."
    )

    with tempfile.TemporaryDirectory(prefix="minilsm_bench_") as tmp:
        db_dir = Path(tmp)
        opts = Options(sync_writes=False, memtable_max_bytes=args.memtable_bytes)
        db = DB.open(str(db_dir), opts)

        for key in insert_order:
            db.put(key, value)
        db.flush()

        sst_count = len(list(db_dir.glob("sst_*.sst")))
        sparse_entries = sum(sst.index_entry_count() for sst in db._sstables)
        print(f"SSTables after load+flush: {sst_count}")
        print(f"Sparse index entries (all SSTables): {sparse_entries}")

        # Inside the real key range: key_00000_x sorts between key_00000 and key_00001.
        missing_keys = [f"key_{i:05d}_x" for i in range(args.lookups)]
        existing_keys = [rng.choice(keys) for _ in range(args.lookups)]

        db.stats.reset()
        missing_lat = _run_lookups(db, missing_keys)
        missing_row = _summarize(
            "missing keys",
            args.lookups,
            missing_lat,
            db.stats.sstables_probed,
            db.stats.disk_reads,
        )

        db.stats.reset()
        existing_lat = _run_lookups(db, existing_keys)
        existing_row = _summarize(
            "existing keys",
            args.lookups,
            existing_lat,
            db.stats.sstables_probed,
            db.stats.disk_reads,
        )

        print()
        _print_table([missing_row, existing_row])

        results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "settings": {
                "keys": args.keys,
                "lookups": args.lookups,
                "memtable_bytes": args.memtable_bytes,
                "seed": args.seed,
                "sstables": sst_count,
                "sparse_index_entries": sparse_entries,
                "sync_writes": False,
            },
            "benchmarks": [missing_row, existing_row],
        }

        out_dir = ROOT / "bench" / "results"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "baseline_stage3a_sparse.json"
        out_path.write_text(json.dumps(results, indent=2) + "\n")
        print(f"\nWrote {out_path}")

        db.close()


if __name__ == "__main__":
    main()
