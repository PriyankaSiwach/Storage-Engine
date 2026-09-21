#!/usr/bin/env python3
"""Read benchmark for minilsm Stage 3b: Bloom on vs off, same workload."""

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
    db: DB,
) -> dict:
    total_ns = sum(latencies_ns)
    elapsed_s = total_ns / 1e9 if total_ns else 0.0
    qps = lookups / elapsed_s if elapsed_s else 0.0
    probed = db.stats.sstables_probed
    disk_reads = db.stats.disk_reads
    bloom_skips = db.stats.bloom_skips
    bloom_fps = db.stats.bloom_false_positives
    # Observed FP rate (missing-key runs): among SSTable checks where the Bloom
    # filter said "maybe" (did not skip), what fraction were false positives?
    # Denominator = probes that passed the filter = sstables_probed - bloom_skips.
    maybe_checks = probed - bloom_skips
    observed_fp_rate = (bloom_fps / maybe_checks) if maybe_checks else 0.0
    return {
        "name": name,
        "lookups": lookups,
        "sstables_probed": probed,
        "sstables_probed_per_lookup": probed / lookups if lookups else 0.0,
        "disk_reads": disk_reads,
        "disk_reads_per_lookup": disk_reads / lookups if lookups else 0.0,
        "bloom_skips": bloom_skips,
        "bloom_false_positives": bloom_fps,
        "observed_fp_rate": observed_fp_rate,
        "p50_us": _percentile_us(latencies_ns, 50),
        "p99_us": _percentile_us(latencies_ns, 99),
        "lookups_per_sec": qps,
    }


def _print_table(title: str, rows: list[dict]) -> None:
    print(f"\n=== {title} ===")
    headers = [
        ("benchmark", "name", "s"),
        ("probed", "sstables_probed", "d"),
        ("disk_reads", "disk_reads", "d"),
        ("disk/lookup", "disk_reads_per_lookup", ".2f"),
        ("bloom_skips", "bloom_skips", "d"),
        ("bloom_fp", "bloom_false_positives", "d"),
        ("fp_rate", "observed_fp_rate", ".4f"),
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


def _run_pair(
    db: DB,
    missing_keys: list[str],
    existing_keys: list[str],
) -> tuple[dict, dict]:
    db.stats.reset()
    missing_lat = _run_lookups(db, missing_keys)
    missing_row = _summarize("missing keys", len(missing_keys), missing_lat, db)

    db.stats.reset()
    existing_lat = _run_lookups(db, existing_keys)
    existing_row = _summarize("existing keys", len(existing_keys), existing_lat, db)
    return missing_row, existing_row


def main() -> None:
    parser = argparse.ArgumentParser(description="minilsm Stage 3b Bloom benchmark")
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

    # Same missing/existing lists for both Bloom settings.
    missing_keys = [f"key_{i:05d}_x" for i in range(args.lookups)]
    existing_keys = [rng.choice(keys) for _ in range(args.lookups)]

    print(
        f"sync_writes=False (load faster; not measuring durability). "
        f"keys={args.keys} lookups={args.lookups} "
        f"memtable_max_bytes={args.memtable_bytes} seed={args.seed}"
    )
    print(
        "Missing keys use names like key_00000_x so they sort INSIDE the real "
        "key range (same workload as Stage 3a)."
    )
    print("Running twice: bloom_bits_per_key=0 then bloom_bits_per_key=10.")

    with tempfile.TemporaryDirectory(prefix="minilsm_bench_") as tmp:
        db_dir = Path(tmp)
        # Write once; Bloom is rebuilt from keys on open, so we can reopen on/off.
        write_opts = Options(
            sync_writes=False,
            memtable_max_bytes=args.memtable_bytes,
            bloom_bits_per_key=0,
        )
        db = DB.open(str(db_dir), write_opts)
        for key in insert_order:
            db.put(key, value)
        db.flush()
        db.close()

        sst_count = len(list(db_dir.glob("sst_*.sst")))
        results_by_mode: dict[str, dict] = {}

        for bloom_bits in (0, 10):
            opts = Options(
                sync_writes=False,
                memtable_max_bytes=args.memtable_bytes,
                bloom_bits_per_key=bloom_bits,
            )
            db = DB.open(str(db_dir), opts)
            sparse_entries = sum(sst.index_entry_count() for sst in db._sstables)
            bloom_bytes = sum(sst.bloom_size_bytes() for sst in db._sstables)

            missing_row, existing_row = _run_pair(db, missing_keys, existing_keys)
            label = f"bloom_bits_per_key={bloom_bits}"
            _print_table(label, [missing_row, existing_row])
            print(
                f"sparse_index_entries={sparse_entries}  "
                f"bloom_memory_bytes={bloom_bytes}"
            )

            results_by_mode[label] = {
                "bloom_bits_per_key": bloom_bits,
                "sstables": sst_count,
                "sparse_index_entries": sparse_entries,
                "bloom_memory_bytes": bloom_bytes,
                "benchmarks": [missing_row, existing_row],
            }
            db.close()

        results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "settings": {
                "keys": args.keys,
                "lookups": args.lookups,
                "memtable_bytes": args.memtable_bytes,
                "seed": args.seed,
                "sstables": sst_count,
                "sync_writes": False,
            },
            "modes": results_by_mode,
        }

        out_dir = ROOT / "bench" / "results"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "stage3b_bloom.json"
        out_path.write_text(json.dumps(results, indent=2) + "\n")
        print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
