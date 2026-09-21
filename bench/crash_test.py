#!/usr/bin/env python3
"""Crash-recovery harness: kill the DB mid-write and check acknowledged ops survive.

Parent and child share op(seed, n) so both agree on the operation sequence.
Protocol on child stdout (unbuffered via os.write):
  I <n>  — about to start operation n
  A <n>  — operation n returned
  C <c>  — compaction count after an ack if it increased
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from minilsm import DB, Options  # noqa: E402

NUM_KEYS = 200


def op(seed: int, n: int) -> tuple[str, str, str | None]:
    """Return operation n for this seed: ("put", key, value) or ("delete", key, None)."""
    rng = random.Random((seed << 20) + n)
    key = f"key_{rng.randrange(NUM_KEYS):03d}"
    if rng.random() < 0.20:
        return "delete", key, None
    # 100-character value that embeds n so every put is unique.
    value = f"{n:010d}" + ("v" * 90)
    return "put", key, value


def apply_ops(seed: int, last_inclusive: int) -> dict[str, str]:
    """Apply operations 0..last_inclusive to a plain dict (delete removes the key)."""
    state: dict[str, str] = {}
    if last_inclusive < 0:
        return state
    for n in range(last_inclusive + 1):
        kind, key, value = op(seed, n)
        if kind == "put":
            assert value is not None
            state[key] = value
        else:
            state.pop(key, None)
    return state


def diff_keys(
    db: DB, expected: dict[str, str], *, limit: int = 10
) -> list[dict]:
    diffs: list[dict] = []
    for i in range(NUM_KEYS):
        key = f"key_{i:03d}"
        want = expected.get(key)
        got = db.get(key)
        if got != want:
            diffs.append({"key": key, "expected": want, "actual": got})
            if len(diffs) >= limit:
                break
    return diffs


def matches_state(db: DB, expected: dict[str, str]) -> bool:
    for i in range(NUM_KEYS):
        key = f"key_{i:03d}"
        if db.get(key) != expected.get(key):
            return False
    return True


def parse_child_output(text: str) -> tuple[int, bool, int]:
    last_acked = -1
    started: set[int] = set()
    acked: set[int] = set()
    compactions_seen = 0
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 2:
            continue
        tag, num_s = parts
        try:
            num = int(num_s)
        except ValueError:
            continue
        if tag == "I":
            started.add(num)
        elif tag == "A":
            acked.add(num)
            if num > last_acked:
                last_acked = num
        elif tag == "C":
            compactions_seen = num
    in_flight = (last_acked + 1) in started and (last_acked + 1) not in acked
    return last_acked, in_flight, compactions_seen


def run_child(dir_path: str, seed: int) -> None:
    opts = Options(
        sync_writes=True,
        memtable_max_bytes=2000,
        bloom_bits_per_key=10,
        compaction_min_files=3,
    )
    db = DB.open(dir_path, opts)
    prev_compactions = db.stats.compactions
    n = 0
    while True:
        os.write(1, f"I {n}\n".encode())
        kind, key, value = op(seed, n)
        if kind == "put":
            assert value is not None
            db.put(key, value)
        else:
            db.delete(key)
        os.write(1, f"A {n}\n".encode())
        if db.stats.compactions > prev_compactions:
            prev_compactions = db.stats.compactions
            os.write(1, f"C {prev_compactions}\n".encode())
        n += 1


def run_one_iteration(
    iteration: int, base_seed: int
) -> tuple[bool, dict]:
    seq_seed = base_seed + iteration
    rng = random.Random(seq_seed)
    work_dir = Path(tempfile.mkdtemp(prefix=f"minilsm_crash_{iteration}_"))
    info: dict = {
        "iteration": iteration,
        "seq_seed": seq_seed,
        "dir": str(work_dir),
        "passed": False,
    }

    child = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child",
            str(work_dir),
            "--seed",
            str(seq_seed),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(ROOT),
    )
    sleep_s = rng.uniform(0.030, 0.400)
    time.sleep(sleep_s)
    child.send_signal(signal.SIGKILL)
    child.wait()
    stdout = child.stdout.read().decode("utf-8", errors="replace") if child.stdout else ""
    info["sleep_s"] = sleep_s

    last_acked, in_flight, compactions_seen = parse_child_output(stdout)
    info["last_acked"] = last_acked
    info["in_flight"] = in_flight
    info["compactions_seen"] = compactions_seen

    sst_count = len(list(work_dir.glob("sst_*.sst")))
    had_tmp_at_kill = any(work_dir.glob("*.tmp"))
    info["sst_count_at_kill"] = sst_count
    info["had_tmp_at_kill"] = had_tmp_at_kill

    try:
        db = DB.open(str(work_dir))
    except Exception as exc:  # noqa: BLE001 — harness must catch open failures
        info["failure"] = f"could not open: {exc}"
        info["kept_dir"] = str(work_dir)
        print(f"FAILURE iter={iteration}: could not open ({exc}); kept {work_dir}")
        return False, info

    state_a = apply_ops(seq_seed, last_acked)
    match_a = matches_state(db, state_a)
    match_b = False
    state_b: dict[str, str] | None = None
    if in_flight:
        state_b = apply_ops(seq_seed, last_acked + 1)
        match_b = matches_state(db, state_b)

    if not (match_a or match_b):
        diffs_a = diff_keys(db, state_a)
        payload: dict = {
            "failure": "state mismatch",
            "last_acked": last_acked,
            "in_flight": in_flight,
            "diffs_vs_A": diffs_a,
        }
        if state_b is not None:
            payload["diffs_vs_B"] = diff_keys(db, state_b)
        info.update(payload)
        info["kept_dir"] = str(work_dir)
        print(
            f"FAILURE iter={iteration} last_acked={last_acked} "
            f"in_flight={in_flight}; kept {work_dir}"
        )
        for d in diffs_a:
            print(f"  {d['key']}: expected={d['expected']!r} actual={d['actual']!r}")
        db.close()
        return False, info

    winning = state_a if match_a else state_b
    assert winning is not None

    # After reopen: leftover .tmp from interrupted writes must be gone.
    remaining_tmp = list(work_dir.glob("*.tmp"))
    if remaining_tmp:
        info["failure"] = f".tmp remained after open: {[p.name for p in remaining_tmp]}"
        info["kept_dir"] = str(work_dir)
        print(f"FAILURE iter={iteration}: {info['failure']}; kept {work_dir}")
        db.close()
        return False, info

    # New put + reopen must still be consistent (recovery is repeatable).
    post_value = "post_recovery_" + ("p" * 86)
    db.put("key_000", post_value)
    winning = dict(winning)
    winning["key_000"] = post_value
    db.close()

    try:
        db2 = DB.open(str(work_dir))
    except Exception as exc:  # noqa: BLE001
        info["failure"] = f"could not reopen after post put: {exc}"
        info["kept_dir"] = str(work_dir)
        print(f"FAILURE iter={iteration}: {info['failure']}; kept {work_dir}")
        return False, info

    if not matches_state(db2, winning):
        diffs = diff_keys(db2, winning)
        info["failure"] = "mismatch after post-recovery put/reopen"
        info["diffs"] = diffs
        info["kept_dir"] = str(work_dir)
        print(f"FAILURE iter={iteration}: {info['failure']}; kept {work_dir}")
        for d in diffs:
            print(f"  {d['key']}: expected={d['expected']!r} actual={d['actual']!r}")
        db2.close()
        return False, info

    db2.close()
    info["passed"] = True
    shutil.rmtree(work_dir, ignore_errors=True)
    return True, info


def run_parent(iterations: int, seed: int) -> int:
    t0 = time.perf_counter()
    passed = 0
    failed = 0
    total_acked = 0
    with_compaction = 0
    with_sst = 0
    failures: list[dict] = []

    for i in range(iterations):
        ok, info = run_one_iteration(i, seed)
        total_acked += max(0, info.get("last_acked", -1) + 1)
        if info.get("compactions_seen", 0) > 0:
            with_compaction += 1
        if info.get("sst_count_at_kill", 0) > 0:
            with_sst += 1
        if ok:
            passed += 1
        else:
            failed += 1
            failures.append(info)

    elapsed = time.perf_counter() - t0
    avg_acked = total_acked / iterations if iterations else 0.0

    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "iterations": iterations,
            "seed": seed,
            "child_options": {
                "sync_writes": True,
                "memtable_max_bytes": 2000,
                "bloom_bits_per_key": 10,
                "compaction_min_files": 3,
            },
        },
        "iterations_run": iterations,
        "passed": passed,
        "failed": failed,
        "average_acked_ops_per_iteration": avg_acked,
        "iterations_with_compaction_before_kill": with_compaction,
        "iterations_with_sstable_at_kill": with_sst,
        "runtime_s": elapsed,
        "failures": failures,
    }

    print()
    print(f"iterations={iterations} passed={passed} failed={failed}")
    print(f"avg_acked_ops={avg_acked:.1f}")
    print(f"iterations_with_compaction={with_compaction}")
    print(f"iterations_with_sstable={with_sst}")
    print(f"runtime_s={elapsed:.2f}")

    out_dir = ROOT / "bench" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "crash_test.json"
    out_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Wrote {out_path}")

    return 1 if failed else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="minilsm crash-recovery harness")
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--child", type=str, default=None, help="child mode: DB directory")
    args = parser.parse_args()

    if args.child is not None:
        run_child(args.child, args.seed)
        return

    raise SystemExit(run_parent(args.iterations, args.seed))


if __name__ == "__main__":
    main()
