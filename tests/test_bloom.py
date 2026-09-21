"""Stage 3b tests: BloomFilter unit + DB integration."""

from __future__ import annotations

import random
from pathlib import Path

from minilsm import DB, Options
from minilsm.bloom import BloomFilter


def test_no_false_negatives() -> None:
    bf = BloomFilter(10_000, 10)
    keys = [f"k{i}" for i in range(10_000)]
    for key in keys:
        bf.add(key)
    for key in keys:
        assert bf.might_contain(key)


def test_false_positive_rate_under_3_percent() -> None:
    bf = BloomFilter(10_000, 10)
    for i in range(10_000):
        bf.add(f"present_{i}")
    false_positives = sum(
        1 for i in range(10_000) if bf.might_contain(f"absent_{i}")
    )
    rate = false_positives / 10_000
    assert rate < 0.03, f"FP rate {rate:.4f} not under 3%"


def test_bloom_disabled_matches_and_no_skips(tmp_path: Path) -> None:
    opts_off = Options(
        memtable_max_bytes=200,
        bloom_bits_per_key=0,
        sync_writes=False,
    )
    opts_on = Options(
        memtable_max_bytes=200,
        bloom_bits_per_key=10,
        sync_writes=False,
    )

    # Build identical data twice so reopen options can differ cleanly.
    def populate(path: Path, opts: Options) -> None:
        db = DB.open(str(path), opts)
        for i in range(300):
            db.put(f"k{i:04d}", f"v{i}")
        db.delete("k0010")
        db.flush()
        db.close()

    dir_off = tmp_path / "off"
    dir_on = tmp_path / "on"
    dir_off.mkdir()
    dir_on.mkdir()
    populate(dir_off, opts_off)
    populate(dir_on, opts_on)

    db_off = DB.open(str(dir_off), opts_off)
    db_on = DB.open(str(dir_on), opts_on)

    probe_keys = [f"k{i:04d}" for i in range(300)] + [f"m{i:04d}" for i in range(100)]
    for key in probe_keys:
        assert db_off.get(key) == db_on.get(key)

    db_off.stats.reset()
    for i in range(200):
        db_off.get(f"missing_{i:04d}")
    assert db_off.stats.bloom_skips == 0
    assert db_off.stats.bloom_false_positives == 0

    db_off.close()
    db_on.close()


def test_db_random_keys_match_dict_with_bloom(tmp_path: Path) -> None:
    rng = random.Random(99)
    expected: dict[str, str | None] = {}
    db = DB.open(
        str(tmp_path),
        Options(memtable_max_bytes=400, bloom_bits_per_key=10, sync_writes=False),
    )
    for i in range(1000):
        key = f"k{rng.randrange(0, 2500):04d}"
        if rng.random() < 0.15 and key in expected and expected[key] is not None:
            db.delete(key)
            expected[key] = None
        else:
            value = f"v{i}"
            db.put(key, value)
            expected[key] = value
    db.flush()

    for key, value in expected.items():
        assert db.get(key) == value
    for _ in range(300):
        missing = f"z{rng.randrange(0, 2500):04d}"
        if missing not in expected:
            assert db.get(missing) is None
    db.close()


def test_tombstone_still_none_with_bloom(tmp_path: Path) -> None:
    db = DB.open(
        str(tmp_path),
        Options(memtable_max_bytes=50, bloom_bits_per_key=10),
    )
    db.put("gone", "was-here")
    db.flush()
    db.delete("gone")
    db.flush()
    assert db.get("gone") is None
    db.close()


def test_bloom_reduces_disk_reads_for_missing_keys(tmp_path: Path) -> None:
    keys = [f"key_{i:05d}" for i in range(2000)]
    missing = [f"key_{i:05d}_x" for i in range(1000)]

    def load(path: Path, bloom: int) -> DB:
        path.mkdir()
        opts = Options(
            memtable_max_bytes=50_000,
            bloom_bits_per_key=bloom,
            sync_writes=False,
        )
        db = DB.open(str(path), opts)
        for key in keys:
            db.put(key, "x" * 20)
        db.flush()
        db.close()
        return DB.open(str(path), opts)

    db_off = load(tmp_path / "off", 0)
    db_off.stats.reset()
    for key in missing:
        assert db_off.get(key) is None
    reads_off = db_off.stats.disk_reads
    db_off.close()

    db_on = load(tmp_path / "on", 10)
    db_on.stats.reset()
    for key in missing:
        assert db_on.get(key) is None
    reads_on = db_on.stats.disk_reads
    assert db_on.stats.bloom_skips > 0
    db_on.close()

    assert reads_on < reads_off * 0.5, (
        f"expected Bloom to cut disk reads a lot; off={reads_off} on={reads_on}"
    )
