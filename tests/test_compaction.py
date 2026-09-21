"""Stage 4a tests: compaction merge, tombstones, crash windows."""

from __future__ import annotations

import random
import shutil
from pathlib import Path

from minilsm import DB, Options


def _opts(**kwargs) -> Options:
    base = dict(
        memtable_max_bytes=64,
        sync_writes=False,
        bloom_bits_per_key=0,
        compaction_min_files=0,
    )
    base.update(kwargs)
    return Options(**base)


def test_compact_three_sstables_leaves_one(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path), _opts(memtable_max_bytes=20))
    for i in range(3):
        db.put(f"a{i}", "x" * 30)
        db.flush()
    assert len(list(tmp_path.glob("sst_*.sst"))) == 3
    db.compact()
    assert len(list(tmp_path.glob("sst_*.sst"))) == 1
    assert len(db._sstables) == 1
    db.close()


def test_newest_value_wins_after_compact(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path), _opts(memtable_max_bytes=10))
    db.put("k", "v1")
    db.flush()
    db.put("k", "v2")
    db.flush()
    db.put("k", "v3")
    db.flush()
    db.compact()
    assert db.get("k") == "v3"
    db.close()


def test_deleted_key_stays_deleted_after_compact(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path), _opts(memtable_max_bytes=10))
    db.put("k", "v")
    db.flush()
    db.delete("k")
    db.flush()
    db.compact()
    assert db.get("k") is None
    db.close()


def test_random_ops_match_dict_after_compact_and_reopen(tmp_path: Path) -> None:
    rng = random.Random(123)
    expected: dict[str, str | None] = {}
    db = DB.open(str(tmp_path), _opts(memtable_max_bytes=200))
    keys = [f"k{i:03d}" for i in range(300)]

    for i in range(5000):
        key = rng.choice(keys)
        roll = rng.random()
        if roll < 0.2 and key in expected and expected[key] is not None:
            db.delete(key)
            expected[key] = None
        else:
            value = f"v{i}"
            db.put(key, value)
            expected[key] = value

    db.flush()
    if len(db._sstables) >= 2:
        db.compact()

    for key, value in expected.items():
        assert db.get(key) == value

    db.close()
    db2 = DB.open(str(tmp_path), _opts(memtable_max_bytes=200))
    for key, value in expected.items():
        assert db2.get(key) == value
    db2.close()


def test_compact_with_zero_or_one_sstable_is_noop(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path), _opts())
    db.compact()  # zero tables
    assert list(tmp_path.glob("sst_*.sst")) == []

    db.put("a", "1")
    db.flush()
    before = list(tmp_path.glob("sst_*.sst"))
    assert len(before) == 1
    db.compact()
    after = list(tmp_path.glob("sst_*.sst"))
    assert [p.name for p in after] == [p.name for p in before]
    db.close()


def test_crash_after_merge_before_old_deleted(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path), _opts(memtable_max_bytes=20))
    expected = {"a": "1", "b": "2", "c": "3"}
    for key, value in expected.items():
        db.put(key, value)
        db.flush()
    db.delete("b")
    db.flush()
    expected["b"] = None

    backup = tmp_path / "backup"
    backup.mkdir()
    for path in tmp_path.glob("sst_*.sst"):
        shutil.copy(path, backup / path.name)

    db.compact()
    # Simulate crash: old inputs still present alongside the merged file.
    for path in backup.iterdir():
        shutil.copy(path, tmp_path / path.name)
    db.close()

    db2 = DB.open(str(tmp_path), _opts(memtable_max_bytes=20))
    for key, value in expected.items():
        assert db2.get(key) == value
    db2.close()


def test_leftover_compaction_tmp_removed_on_open(tmp_path: Path) -> None:
    junk = tmp_path / "sst_000050.sst.tmp"
    junk.write_bytes(b"interrupted compaction")
    db = DB.open(str(tmp_path), _opts())
    assert not junk.exists()
    db.close()


def test_auto_compaction_keeps_file_count_below_limit(tmp_path: Path) -> None:
    db = DB.open(
        str(tmp_path),
        _opts(memtable_max_bytes=30, compaction_min_files=3),
    )
    for i in range(40):
        db.put(f"k{i:03d}", "x" * 20)
        n = len(list(tmp_path.glob("sst_*.sst")))
        # After each put returns, auto-compact should have run if needed.
        assert n < 3, f"too many SSTables after put {i}: {n}"
    db.close()
