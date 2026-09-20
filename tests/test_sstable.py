"""Stage 2–3a tests: SSTable flush, sparse index, recovery, crash windows."""

from __future__ import annotations

import random
import shutil
from pathlib import Path

from minilsm import DB, Options
from minilsm.sstable import SSTable


def _tiny(tmp_path: Path, max_bytes: int = 64) -> DB:
    return DB.open(str(tmp_path), Options(memtable_max_bytes=max_bytes))


def test_flush_creates_sstable_and_fresh_wal(tmp_path: Path) -> None:
    db = _tiny(tmp_path, max_bytes=20)
    db.put("hello", "world-value")  # len 5+11 = 16, may need more
    db.put("abcdef", "ghijkl")  # force over threshold
    assert list(tmp_path.glob("sst_*.sst"))
    wal = tmp_path / "wal.log"
    assert wal.exists()
    # Fresh WAL after flush should not still hold the flushed payload size.
    assert wal.stat().st_size < 100
    db.close()


def test_get_from_sstable_only(tmp_path: Path) -> None:
    db = _tiny(tmp_path, max_bytes=10)
    db.put("only_sst", "value")
    db.flush()
    assert db._memtable.get_entry("only_sst") is None
    assert db.get("only_sst") == "value"
    db.close()


def test_overwrite_after_flush_returns_memtable_value(tmp_path: Path) -> None:
    db = _tiny(tmp_path, max_bytes=10)
    db.put("k", "old")
    db.flush()
    db.put("k", "new")
    assert db.get("k") == "new"
    db.close()


def test_newer_sstable_wins_across_flushes(tmp_path: Path) -> None:
    db = _tiny(tmp_path, max_bytes=10)
    db.put("k", "v1")
    db.flush()
    db.put("k", "v2")
    db.flush()
    assert db.get("k") == "v2"
    db.close()


def test_delete_tombstone_hides_old_sstable_value(tmp_path: Path) -> None:
    db = _tiny(tmp_path, max_bytes=10)
    db.put("k", "v")
    db.flush()
    db.delete("k")
    assert db.get("k") is None
    db.flush()
    assert db.get("k") is None
    db.close()


def test_reopen_after_flushes_recovers_all(tmp_path: Path) -> None:
    db = _tiny(tmp_path, max_bytes=30)
    db.put("a", "1")
    db.put("b", "2")
    db.flush()
    db.put("c", "3")  # still in WAL / MemTable
    db.close()

    db2 = DB.open(str(tmp_path), Options(memtable_max_bytes=30))
    assert db2.get("a") == "1"
    assert db2.get("b") == "2"
    assert db2.get("c") == "3"
    db2.close()


def test_leftover_tmp_removed_on_open(tmp_path: Path) -> None:
    junk = tmp_path / "sst_000099.sst.tmp"
    junk.write_bytes(b"not a real sstable")
    db = DB.open(str(tmp_path))
    assert not junk.exists()
    db.close()


def test_crash_between_sst_rename_and_wal_delete(tmp_path: Path) -> None:
    opts = Options(memtable_max_bytes=1024)
    db = DB.open(str(tmp_path), opts)
    db.put("a", "1")
    db.put("b", "2")

    wal = tmp_path / "wal.log"
    shutil.copy(wal, tmp_path / "wal.bak")
    db.flush()
    # Simulate crash after SST is durable but before WAL was deleted.
    shutil.copy(tmp_path / "wal.bak", wal)
    db.close()

    db2 = DB.open(str(tmp_path), opts)
    assert db2.get("a") == "1"
    assert db2.get("b") == "2"
    db2.close()


def test_many_puts_with_multiple_flushes(tmp_path: Path) -> None:
    db = _tiny(tmp_path, max_bytes=200)
    n = 1000
    for i in range(n):
        db.put(f"k{i:04d}", f"v{i}")
    for i in range(n):
        assert db.get(f"k{i:04d}") == f"v{i}"
    assert len(list(tmp_path.glob("sst_*.sst"))) >= 2
    db.close()

    db2 = DB.open(str(tmp_path), Options(memtable_max_bytes=200))
    for i in range(n):
        assert db2.get(f"k{i:04d}") == f"v{i}"
    db2.close()


# --- Stage 3a: sparse index ---


def _write_sst_with_keys(
    tmp_path: Path, keys: list[str], *, index_interval: int
) -> SSTable:
    items = [(k, (f"v-{k}", False)) for k in keys]
    path = tmp_path / "sst_000001.sst"
    SSTable.write(path, items)
    return SSTable(path, index_interval=index_interval)


def test_sparse_index_entry_count(tmp_path: Path) -> None:
    keys = [f"k{i:02d}" for i in range(20)]
    sst = _write_sst_with_keys(tmp_path, keys, index_interval=4)
    assert sst.index_entry_count() == 5
    sst.close()


def test_sparse_index_finds_first_middle_last_in_block(tmp_path: Path) -> None:
    # interval=4 → blocks [0..3], [4..7], ...
    keys = [f"k{i:02d}" for i in range(20)]
    sst = _write_sst_with_keys(tmp_path, keys, index_interval=4)
    for key in ("k00", "k02", "k03"):  # first, middle, last of first block
        found, value, is_tombstone, did_read = sst.get(key)
        assert found and not is_tombstone and did_read
        assert value == f"v-{key}"
    sst.close()


def test_sparse_missing_between_index_keys_one_disk_read(tmp_path: Path) -> None:
    keys = [f"k{i:02d}" for i in range(20)]
    path = tmp_path / "data"
    path.mkdir()
    db = DB.open(str(path), Options(index_interval=4, memtable_max_bytes=10**9))
    for k in keys:
        db.put(k, f"v-{k}")
    db.flush()

    # k00..k03 indexed at k00; "k01a" sorts between k01 and k02 — in that block.
    db.stats.reset()
    assert db.get("k01a") is None
    assert db.stats.disk_reads == 1
    db.close()


def test_sparse_key_before_first_zero_disk_reads(tmp_path: Path) -> None:
    keys = [f"k{i:02d}" for i in range(8)]
    path = tmp_path / "data"
    path.mkdir()
    db = DB.open(str(path), Options(index_interval=4, memtable_max_bytes=10**9))
    for k in keys:
        db.put(k, "v")
    db.flush()

    db.stats.reset()
    assert db.get("a") is None  # before k00
    assert db.stats.disk_reads == 0
    db.close()


def test_sparse_key_after_last_not_found(tmp_path: Path) -> None:
    keys = [f"k{i:02d}" for i in range(8)]
    sst = _write_sst_with_keys(tmp_path, keys, index_interval=4)
    found, value, is_tombstone, did_read = sst.get("zzz")
    assert not found and value is None and not is_tombstone and did_read
    sst.close()


def test_sparse_tombstone_hides_key(tmp_path: Path) -> None:
    path = tmp_path / "t.sst"
    SSTable.write(
        path,
        [
            ("a", ("1", False)),
            ("b", ("", True)),
            ("c", ("3", False)),
        ],
    )
    sst = SSTable(path, index_interval=2)
    found, value, is_tombstone, _ = sst.get("b")
    assert found and is_tombstone and value is None
    sst.close()


def test_sparse_matches_dict_for_random_keys(tmp_path: Path) -> None:
    rng = random.Random(7)
    expected: dict[str, str] = {}
    db = DB.open(
        str(tmp_path),
        Options(index_interval=4, memtable_max_bytes=500),
    )
    for i in range(1000):
        key = f"k{rng.randrange(0, 2000):04d}"
        value = f"v{i}"
        expected[key] = value
        db.put(key, value)
    db.flush()

    for key, value in expected.items():
        assert db.get(key) == value
    for _ in range(200):
        missing = f"m{rng.randrange(0, 2000):04d}"
        if missing not in expected:
            assert db.get(missing) is None
    db.close()
