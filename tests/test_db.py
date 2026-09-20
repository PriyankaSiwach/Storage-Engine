"""Stage 1 tests for minilsm: MemTable + WAL durability and recovery."""

from __future__ import annotations

from pathlib import Path

from minilsm import DB


def test_put_then_get(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path))
    db.put("a", "1")
    assert db.get("a") == "1"
    db.close()


def test_overwrite(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path))
    db.put("k", "old")
    db.put("k", "new")
    assert db.get("k") == "new"
    db.close()


def test_delete_returns_none(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path))
    db.put("k", "v")
    db.delete("k")
    assert db.get("k") is None
    db.close()


def test_recovery_after_reopen(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path))
    db.put("a", "1")
    db.put("b", "2")
    db.close()

    db2 = DB.open(str(tmp_path))
    assert db2.get("a") == "1"
    assert db2.get("b") == "2"
    db2.close()


def test_delete_survives_reopen(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path))
    db.put("k", "v")
    db.delete("k")
    db.close()

    db2 = DB.open(str(tmp_path))
    assert db2.get("k") is None
    db2.close()


def test_torn_write_truncates_and_recovers(tmp_path: Path) -> None:
    db = DB.open(str(tmp_path))
    db.put("a", "1")
    db.put("b", "2")
    db.close()

    wal_path = tmp_path / "wal.log"
    with open(wal_path, "ab") as f:
        f.write(b"\x00\x01\x02\xffgarbage")

    db2 = DB.open(str(tmp_path))
    assert db2.get("a") == "1"
    assert db2.get("b") == "2"
    db2.put("c", "3")
    assert db2.get("c") == "3"
    db2.close()

    db3 = DB.open(str(tmp_path))
    assert db3.get("a") == "1"
    assert db3.get("b") == "2"
    assert db3.get("c") == "3"
    db3.close()
