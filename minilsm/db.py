from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from minilsm.memtable import MemTable
from minilsm.options import Options
from minilsm.records import DELETE, PUT
from minilsm.sstable import SSTable, list_sst_numbers, sst_path
from minilsm.wal import WAL


@dataclass
class Stats:
    """Runtime counters for instrumentation (e.g. Bloom filter impact)."""

    sstables_probed: int = 0
    disk_reads: int = 0

    def reset(self) -> None:
        self.sstables_probed = 0
        self.disk_reads = 0


class DB:
    """Minimal durable key-value store: MemTable + WAL + SSTables (Stage 3a)."""

    def __init__(
        self,
        dir_path: Path,
        options: Options,
        memtable: MemTable,
        wal: WAL,
        sstables: list[SSTable],
        next_sst_number: int,
    ) -> None:
        self._dir = dir_path
        self._options = options
        self._memtable = memtable
        self._wal = wal
        self._sstables = sstables  # oldest -> newest
        self._next_sst_number = next_sst_number
        self._closed = False
        self.stats = Stats()

    @classmethod
    def open(cls, dir_path: str, options: Options | None = None) -> DB:
        opts = options if options is not None else Options()
        path = Path(dir_path)
        path.mkdir(parents=True, exist_ok=True)

        # Incomplete atomic writes leave *.tmp; discard them on open.
        for tmp in path.glob("*.tmp"):
            tmp.unlink()

        numbers = list_sst_numbers(path)
        sstables = [
            SSTable(sst_path(path, n), index_interval=opts.index_interval)
            for n in numbers
        ]
        next_sst_number = (numbers[-1] + 1) if numbers else 1

        wal_path = path / "wal.log"
        memtable = MemTable()

        # Replay WAL after loading SSTables so MemTable holds only unflushed work.
        for key, value, record_type in WAL.replay(wal_path):
            if record_type == PUT:
                memtable.put(key, value)
            elif record_type == DELETE:
                memtable.delete(key)

        wal = WAL(wal_path, sync_writes=opts.sync_writes)
        return cls(path, opts, memtable, wal, sstables, next_sst_number)

    def put(self, key: str, value: str) -> None:
        self._ensure_open()
        # WAL first, then MemTable: a crash mid-update still recovers from the log.
        self._wal.append(key, value, PUT)
        self._memtable.put(key, value)
        self._maybe_flush()

    def get(self, key: str) -> str | None:
        self._ensure_open()

        entry = self._memtable.get_entry(key)
        if entry is not None:
            # MemTable is newest; a tombstone here must hide older SST values.
            value, is_tombstone = entry
            return None if is_tombstone else value

        # Newer SSTables override older ones for the same key.
        for sst in reversed(self._sstables):
            self.stats.sstables_probed += 1
            found, value, is_tombstone, did_disk_read = sst.get(key)
            if did_disk_read:
                self.stats.disk_reads += 1
            if found:
                return None if is_tombstone else value
        return None

    def delete(self, key: str) -> None:
        self._ensure_open()
        # Same ordering as put: durable tombstone in WAL before MemTable update.
        self._wal.append(key, "", DELETE)
        self._memtable.delete(key)
        self._maybe_flush()

    def flush(self) -> None:
        """Flush MemTable to a new SSTable, then rotate the WAL.

        The SSTable must be fully durable before wal.log is deleted. Otherwise a
        crash could lose data that lived only in the WAL.
        """
        self._ensure_open()
        items = self._memtable.sorted_items()
        if not items:
            return

        path = sst_path(self._dir, self._next_sst_number)
        SSTable.write(path, items)
        self._sstables.append(
            SSTable(path, index_interval=self._options.index_interval)
        )
        self._next_sst_number += 1

        # SSTable is on disk and renamed; safe to drop the WAL that mirrored it.
        self._wal.close()
        wal_path = self._dir / "wal.log"
        wal_path.unlink(missing_ok=True)
        self._wal = WAL(wal_path, sync_writes=self._options.sync_writes)
        self._memtable.clear()

    def close(self) -> None:
        if self._closed:
            return
        self._wal.close()
        for sst in self._sstables:
            sst.close()
        self._closed = True

    def _maybe_flush(self) -> None:
        if self._memtable.approximate_size() >= self._options.memtable_max_bytes:
            self.flush()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("DB is closed")
