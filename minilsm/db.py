from __future__ import annotations

import heapq
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from minilsm.memtable import MemTable
from minilsm.options import Options
from minilsm.records import DELETE, PUT
from minilsm.sstable import SSTable, list_sst_numbers, sst_path
from minilsm.wal import WAL


@dataclass
class Stats:
    """Runtime counters for instrumentation (reads, Bloom, compaction, WA)."""

    sstables_probed: int = 0
    disk_reads: int = 0
    bloom_skips: int = 0
    bloom_false_positives: int = 0
    compactions: int = 0
    sstable_bytes_written: int = 0
    user_bytes_written: int = 0

    def reset(self) -> None:
        # Resets everything. Read benchmarks that also need write_amplification
        # should zero only the read counters (probed/disk/bloom), not these totals.
        self.sstables_probed = 0
        self.disk_reads = 0
        self.bloom_skips = 0
        self.bloom_false_positives = 0
        self.compactions = 0
        self.sstable_bytes_written = 0
        self.user_bytes_written = 0


class DB:
    """Minimal durable key-value store: MemTable + WAL + SSTables (Stage 4a)."""

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
            SSTable(
                sst_path(path, n),
                index_interval=opts.index_interval,
                bloom_bits_per_key=opts.bloom_bits_per_key,
            )
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
        self.stats.user_bytes_written += len(key) + len(value)
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
            (
                found,
                value,
                is_tombstone,
                did_disk_read,
                bloom_skip,
                bloom_fp,
            ) = sst.get(key)
            if bloom_skip:
                self.stats.bloom_skips += 1
            if bloom_fp:
                self.stats.bloom_false_positives += 1
            if did_disk_read:
                self.stats.disk_reads += 1
            if found:
                return None if is_tombstone else value
        return None

    def delete(self, key: str) -> None:
        self._ensure_open()
        self.stats.user_bytes_written += len(key) + len("")
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
        written = SSTable.write(path, items)
        self.stats.sstable_bytes_written += written
        self._sstables.append(
            SSTable(
                path,
                index_interval=self._options.index_interval,
                bloom_bits_per_key=self._options.bloom_bits_per_key,
            )
        )
        self._next_sst_number += 1

        # SSTable is on disk and renamed; safe to drop the WAL that mirrored it.
        self._wal.close()
        wal_path = self._dir / "wal.log"
        wal_path.unlink(missing_ok=True)
        self._wal = WAL(wal_path, sync_writes=self._options.sync_writes)
        self._memtable.clear()
        self._maybe_compact()

    def compact(self) -> None:
        """Merge all SSTables into one, keeping the newest value per key.

        Tombstones are kept on purpose: if we dropped them and a crash happened
        between writing the merged file and deleting the old files, an older
        put in a surviving SSTable could resurrect a deleted key.
        MemTable and WAL are not touched.
        """
        self._ensure_open()
        if len(self._sstables) < 2:
            return

        # Tag each stream so heapq.merge orders equal keys with newest file first.
        # self._sstables is oldest -> newest; negate the index for that order.
        def tagged(sst: SSTable, index: int) -> Iterator[tuple[str, int, str, int]]:
            for key, value, record_type in sst.iter_records():
                yield key, -index, value, record_type

        streams = [
            tagged(sst, i) for i, sst in enumerate(self._sstables)
        ]

        def merged_records() -> Iterator[tuple[str, str, int]]:
            last_key: str | None = None
            for key, _neg_idx, value, record_type in heapq.merge(*streams):
                if last_key is not None and key == last_key:
                    continue  # older duplicate; newest already emitted
                last_key = key
                # Keep tombstones — see compact() docstring.
                yield key, value, record_type

        out_path = sst_path(self._dir, self._next_sst_number)
        written = SSTable.write_records(out_path, merged_records())
        self.stats.sstable_bytes_written += written
        self.stats.compactions += 1

        # New file is durable; only then remove the inputs.
        old_tables = self._sstables
        new_table = SSTable(
            out_path,
            index_interval=self._options.index_interval,
            bloom_bits_per_key=self._options.bloom_bits_per_key,
        )
        self._sstables = [new_table]
        self._next_sst_number += 1

        for sst in old_tables:
            path = sst.path
            sst.close()
            path.unlink(missing_ok=True)

    def write_amplification(self) -> float:
        if self.stats.user_bytes_written == 0:
            return 0.0
        return self.stats.sstable_bytes_written / self.stats.user_bytes_written

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

    def _maybe_compact(self) -> None:
        min_files = self._options.compaction_min_files
        if min_files > 0 and len(self._sstables) >= min_files:
            self.compact()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("DB is closed")
