from __future__ import annotations

import os
from pathlib import Path

from minilsm.records import DELETE, PUT, encode_record, try_parse_record


class WAL:
    """Append-only write-ahead log for crash recovery."""

    def __init__(self, path: Path, *, sync_writes: bool = True) -> None:
        self._path = path
        self._sync_writes = sync_writes
        self._file = open(path, "ab+")

    def append(self, key: str, value: str, record_type: int) -> None:
        self._file.write(encode_record(key, value, record_type))
        self._file.flush()
        # Durability switch: fsync only when sync_writes is enabled.
        if self._sync_writes:
            os.fsync(self._file.fileno())

    def close(self) -> None:
        self._file.close()

    @staticmethod
    def replay(path: Path) -> list[tuple[str, str, int]]:
        """Replay wal.log and rebuild logical records.

        A torn write at the end (incomplete record or bad CRC) is expected after
        a crash. Stop at the last good record and truncate the file there —
        do not raise, so the DB can open and keep serving.
        """
        if not path.exists():
            return []

        data = path.read_bytes()
        records: list[tuple[str, str, int]] = []
        offset = 0
        good_end = 0

        while offset < len(data):
            parsed = try_parse_record(data, offset)
            if parsed is None:
                # Torn or corrupt trailing bytes — stop at last good record.
                break
            key, value, record_type, next_offset = parsed
            records.append((key, value, record_type))
            offset = next_offset
            good_end = offset

        if good_end < len(data):
            # Truncate past the last valid record so new appends stay clean.
            with open(path, "r+b") as f:
                f.truncate(good_end)

        return records


# Re-export for callers that imported PUT/DELETE from wal.
__all__ = ["WAL", "PUT", "DELETE"]
