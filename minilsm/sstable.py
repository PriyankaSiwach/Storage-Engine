from __future__ import annotations

import bisect
import os
import re
from pathlib import Path

from minilsm.bloom import BloomFilter
from minilsm.records import DELETE, PUT, encode_record, parse_record

_SST_NAME = re.compile(r"^sst_(\d+)\.sst$")


def sst_path(dir_path: Path, number: int) -> Path:
    return dir_path / f"sst_{number:06d}.sst"


def list_sst_numbers(dir_path: Path) -> list[int]:
    numbers: list[int] = []
    for path in dir_path.iterdir():
        match = _SST_NAME.match(path.name)
        if match:
            numbers.append(int(match.group(1)))
    return sorted(numbers)


class SSTable:
    """Immutable sorted table on disk with a sparse index and optional Bloom filter."""

    def __init__(
        self,
        path: Path,
        *,
        index_interval: int = 16,
        bloom_bits_per_key: int = 10,
    ) -> None:
        if index_interval < 1:
            raise ValueError("index_interval must be >= 1")
        self.path = path
        self._index_interval = index_interval
        self._bloom_bits_per_key = bloom_bits_per_key
        self._file = open(path, "rb")
        # Sparse index: every index_interval-th record (always includes the first).
        self._index: list[tuple[str, int]] = []
        self._index_keys: list[str] = []
        self._bloom: BloomFilter | None = None
        self._file_size = path.stat().st_size
        self._build_index()

    @staticmethod
    def write(path: Path, sorted_items: list[tuple[str, tuple[str, bool]]]) -> None:
        """Atomically write an SSTable.

        Write to path+".tmp", fsync, then rename. A crash mid-write leaves only
        a .tmp that open() can delete — never a half-written final SSTable.
        """
        tmp_path = Path(str(path) + ".tmp")
        with open(tmp_path, "wb") as f:
            for key, (value, is_tombstone) in sorted_items:
                # Tombstones must be persisted so deletes hide older SST values.
                record_type = DELETE if is_tombstone else PUT
                payload = value if not is_tombstone else ""
                f.write(encode_record(key, payload, record_type))
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, path)
        # Make the rename durable: without a directory fsync, a crash can lose
        # the new directory entry even though the file data was synced.
        _fsync_directory(path.parent)

    def _build_index(self) -> None:
        data = self._file.read()
        offset = 0
        record_num = 0
        all_keys: list[str] = []
        while offset < len(data):
            # SSTables are written atomically; a bad CRC here is real corruption.
            key, _value, _record_type, next_offset = parse_record(data, offset)
            all_keys.append(key)  # include tombstones so deletes stay visible to Bloom
            if record_num % self._index_interval == 0:
                self._index.append((key, offset))
                self._index_keys.append(key)
            record_num += 1
            offset = next_offset

        if self._bloom_bits_per_key > 0 and all_keys:
            self._bloom = BloomFilter(len(all_keys), self._bloom_bits_per_key)
            for key in all_keys:
                self._bloom.add(key)

        self._file.seek(0)

    def index_entry_count(self) -> int:
        return len(self._index)

    def bloom_size_bytes(self) -> int:
        return self._bloom.size_bytes() if self._bloom is not None else 0

    def get(
        self, key: str
    ) -> tuple[bool, str | None, bool, bool, bool, bool]:
        """Return (found, value, is_tombstone, did_disk_read, bloom_skip, bloom_fp).

        Bloom "definitely not" → skip the block read. "Maybe" + miss after the
        block scan → false positive.
        """
        if self._bloom is not None and not self._bloom.might_contain(key):
            return False, None, False, False, True, False

        if not self._index:
            return False, None, False, False, False, False

        # Last indexed key <= target; if target is before the first key, skip I/O.
        idx = bisect.bisect_right(self._index_keys, key) - 1
        if idx < 0:
            # Bloom said maybe but key is before this file's range — treat as miss.
            bloom_fp = self._bloom is not None
            return False, None, False, False, False, bloom_fp

        start = self._index[idx][1]
        end = (
            self._index[idx + 1][1]
            if idx + 1 < len(self._index)
            else self._file_size
        )

        self._file.seek(start)
        block = self._file.read(end - start)

        offset = 0
        while offset < len(block):
            rec_key, value, record_type, next_offset = parse_record(block, offset)
            if rec_key == key:
                is_tombstone = record_type == DELETE
                if is_tombstone:
                    return True, None, True, True, False, False
                return True, value, False, True, False, False
            if rec_key > key:
                bloom_fp = self._bloom is not None
                return False, None, False, True, False, bloom_fp
            offset = next_offset

        bloom_fp = self._bloom is not None
        return False, None, False, True, False, bloom_fp

    def close(self) -> None:
        self._file.close()


def _fsync_directory(dir_path: Path) -> None:
    fd = os.open(dir_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
