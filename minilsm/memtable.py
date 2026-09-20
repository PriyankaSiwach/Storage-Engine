from __future__ import annotations


class MemTable:
    """In-memory key -> (value, is_tombstone) map.

    Deletes leave a tombstone rather than removing the key so later stages
    (SSTables / compaction) can propagate the deletion correctly.
    """

    def __init__(self) -> None:
        self._entries: dict[str, tuple[str, bool]] = {}
        self._approx_bytes: int = 0

    def put(self, key: str, value: str) -> None:
        self._replace(key, value, is_tombstone=False)

    def delete(self, key: str) -> None:
        # Keep the key with a tombstone; do not remove it.
        self._replace(key, "", is_tombstone=True)

    def _replace(self, key: str, value: str, *, is_tombstone: bool) -> None:
        old = self._entries.get(key)
        if old is not None:
            old_value, _ = old
            self._approx_bytes -= len(key) + len(old_value)
        self._entries[key] = (value, is_tombstone)
        self._approx_bytes += len(key) + len(value)

    def get(self, key: str) -> str | None:
        entry = self.get_entry(key)
        if entry is None:
            return None
        value, is_tombstone = entry
        if is_tombstone:
            return None
        return value

    def get_entry(self, key: str) -> tuple[str, bool] | None:
        """Return (value, is_tombstone) if the key is present, else None."""
        return self._entries.get(key)

    def approximate_size(self) -> int:
        return self._approx_bytes

    def clear(self) -> None:
        self._entries.clear()
        self._approx_bytes = 0

    def sorted_items(self) -> list[tuple[str, tuple[str, bool]]]:
        """Return entries sorted by key (used by Stage 2 flush)."""
        return sorted(self._entries.items(), key=lambda item: item[0])
