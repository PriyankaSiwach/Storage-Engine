"""Bloom filter: probabilistic set membership (no false negatives)."""

from __future__ import annotations

import hashlib
import math


class BloomFilter:
    """Space-efficient approximate set of keys.

    WHY no false negatives: every added key sets k bits; might_contain requires
    all k bits still set, so an added key always returns True.

    WHY false positives are possible: a never-added key can collide on the same
    k bit positions by chance, so might_contain may return True incorrectly.
    """

    def __init__(self, num_keys: int, bits_per_key: int) -> None:
        if bits_per_key < 1:
            raise ValueError("bits_per_key must be >= 1")
        nbytes = max(1, math.ceil(num_keys * bits_per_key / 8))
        self._bits = bytearray(nbytes)
        self._num_bits = nbytes * 8
        self._k = max(1, round(bits_per_key * 0.693))

    def _indices(self, key: str) -> list[int]:
        digest = hashlib.blake2b(key.encode("utf-8"), digest_size=16).digest()
        h1 = int.from_bytes(digest[:8], "little")
        h2 = int.from_bytes(digest[8:], "little")
        return [(h1 + i * h2) % self._num_bits for i in range(self._k)]

    def add(self, key: str) -> None:
        for bit in self._indices(key):
            self._bits[bit // 8] |= 1 << (bit % 8)

    def might_contain(self, key: str) -> bool:
        for bit in self._indices(key):
            if not (self._bits[bit // 8] & (1 << (bit % 8))):
                return False
        return True

    def size_bytes(self) -> int:
        return len(self._bits)
