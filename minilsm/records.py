"""Shared binary record format for WAL and SSTables.

Layout: [crc32:4][key_len:4][value_len:4][type:1][key][value]
CRC covers everything after the crc field. type: 0=put, 1=delete.
"""

from __future__ import annotations

import struct
import zlib

PUT: int = 0
DELETE: int = 1

CRC_SIZE = 4
_LENS = struct.Struct("<II")  # key_len, value_len
_TYPE = struct.Struct("<B")
FIXED_AFTER_CRC = _LENS.size + _TYPE.size


class RecordError(Exception):
    """Raised when a record is incomplete or has a bad CRC (strict reads)."""


def encode_record(key: str, value: str, record_type: int) -> bytes:
    key_bytes = key.encode("utf-8")
    value_bytes = value.encode("utf-8")
    body = (
        _LENS.pack(len(key_bytes), len(value_bytes))
        + _TYPE.pack(record_type)
        + key_bytes
        + value_bytes
    )
    crc = zlib.crc32(body) & 0xFFFFFFFF
    return struct.pack("<I", crc) + body


def try_parse_record(
    data: bytes, offset: int
) -> tuple[str, str, int, int] | None:
    """Parse one record. Return (key, value, type, next_offset) or None if torn."""
    if offset + CRC_SIZE + FIXED_AFTER_CRC > len(data):
        return None

    (crc,) = struct.unpack_from("<I", data, offset)
    key_len, value_len = _LENS.unpack_from(data, offset + CRC_SIZE)
    record_type = _TYPE.unpack_from(data, offset + CRC_SIZE + _LENS.size)[0]
    payload_start = offset + CRC_SIZE + FIXED_AFTER_CRC
    payload_end = payload_start + key_len + value_len

    if payload_end > len(data):
        return None

    body = data[offset + CRC_SIZE : payload_end]
    if (zlib.crc32(body) & 0xFFFFFFFF) != crc:
        return None

    key = data[payload_start : payload_start + key_len].decode("utf-8")
    value = data[payload_start + key_len : payload_end].decode("utf-8")
    return key, value, record_type, payload_end


def parse_record(data: bytes, offset: int) -> tuple[str, str, int, int]:
    """Parse one record strictly. Raise RecordError on incomplete/bad CRC."""
    if offset + CRC_SIZE + FIXED_AFTER_CRC > len(data):
        raise RecordError("incomplete record header")

    (crc,) = struct.unpack_from("<I", data, offset)
    key_len, value_len = _LENS.unpack_from(data, offset + CRC_SIZE)
    record_type = _TYPE.unpack_from(data, offset + CRC_SIZE + _LENS.size)[0]
    payload_start = offset + CRC_SIZE + FIXED_AFTER_CRC
    payload_end = payload_start + key_len + value_len

    if payload_end > len(data):
        raise RecordError("incomplete record payload")

    body = data[offset + CRC_SIZE : payload_end]
    if (zlib.crc32(body) & 0xFFFFFFFF) != crc:
        raise RecordError("bad CRC")

    key = data[payload_start : payload_start + key_len].decode("utf-8")
    value = data[payload_start + key_len : payload_end].decode("utf-8")
    return key, value, record_type, payload_end


def read_record_at(file_obj, offset: int) -> tuple[str, str, int]:
    """Seek to offset and read one record from an open binary file (strict)."""
    file_obj.seek(offset)
    header = file_obj.read(CRC_SIZE + FIXED_AFTER_CRC)
    if len(header) < CRC_SIZE + FIXED_AFTER_CRC:
        raise RecordError("incomplete record header")

    (crc,) = struct.unpack_from("<I", header, 0)
    key_len, value_len = _LENS.unpack_from(header, CRC_SIZE)
    record_type = _TYPE.unpack_from(header, CRC_SIZE + _LENS.size)[0]

    payload = file_obj.read(key_len + value_len)
    if len(payload) < key_len + value_len:
        raise RecordError("incomplete record payload")

    body = header[CRC_SIZE:] + payload
    if (zlib.crc32(body) & 0xFFFFFFFF) != crc:
        raise RecordError("bad CRC")

    key = payload[:key_len].decode("utf-8")
    value = payload[key_len:].decode("utf-8")
    return key, value, record_type
