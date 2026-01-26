"""Spider helper utilities."""

from __future__ import annotations

from spider_py import Int8

_SIGNED_BYTE_CUTOFF = 128
_SIGNED_BYTE_OFFSET = 256


def int8_list_to_utf8_str(values: list[Int8]) -> str:
    """
    Convert an Int8 list representing UTF-8 bytes into a string.

    Spider represents strings as List<int8> (signed bytes). We map each
    element to an unsigned byte before decoding.
    """
    byte_values = bytes((int(v) & 0xFF) for v in values)
    return byte_values.decode("utf-8")


def utf8_str_to_int8_list(value: str) -> list[Int8]:
    """
    Convert a string to a Spider List<int8> representation.

    UTF-8 bytes are mapped to signed int8 values (-128..127).
    """
    result: list[Int8] = []
    for b in value.encode("utf-8"):
        signed = b if b < _SIGNED_BYTE_CUTOFF else b - _SIGNED_BYTE_OFFSET
        result.append(Int8(signed))
    return result
