# vson/__init__.py
"""
VSON - Vesion Snapshot Object Notation
High-performance serialization format for trading market data

Features:
- 75-80% smaller than JSON
- 30-40% faster parsing
- Zero data loss - lossless compression
- Native market data support (OHLC, depth, volume)
- Four encoding modes: default, incremental_a, delta_b, depth_c
"""

__version__ = "1.0.0"
__author__ = "VSON Development Team"
__license__ = "MIT"

from .core import VSONSmart, smart_encode, smart_decode, EncodingMode
from .schema import VSONSchema, Field
from .exceptions import (
    VSONError,
    VSONParseError,
    VSONEncodingError,
    VSONSchemaError,
    VSONTypeError,
    VSONIOError,
)

__all__ = [
    # Core API
    "smart_encode",
    "smart_decode",
    "VSONSmart",
    
    # Schema
    "VSONSchema",
    "Field",
    
    # Encoding modes
    "EncodingMode",
    
    # Exceptions
    "VSONError",
    "VSONParseError",
    "VSONEncodingError",
    "VSONSchemaError",
    "VSONTypeError",
    "VSONIOError",
]
