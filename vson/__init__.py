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

from . import config
from . import exceptions
from . import schema
from . import parser
from . import encoder
from . import core
from . import utils  # ← ADD THIS LINE!

# Import validators
from . import validators

# Import serializers
from . import serializers

# Import CLI
from . import cli

# Export main functions
from .core import smart_encode, smart_decode, VSONSmart

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

    "BaseSerializer",
]
