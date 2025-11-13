# vson/config.py
"""
VSON Configuration and Constants

This module contains all configuration settings, constants, and default values
for the VSON library.
"""


class Config:
    """
    VSON Configuration class - All settings in one place
    
    Attributes:
        VSON_VERSION: Current version of VSON format
        SUPPORTED_TYPES: List of supported data types
        COMPRESSION_METHODS: Available compression algorithms
        DEFAULT_ENCODING: Default text encoding
        MAX_NESTING: Maximum nesting depth for objects
        MAX_ARRAY_SIZE: Maximum array elements
        CHUNK_SIZE: Default chunk size for streaming
        BUFFER_SIZE: I/O buffer size
        COMPRESSION_LEVEL: Default compression level (0-9, 6=default)
        STRICT_MODE: Enable strict validation
        VALIDATE_ON_ENCODE: Validate data during encoding
        VALIDATE_ON_DECODE: Validate data during decoding
    """
    
    # =====================================================================
    # VERSION INFORMATION
    # =====================================================================
    VSON_VERSION = "1.0.0"
    FORMAT_VERSION = "1.0"
    
    # =====================================================================
    # DATA TYPE SUPPORT
    # =====================================================================
    SUPPORTED_TYPES = [
        'int',      # Integer: 42, -10, 0
        'float',    # Float: 3.14, 1.5e-2, -0.5
        'str',      # String: any text
        'bool',     # Boolean: true, false
        'list',     # Array: [1, 2, 3]
        'dict',     # Object: {key: value}
        'None',     # Null: empty values
    ]
    
    # =====================================================================
    # COMPRESSION SETTINGS
    # =====================================================================
    COMPRESSION_METHODS = ['gzip', 'brotli']
    DEFAULT_COMPRESSION = None
    COMPRESSION_LEVEL = 6  # 0=no compression, 9=max (gzip)
    ENABLE_COMPRESSION = True
    
    # =====================================================================
    # ENCODING SETTINGS
    # =====================================================================
    DEFAULT_ENCODING = 'utf-8'
    DEFAULT_ERRORS = 'strict'  # strict, ignore, replace, backslashreplace
    
    # =====================================================================
    # SIZE LIMITS
    # =====================================================================
    MAX_NESTING = 10          # Maximum nesting depth
    MAX_ARRAY_SIZE = 1000000  # Maximum array elements
    MAX_STRING_SIZE = 1000000 # Maximum string length
    MAX_FILE_SIZE = 1000000000  # 1GB
    
    # =====================================================================
    # STREAMING SETTINGS
    # =====================================================================
    CHUNK_SIZE = 1000         # Records per chunk
    BUFFER_SIZE = 8192        # I/O buffer size (8KB)
    STREAMING_MODE = False    # Default streaming disabled
    
    # =====================================================================
    # VALIDATION SETTINGS
    # =====================================================================
    STRICT_MODE = False               # Strict validation
    VALIDATE_ON_ENCODE = False        # Validate during encode
    VALIDATE_ON_DECODE = True         # Validate during decode
    AUTO_FIX_TYPES = True             # Auto-fix type mismatches
    ALLOW_UNKNOWN_FIELDS = True       # Allow fields not in schema
    ALLOW_MISSING_OPTIONAL = True     # Allow missing optional fields
    
    # =====================================================================
    # FORMAT DELIMITERS
    # =====================================================================
    FIELD_DELIMITER = ','             # CSV field delimiter
    HEADER_DELIMITER = ':'            # Key-value delimiter
    ARRAY_START_CHAR = '['            # Array start
    ARRAY_END_CHAR = ']'              # Array end
    FIELD_START_CHAR = '{'            # Field list start
    FIELD_END_CHAR = '}'              # Field list end
    COMMENT_CHAR = '#'                # Comment start
    
    # =====================================================================
    # PERFORMANCE SETTINGS
    # =====================================================================
    USE_CACHING = True                # Cache parsed schemas
    MAX_CACHE_SIZE = 100              # Maximum cached items
    ENABLE_DELTA_COMPRESSION = True   # Delta compression default
    ENABLE_DEPTH_EMBEDDING = True     # Depth embedding default
    
    # =====================================================================
    # MARKET DATA SETTINGS
    # =====================================================================
    DEPTH_LEVELS = 5                  # Number of depth levels (buy/sell)
    OHLC_PRECISION = 6                # Decimal places for OHLC
    VOLUME_PRECISION = 0              # Decimal places for volume
    PRICE_PRECISION = 6               # Decimal places for prices
    
    # =====================================================================
    # INCREMENTAL MODE SETTINGS
    # =====================================================================
    AUTO_BACKUP = True                # Auto-backup before append
    BACKUP_EXTENSION = '.backup'      # Backup file extension
    UPDATE_METADATA = True            # Update metadata on append
    MERGE_DUPLICATES = False          # Merge duplicate records
    
    # =====================================================================
    # ERROR HANDLING
    # =====================================================================
    LOG_ERRORS = True                 # Log errors
    SHOW_WARNINGS = True              # Show warnings
    RAISE_ON_ERROR = True             # Raise exceptions
    ERROR_LOG_FILE = None             # Log file path (None = stdout)
    
    # =====================================================================
    # DEBUG SETTINGS
    # =====================================================================
    DEBUG_MODE = False                # Enable debug output
    VERBOSE = False                   # Verbose output
    SHOW_PROGRESS = True              # Show progress indicators
    PROFILE = False                   # Enable profiling
    
    # =====================================================================
    # CLASS METHODS
    # =====================================================================
    
    @classmethod
    def get(cls, key: str, default=None):
        """Get configuration value"""
        return getattr(cls, key, default)
    
    @classmethod
    def set(cls, key: str, value) -> None:
        """Set configuration value"""
        setattr(cls, key, value)
    
    @classmethod
    def to_dict(cls) -> dict:
        """Convert all settings to dictionary"""
        return {
            k: v for k, v in cls.__dict__.items()
            if not k.startswith('_') and k.isupper()
        }
    
    @classmethod
    def reset(cls) -> None:
        """Reset all settings to defaults"""
        # Reset each setting to its default value
        cls.STRICT_MODE = False
        cls.VALIDATE_ON_ENCODE = False
        cls.VALIDATE_ON_DECODE = True
        cls.DEBUG_MODE = False
        cls.VERBOSE = False
        cls.SHOW_PROGRESS = True


# =========================================================================
# PREDEFINED CONFIGURATION PROFILES
# =========================================================================

class ConfigProfiles:
    """Predefined configuration profiles for different use cases"""
    
    @staticmethod
    def performance():
        """Optimized for performance (speed)"""
        Config.ENABLE_COMPRESSION = False
        Config.VALIDATE_ON_DECODE = False
        Config.STRICT_MODE = False
        Config.DEBUG_MODE = False
    
    @staticmethod
    def strict():
        """Strict validation profile"""
        Config.STRICT_MODE = True
        Config.VALIDATE_ON_ENCODE = True
        Config.VALIDATE_ON_DECODE = True
        Config.ALLOW_UNKNOWN_FIELDS = False
    
    @staticmethod
    def trading():
        """Optimized for trading data"""
        Config.ENABLE_COMPRESSION = True
        Config.COMPRESSION_LEVEL = 6
        Config.VALIDATE_ON_DECODE = True
        Config.SHOW_PROGRESS = True
    
    @staticmethod
    def archival():
        """Optimized for long-term storage"""
        Config.ENABLE_COMPRESSION = True
        Config.COMPRESSION_LEVEL = 9
        Config.VALIDATE_ON_DECODE = False
        Config.DEBUG_MODE = False
