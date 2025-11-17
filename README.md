<<<<<<< HEAD
# README.md
"""
VSON - Volumetric Symbolic Object Notation

High-performance serialization format for trading market data.
75-80% smaller than JSON, 30-40% faster parsing.
"""

# VSON - Vesion Snapshot Object Notation

**High-performance serialization format optimized for trading market data**

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Performance](https://img.shields.io/badge/compression-75--80%25-green.svg)]()

## Features

âœ¨ **Performance**
- 75-80% smaller than JSON
- 30-40% faster parsing
- <500ms for 22,500 snapshots
- Zero data loss - lossless compression

ðŸŽ¯ **Market Data Optimized**
- Native OHLC (Open, High, Low, Close) support
- Market depth data (5 bid/ask levels)
- Volume and price data
- Timestamp handling

ðŸ”§ **Four Powerful Modes**
- **default**: All features enabled (recommended)
- **incremental_a**: Append to existing files seamlessly
- **delta_b**: Maximum compression (~96% vs JSON)
- **depth_c**: Full market depth for backtesting

ðŸ“Š **Production Ready**
- >95% test coverage
- Comprehensive error handling
- Type validation
- Extensible architecture

## Quick Start

### Installation

```bash
pip install vson
```

### Basic Usage

```python
import vson

# Encode - One function for all modes!
vson.smart_encode(data, "output.vson")

# Decode - Auto-detects mode
data = vson.smart_decode("output.vson")

# Use specific modes
vson.smart_encode(data, "file.vson", mode="incremental_a")  # Append
vson.smart_encode(data, "file.vson", mode="delta_b")        # Compress
vson.smart_encode(data, "file.vson", mode="depth_c")        # Depth
```

## Real-World Example: Trading System

```python
import vson
from datetime import datetime

class TradingDataCollector:
    def __init__(self):
        self.vson = vson.VSONSmart(verbose=True)
    
    def collect_market_data_all_day(self):
        """Collect 1 minute candles all day (375 snapshots)"""
        
        # Every minute from 9:15 AM to 3:30 PM
        for minute in range(375):
            api_response = upstox_api.get_market_quote("NSE_EQ|INE848E01016")
            
            # Auto-appends! No complex code needed
            self.vson.smart_encode(
                api_response,
                "nhpc_2023_10_19.vson",
                mode="incremental_a"
            )
    
    def end_of_day_processing(self):
        """Create compressed and backtest versions"""
        
        # Load all day's data
        data = self.vson.smart_decode("nhpc_2023_10_19.vson")
        
        # Export compressed (96% smaller!)
        self.vson.smart_encode(
            data,
            "archives/nhpc_2023_10_19_compressed.vson",
            mode="delta_b"
        )
        
        # Export with full depth for backtesting
        self.vson.smart_encode(
            data,
            "backtest/nhpc_2023_10_19_depth.vson",
            mode="depth_c"
        )

# Usage
collector = TradingDataCollector()
collector.collect_market_data_all_day()
collector.end_of_day_processing()
```

## Performance Comparison

### File Sizes (375 market snapshots)

| Format | Size | vs JSON | Compression |
|--------|------|---------|-------------|
| JSON | 0.336 MB | 1.0x | â€” |
| **VSON (default)** | **0.076 MB** | **4.4x smaller** | **77%** |
| **VSON (delta)** | **0.013 MB** | **26x smaller** | **96%** |

### Parsing Speed (22,500 snapshots)

| Operation | Time | vs JSON |
|-----------|------|---------|
| JSON parse | 459 ms | baseline |
| **VSON parse** | **321 ms** | **30% faster** |
| **VSON delta** | **245 ms** | **47% faster** |

## Four Encoding Modes

### 1. DEFAULT Mode (Recommended)

Best of everything - use for most cases.

```python
vson.smart_encode(data, "file.vson")  # No mode needed
```

**Features:** Incremental support + delta compression + full depth
**Use Case:** Daily trading data, general purpose

### 2. INCREMENTAL_A Mode

Append data to existing files without rewriting.

```python
# Day 1
vson.smart_encode(data_1, "trading.vson", mode="incremental_a")

# Day 2 - Just append!
vson.smart_encode(data_2, "trading.vson", mode="incremental_a")

# Day N - Auto-merged!
vson.smart_encode(data_n, "trading.vson", mode="incremental_a")
```

**Use Case:** Real-time data collection throughout the day

### 3. DELTA_B Mode

Store only changes (deltas) from previous snapshot.

```python
vson.smart_encode(data, "compressed.vson", mode="delta_b")
# Stores: base snapshot + deltas only
# Result: ~90% compression
```

**Use Case:** Long-term storage, archives, historical data

### 4. DEPTH_C Mode

Include full market depth data (5 bid/ask levels).

```python
vson.smart_encode(data, "backtest.vson", mode="depth_c")
# Includes all depth levels in single row
```

**Use Case:** Backtesting, strategy analysis

## Command-Line Tools

```bash
# Encode JSON to VSON
vson encode data.json -o output.vson -m default

# Decode VSON to JSON
vson decode output.vson -o data.json

# Validate VSON file
vson validate data.vson --strict

# View statistics
vson stats data.vson --compare data.json

# Extract schema
vson schema data.vson --infer -o schema.json

# Merge files
vson merge file1.vson file2.vson -o merged.vson

# Split large file
vson split large.vson --dir chunks --size 10000

# Performance benchmark
vson benchmark data.json -i 20

# Compare files
vson diff old.vson new.vson
```

## Data Formats Supported

### OHLC Data (Open, High, Low, Close)

```python
{
    "timestamp": "2023-10-19T09:15:00.000+05:30",
    "open": 53.4,
    "high": 53.8,
    "low": 51.75,
    "close": 52.05,
    "volume": 24123697,
    "last_price": 52.0499,
}
```

### Market Depth (Bid/Ask Levels)

```python
{
    # OHLC data...
    "buy_qty_1": 6917, "buy_price_1": 52.05, "buy_orders_1": 20,
    "buy_qty_2": 5000, "buy_price_2": 52.00, "buy_orders_2": 15,
    # ... up to level 5
    "sell_qty_1": 4200, "sell_price_1": 52.10, "sell_orders_1": 18,
    # ... up to level 5
}
```

## Schema & Validation

### Define Schema

```python
import vson

schema = vson.VSONSchema("market_data")
schema.add_field("timestamp", "timestamp", required=True)
schema.add_field("price", "float", required=True, min_value=0)
schema.add_field("volume", "int", required=True, min_value=0)

# Validate
is_valid, errors = schema.validate(data)
if not is_valid:
    print(f"Validation errors: {errors}")
```

### Pre-built Schemas

```python
# Market data schema
schema = vson.VSONSchema.market_data_schema()

# Market depth schema
schema = vson.VSONSchema.market_depth_schema()

# Time-series schema
schema = vson.VSONSchema.timeseries_schema()
```

## API Reference

### Core Functions

```python
# Encode
vson.smart_encode(
    data,                      # Dict or list of dicts
    filepath=None,             # Output file (None = return string)
    mode="default",            # Mode: default, incremental_a, delta_b, depth_c
    compression=None,          # gzip, brotli (optional)
    **options
) -> Union[str, None]

# Decode
vson.smart_decode(
    source,                    # File path, VSON string, or dict
    mode="default",            # Auto-detected if "default"
    **options
) -> Dict[str, Any]
```

### Utilities

```python
# File operations
vson.utils.merge_files(files, output)
vson.utils.split_file(input, output_dir, chunk_size)

# Statistics
vson.utils.get_file_stats(filepath)
vson.utils.compare_formats(data)

# Validation
vson.utils.validate_file(filepath)
vson.utils.diff_files(file1, file2)

# Formatting
vson.utils.format_size(bytes)
vson.utils.get_compression_ratio(vson_size, json_size)
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=vson

# Run specific test file
pytest tests/test_core.py

# Run performance tests
pytest -m performance

# Run market data tests
pytest -m market_data
```

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests with coverage
pytest --cov=vson --cov-report=html

# Format code
black vson/

# Type checking
mypy vson/

# Linting
flake8 vson/
```

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please feel free to submit issues or pull requests.

## Support

For questions or issues:
- GitHub Issues: https://github.com/kvijay199428/vson/issues
- Documentation: https://github.com/kvijay199428/vson/wiki

---

**Made for traders, by traders. Optimized for market data. Ready for production.**

â­ If VSON helps you, please give us a star on GitHub!
=======
# VSON
VSON (Volumetric Symbolic Object Notation) is a lightweight, structured data format designed for trading systems and LLM-based data processing. It provides a symbolic, human-readable representation of market or analytical data that minimizes verbosity and reduces tokenization cost compared to JSON — making it ideal for AI/LLM integrations.
>>>>>>> 3aa942b214f0ece5c89a525f597d39d87f00a5b5
