# candle_history.py
"""
Upstox Historical Candle Data V3 Collector

Retrieves historical OHLC candle data and stores efficiently using VSON format.

Features:
- Fetch candle data in multiple units (minutes, hours, days, weeks, months)
- Support for custom intervals
- Direct VSON parsing (no JSON intermediate)
- Incremental append mode for continuous data collection
- Delta compression for efficient storage
- Comprehensive data validation
- Performance monitoring
"""

import requests
import vson
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import time


class UpstoxCandleHistoryCollector:
    """
    Collect and store Upstox historical candle data using VSON format.
    
    Supports multiple timeframes:
    - minutes (1-300)
    - hours (1-5)
    - days (1)
    - weeks (1)
    - months (1)
    
    Example:
        collector = UpstoxCandleHistoryCollector(access_token="your_token")
        
        # Fetch 1-minute candles
        candles = collector.fetch_candle_history(
            instrument_key="NSE_EQ|INE848E01016",
            unit="minutes",
            interval=1,
            from_date="2025-01-01",
            to_date="2025-01-02"
        )
        
        # Save to VSON
        collector.save_to_vson(candles, "nhpc_candles.vson")
    """
    
    # API Configuration
    UPSTOX_BASE_URL = "https://api.upstox.com/v3"
    CANDLE_ENDPOINT = "/historical-candle"
    
    # Historical availability
    HISTORICAL_LIMITS = {
        "minutes": {
            "max_interval": 300,
            "available_from": "2022-01-01",
            "retrieval_limit": {
                "1-15": "1 month",
                "16-300": "1 quarter"
            }
        },
        "hours": {
            "max_interval": 5,
            "available_from": "2022-01-01",
            "retrieval_limit": "1 quarter"
        },
        "days": {
            "max_interval": 1,
            "available_from": "2000-01-01",
            "retrieval_limit": "1 decade"
        },
        "weeks": {
            "max_interval": 1,
            "available_from": "2000-01-01",
            "retrieval_limit": "No limit"
        },
        "months": {
            "max_interval": 1,
            "available_from": "2000-01-01",
            "retrieval_limit": "No limit"
        }
    }
    
    def __init__(
        self,
        access_token: str,
        output_dir: str = "candle_data",
        verbose: bool = True
    ):
        """
        Initialize candle history collector
        
        Args:
            access_token: Upstox API access token
            output_dir: Output directory for VSON files
            verbose: Print progress messages
        """
        self.access_token = access_token
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.verbose = verbose
        
        self.vson = vson.VSONSmart(verbose=verbose)
        
        self.stats = {
            "total_requests": 0,
            "total_candles": 0,
            "parse_time_ms": 0,
            "encoding_time_ms": 0,
            "errors": [],
        }
    
    # =====================================================================
    # API METHODS
    # =====================================================================
    
    def fetch_candle_history(
        self,
        instrument_key: str,
        unit: str = "minutes",
        interval: int = 1,
        from_date: str = None,
        to_date: str = None
    ) -> Dict[str, Any]:
        """
        Fetch historical candle data from Upstox API
        
        Args:
            instrument_key: Instrument key (e.g., "NSE_EQ|INE848E01016")
            unit: Unit type (minutes, hours, days, weeks, months)
            interval: Interval within the unit (1-300 for minutes, 1-5 for hours, etc.)
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
        
        Returns:
            Dictionary with candle data
        
        Example:
            candles = collector.fetch_candle_history(
                instrument_key="NSE_EQ|INE848E01016",
                unit="minutes",
                interval=1,
                from_date="2025-01-01",
                to_date="2025-01-02"
            )
        """
        
        # Validate inputs
        is_valid, error = self._validate_inputs(unit, interval, from_date, to_date)
        if not is_valid:
            if self.verbose:
                print(f"âŒ Validation error: {error}")
            self.stats["errors"].append(error)
            return {}
        
        # Default dates if not provided
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        if self.verbose:
            print(f"\nðŸ“Š Fetching {unit} candles (interval: {interval})")
            print(f"   Instrument: {instrument_key}")
            print(f"   Date range: {from_date} to {to_date}")
        
        # Fetch from API
        candles = self._fetch_from_api(
            instrument_key,
            unit,
            interval,
            from_date,
            to_date
        )
        
        self.stats["total_candles"] += len(candles)
        return candles
    
    def _validate_inputs(
        self,
        unit: str,
        interval: int,
        from_date: str = None,
        to_date: str = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate API request parameters
        
        Args:
            unit: Unit type
            interval: Interval value
            from_date: Start date
            to_date: End date
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check unit
        if unit not in self.HISTORICAL_LIMITS:
            return False, f"Invalid unit: {unit}. Must be: {list(self.HISTORICAL_LIMITS.keys())}"
        
        # Check interval
        max_interval = self.HISTORICAL_LIMITS[unit]["max_interval"]
        if interval > max_interval:
            return False, f"Interval {interval} exceeds max {max_interval} for {unit}"
        
        # Validate dates if provided
        if from_date:
            try:
                datetime.strptime(from_date, "%Y-%m-%d")
            except ValueError:
                return False, f"Invalid from_date format: {from_date}. Use YYYY-MM-DD"
        
        if to_date:
            try:
                datetime.strptime(to_date, "%Y-%m-%d")
            except ValueError:
                return False, f"Invalid to_date format: {to_date}. Use YYYY-MM-DD"
        
        return True, None
    
    def _fetch_from_api(
        self,
        instrument_key: str,
        unit: str,
        interval: int,
        from_date: str,
        to_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch candle data from Upstox API and transform directly to VSON format
        
        Args:
            instrument_key: Instrument key
            unit: Unit type
            interval: Interval
            from_date: Start date
            to_date: End date
        
        Returns:
            List of candle records (VSON-ready format)
        """
        try:
            # Build URL
            url = (
                f"{self.UPSTOX_BASE_URL}{self.CANDLE_ENDPOINT}/"
                f"{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
            )
            
            # Request headers
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}"
            }
            
            if self.verbose:
                print("   Sending request...", end=" ")
            
            # Make request
            start_time = time.perf_counter()
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            parse_time = (time.perf_counter() - start_time) * 1000
            self.stats["parse_time_ms"] += parse_time
            self.stats["total_requests"] += 1
            
            if self.verbose:
                print(f"âœ… {parse_time:.1f}ms")
            
            # Parse response
            raw_data = response.json()
            
            # Check status
            if raw_data.get("status") != "success":
                error_msg = f"API error: {raw_data.get('message', 'Unknown error')}"
                self.stats["errors"].append(error_msg)
                if self.verbose:
                    print(f"   âŒ {error_msg}")
                return []
            
            # Transform candles to VSON format
            candles = self._transform_candles(
                raw_data.get("data", {}).get("candles", []),
                instrument_key,
                unit,
                interval
            )
            
            if self.verbose:
                print(f"   âœ… Received {len(candles)} candles")
            
            return candles
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {str(e)}"
            self.stats["errors"].append(error_msg)
            if self.verbose:
                print(f"   âŒ {error_msg}")
            return []
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.stats["errors"].append(error_msg)
            if self.verbose:
                print(f"   âŒ {error_msg}")
            return []
    
    # =====================================================================
    # DATA TRANSFORMATION (Direct VSON Format)
    # =====================================================================
    
    def _transform_candles(
        self,
        raw_candles: List[List],
        instrument_key: str,
        unit: str,
        interval: int
    ) -> List[Dict[str, Any]]:
        """
        Transform raw candle data to VSON-compatible format
        
        API returns candles as array of arrays:
        [timestamp, open, high, low, close, volume, oi]
        
        We transform to dict for VSON storage.
        
        Args:
            raw_candles: Raw candle data from API
            instrument_key: Instrument key
            unit: Unit type
            interval: Interval
        
        Returns:
            List of VSON-ready candle records
        """
        transformed = []
        
        for raw_candle in raw_candles:
            try:
                # Extract array elements
                if len(raw_candle) >= 6:
                    timestamp = raw_candle[0]
                    open_price = float(raw_candle[1])
                    high_price = float(raw_candle[2])
                    low_price = float(raw_candle[3])
                    close_price = float(raw_candle[4])
                    volume = int(raw_candle[5])
                    oi = int(raw_candle[6]) if len(raw_candle) > 6 else 0
                else:
                    continue
                
                # Convert timestamp (milliseconds to ISO format)
                dt = datetime.fromtimestamp(timestamp / 1000)
                timestamp_iso = dt.isoformat() + "+05:30"
                
                # Build VSON-ready record
                candle = {
                    "timestamp": timestamp_iso,
                    "timestamp_ms": timestamp,
                    "instrument_key": instrument_key,
                    "unit": unit,
                    "interval": interval,
                    
                    # OHLC
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    
                    # Volume & OI
                    "volume": volume,
                    "oi": oi,
                    
                    # Calculated fields
                    "change": close_price - open_price,
                    "change_pct": ((close_price - open_price) / open_price * 100) if open_price > 0 else 0,
                    "range": high_price - low_price,
                    "range_pct": ((high_price - low_price) / low_price * 100) if low_price > 0 else 0,
                }
                
                transformed.append(candle)
            
            except (ValueError, TypeError, IndexError) as e:
                if self.verbose:
                    print(f"   âš ï¸  Error transforming candle: {e}")
                continue
        
        return transformed
    
    # =====================================================================
    # SAVE TO VSON
    # =====================================================================
    
    def save_to_vson(
        self,
        candles: List[Dict[str, Any]],
        filename: str,
        mode: str = "default"
    ) -> Path:
        """
        Save candle data directly to VSON file
        
        Args:
            candles: VSON-ready candle records
            filename: Output filename
            mode: VSON mode (default, incremental_a, delta_b, depth_c)
        
        Returns:
            Path to saved file
        """
        if self.verbose:
            print(f"\nðŸ’¾ Encoding {len(candles)} candles to VSON ({mode} mode)")
        
        filepath = self.output_dir / filename
        
        # Prepare data
        data = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "total_candles": len(candles),
            "candles": candles,
        }
        
        # Encode directly to VSON
        start_time = time.perf_counter()
        vson.smart_encode(data, filepath, mode=mode)
        encode_time = (time.perf_counter() - start_time) * 1000
        self.stats["encoding_time_ms"] += encode_time
        
        file_size = filepath.stat().st_size
        
        if self.verbose:
            print(f"   âœ… Saved {vson.utils.format_size(file_size)} in {encode_time:.1f}ms")
        
        return filepath
    
    # =====================================================================
    # STATISTICS
    # =====================================================================
    
    def print_statistics(self):
        """Print collection statistics"""
        print("\n" + "=" * 70)
        print("ðŸ“Š CANDLE DATA COLLECTION STATISTICS")
        print("=" * 70)
        print(f"API Requests: {self.stats['total_requests']}")
        print(f"Total Candles: {self.stats['total_candles']}")
        print(f"Parse Time: {self.stats['parse_time_ms']:.1f}ms")
        print(f"Encoding Time: {self.stats['encoding_time_ms']:.1f}ms")
        print(f"Total Time: {self.stats['parse_time_ms'] + self.stats['encoding_time_ms']:.1f}ms")
        print(f"Errors: {len(self.stats['errors'])}")
        
        if self.stats["errors"]:
            print("\nErrors encountered:")
            for error in self.stats["errors"]:
                print(f"  - {error}")
        
        print("=" * 70)


# =========================================================================
# REAL-WORLD EXAMPLES
# =========================================================================

def example_1_fetch_minute_candles():
    """Example 1: Fetch 1-minute candles"""
    print("\n" + "=" * 70)
    print("EXAMPLE 1: Fetch 1-Minute Candles")
    print("=" * 70)
    
    collector = UpstoxCandleHistoryCollector(
        access_token="eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3RUFIQkoiLCJqdGkiOiI2OTE0NzdhZjIyMmZiZTMzN2JiNmE5MDMiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYyOTQ5MDM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjI5ODQ4MDB9.ns-_11HXjFi_cF6WG8ZkKCSeIAPwPhKJRPB2RCu6tj0",
        output_dir="candle_data"
    )
    
    # Fetch 1-minute candles for last 2 days
    candles = collector.fetch_candle_history(
        instrument_key="NSE_EQ|INE848E01016",
        unit="minutes",
        interval=1,
        to_date="2025-11-02",
        from_date="2025-1-01"
    )
    
    # Save to VSON
    collector.save_to_vson(candles, "nhpc_1min.vson", mode="default")
    
    # Statistics
    collector.print_statistics()


def example_2_fetch_hourly_candles():
    """Example 2: Fetch hourly candles"""
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Fetch Hourly Candles")
    print("=" * 70)
    
    collector = UpstoxCandleHistoryCollector(
        access_token="eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3RUFIQkoiLCJqdGkiOiI2OTE0NzdhZjIyMmZiZTMzN2JiNmE5MDMiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYyOTQ5MDM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjI5ODQ4MDB9.ns-_11HXjFi_cF6WG8ZkKCSeIAPwPhKJRPB2RCu6tj0",
        output_dir="candle_data"
    )
    
    # Fetch 1-hour candles for last 3 months
    candles = collector.fetch_candle_history(
        instrument_key="NSE_EQ|INE848E01016",
        unit="hours",
        interval=1,
        from_date="2024-10-02",
        to_date="2025-01-02"
    )
    
    # Save with compression
    collector.save_to_vson(candles, "nhpc_1hour_compressed.vson", mode="delta_b")
    
    # Statistics
    collector.print_statistics()


def example_3_fetch_daily_candles():
    """Example 3: Fetch daily candles for long-term analysis"""
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Fetch Daily Candles (10 Years)")
    print("=" * 70)
    
    collector = UpstoxCandleHistoryCollector(
        access_token="eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3RUFIQkoiLCJqdGkiOiI2OTE0NzdhZjIyMmZiZTMzN2JiNmE5MDMiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYyOTQ5MDM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjI5ODQ4MDB9.ns-_11HXjFi_cF6WG8ZkKCSeIAPwPhKJRPB2RCu6tj0",
        output_dir="candle_data"
    )
    
    # Fetch daily candles for 10 years
    candles = collector.fetch_candle_history(
        instrument_key="NSE_EQ|INE848E01016",
        unit="days",
        interval=1,
        from_date="2015-01-01",
        to_date="2025-01-02"
    )
    
    # Save (no limit for daily data)
    collector.save_to_vson(candles, "nhpc_daily_10years.vson", mode="default")
    
    # Statistics
    collector.print_statistics()


def example_4_multiple_timeframes():
    """Example 4: Fetch multiple timeframes and save separately"""
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Multiple Timeframes")
    print("=" * 70)
    
    collector = UpstoxCandleHistoryCollector(
        access_token="eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3RUFIQkoiLCJqdGkiOiI2OTE0NzdhZjIyMmZiZTMzN2JiNmE5MDMiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYyOTQ5MDM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjI5ODQ4MDB9.ns-_11HXjFi_cF6WG8ZkKCSeIAPwPhKJRPB2RCu6tj0",
        output_dir="candle_data"
    )
    
    instruments = [
        ("NSE_EQ|INE848E01016", "NHPC"),
        ("NSE_EQ|INE100A01010", "SBIN"),
        ("NSE_EQ|INE002A01018", "ACC"),
    ]
    
    timeframes = [
        ("minutes", 5, "2025-01-01", "2025-01-02"),
        ("hours", 1, "2024-10-02", "2025-01-02"),
        ("days", 1, "2023-01-01", "2025-01-02"),
    ]
    
    for symbol, name in instruments:
        print(f"\nðŸ”„ Fetching {name} candles...")
        
        for unit, interval, from_date, to_date in timeframes:
            print(f"   ðŸ“Š {unit} (interval: {interval})", end="...")
            
            candles = collector.fetch_candle_history(
                instrument_key=symbol,
                unit=unit,
                interval=interval,
                from_date=from_date,
                to_date=to_date
            )
            
            if candles:
                filename = f"{name.lower()}_{unit}_{interval}.vson"
                collector.save_to_vson(candles, filename, mode="delta_b")
                print(f" âœ… {len(candles)} candles")
            else:
                print(" âš ï¸  No data")
    
    # Statistics
    collector.print_statistics()


def example_5_incremental_collection():
    """Example 5: Incremental collection throughout the day"""
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Incremental Collection")
    print("=" * 70)
    
    collector = UpstoxCandleHistoryCollector(
        access_token="eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3RUFIQkoiLCJqdGkiOiI2OTE0NzdhZjIyMmZiZTMzN2JiNmE5MDMiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYyOTQ5MDM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjI5ODQ4MDB9.ns-_11HXjFi_cF6WG8ZkKCSeIAPwPhKJRPB2RCu6tj0",
        output_dir="candle_data"
    )
    
    # Collect 5-minute candles every hour
    print("\nðŸ“Š Starting incremental collection (simulated 5 iterations)...")
    for hour in range(5):
        print(f"\n[Hour {hour+1}] Fetching 5-minute candles...")
        
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        candles = collector.fetch_candle_history(
            instrument_key="NSE_EQ|INE848E01016",
            unit="minutes",
            interval=5,
            from_date=yesterday,
            to_date=today
        )
        
        # Append incrementally (auto-merges!)
        collector.save_to_vson(
            candles,
            "nhpc_5min_incremental.vson",
            mode="incremental_a"
        )
        
        if hour < 4:
            time.sleep(1)  # Simulate hourly collection
    
    # Statistics
    collector.print_statistics()


# =========================================================================
# ANALYSIS UTILITIES
# =========================================================================

def analyze_candles(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze candle data
    
    Args:
        candles: List of candle records
    
    Returns:
        Analysis results
    """
    if not candles:
        return {}
    
    closes = [c['close'] for c in candles]
    volumes = [c['volume'] for c in candles]
    
    analysis = {
        "total_candles": len(candles),
        "highest_close": max(closes),
        "lowest_close": min(closes),
        "highest_volume": max(volumes),
        "total_volume": sum(volumes),
        "avg_close": sum(closes) / len(closes),
        "avg_volume": sum(volumes) / len(volumes),
        "volatility": (max(closes) - min(closes)) / min(closes) * 100,
    }
    
    return analysis


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("UPSTOX HISTORICAL CANDLE DATA V3 COLLECTOR")
    print("=" * 70)
    
    # Uncomment examples to run:
    example_1_fetch_minute_candles()
    # example_2_fetch_hourly_candles()
    # example_3_fetch_daily_candles()
    # example_4_multiple_timeframes()
    # example_5_incremental_collection()
    
    print("\nâœ… Candle history collector ready!")
    print("\nTo use:")
    print("1. Replace 'YOUR_ACCESS_TOKEN' with your actual token")
    print("2. Uncomment the example you want to run")
    print("3. Run: python candle_history.py")
    print("\nFeatures:")
    print("âœ“ Minutes (1-300 intervals)")
    print("âœ“ Hours (1-5 intervals)")
    print("âœ“ Days (up to 10 years)")
    print("âœ“ Weeks (unlimited)")
    print("âœ“ Months (unlimited)")
