# vson/validators/market_data.py
"""
Market Data Validators

Specialized validators for trading market data (OHLC, depth, volume, etc.)
"""

from typing import Dict, Any, List, Tuple, Optional


class MarketDataValidator:
    """
    Validate market-specific data formats.
    
    Validates:
    - OHLC (Open, High, Low, Close) relationships
    - Market depth (bid/ask levels)
    - Prices and volumes
    - Timestamp sequences
    - Data anomalies
    """
    
    @staticmethod
    def validate_ohlc(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate OHLC data consistency
        
        Args:
            record: Market data record
        
        Returns:
            Tuple of (is_valid, errors)
        
        Rules:
        - All four fields must be present
        - Low <= Open <= High
        - Low <= Close <= High
        - All prices > 0
        """
        errors = []
        
        # Check required fields
        required_fields = ['open', 'high', 'low', 'close']
        for field in required_fields:
            if field not in record:
                errors.append(f"OHLC: Missing '{field}' field")
        
        if errors:
            return False, errors
        
        open_p = float(record.get('open', 0))
        high_p = float(record.get('high', 0))
        low_p = float(record.get('low', 0))
        close_p = float(record.get('close', 0))
        
        # Check price relationships
        if not (low_p <= open_p <= high_p):
            errors.append(f"OHLC: Open {open_p} not between low {low_p} and high {high_p}")
        
        if not (low_p <= close_p <= high_p):
            errors.append(f"OHLC: Close {close_p} not between low {low_p} and high {high_p}")
        
        if high_p < low_p:
            errors.append(f"OHLC: High {high_p} is less than low {low_p}")
        
        # Check for positive prices
        if open_p <= 0:
            errors.append(f"OHLC: Open price must be positive: {open_p}")
        if high_p <= 0:
            errors.append(f"OHLC: High price must be positive: {high_p}")
        if low_p <= 0:
            errors.append(f"OHLC: Low price must be positive: {low_p}")
        if close_p <= 0:
            errors.append(f"OHLC: Close price must be positive: {close_p}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_depth(record: Dict[str, Any], levels: int = 5) -> Tuple[bool, List[str]]:
        """
        Validate market depth data
        
        Args:
            record: Market data record
            levels: Number of depth levels (default 5)
        
        Returns:
            Tuple of (is_valid, errors)
        
        Validates:
        - All depth levels present or absent (consistency)
        - Prices in correct order (bid prices descending, ask ascending)
        - Quantities are non-negative
        """
        errors = []
        
        # Check bid levels
        bid_prices = []
        for i in range(1, levels + 1):
            qty_field = f'bid_qty_{i}'
            price_field = f'bid_price_{i}'
            
            if qty_field in record and price_field in record:
                qty = int(record[qty_field])
                price = float(record[price_field])
                
                if qty < 0:
                    errors.append(f"Depth: bid_qty_{i} negative: {qty}")
                
                if price < 0:
                    errors.append(f"Depth: bid_price_{i} negative: {price}")
                
                bid_prices.append(price)
        
        # Check bid prices are descending
        for i in range(len(bid_prices) - 1):
            if bid_prices[i] <= bid_prices[i + 1]:
                errors.append(f"Depth: bid prices not descending at level {i+1}")
        
        # Check ask levels
        ask_prices = []
        for i in range(1, levels + 1):
            qty_field = f'ask_qty_{i}'
            price_field = f'ask_price_{i}'
            
            if qty_field in record and price_field in record:
                qty = int(record[qty_field])
                price = float(record[price_field])
                
                if qty < 0:
                    errors.append(f"Depth: ask_qty_{i} negative: {qty}")
                
                if price < 0:
                    errors.append(f"Depth: ask_price_{i} negative: {price}")
                
                ask_prices.append(price)
        
        # Check ask prices are ascending
        for i in range(len(ask_prices) - 1):
            if ask_prices[i] >= ask_prices[i + 1]:
                errors.append(f"Depth: ask prices not ascending at level {i+1}")
        
        # Check bid-ask spread (bid < ask)
        if bid_prices and ask_prices:
            if bid_prices[0] >= ask_prices[0]:
                errors.append(
                    f"Depth: bid {bid_prices[0]} >= ask {ask_prices[0]} (inverted spread)"
                )
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_volume(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate volume data
        
        Args:
            record: Market data record
        
        Returns:
            Tuple of (is_valid, errors)
        
        Validates:
        - Volume is non-negative
        - Volume is integer
        """
        errors = []
        
        if 'volume' not in record:
            return True, []
        
        volume = record['volume']
        
        # Check type
        if not isinstance(volume, (int, float)):
            errors.append(f"Volume must be numeric, got {type(volume).__name__}")
        
        # Check non-negative
        if float(volume) < 0:
            errors.append(f"Volume cannot be negative: {volume}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_timestamp_sequence(
        records: List[Dict[str, Any]]
    ) -> Tuple[bool, List[str]]:
        """
        Validate timestamp sequence
        
        Args:
            records: List of market data records
        
        Returns:
            Tuple of (is_valid, errors)
        
        Validates:
        - Timestamps are in ascending order
        - No duplicate timestamps
        """
        errors = []
        
        if len(records) < 2:
            return True, []
        
        prev_timestamp = None
        
        for i, record in enumerate(records):
            timestamp = record.get('timestamp')
            
            if timestamp is None:
                errors.append(f"Record {i}: Missing timestamp")
                continue
            
            if prev_timestamp:
                if timestamp < prev_timestamp:
                    errors.append(
                        f"Record {i}: Timestamp {timestamp} less than "
                        f"previous {prev_timestamp}"
                    )
                elif timestamp == prev_timestamp:
                    errors.append(f"Record {i}: Duplicate timestamp {timestamp}")
            
            prev_timestamp = timestamp
        
        return len(errors) == 0, errors
    
    @staticmethod
    def detect_anomalies(
        record: Dict[str, Any],
        prev_record: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Detect data anomalies in market data
        
        Args:
            record: Current market data record
            prev_record: Previous record (for comparison)
        
        Returns:
            List of anomaly descriptions
        
        Detects:
        - Gap up/down (large price movement)
        - Volume spike
        - Spread anomalies
        """
        anomalies = []
        
        if not prev_record:
            return anomalies
        
        # Get prices
        prev_close = float(prev_record.get('close', 0))
        curr_open = float(record.get('open', 0))
        curr_close = float(record.get('close', 0))
        curr_volume = float(record.get('volume', 0))
        prev_volume = float(prev_record.get('volume', 0))
        
        # Check gap (>5% movement)
        if prev_close > 0:
            gap_pct = abs((curr_open - prev_close) / prev_close * 100)
            if gap_pct > 5:
                anomalies.append(f"Gap alert: {gap_pct:.1f}% move from {prev_close} to {curr_open}")
        
        # Check volume spike (>200% increase)
        if prev_volume > 0:
            volume_ratio = curr_volume / prev_volume
            if volume_ratio > 2:
                anomalies.append(f"Volume spike: {volume_ratio:.1f}x increase")
        
        # Check price volatility (intraday range >10%)
        high = float(record.get('high', curr_close))
        low = float(record.get('low', curr_close))
        if low > 0:
            volatility = ((high - low) / low * 100)
            if volatility > 10:
                anomalies.append(f"High volatility: {volatility:.1f}% range")
        
        return anomalies
    
    @staticmethod
    def validate_market_hours(
        timestamp: str,
        market: str = 'NSE'
    ) -> Tuple[bool, str]:
        """
        Validate timestamp is within market hours
        
        Args:
            timestamp: ISO format timestamp
            market: Market identifier (NSE, BSE, etc.)
        
        Returns:
            Tuple of (is_within_hours, message)
        
        Markets:
        - NSE: 09:15 - 15:30 IST
        - BSE: 09:15 - 15:30 IST
        """
        # Extract time from ISO timestamp
        if 'T' not in timestamp:
            return False, "Invalid timestamp format"
        
        time_part = timestamp.split('T')[1][:8]  # HH:MM:SS
        hour, minute, second = map(int, time_part.split(':'))
        
        # NSE/BSE hours: 09:15 - 15:30
        if market.upper() in ('NSE', 'BSE'):
            if hour < 9 or hour > 15:
                return False, f"{market} closes outside trading hours"
            if hour == 9 and minute < 15:
                return False, f"{market} opens at 09:15"
            if hour == 15 and minute > 30:
                return False, f"{market} closes at 15:30"
            
            return True, "Within market hours"
        
        return True, "Market hours validation not configured"


class AnomalyDetector:
    """
    Detect anomalies in market data
    """
    
    @staticmethod
    def detect_outliers(
        records: List[Dict[str, Any]],
        field: str,
        threshold: float = 3.0
    ) -> List[Tuple[int, Any, float]]:
        """
        Detect statistical outliers
        
        Args:
            records: List of records
            field: Field name to analyze
            threshold: Standard deviations (default 3)
        
        Returns:
            List of (index, value, std_devs) tuples
        """
        if not records:
            return []
        
        # Get values
        values = []
        for r in records:
            try:
                val = float(r.get(field, 0))
                values.append(val)
            except (ValueError, TypeError):
                values.append(0)
        
        if not values:
            return []
        
        # Calculate statistics
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std_dev = variance ** 0.5
        
        # Find outliers
        outliers = []
        for i, val in enumerate(values):
            if std_dev > 0:
                z_score = abs((val - mean) / std_dev)
                if z_score > threshold:
                    outliers.append((i, val, z_score))
        
        return outliers
