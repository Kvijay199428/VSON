# vson/serializers/market_data.py
"""
Market Data Serializer

Specialized serializer for trading market data (OHLC, depth, volume).
"""

from typing import Dict, Any, List, Optional
from .base import BaseSerializer  # ← ADD THIS LINE!


class MarketDataSerializer(BaseSerializer):
    """
    Serialize market data (OHLC, depth, volume, etc.).
    
    Handles:
    - OHLC data (Open, High, Low, Close)
    - Market depth (bid/ask levels)
    - Volume and price data
    - Timestamp handling
    """
    
    def __init__(self):
        """Initialize market data serializer"""
        schema = self._get_market_data_schema()
        super().__init__(schema)
    
    def serialize(self, data: Dict[str, Any]) -> str:
        """
        Serialize market data
        
        Args:
            data: Market data dictionary
        
        Returns:
            VSON formatted string
        """
        # Import here to avoid circular dependency
        from ..core import smart_encode
        
        return smart_encode(data)
    
    def deserialize(self, data: str) -> Dict[str, Any]:
        """
        Deserialize VSON to market data
        
        Args:
            data: VSON formatted string
        
        Returns:
            Market data dictionary
        """
        # Import here to avoid circular dependency
        from ..core import smart_decode
        
        return smart_decode(data)
    
    def serialize_ohlc(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize OHLC record
        
        Args:
            record: OHLC record
        
        Returns:
            Serialized record
        """
        return {
            'timestamp': record.get('timestamp'),
            'open': float(record.get('open', 0)),
            'high': float(record.get('high', 0)),
            'low': float(record.get('low', 0)),
            'close': float(record.get('close', 0)),
        }
    
    def serialize_depth(self, depth_data: List[Dict]) -> List[Dict]:
        """
        Serialize market depth data
        
        Args:
            depth_data: Depth level data
        
        Returns:
            Serialized depth data
        """
        serialized = []
        
        for level in depth_data:
            serialized_level = {
                'level': int(level.get('level', 0)),
                'bid_qty': int(level.get('bid_qty', 0)),
                'bid_price': float(level.get('bid_price', 0)),
                'ask_qty': int(level.get('ask_qty', 0)),
                'ask_price': float(level.get('ask_price', 0)),
            }
            serialized.append(serialized_level)
        
        return serialized
    
    def serialize_volume(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize volume data
        
        Args:
            record: Record with volume data
        
        Returns:
            Serialized volume record
        """
        return {
            'timestamp': record.get('timestamp'),
            'volume': int(record.get('volume', 0)),
            'last_price': float(record.get('last_price', 0)),
        }
    
    def validate(self, data: Dict[str, Any]) -> tuple:
        """
        Validate market data
        
        Args:
            data: Market data to validate
        
        Returns:
            Tuple of (is_valid, errors)
        """
        errors = []
        
        # Import validator here to avoid circular dependency
        from ..validators.market_data import MarketDataValidator
        
        # Check OHLC
        if all(k in data for k in ['open', 'high', 'low', 'close']):
            is_valid, ohlc_errors = MarketDataValidator.validate_ohlc(data)
            errors.extend(ohlc_errors)
        
        # Check depth
        if any(f'bid_qty_{i}' in data for i in range(1, 6)):
            is_valid, depth_errors = MarketDataValidator.validate_depth(data)
            errors.extend(depth_errors)
        
        # Check volume
        if 'volume' in data:
            is_valid, vol_errors = MarketDataValidator.validate_volume(data)
            errors.extend(vol_errors)
        
        return len(errors) == 0, errors
    
    @staticmethod
    def _get_market_data_schema() -> Dict[str, Any]:
        """Get market data schema"""
        return {
            'timestamp': {'type': 'timestamp', 'required': True},
            'open': {'type': 'price', 'required': True},
            'high': {'type': 'price', 'required': True},
            'low': {'type': 'price', 'required': True},
            'close': {'type': 'price', 'required': True},
            'volume': {'type': 'volume'},
            'last_price': {'type': 'price'},
        }
