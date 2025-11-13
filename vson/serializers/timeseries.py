# vson/serializers/timeseries.py
"""
Time-Series Data Serializer

Specialized serializer for time-series data with delta compression support.
"""

from typing import Dict, Any, List, Optional  # ← ADD THIS LINE!
from .base import BaseSerializer  # ← MUST IMPORT!


class TimeSeriesSerializer(BaseSerializer):
    """
    Serialize time-series data.
    
    Handles:
    - Time-indexed data
    - Delta compression
    - Efficient repeated value handling
    - Range queries
    """
    
    def __init__(self):
        """Initialize time-series serializer"""
        schema = self._get_timeseries_schema()
        super().__init__(schema)
        self.use_delta = True
        self.base_record = None
    
    def serialize(self, data: Dict[str, Any]) -> str:
        """
        Serialize time-series data
        
        Args:
            data: Time-series data
        
        Returns:
            VSON formatted string
        """
        # Import here to avoid circular dependency
        from ..core import smart_encode
        
        if self.use_delta:
            return smart_encode(data, mode="delta_b")
        else:
            return smart_encode(data, mode="default")
    
    def deserialize(self, data: str) -> Dict[str, Any]:
        """
        Deserialize VSON to time-series
        
        Args:
            data: VSON formatted string
        
        Returns:
            Time-series dictionary
        """
        # Import here to avoid circular dependency
        from ..core import smart_decode
        
        return smart_decode(data, mode="delta_b")
    
    def apply_delta_compression(self, records: List[Dict]) -> Dict[str, Any]:
        """
        Apply delta compression to records
        
        Args:
            records: List of time-series records
        
        Returns:
            Delta-compressed structure with base and deltas
        """
        if not records:
            return {'base': {}, 'deltas': []}
        
        base = records[0]
        deltas = []
        
        for i in range(1, len(records)):
            delta = self._calculate_delta(records[i], records[i-1])
            deltas.append(delta)
        
        return {
            'base': base,
            'deltas': deltas,
            'total_records': len(records),
        }
    
    def reconstruct_from_deltas(
        self,
        base: Dict,
        deltas: List[Dict]
    ) -> List[Dict]:
        """
        Reconstruct time-series from deltas
        
        Args:
            base: Base record
            deltas: List of delta records
        
        Returns:
            Reconstructed time-series records
        """
        records = [base.copy()]
        current = base.copy()
        
        for delta in deltas:
            # Apply delta to get next record
            new_record = current.copy()
            
            for key, delta_value in delta.items():
                if key.startswith('delta_'):
                    original_key = key.replace('delta_', '')
                    if original_key in current:
                        current_val = float(current.get(original_key, 0))
                        new_record[original_key] = current_val + float(delta_value or 0)
                elif key == 'timestamp':
                    new_record['timestamp'] = delta_value
            
            records.append(new_record)
            current = new_record
        
        return records
    
    def compress_repeated_values(self, records: List[Dict], field: str) -> List[Dict]:
        """
        Compress repeated values in field
        
        Args:
            records: Time-series records
            field: Field name to compress
        
        Returns:
            Records with run-length encoding for field
        """
        if not records:
            return []
        
        compressed = []
        current_value = records[0].get(field)
        count = 1
        
        for i in range(1, len(records)):
            value = records[i].get(field)
            
            if value == current_value:
                count += 1
            else:
                compressed.append({
                    **records[i-1],
                    f'{field}_repeat_count': count,
                })
                current_value = value
                count = 1
        
        # Add last record
        if records:
            compressed.append({
                **records[-1],
                f'{field}_repeat_count': count,
            })
        
        return compressed
    
    @staticmethod
    def _calculate_delta(current: Dict, previous: Dict) -> Dict:
        """Calculate delta between records"""
        delta = {'timestamp': current.get('timestamp')}
        
        for key, value in current.items():
            if key == 'timestamp':
                continue
            
            prev_value = previous.get(key)
            
            if isinstance(value, (int, float)) and isinstance(prev_value, (int, float)):
                delta[f'delta_{key}'] = value - prev_value
            else:
                delta[f'delta_{key}'] = value
        
        return delta
    
    @staticmethod
    def _get_timeseries_schema() -> Dict[str, Any]:
        """Get time-series schema"""
        return {
            'timestamp': {'type': 'timestamp', 'required': True},
            'value': {'type': 'float', 'required': True},
        }
