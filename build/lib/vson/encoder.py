# vson/encoder.py
"""
VSON Encoder Module

This module provides encoding functionality to convert Python dictionaries
into VSON format strings.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

from .exceptions import VSONEncodingError
from .config import Config


class VSONEncoder:
    """
    Encode Python objects to VSON format.
    
    The encoder handles:
    - Header metadata encoding
    - Array definition generation
    - Row serialization with type handling
    - Delta compression
    - Optional compression
    
    Example:
        encoder = VSONEncoder()
        vson_str = encoder.encode(data_dict)
    """
    
    def __init__(self, schema=None):
        """
        Initialize encoder
        
        Args:
            schema: Optional schema for validation
        """
        self.schema = schema
    
    def encode(
        self,
        data: Dict[str, Any],
        compression: Optional[str] = None,
        delta: bool = False
    ) -> str:
        """
        Encode dictionary to VSON string
        
        Args:
            data: Dictionary to encode
            compression: Compression method (gzip, brotli, None)
            delta: Apply delta compression
        
        Returns:
            VSON formatted string
        
        Raises:
            VSONEncodingError: If encoding fails
        """
        if not data:
            raise VSONEncodingError("Cannot encode empty data")
        
        try:
            lines = []
            
            # Separate header from arrays
            metadata, arrays = self._separate_metadata_and_arrays(data)
            
            # Encode header
            lines.extend(self._encode_header(metadata))
            
            # Blank line
            lines.append("")
            
            # Encode arrays
            for array_name, array_data in arrays.items():
                lines.extend(self._encode_array(array_name, array_data, delta))
                lines.append("")  # Blank line between arrays
            
            vson_str = "\n".join(lines)
            
            # Apply compression if requested
            if compression:
                vson_str = self._apply_compression(vson_str, compression)
            
            return vson_str
        
        except VSONEncodingError:
            raise
        except Exception as e:
            raise VSONEncodingError(f"Encoding failed: {str(e)}")
    
    def _separate_metadata_and_arrays(
        self,
        data: Dict[str, Any]
    ) -> tuple:
        """
        Separate metadata from array data
        
        Args:
            data: Mixed data dictionary
        
        Returns:
            Tuple of (metadata_dict, arrays_dict)
        """
        metadata = {}
        arrays = {}
        
        for key, value in data.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                # This is an array
                arrays[key] = value
            else:
                # This is metadata
                metadata[key] = value
        
        return metadata, arrays
    
    def _encode_header(self, metadata: Dict[str, Any]) -> List[str]:
        """
        Encode metadata header section
        
        Args:
            metadata: Metadata dictionary
        
        Returns:
            List of header lines
        """
        lines = []
        
        for key, value in metadata.items():
            serialized_value = self._serialize_value(value)
            lines.append(f"{key}{Config.HEADER_DELIMITER} {serialized_value}")
        
        return lines
    
    def _encode_array(
        self,
        array_name: str,
        records: List[Dict[str, Any]],
        delta: bool = False
    ) -> List[str]:
        """
        Encode array section
        
        Args:
            array_name: Name of array
            records: List of record dictionaries
            delta: Apply delta compression
        
        Returns:
            List of array lines
        """
        if not records:
            return []
        
        lines = []
        
        # Get field names from first record
        field_names = list(records[0].keys())
        
        # Array header: name[count]{field1,field2,...}:
        field_str = ", ".join(field_names)
        lines.append(f"{array_name}[{len(records)}]{{{field_str}}}:")
        
        # Array data rows
        if delta and len(records) > 1:
            # Encode with delta compression
            lines.append(self._encode_row(records[0], field_names))
            
            for i in range(1, len(records)):
                delta_record = self._calculate_delta(records[i], records[i-1])
                lines.append(self._encode_row(delta_record, field_names))
        else:
            # Encode all records
            for record in records:
                lines.append(self._encode_row(record, field_names))
        
        return lines
    
    def _encode_row(self, record: Dict[str, Any], field_names: List[str]) -> str:
        """
        Encode single record row
        
        Args:
            record: Record dictionary
            field_names: Field names in order
        
        Returns:
            CSV formatted row string
        """
        values = []
        
        for field_name in field_names:
            value = record.get(field_name, "")
            serialized = self._serialize_value(value)
            values.append(serialized)
        
        return Config.FIELD_DELIMITER.join(values)
    
    @staticmethod
    def _serialize_value(value: Any) -> str:
        """
        Serialize value to string representation
        
        Args:
            value: Value to serialize
        
        Returns:
            String representation
        """
        if value is None or value == "":
            return ""
        
        if isinstance(value, bool):
            return "true" if value else "false"
        
        if isinstance(value, float):
            # Format float, removing trailing zeros
            formatted = f"{value:.10f}".rstrip('0').rstrip('.')
            return formatted
        
        if isinstance(value, (int, str)):
            return str(value)
        
        if isinstance(value, (list, dict)):
            # Convert to JSON string
            import json
            return json.dumps(value)
        
        return str(value)
    
    @staticmethod
    def _calculate_delta(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate delta between two records
        
        Args:
            current: Current record
            previous: Previous record
        
        Returns:
            Delta record
        """
        delta = {}
        
        for key, value in current.items():
            prev_value = previous.get(key)
            
            if key == "timestamp":
                delta[key] = value
            elif isinstance(value, (int, float)) and isinstance(prev_value, (int, float)):
                delta[key] = value - prev_value
            else:
                delta[key] = value
        
        return delta
    
    @staticmethod
    def _apply_compression(vson_str: str, method: str) -> str:
        """
        Apply compression to VSON string
        
        Args:
            vson_str: VSON string
            method: Compression method
        
        Returns:
            Compressed string (base64 encoded for text representation)
        """
        if method == "gzip":
            import gzip
            import base64
            compressed = gzip.compress(vson_str.encode(Config.DEFAULT_ENCODING),
                                      compresslevel=Config.COMPRESSION_LEVEL)
            return base64.b64encode(compressed).decode('ascii')
        
        elif method == "brotli":
            try:
                import brotli
                import base64
                compressed = brotli.compress(
                    vson_str.encode(Config.DEFAULT_ENCODING),
                    quality=Config.COMPRESSION_LEVEL
                )
                return base64.b64encode(compressed).decode('ascii')
            except ImportError:
                raise VSONEncodingError("brotli library not installed")
        
        else:
            raise VSONEncodingError(f"Unknown compression method: {method}")


class DeltaEncoder:
    """
    Encode data with delta compression for time-series.
    
    Delta compression stores:
    - One complete base snapshot
    - Only deltas (changes) for subsequent snapshots
    - Typically achieves 90% compression
    
    Example:
        encoder = DeltaEncoder()
        vson_str = encoder.encode_with_delta(records)
    """
    
    def __init__(self):
        self.encoder = VSONEncoder()
    
    def encode_with_delta(self, records: List[Dict[str, Any]]) -> str:
        """
        Encode records with delta compression
        
        Args:
            records: List of records
        
        Returns:
            VSON string with delta compression
        """
        if not records:
            raise VSONEncodingError("No records to encode")
        
        lines = []
        
        # Base snapshot
        base = records[0]
        base_fields = list(base.keys())
        field_str = ", ".join(base_fields)
        
        lines.append(f"base{{{field_str}}}:")
        lines.append(self.encoder._encode_row(base, base_fields))
        lines.append("")
        
        # Deltas
        if len(records) > 1:
            delta_fields = self._get_delta_fields(base_fields)
            delta_field_str = ", ".join(delta_fields)
            
            lines.append(f"deltas[{len(records) - 1}]{{{delta_field_str}}}:")
            
            for i in range(1, len(records)):
                delta = self._calculate_delta_record(
                    records[i],
                    records[i - 1],
                    delta_fields
                )
                lines.append(self.encoder._encode_row(delta, delta_fields))
        
        lines.append("")
        lines.append("# Compression: delta (base + deltas)")
        
        return "\n".join(lines)
    
    @staticmethod
    def _get_delta_fields(base_fields: List[str]) -> List[str]:
        """
        Get delta field names from base fields
        
        Args:
            base_fields: Original field names
        
        Returns:
            Delta field names
        """
        delta_fields = ["timestamp"]
        
        for field in base_fields:
            if field != "timestamp":
                delta_fields.append(f"delta_{field}")
        
        return delta_fields
    
    @staticmethod
    def _calculate_delta_record(
        current: Dict[str, Any],
        previous: Dict[str, Any],
        delta_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Calculate delta record
        
        Args:
            current: Current record
            previous: Previous record
            delta_fields: Delta field names
        
        Returns:
            Delta record
        """
        delta = {}
        
        delta["timestamp"] = current.get("timestamp", "")
        
        for field in delta_fields[1:]:  # Skip timestamp
            original_field = field.replace("delta_", "")
            
            current_val = current.get(original_field, 0)
            previous_val = previous.get(original_field, 0)
            
            if isinstance(current_val, (int, float)) and isinstance(previous_val, (int, float)):
                delta[field] = current_val - previous_val
            else:
                delta[field] = current_val
        
        return delta
