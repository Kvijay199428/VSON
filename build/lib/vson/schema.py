# vson/schema.py
"""
VSON Schema Module

This module provides schema definition, validation, and management for VSON data.
Schemas ensure data integrity and enable type checking during encoding/decoding.
"""

from typing import Dict, Any, List, Tuple, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

from .exceptions import VSONSchemaError, VSONTypeError, VSONValidationError


class FieldType(Enum):
    """Enumeration of supported field types"""
    INT = "int"
    FLOAT = "float"
    STRING = "str"
    BOOLEAN = "bool"
    LIST = "list"
    DICT = "dict"
    NONE = "None"
    TIMESTAMP = "timestamp"
    PRICE = "price"
    VOLUME = "volume"


@dataclass
class Field:
    """
    Field definition in a VSON schema.
    
    Attributes:
        name: Field name (required)
        field_type: Data type (required)
        required: Whether field is required in data
        default: Default value if field missing
        validator: Custom validator function
        min_value: Minimum value (for numeric types)
        max_value: Maximum value (for numeric types)
        min_length: Minimum length (for string types)
        max_length: Maximum length (for string types)
        pattern: Regex pattern (for string types)
        description: Field description
    """
    
    name: str
    field_type: str
    required: bool = False
    default: Any = None
    validator: Optional[Callable] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    description: Optional[str] = None
    
    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate value against field rules
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            if self.required:
                return False, f"Field '{self.name}' is required"
            return True, None
        
        # Type validation
        if not self._validate_type(value):
            return False, f"Field '{self.name}' has invalid type"
        
        # Range validation
        if isinstance(value, (int, float)):
            if self.min_value is not None and value < self.min_value:
                return False, f"Field '{self.name}' below minimum {self.min_value}"
            if self.max_value is not None and value > self.max_value:
                return False, f"Field '{self.name}' above maximum {self.max_value}"
        
        # Length validation
        if isinstance(value, str):
            if self.min_length and len(value) < self.min_length:
                return False, f"Field '{self.name}' too short"
            if self.max_length and len(value) > self.max_length:
                return False, f"Field '{self.name}' too long"
        
        # Custom validator
        if self.validator:
            try:
                if not self.validator(value):
                    return False, f"Field '{self.name}' failed custom validation"
            except Exception as e:
                return False, f"Field '{self.name}' validation error: {e}"
        
        return True, None
    
    def _validate_type(self, value: Any) -> bool:
        """Validate value type"""
        type_map = {
            'int': (int,),
            'float': (int, float),
            'str': (str,),
            'bool': (bool,),
            'list': (list,),
            'dict': (dict,),
            'None': (type(None),),
            'timestamp': (str,),  # ISO format string
            'price': (int, float),
            'volume': (int,),
        }
        
        expected = type_map.get(self.field_type, ())
        return isinstance(value, expected)


class VSONSchema:
    """
    VSON Schema definition and validation.
    
    A schema defines the structure and types of data in VSON format.
    It enables validation, type checking, and schema inference.
    
    Example:
        schema = VSONSchema("market_data")
        schema.add_field("timestamp", "timestamp", required=True)
        schema.add_field("price", "float", required=True, min_value=0)
        schema.add_field("volume", "volume", required=True)
    """
    
    def __init__(self, name: str = "default", description: str = ""):
        """
        Initialize schema
        
        Args:
            name: Schema name
            description: Schema description
        """
        self.name = name
        self.description = description
        self.fields: Dict[str, Field] = {}
        self.version = "1.0"
        self.created = None
        self.updated = None
    
    def add_field(
        self,
        name: str,
        field_type: str,
        required: bool = False,
        default: Any = None,
        validator: Optional[Callable] = None,
        **kwargs
    ) -> None:
        """
        Add field to schema
        
        Args:
            name: Field name
            field_type: Data type
            required: Whether field is required
            default: Default value
            validator: Custom validator function
            **kwargs: Additional field arguments (min_value, max_value, etc.)
        """
        if field_type not in [ft.value for ft in FieldType]:
            raise VSONSchemaError(
                f"Unknown field type: {field_type}",
                schema_name=self.name
            )
        
        field = Field(
            name=name,
            field_type=field_type,
            required=required,
            default=default,
            validator=validator,
            **kwargs
        )
        
        self.fields[name] = field
    
    def validate(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate data against schema
        
        Args:
            data: Data to validate
        
        Returns:
            Tuple of (is_valid, error_list)
        """
        errors = []
        
        for field_name, field in self.fields.items():
            value = data.get(field_name, field.default)
            is_valid, error_msg = field.validate(value)
            
            if not is_valid:
                errors.append(error_msg)
        
        return len(errors) == 0, errors
    
    def validate_strict(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Strict validation - disallow extra fields
        
        Args:
            data: Data to validate
        
        Returns:
            Tuple of (is_valid, error_list)
        """
        is_valid, errors = self.validate(data)
        
        # Check for unknown fields
        for key in data.keys():
            if key not in self.fields:
                errors.append(f"Unknown field: {key}")
                is_valid = False
        
        return is_valid, errors
    
    def infer_schema(
        self,
        data: List[Dict[str, Any]],
        auto_required: bool = False
    ) -> 'VSONSchema':
        """
        Infer schema from data
        
        Args:
            data: List of data records
            auto_required: Mark fields as required if present in all records
        
        Returns:
            self for chaining
        """
        if not data:
            return self
        
        first_item = data[0]
        field_counts = {}
        total_records = len(data)
        
        for record in data:
            for key in record.keys():
                field_counts[key] = field_counts.get(key, 0) + 1
        
        for key, value in first_item.items():
            # Infer type
            if isinstance(value, bool):
                field_type = "bool"
            elif isinstance(value, int):
                field_type = "int"
            elif isinstance(value, float):
                field_type = "float"
            elif isinstance(value, str):
                field_type = "str"
            elif isinstance(value, list):
                field_type = "list"
            elif isinstance(value, dict):
                field_type = "dict"
            else:
                field_type = "str"
            
            # Check if required
            required = auto_required and field_counts.get(key, 0) == total_records
            
            self.add_field(key, field_type, required=required)
        
        return self
    
    def export_schema(self) -> Dict[str, Any]:
        """
        Export schema as dictionary
        
        Returns:
            Dictionary representation of schema
        """
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "fields": {
                name: {
                    "type": field.field_type,
                    "required": field.required,
                    "default": field.default,
                    "description": field.description,
                }
                for name, field in self.fields.items()
            }
        }
    
    def to_json(self) -> str:
        """Export schema as JSON string"""
        import json
        return json.dumps(self.export_schema(), indent=2)
    
    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> 'VSONSchema':
        """
        Create schema from dictionary
        
        Args:
            schema_dict: Dictionary representation
        
        Returns:
            VSONSchema instance
        """
        schema = cls(
            name=schema_dict.get("name", "imported"),
            description=schema_dict.get("description", "")
        )
        
        for field_name, field_def in schema_dict.get("fields", {}).items():
            schema.add_field(
                name=field_name,
                field_type=field_def.get("type", "str"),
                required=field_def.get("required", False),
                default=field_def.get("default"),
                description=field_def.get("description")
            )
        
        return schema
    
    # =====================================================================
    # PRE-BUILT SCHEMAS
    # =====================================================================
    
    @staticmethod
    def market_data_schema() -> 'VSONSchema':
        """
        Pre-built schema for market data (OHLC + volume)
        
        Returns:
            VSONSchema configured for market data
        """
        schema = VSONSchema(
            name="market_data",
            description="OHLC and volume data"
        )
        
        # Core fields
        schema.add_field("timestamp", "timestamp", required=True,
                        description="ISO 8601 timestamp")
        schema.add_field("open", "price", required=True,
                        min_value=0, description="Opening price")
        schema.add_field("high", "price", required=True,
                        min_value=0, description="High price")
        schema.add_field("low", "price", required=True,
                        min_value=0, description="Low price")
        schema.add_field("close", "price", required=True,
                        min_value=0, description="Closing price")
        schema.add_field("volume", "volume", required=True,
                        min_value=0, description="Trading volume")
        schema.add_field("last_price", "price",
                        description="Last traded price")
        
        return schema
    
    @staticmethod
    def market_depth_schema() -> 'VSONSchema':
        """
        Pre-built schema for market depth data
        
        Returns:
            VSONSchema with depth levels
        """
        schema = VSONSchema(
            name="market_depth",
            description="Market depth (bid/ask)"
        )
        
        schema.add_field("timestamp", "timestamp", required=True)
        
        # Bid levels
        for i in range(1, 6):
            schema.add_field(f"bid_qty_{i}", "volume")
            schema.add_field(f"bid_price_{i}", "price")
            schema.add_field(f"bid_orders_{i}", "int")
        
        # Ask levels
        for i in range(1, 6):
            schema.add_field(f"ask_qty_{i}", "volume")
            schema.add_field(f"ask_price_{i}", "price")
            schema.add_field(f"ask_orders_{i}", "int")
        
        return schema
    
    @staticmethod
    def timeseries_schema() -> 'VSONSchema':
        """
        Pre-built schema for time-series data
        
        Returns:
            VSONSchema for time-series
        """
        schema = VSONSchema(
            name="timeseries",
            description="Time-series data"
        )
        
        schema.add_field("timestamp", "timestamp", required=True)
        schema.add_field("value", "float", required=True)
        
        return schema
