# vson/validators/data_types.py
"""
Data Type Validators

Provides validators for all supported data types.
"""

from typing import Any, Tuple, List, Optional


class DataTypeValidator:
    """
    Validates data types for VSON format.
    
    Supports:
    - Primitives: int, float, str, bool, None
    - Collections: list, dict
    - Custom: timestamp, price, volume
    """
    
    # Type validators - each returns (is_valid: bool, error_message: str)
    
    @staticmethod
    def validate_int(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate integer type
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if isinstance(value, bool):
            return False, "Boolean is not valid as int"
        
        if not isinstance(value, int):
            return False, f"Expected int, got {type(value).__name__}"
        
        return True, None
    
    @staticmethod
    def validate_float(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate float type
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if isinstance(value, bool):
            return False, "Boolean is not valid as float"
        
        if not isinstance(value, (int, float)):
            return False, f"Expected float, got {type(value).__name__}"
        
        return True, None
    
    @staticmethod
    def validate_string(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate string type
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if not isinstance(value, str):
            return False, f"Expected str, got {type(value).__name__}"
        
        return True, None
    
    @staticmethod
    def validate_bool(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate boolean type
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if not isinstance(value, bool):
            return False, f"Expected bool, got {type(value).__name__}"
        
        return True, None
    
    @staticmethod
    def validate_list(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate list type
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if not isinstance(value, list):
            return False, f"Expected list, got {type(value).__name__}"
        
        return True, None
    
    @staticmethod
    def validate_dict(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate dictionary type
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if not isinstance(value, dict):
            return False, f"Expected dict, got {type(value).__name__}"
        
        return True, None
    
    @staticmethod
    def validate_timestamp(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate ISO 8601 timestamp format
        
        Args:
            value: Value to validate (should be ISO string)
        
        Returns:
            Tuple of (is_valid, error_message)
        
        Example:
            Valid: "2023-10-19T09:15:00.000+05:30"
        """
        if value is None:
            return True, None
        
        if not isinstance(value, str):
            return False, f"Timestamp must be string, got {type(value).__name__}"
        
        # Check basic ISO format
        if not ('T' in value or '-' in value):
            return False, "Invalid timestamp format (expected ISO 8601)"
        
        # Check for required components
        if not ('-' in value or ':' in value):
            return False, "Timestamp missing date or time components"
        
        return True, None
    
    @staticmethod
    def validate_price(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate price (non-negative number)
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if not isinstance(value, (int, float)):
            return False, f"Price must be numeric, got {type(value).__name__}"
        
        if value < 0:
            return False, f"Price cannot be negative: {value}"
        
        return True, None
    
    @staticmethod
    def validate_volume(value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate volume (non-negative integer)
        
        Args:
            value: Value to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if isinstance(value, bool):
            return False, "Boolean not valid as volume"
        
        if not isinstance(value, int):
            return False, f"Volume must be integer, got {type(value).__name__}"
        
        if value < 0:
            return False, f"Volume cannot be negative: {value}"
        
        return True, None
    
    @staticmethod
    def validate_by_type(value: Any, field_type: str) -> Tuple[bool, Optional[str]]:
        """
        Validate value by type name
        
        Args:
            value: Value to validate
            field_type: Type name (int, float, str, bool, list, dict, etc.)
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        validators = {
            'int': DataTypeValidator.validate_int,
            'float': DataTypeValidator.validate_float,
            'str': DataTypeValidator.validate_string,
            'bool': DataTypeValidator.validate_bool,
            'list': DataTypeValidator.validate_list,
            'dict': DataTypeValidator.validate_dict,
            'timestamp': DataTypeValidator.validate_timestamp,
            'price': DataTypeValidator.validate_price,
            'volume': DataTypeValidator.validate_volume,
        }
        
        validator = validators.get(field_type)
        
        if not validator:
            return False, f"Unknown type: {field_type}"
        
        return validator(value)
    
    @staticmethod
    def validate_range(
        value: Any,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate value is within range
        
        Args:
            value: Value to validate
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if not isinstance(value, (int, float)):
            return False, "Range validation requires numeric value"
        
        if min_value is not None and value < min_value:
            return False, f"Value {value} below minimum {min_value}"
        
        if max_value is not None and value > max_value:
            return False, f"Value {value} above maximum {max_value}"
        
        return True, None
    
    @staticmethod
    def validate_length(
        value: Any,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate string/list length
        
        Args:
            value: Value to validate
            min_length: Minimum length
            max_length: Maximum length
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None:
            return True, None
        
        if not isinstance(value, (str, list)):
            return False, "Length validation requires string or list"
        
        length = len(value)
        
        if min_length is not None and length < min_length:
            return False, f"Length {length} below minimum {min_length}"
        
        if max_length is not None and length > max_length:
            return False, f"Length {length} above maximum {max_length}"
        
        return True, None


class TypeConverter:
    """
    Convert values to target types
    """
    
    @staticmethod
    def to_int(value: Any) -> int:
        """Convert to integer"""
        if isinstance(value, bool):
            raise ValueError("Cannot convert bool to int")
        return int(value)
    
    @staticmethod
    def to_float(value: Any) -> float:
        """Convert to float"""
        if isinstance(value, bool):
            raise ValueError("Cannot convert bool to float")
        return float(value)
    
    @staticmethod
    def to_string(value: Any) -> str:
        """Convert to string"""
        return str(value)
    
    @staticmethod
    def to_bool(value: Any) -> bool:
        """Convert to boolean"""
        if isinstance(value, str):
            return value.lower() in ('true', 'yes', '1')
        return bool(value)
    
    @staticmethod
    def to_type(value: Any, target_type: str) -> Any:
        """Convert value to target type"""
        converters = {
            'int': TypeConverter.to_int,
            'float': TypeConverter.to_float,
            'str': TypeConverter.to_string,
            'bool': TypeConverter.to_bool,
        }
        
        converter = converters.get(target_type)
        if not converter:
            return value
        
        try:
            return converter(value)
        except Exception:
            return value
