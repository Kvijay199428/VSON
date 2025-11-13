# vson/validators/schema_validator.py
"""
Schema Validators

Validates data against schema definitions.
"""

from typing import Dict, Any, List, Tuple, Optional

from .data_types import DataTypeValidator


class SchemaValidator:
    """
    Validate data against schema definitions.
    
    Checks:
    - Required fields present
    - Field types correct
    - Field values in valid range
    """
    
    @staticmethod
    def validate_required_fields(
        data: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate all required fields are present
        
        Args:
            data: Data to validate
            schema: Schema definition
        
        Returns:
            Tuple of (is_valid, errors)
        """
        errors = []
        
        for field_name, field_def in schema.items():
            if field_def.get('required', False):
                if field_name not in data:
                    errors.append(f"Required field '{field_name}' missing")
                elif data[field_name] is None or data[field_name] == '':
                    errors.append(f"Required field '{field_name}' is empty")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_field_types(
        data: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate field types match schema
        
        Args:
            data: Data to validate
            schema: Schema definition
        
        Returns:
            Tuple of (is_valid, errors)
        """
        errors = []
        
        for field_name, field_def in schema.items():
            if field_name not in data:
                continue
            
            value = data[field_name]
            expected_type = field_def.get('type', 'str')
            
            is_valid, error = DataTypeValidator.validate_by_type(value, expected_type)
            
            if not is_valid:
                errors.append(f"Field '{field_name}': {error}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_field_constraints(
        data: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate field value constraints
        
        Args:
            data: Data to validate
            schema: Schema definition
        
        Returns:
            Tuple of (is_valid, errors)
        """
        errors = []
        
        for field_name, field_def in schema.items():
            if field_name not in data:
                continue
            
            value = data[field_name]
            
            # Range check
            if 'min_value' in field_def or 'max_value' in field_def:
                is_valid, error = DataTypeValidator.validate_range(
                    value,
                    min_value=field_def.get('min_value'),
                    max_value=field_def.get('max_value')
                )
                if not is_valid:
                    errors.append(f"Field '{field_name}': {error}")
            
            # Length check
            if 'min_length' in field_def or 'max_length' in field_def:
                is_valid, error = DataTypeValidator.validate_length(
                    value,
                    min_length=field_def.get('min_length'),
                    max_length=field_def.get('max_length')
                )
                if not is_valid:
                    errors.append(f"Field '{field_name}': {error}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_no_unknown_fields(
        data: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate no unknown fields in data
        
        Args:
            data: Data to validate
            schema: Schema definition
        
        Returns:
            Tuple of (is_valid, errors)
        """
        errors = []
        
        known_fields = set(schema.keys())
        data_fields = set(data.keys())
        unknown = data_fields - known_fields
        
        if unknown:
            errors.append(f"Unknown fields: {', '.join(unknown)}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_complete(
        data: Dict[str, Any],
        schema: Dict[str, Any],
        strict: bool = False
    ) -> Tuple[bool, List[str]]:
        """
        Complete schema validation
        
        Args:
            data: Data to validate
            schema: Schema definition
            strict: Disallow unknown fields
        
        Returns:
            Tuple of (is_valid, errors)
        """
        all_errors = []
        
        # Check required fields
        _, errors = SchemaValidator.validate_required_fields(data, schema)
        all_errors.extend(errors)
        
        # Check field types
        _, errors = SchemaValidator.validate_field_types(data, schema)
        all_errors.extend(errors)
        
        # Check constraints
        _, errors = SchemaValidator.validate_field_constraints(data, schema)
        all_errors.extend(errors)
        
        # Check unknown fields if strict
        if strict:
            _, errors = SchemaValidator.validate_no_unknown_fields(data, schema)
            all_errors.extend(errors)
        
        return len(all_errors) == 0, all_errors


class SchemaInferencer:
    """
    Infer schema from data samples
    """
    
    @staticmethod
    def infer_from_data(
        records: List[Dict[str, Any]],
        auto_required: bool = False
    ) -> Dict[str, Any]:
        """
        Infer schema from data records
        
        Args:
            records: List of data records
            auto_required: Mark fields required if in all records
        
        Returns:
            Schema dictionary
        """
        if not records:
            return {}
        
        schema = {}
        field_counts = {}
        total_records = len(records)
        
        # Count field occurrences
        for record in records:
            for key in record.keys():
                field_counts[key] = field_counts.get(key, 0) + 1
        
        # Build schema from first record
        first_record = records[0]
        
        for key, value in first_record.items():
            # Infer type
            field_type = SchemaInferencer._infer_type(value)
            
            # Check if required
            count = field_counts.get(key, 0)
            required = auto_required and count == total_records
            
            schema[key] = {
                'type': field_type,
                'required': required,
                'present_in_records': count,
            }
        
        return schema
    
    @staticmethod
    def _infer_type(value: Any) -> str:
        """Infer type from value"""
        if isinstance(value, bool):
            return 'bool'
        elif isinstance(value, int):
            return 'int'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            # Check if timestamp-like
            if 'T' in value or '-' in value and ':' in value:
                return 'timestamp'
            return 'str'
        elif isinstance(value, list):
            return 'list'
        elif isinstance(value, dict):
            return 'dict'
        else:
            return 'str'
