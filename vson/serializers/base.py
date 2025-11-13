# vson/serializers/base.py
"""
Base Serializer Class

Abstract base class for all serializers.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class BaseSerializer(ABC):
    """
    Abstract base class for VSON serializers.
    
    All serializers must inherit from this class and implement
    serialize() and deserialize() methods.
    """
    
    def __init__(self, schema: Optional[Dict[str, Any]] = None):
        """
        Initialize serializer
        
        Args:
            schema: Optional data schema
        """
        self.schema = schema
    
    @abstractmethod
    def serialize(self, data: Dict[str, Any]) -> str:
        """
        Serialize data to VSON string
        
        Args:
            data: Data to serialize
        
        Returns:
            VSON formatted string
        """
        pass
    
    @abstractmethod
    def deserialize(self, data: str) -> Dict[str, Any]:
        """
        Deserialize VSON string to data
        
        Args:
            data: VSON formatted string
        
        Returns:
            Deserialized data dictionary
        """
        pass
    
    def get_schema(self) -> Optional[Dict[str, Any]]:
        """
        Get schema for this serializer
        
        Returns:
            Schema dictionary or None
        """
        return self.schema
    
    def set_schema(self, schema: Dict[str, Any]) -> None:
        """
        Set schema for this serializer
        
        Args:
            schema: Schema dictionary
        """
        self.schema = schema
    
    def validate(self, data: Dict[str, Any]) -> tuple:
        """
        Validate data against schema
        
        Args:
            data: Data to validate
        
        Returns:
            Tuple of (is_valid, errors)
        """
        if not self.schema:
            return True, []
        
        errors = []
        
        # Basic validation
        if not isinstance(data, dict):
            errors.append("Data must be dictionary")
        
        return len(errors) == 0, errors
