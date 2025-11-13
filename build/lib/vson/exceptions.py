# vson/exceptions.py
"""
VSON Custom Exceptions

This module defines all custom exception classes for the VSON library.
Each exception provides specific error information and context.
"""


class VSONError(Exception):
    """
    Base exception class for all VSON-related errors.
    
    All other VSON exceptions inherit from this class, allowing
    users to catch all VSON errors with a single except block.
    
    Example:
        try:
            vson.smart_decode("file.vson")
        except vson.VSONError as e:
            print(f"VSON error: {e}")
    """
    
    def __init__(self, message: str, context: dict = None):
        """
        Initialize VSONError
        
        Args:
            message: Error message
            context: Additional context information
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
    
    def __str__(self) -> str:
        return f"VSONError: {self.message}"


class VSONParseError(VSONError):
    """
    Raised when parsing VSON fails.
    
    This exception is raised when the VSON parser encounters invalid format,
    unexpected structure, or malformed data.
    
    Attributes:
        line: Line number where error occurred
        column: Column number where error occurred
        char: Character that caused the error
    
    Example:
        try:
            vson.smart_decode("invalid.vson")
        except vson.VSONParseError as e:
            print(f"Parse error at line {e.line}, col {e.column}: {e.message}")
    """
    
    def __init__(
        self,
        message: str,
        line: int = None,
        column: int = None,
        char: str = None,
        context: dict = None
    ):
        """
        Initialize VSONParseError
        
        Args:
            message: Error message
            line: Line number (1-indexed)
            column: Column number (1-indexed)
            char: Character that caused error
            context: Additional context
        """
        super().__init__(message, context)
        self.line = line
        self.column = column
        self.char = char
    
    def __str__(self) -> str:
        if self.line is not None and self.column is not None:
            return (
                f"VSONParseError at line {self.line}, column {self.column}: "
                f"{self.message}"
            )
        return f"VSONParseError: {self.message}"


class VSONEncodingError(VSONError):
    """
    Raised when encoding fails.
    
    This exception is raised when converting Python objects to VSON format fails.
    Common causes include unsupported types or invalid data structures.
    
    Example:
        try:
            vson.smart_encode(invalid_data)
        except vson.VSONEncodingError as e:
            print(f"Encoding failed: {e}")
    """
    
    def __init__(self, message: str, obj_type: str = None, context: dict = None):
        """
        Initialize VSONEncodingError
        
        Args:
            message: Error message
            obj_type: Type of object that failed to encode
            context: Additional context
        """
        super().__init__(message, context)
        self.obj_type = obj_type
    
    def __str__(self) -> str:
        if self.obj_type:
            return f"VSONEncodingError for type {self.obj_type}: {self.message}"
        return f"VSONEncodingError: {self.message}"


class VSONSchemaError(VSONError):
    """
    Raised for schema-related errors.
    
    This exception is raised when:
    - Schema definition is invalid
    - Data doesn't match schema
    - Schema validation fails
    
    Attributes:
        schema_name: Name of the schema
        field_name: Name of the field that caused error
    
    Example:
        try:
            schema = vson.VSONSchema("data")
            schema.add_field("price", "invalid_type")  # Invalid type
        except vson.VSONSchemaError as e:
            print(f"Schema error: {e}")
    """
    
    def __init__(
        self,
        message: str,
        schema_name: str = None,
        field_name: str = None,
        context: dict = None
    ):
        """
        Initialize VSONSchemaError
        
        Args:
            message: Error message
            schema_name: Name of schema
            field_name: Name of problematic field
            context: Additional context
        """
        super().__init__(message, context)
        self.schema_name = schema_name
        self.field_name = field_name
    
    def __str__(self) -> str:
        parts = ["VSONSchemaError"]
        if self.schema_name:
            parts.append(f" in schema '{self.schema_name}'")
        if self.field_name:
            parts.append(f" field '{self.field_name}'")
        parts.append(f": {self.message}")
        return "".join(parts)


class VSONTypeError(VSONError):
    """
    Raised for type mismatch errors.
    
    This exception is raised when:
    - Data type doesn't match expected type
    - Conversion fails
    - Type validation fails
    
    Attributes:
        expected_type: Expected data type
        actual_type: Actual data type provided
        value: Value that caused the error
    
    Example:
        try:
            schema.validate({"price": "not_a_number"})
        except vson.VSONTypeError as e:
            print(f"Expected {e.expected_type}, got {e.actual_type}")
    """
    
    def __init__(
        self,
        message: str,
        expected_type: str = None,
        actual_type: str = None,
        value=None,
        context: dict = None
    ):
        """
        Initialize VSONTypeError
        
        Args:
            message: Error message
            expected_type: Expected type name
            actual_type: Actual type name
            value: Value that caused error
            context: Additional context
        """
        super().__init__(message, context)
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.value = value
    
    def __str__(self) -> str:
        if self.expected_type and self.actual_type:
            return (
                f"VSONTypeError: expected {self.expected_type}, "
                f"got {self.actual_type}: {self.message}"
            )
        return f"VSONTypeError: {self.message}"


class VSONIOError(VSONError):
    """
    Raised for file I/O errors.
    
    This exception is raised when:
    - File cannot be opened
    - File read/write fails
    - Path is invalid
    
    Attributes:
        filepath: Path to file that caused error
        operation: Operation that failed (read, write, etc.)
    
    Example:
        try:
            vson.smart_decode("nonexistent.vson")
        except vson.VSONIOError as e:
            print(f"I/O error on {e.filepath}: {e}")
    """
    
    def __init__(
        self,
        message: str,
        filepath: str = None,
        operation: str = None,
        context: dict = None
    ):
        """
        Initialize VSONIOError
        
        Args:
            message: Error message
            filepath: Path to file
            operation: Operation (read, write, append, etc.)
            context: Additional context
        """
        super().__init__(message, context)
        self.filepath = filepath
        self.operation = operation
    
    def __str__(self) -> str:
        parts = ["VSONIOError"]
        if self.operation:
            parts.append(f" during {self.operation}")
        if self.filepath:
            parts.append(f" on '{self.filepath}'")
        parts.append(f": {self.message}")
        return "".join(parts)


class VSONValidationError(VSONError):
    """
    Raised for validation errors.
    
    This exception is raised when data validation fails.
    
    Attributes:
        errors: List of validation errors
    
    Example:
        try:
            schema.validate(data)
        except vson.VSONValidationError as e:
            print(f"Validation failed: {e.errors}")
    """
    
    def __init__(self, message: str, errors: list = None, context: dict = None):
        """
        Initialize VSONValidationError
        
        Args:
            message: Error message
            errors: List of validation errors
            context: Additional context
        """
        super().__init__(message, context)
        self.errors = errors or []
    
    def __str__(self) -> str:
        if self.errors:
            error_str = "\n  ".join(self.errors)
            return f"VSONValidationError: {self.message}\n  {error_str}"
        return f"VSONValidationError: {self.message}"


class VSONCompressionError(VSONError):
    """
    Raised for compression-related errors.
    
    This exception is raised when compression/decompression fails.
    
    Attributes:
        compression_method: Compression method that failed
    
    Example:
        try:
            vson.smart_encode(data, compression="unsupported")
        except vson.VSONCompressionError as e:
            print(f"Compression error: {e}")
    """
    
    def __init__(
        self,
        message: str,
        compression_method: str = None,
        context: dict = None
    ):
        """
        Initialize VSONCompressionError
        
        Args:
            message: Error message
            compression_method: Method that failed
            context: Additional context
        """
        super().__init__(message, context)
        self.compression_method = compression_method
    
    def __str__(self) -> str:
        if self.compression_method:
            return (
                f"VSONCompressionError ({self.compression_method}): "
                f"{self.message}"
            )
        return f"VSONCompressionError: {self.message}"


class VSONTimeoutError(VSONError):
    """
    Raised when operation times out.
    
    This exception is raised when:
    - Parsing takes too long
    - Encoding takes too long
    - File operations timeout
    
    Attributes:
        timeout_seconds: Timeout duration in seconds
    """
    
    def __init__(
        self,
        message: str,
        timeout_seconds: float = None,
        context: dict = None
    ):
        """
        Initialize VSONTimeoutError
        
        Args:
            message: Error message
            timeout_seconds: Timeout duration
            context: Additional context
        """
        super().__init__(message, context)
        self.timeout_seconds = timeout_seconds
    
    def __str__(self) -> str:
        if self.timeout_seconds:
            return (
                f"VSONTimeoutError (timeout: {self.timeout_seconds}s): "
                f"{self.message}"
            )
        return f"VSONTimeoutError: {self.message}"


# =========================================================================
# ERROR HELPER FUNCTIONS
# =========================================================================

def raise_parse_error(message: str, line: int = None, column: int = None) -> None:
    """Helper to raise parse error"""
    raise VSONParseError(message, line=line, column=column)


def raise_encoding_error(message: str, obj_type: str = None) -> None:
    """Helper to raise encoding error"""
    raise VSONEncodingError(message, obj_type=obj_type)


def raise_schema_error(
    message: str,
    schema_name: str = None,
    field_name: str = None
) -> None:
    """Helper to raise schema error"""
    raise VSONSchemaError(message, schema_name=schema_name, field_name=field_name)


def raise_type_error(
    message: str,
    expected_type: str = None,
    actual_type: str = None
) -> None:
    """Helper to raise type error"""
    raise VSONTypeError(
        message,
        expected_type=expected_type,
        actual_type=actual_type
    )


def raise_io_error(
    message: str,
    filepath: str = None,
    operation: str = None
) -> None:
    """Helper to raise I/O error"""
    raise VSONIOError(message, filepath=filepath, operation=operation)
