# vson/parser.py
"""
VSON Parser Module

This module provides parsing functionality to convert VSON strings
into Python dictionaries and objects.
"""

from typing import Dict, Any, List, Tuple, Optional
import re

from .exceptions import VSONParseError
from .config import Config


class VSONParser:
    """
    Parse VSON format strings into Python dictionaries.
    
    The parser handles:
    - Header metadata (key: value pairs)
    - Array definitions (name[count]{fields}:)
    - Data rows (CSV format)
    - Type inference
    - Error reporting with line/column information
    
    Example:
        parser = VSONParser()
        data = parser.parse(vson_string)
    """
    
    def __init__(self, schema=None, strict: bool = False):
        """
        Initialize parser
        
        Args:
            schema: Optional schema for validation
            strict: Enable strict mode validation
        """
        self.schema = schema
        self.strict = strict
        self.current_line = 0
        self.current_col = 0
        self._line_buffer = []
    
    def parse(self, vson_str: str, validate: bool = True) -> Dict[str, Any]:
        """
        Parse VSON string to dictionary
        
        Args:
            vson_str: VSON formatted string
            validate: Enable validation
        
        Returns:
            Parsed data as dictionary
        
        Raises:
            VSONParseError: If parsing fails
        """
        if not vson_str or not vson_str.strip():
            raise VSONParseError("Empty VSON string")
        
        lines = vson_str.strip().split('\n')
        result = {}
        
        try:
            i = 0
            
            # Parse header (metadata)
            i = self._parse_header(lines, result, i)
            
            # Parse arrays (data sections)
            while i < len(lines):
                i = self._skip_comments_and_blanks(lines, i)
                if i >= len(lines):
                    break
                
                line = lines[i].strip()
                
                # Check for array definition
                if self._is_array_definition(line):
                    array_name, array_data, next_i = self._parse_array(lines, i)
                    if array_name:
                        result[array_name] = array_data
                    i = next_i
                else:
                    i += 1
            
            return result
        
        except VSONParseError:
            raise
        except Exception as e:
            raise VSONParseError(
                f"Parse error: {str(e)}",
                line=self.current_line,
                column=self.current_col
            )
    
    def _parse_header(
        self,
        lines: List[str],
        result: Dict,
        start_idx: int
    ) -> int:
        """
        Parse header section (key: value pairs)
        
        Args:
            lines: All lines of VSON
            result: Result dictionary to populate
            start_idx: Starting line index
        
        Returns:
            Next line index to process
        """
        i = start_idx
        
        while i < len(lines):
            line = lines[i].strip()
            self.current_line = i + 1
            
            # Skip empty lines and comments
            if not line or line.startswith(Config.COMMENT_CHAR):
                i += 1
                continue
            
            # Check if this is an array definition (end of header)
            if self._is_array_definition(line):
                break
            
            # Parse key: value
            if Config.HEADER_DELIMITER in line:
                key, value = line.split(Config.HEADER_DELIMITER, 1)
                result[key.strip()] = value.strip()
                i += 1
            else:
                break
        
        return i
    
    def _parse_array(
        self,
        lines: List[str],
        start_idx: int
    ) -> Tuple[Optional[str], List[Dict], int]:
        """
        Parse array section
        
        Args:
            lines: All lines
            start_idx: Starting line index
        
        Returns:
            Tuple of (array_name, array_data, next_index)
        """
        line = lines[start_idx].strip()
        self.current_line = start_idx + 1
        
        try:
            # Parse array header: name[count]{fields}:
            array_name, field_names = self._parse_array_header(line)
            array_data = []
            
            i = start_idx + 1
            
            # Skip blank lines after header
            i = self._skip_comments_and_blanks(lines, i)
            
            # Parse data rows
            while i < len(lines):
                line = lines[i].strip()
                self.current_line = i + 1
                
                # Skip empty lines and comments
                if not line or line.startswith(Config.COMMENT_CHAR):
                    i += 1
                    continue
                
                # Check for next array definition
                if self._is_array_definition(line):
                    break
                
                # Parse row
                values = self._parse_row(line)
                if values:
                    row_dict = {}
                    for field_name, value in zip(field_names, values):
                        row_dict[field_name] = value
                    array_data.append(row_dict)
                
                i += 1
            
            return array_name, array_data, i
        
        except Exception as e:
            raise VSONParseError(
                f"Array parse error: {str(e)}",
                line=self.current_line
            )
    
    def _parse_array_header(self, line: str) -> Tuple[str, List[str]]:
        """
        Parse array definition line
        Format: arrayname[count]{field1,field2,...}:
        
        Args:
            line: Array header line
        
        Returns:
            Tuple of (array_name, field_names)
        """
        # Extract name
        if not line.endswith(':'):
            line = line.rstrip(':')
        
        match = re.match(
            r'(\w+)\[\d+\]\{(.+?)\}',
            line
        )
        
        if not match:
            raise VSONParseError(
                f"Invalid array definition: {line}",
                line=self.current_line
            )
        
        array_name = match.group(1)
        fields_str = match.group(2)
        field_names = [f.strip() for f in fields_str.split(',')]
        
        return array_name, field_names
    
    def _parse_row(self, line: str) -> Optional[List[Any]]:
        """
        Parse data row with type inference
        
        Args:
            line: CSV formatted row
        
        Returns:
            List of typed values
        """
        if not line:
            return None
        
        # Split by delimiter
        values = line.split(Config.FIELD_DELIMITER)
        
        # Convert types
        result = []
        for value in values:
            value = value.strip()
            converted = self._infer_type(value)
            result.append(converted)
        
        return result
    
    @staticmethod
    def _infer_type(value: str) -> Any:
        """
        Infer and convert value type
        
        Args:
            value: String value
        
        Returns:
            Value with inferred type
        """
        if not value or value == "":
            return None
        
        # Boolean
        if value.lower() == 'true':
            return True
        if value.lower() == 'false':
            return False
        
        # None/null
        if value.lower() in ('none', 'null', 'nil'):
            return None
        
        # Numeric
        try:
            # Try integer first
            if '.' not in value and 'e' not in value.lower():
                return int(value)
            # Then float
            return float(value)
        except ValueError:
            pass
        
        # String (default)
        return value
    
    def _is_array_definition(self, line: str) -> bool:
        """Check if line is array definition"""
        return (
            '[' in line and
            ']' in line and
            '{' in line and
            '}' in line
        )
    
    @staticmethod
    def _skip_comments_and_blanks(lines: List[str], start_idx: int) -> int:
        """Skip comment and blank lines"""
        i = start_idx
        while i < len(lines):
            line = lines[i].strip()
            if line and not line.startswith(Config.COMMENT_CHAR):
                break
            i += 1
        return i


class StreamingVSONParser:
    """
    Parse VSON files in streaming mode for large datasets.
    
    Useful for:
    - Processing very large files without loading into memory
    - Real-time data processing
    - Incremental parsing
    
    Example:
        parser = StreamingVSONParser("large_file.vson", chunk_size=1000)
        for chunk in parser:
            process(chunk)
    """
    
    def __init__(self, filepath: str, chunk_size: int = Config.CHUNK_SIZE):
        """
        Initialize streaming parser
        
        Args:
            filepath: Path to VSON file
            chunk_size: Records per chunk
        """
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.parser = VSONParser()
    
    def __iter__(self):
        """Iterate over chunks"""
        with open(self.filepath, 'r', encoding=Config.DEFAULT_ENCODING) as f:
            chunk = []
            for line in f:
                line = line.strip()
                if line and not line.startswith(Config.COMMENT_CHAR):
                    chunk.append(line)
                
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
            
            if chunk:
                yield chunk
    
    def parse_chunks(self) -> List[Dict[str, Any]]:
        """Parse all chunks and collect"""
        all_data = []
        for chunk in self:
            all_data.extend(chunk)
        return all_data
