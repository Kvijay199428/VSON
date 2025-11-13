# vson/core.py - COMPLETE SMART INTERFACE
"""
VSON Core Module - Smart Single-Function Interface

This is the main API providing:
- smart_encode() - Unified encoding for all modes
- smart_decode() - Unified decoding with auto-detection
- Four modes: default, incremental_a, delta_b, depth_c
"""

from typing import Dict, Any, Optional, List, Union, Iterator
from pathlib import Path
from enum import Enum
from datetime import datetime
import json

from .encoder import VSONEncoder, DeltaEncoder
from .parser import VSONParser, StreamingVSONParser
from .schema import VSONSchema
from .exceptions import (
    VSONError, VSONEncodingError, VSONParseError, VSONIOError
)
from .config import Config


class EncodingMode(Enum):
    """Supported encoding modes"""
    DEFAULT = "default"              # All features
    INCREMENTAL_A = "incremental_a"  # Append to file
    DELTA_B = "delta_b"              # Delta compression
    DEPTH_C = "depth_c"              # Depth embedding


class VSONSmart:
    """Smart VSON handler - unified interface for all operations"""
    
    def __init__(self, schema: Optional[VSONSchema] = None, verbose: bool = False):
        """Initialize smart handler"""
        self.schema = schema or VSONSchema.market_data_schema()
        self.encoder = VSONEncoder(schema)
        self.parser = VSONParser(schema)
        self.delta_encoder = DeltaEncoder()
        self.verbose = verbose
        self._compression_state = {}
    
    def smart_encode(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        filepath: Optional[Union[str, Path]] = None,
        mode: str = "default",
        **options
    ) -> Union[str, None]:
        """
        Smart encode - unified function for all modes
        
        Modes:
        - default: All features (incremental + delta + depth)
        - incremental_a: Append to existing file
        - delta_b: Delta compression (base + deltas)
        - depth_c: Full depth embedding
        """
        
        # Validate input
        if not data:
            raise VSONEncodingError("Data cannot be empty")
        
        # Normalize to list
        data_list = data if isinstance(data, list) else [data]
        
        if self.verbose:
            print(f"ðŸ”„ Encoding {len(data_list)} records in {mode} mode")
        
        # Route to handler
        if mode == "default":
            vson_str = self._encode_default(data_list, **options)
        elif mode == "incremental_a":
            vson_str = self._encode_incremental(data_list, filepath, **options)
        elif mode == "delta_b":
            vson_str = self._encode_delta(data_list, **options)
        elif mode == "depth_c":
            vson_str = self._encode_with_depth(data_list, **options)
        else:
            raise VSONEncodingError(f"Unknown mode: {mode}")
        
        # Write to file
        if filepath:
            self._write_file(vson_str, filepath, **options)
            if self.verbose:
                print(f"âœ… Saved to {filepath}")
            return None
        
        return vson_str
    
    def smart_decode(
        self,
        source: Union[str, Path, Dict],
        mode: str = "default",
        **options
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Smart decode - auto-detect and decode"""
        
        # Determine source type
        if isinstance(source, dict):
            return source
        
        if isinstance(source, Path) or (isinstance(source, str) and '\\n' not in source and not source.startswith('[')):
            # File path
            vson_str = Path(source).read_text(encoding='utf-8')
            detected_mode = self._detect_mode(vson_str)
        else:
            # VSON string
            vson_str = source
            detected_mode = self._detect_mode(vson_str)
        
        # Use detected mode if default
        final_mode = mode if mode != "default" else detected_mode
        
        if self.verbose:
            print(f"ðŸ“– Decoding in {final_mode} mode")
        
        # Route to handler
        if final_mode == "delta_b":
            return self._decode_delta(vson_str, **options)
        elif final_mode == "depth_c":
            return self._decode_with_depth(vson_str, **options)
        elif final_mode == "incremental_a":
            return self._decode_incremental(vson_str, **options)
        else:
            return self._decode_default(vson_str, **options)
    
    # =====================================================================
    # MODE IMPLEMENTATIONS
    # =====================================================================
    
    def _encode_default(self, data_list: List[Dict], **options) -> str:
        """DEFAULT: All features enabled"""
        
        metadata = self._extract_metadata(data_list[0])
        
        lines = []
        for key, value in metadata.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        
        # Prepare records
        sample = data_list[0]
        fields = list(sample.keys())
        
        # Array header
        field_str = ", ".join(fields)
        lines.append(f"snapshots[{len(data_list)}]{{{field_str}}}:")
        lines.append("")
        
        # Data rows
        for record in data_list:
            values = [self.encoder._serialize_value(record.get(f, "")) for f in fields]
            lines.append(",".join(values))
        
        return "\n".join(lines)
    
    def _encode_incremental(
        self,
        new_data: List[Dict],
        filepath: Optional[Union[str, Path]] = None,
        **options
    ) -> str:
        """INCREMENTAL_A: Append to file"""
        
        existing_data = []
        
        if filepath and Path(filepath).exists():
            existing_content = Path(filepath).read_text()
            parsed = self.parser.parse(existing_content)
            existing_data = parsed.get("snapshots", [])
            
            if self.verbose:
                print(f"   Loaded {len(existing_data)} existing records")
        
        # Merge data
        merged = existing_data + new_data
        
        lines = []
        metadata = self._extract_metadata(new_data[0] if new_data else existing_data[0])
        metadata["total_snapshots"] = len(merged)
        metadata["last_update"] = datetime.now().isoformat()
        
        for key, value in metadata.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        
        # Array
        if merged:
            fields = list(merged[0].keys())
            field_str = ", ".join(fields)
            lines.append(f"snapshots[{len(merged)}]{{{field_str}}}:")
            lines.append("")
            
            for record in merged:
                values = [self.encoder._serialize_value(record.get(f, "")) for f in fields]
                lines.append(",".join(values))
        
        return "\n".join(lines)
    
    def _encode_delta(self, data_list: List[Dict], **options) -> str:
        """DELTA_B: Delta compression"""
        
        if not data_list:
            raise VSONEncodingError("No data for delta encoding")
        
        lines = []
        
        # Metadata
        metadata = self._extract_metadata(data_list[0])
        for key, value in metadata.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        lines.append("# DELTA COMPRESSION MODE")
        lines.append("")
        
        # Base snapshot
        base = data_list[0]
        base_fields = list(base.keys())
        base_field_str = ", ".join(base_fields)
        
        lines.append(f"base{{{base_field_str}}}:")
        values = [self.encoder._serialize_value(base.get(f, "")) for f in base_fields]
        lines.append(",".join(values))
        lines.append("")
        
        # Deltas
        if len(data_list) > 1:
            delta_fields = ["timestamp"] + [f"delta_{f}" for f in base_fields if f != "timestamp"]
            delta_field_str = ", ".join(delta_fields)
            
            lines.append(f"deltas[{len(data_list) - 1}]{{{delta_field_str}}}:")
            lines.append("")
            
            for i in range(1, len(data_list)):
                current = data_list[i]
                previous = data_list[i - 1]
                
                delta_record = {}
                delta_record["timestamp"] = current.get("timestamp", "")
                
                for field in base_fields:
                    if field != "timestamp":
                        curr_val = current.get(field, 0)
                        prev_val = previous.get(field, 0)
                        
                        if isinstance(curr_val, (int, float)) and isinstance(prev_val, (int, float)):
                            delta_record[f"delta_{field}"] = curr_val - prev_val
                        else:
                            delta_record[f"delta_{field}"] = curr_val
                
                delta_values = [self.encoder._serialize_value(delta_record.get(f, "")) for f in delta_fields]
                lines.append(",".join(delta_values))
        
        return "\n".join(lines)
    
    def _encode_with_depth(self, data_list: List[Dict], **options) -> str:
        """DEPTH_C: Full depth embedding"""
        
        metadata = self._extract_metadata(data_list[0])
        
        lines = []
        for key, value in metadata.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        
        # All fields including depth
        all_fields = [
            "timestamp", "open", "high", "low", "close", "volume", "last_price",
            "buy_qty_1", "buy_price_1", "buy_orders_1",
            "buy_qty_2", "buy_price_2", "buy_orders_2",
            "buy_qty_3", "buy_price_3", "buy_orders_3",
            "buy_qty_4", "buy_price_4", "buy_orders_4",
            "buy_qty_5", "buy_price_5", "buy_orders_5",
            "sell_qty_1", "sell_price_1", "sell_orders_1",
            "sell_qty_2", "sell_price_2", "sell_orders_2",
            "sell_qty_3", "sell_price_3", "sell_orders_3",
            "sell_qty_4", "sell_price_4", "sell_orders_4",
            "sell_qty_5", "sell_price_5", "sell_orders_5",
        ]
        
        field_str = ", ".join(all_fields)
        lines.append(f"snapshots[{len(data_list)}]{{{field_str}}}:")
        lines.append("")
        
        for record in data_list:
            values = [self.encoder._serialize_value(record.get(f, "")) for f in all_fields]
            lines.append(",".join(values))
        
        return "\n".join(lines)
    
    # =====================================================================
    # DECODE IMPLEMENTATIONS
    # =====================================================================
    
    def _decode_default(self, vson_str: str, **options) -> Dict:
        """DEFAULT decode"""
        return self.parser.parse(vson_str)
    
    def _decode_incremental(self, vson_str: str, **options) -> Dict:
        """INCREMENTAL decode"""
        return self.parser.parse(vson_str)
    
    def _decode_delta(self, vson_str: str, **options) -> Dict:
        """DELTA decode - reconstruct from deltas"""
        
        lines = vson_str.strip().split('\n')
        base_record = None
        deltas = []
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if line.startswith("base{"):
                base_fields = line.split('{')[1].split('}')[0].split(',')
                base_fields = [f.strip() for f in base_fields]
                i += 1
                if i < len(lines):
                    values = lines[i].split(',')
                    base_record = dict(zip(base_fields, values))
                i += 1
            
            elif line.startswith("deltas["):
                delta_fields = line.split('{')[1].split('}')[0].split(',')
                delta_fields = [f.strip() for f in delta_fields]
                i += 1
                
                while i < len(lines) and lines[i].strip() and not lines[i].strip().startswith('#'):
                    values = lines[i].strip().split(',')
                    delta_rec = dict(zip(delta_fields, values))
                    deltas.append(delta_rec)
                    i += 1
            else:
                i += 1
        
        # Reconstruct
        snapshots = []
        if base_record:
            snapshots.append(base_record)
            
            current = {k: float(v) if v and v.replace('.','').isdigit() else v for k, v in base_record.items()}
            
            for delta in deltas:
                new_rec = current.copy()
                new_rec["timestamp"] = delta.get("timestamp", current.get("timestamp"))
                
                for key, val in delta.items():
                    if key.startswith("delta_"):
                        orig_key = key.replace("delta_", "")
                        if isinstance(current.get(orig_key), (int, float)):
                            new_rec[orig_key] = float(current.get(orig_key, 0)) + float(val or 0)
                
                snapshots.append(new_rec)
                current = new_rec
        
        return {"snapshots": snapshots}
    
    def _decode_with_depth(self, vson_str: str, **options) -> Dict:
        """DEPTH decode"""
        return self.parser.parse(vson_str)
    
    # =====================================================================
    # HELPER METHODS
    # =====================================================================
    
    def _detect_mode(self, vson_str: str) -> str:
        """Auto-detect encoding mode"""
        if "# Mode: delta_b" in vson_str or "base{" in vson_str:
            return "delta_b"
        elif "# Mode: depth_c" in vson_str or "buy_qty_5" in vson_str:
            return "depth_c"
        elif "incremental_mode" in vson_str:
            return "incremental_a"
        else:
            return "default"
    
    def _extract_metadata(self, record: Dict) -> Dict:
        """Extract metadata from record"""
        metadata = {}
        
        meta_keys = ["status", "symbol", "instrument_key", "date", "period"]
        for key in meta_keys:
            if key in record:
                metadata[key] = record[key]
        
        if "timestamp" in record:
            metadata["timestamp"] = record["timestamp"]
        
        return metadata
    
    def _write_file(self, vson_str: str, filepath: Union[str, Path], **options) -> None:
        """Write to file"""
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(vson_str, encoding='utf-8')


# =========================================================================
# MODULE-LEVEL FUNCTIONS
# =========================================================================

_vson_smart = VSONSmart()

def smart_encode(
    data: Union[Dict, List[Dict]],
    filepath: Optional[Union[str, Path]] = None,
    mode: str = "default",
    **options
) -> Union[str, None]:
    """Encode with smart interface"""
    return _vson_smart.smart_encode(data, filepath, mode, **options)

def smart_decode(
    source: Union[str, Path, Dict],
    mode: str = "default",
    **options
) -> Union[Dict, List[Dict]]:
    """Decode with smart interface"""
    return _vson_smart.smart_decode(source, mode, **options)
