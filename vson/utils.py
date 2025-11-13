# vson/utils.py
"""
VSON Utility Functions

Helper functions for file operations, formatting, validation, and analysis.
"""

from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import os


# =========================================================================
# FILE SIZE UTILITIES
# =========================================================================

def format_size(bytes_size: int) -> str:
    """
    Format bytes to human-readable size
    
    Args:
        bytes_size: Size in bytes
    
    Returns:
        Formatted size string (e.g., "2.5 MB")
    
    Example:
        >>> format_size(2621440)
        '2.50 MB'
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.2f} PB"


def estimate_compression(json_size: int) -> int:
    """
    Estimate VSON size from JSON size
    
    Args:
        json_size: Original JSON size in bytes
    
    Returns:
        Estimated VSON size in bytes
    """
    return int(json_size * 0.25)  # ~75% compression ratio


def get_compression_ratio(vson_size: int, json_size: int) -> float:
    """
    Calculate compression ratio
    
    Args:
        vson_size: VSON file size
        json_size: JSON file size
    
    Returns:
        Compression ratio (e.g., 4.0 means 4x smaller)
    """
    if json_size == 0:
        return 0
    return json_size / vson_size


# =========================================================================
# FILE OPERATIONS
# =========================================================================

def merge_files(
    file_paths: List[Path],
    output_path: Path,
    verbose: bool = False
) -> None:
    """
    Merge multiple VSON files
    
    Args:
        file_paths: List of input VSON file paths
        output_path: Output file path
        verbose: Print progress
    
    Raises:
        FileNotFoundError: If input files don't exist
        IOError: If write fails
    
    Example:
        merge_files([Path("file1.vson"), Path("file2.vson")], Path("merged.vson"))
    """
    if not file_paths:
        raise ValueError("No files to merge")
    
    import json as _json
    
    merged_data = {"snapshots": []}
    
    for i, file_path in enumerate(file_paths):
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Import here to avoid circular dependency
        from .core import smart_decode
        
        if verbose:
            print(f"Reading {i + 1}/{len(file_paths)}: {file_path}")
        
        data = smart_decode(file_path)
        
        if isinstance(data, dict):
            if "snapshots" in data:
                merged_data["snapshots"].extend(data["snapshots"])
    
    # Import here to avoid circular dependency
    from .core import smart_encode
    
    smart_encode(merged_data, output_path)
    
    if verbose:
        print(f"âœ… Merged {len(file_paths)} files â†’ {output_path}")


def split_file(
    input_path: Path,
    output_dir: Path,
    chunk_size: int = 10000,
    verbose: bool = False
) -> List[Path]:
    """
    Split large VSON file into chunks
    
    Args:
        input_path: Input file path
        output_dir: Output directory for chunks
        chunk_size: Records per chunk
        verbose: Print progress
    
    Returns:
        List of created file paths
    
    Example:
        files = split_file(Path("large.vson"), Path("chunks/"), chunk_size=5000)
    """
    from .core import smart_decode, smart_encode
    
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if verbose:
        print(f"Reading {input_path}...")
    
    data = smart_decode(input_path)
    snapshots = data.get("snapshots", [])
    
    created_files = []
    num_chunks = (len(snapshots) + chunk_size - 1) // chunk_size
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        chunk_snapshots = snapshots[start_idx:end_idx]
        
        # Create chunk file
        chunk_file = output_dir / f"chunk_{i:04d}.vson"
        smart_encode({"snapshots": chunk_snapshots}, chunk_file)
        created_files.append(chunk_file)
        
        if verbose:
            print(f"  Chunk {i + 1}/{num_chunks}: {len(chunk_snapshots)} records â†’ {chunk_file}")
    
    return created_files


def validate_file(filepath: Path) -> Tuple[bool, List[str]]:
    """
    Validate VSON file integrity
    
    Args:
        filepath: Path to VSON file
    
    Returns:
        Tuple of (is_valid, errors)
    
    Example:
        is_valid, errors = validate_file(Path("data.vson"))
        if not is_valid:
            print(f"Errors: {errors}")
    """
    errors = []
    
    if not filepath.exists():
        return False, [f"File not found: {filepath}"]
    
    try:
        # Try to parse file
        from .core import smart_decode
        data = smart_decode(filepath)
        
        if not isinstance(data, dict):
            errors.append("Root must be dictionary")
        
        return len(errors) == 0, errors
    
    except Exception as e:
        return False, [str(e)]


def diff_files(
    file1: Path,
    file2: Path,
    verbose: bool = False
) -> Dict[str, Any]:
    """
    Compare two VSON files
    
    Args:
        file1: First file path
        file2: Second file path
        verbose: Print details
    
    Returns:
        Difference report
    
    Example:
        diff = diff_files(Path("old.vson"), Path("new.vson"))
        print(f"Records added: {diff['added']}")
    """
    from .core import smart_decode
    
    if not file1.exists():
        raise FileNotFoundError(f"File not found: {file1}")
    if not file2.exists():
        raise FileNotFoundError(f"File not found: {file2}")
    
    data1 = smart_decode(file1)
    data2 = smart_decode(file2)
    
    snapshots1 = data1.get("snapshots", [])
    snapshots2 = data2.get("snapshots", [])
    
    added = len(snapshots2) - len(snapshots1)
    
    diff_report = {
        "file1": str(file1),
        "file2": str(file2),
        "records_file1": len(snapshots1),
        "records_file2": len(snapshots2),
        "records_added": max(added, 0),
        "records_removed": max(-added, 0),
    }
    
    if verbose:
        print(f"File 1: {len(snapshots1)} records")
        print(f"File 2: {len(snapshots2)} records")
        print(f"Difference: {added:+d} records")
    
    return diff_report


# =========================================================================
# FILE STATISTICS
# =========================================================================

def get_file_stats(filepath: Path) -> Dict[str, Any]:
    """
    Get VSON file statistics
    
    Args:
        filepath: Path to VSON file
    
    Returns:
        Dictionary with statistics
    
    Example:
        stats = get_file_stats(Path("data.vson"))
        print(f"Records: {stats['num_records']}")
        print(f"Size: {stats['file_size_formatted']}")
    """
    from .core import smart_decode
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    file_size = filepath.stat().st_size
    data = smart_decode(filepath)
    snapshots = data.get("snapshots", [])
    
    # Calculate field stats
    fields = {}
    if snapshots:
        first_record = snapshots[0]
        fields = {k: type(v).__name__ for k, v in first_record.items()}
    
    stats = {
        "file_path": str(filepath),
        "file_size": file_size,
        "file_size_formatted": format_size(file_size),
        "num_records": len(snapshots),
        "num_fields": len(fields),
        "fields": fields,
    }
    
    return stats


def compare_formats(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare VSON vs JSON for given data
    
    Args:
        data: Data dictionary
    
    Returns:
        Comparison statistics
    
    Example:
        comparison = compare_formats(market_data)
        print(f"VSON: {comparison['vson_size']}")
        print(f"JSON: {comparison['json_size']}")
        print(f"Compression: {comparison['compression_ratio']:.1f}x")
    """
    import json as _json
    from .core import smart_encode
    
    # VSON encoding
    vson_str = smart_encode(data)
    vson_size = len(vson_str.encode('utf-8'))
    
    # JSON encoding
    json_str = _json.dumps(data)
    json_size = len(json_str.encode('utf-8'))
    
    # Calculate ratio
    ratio = json_size / vson_size if vson_size > 0 else 0
    savings = ((json_size - vson_size) / json_size * 100) if json_size > 0 else 0
    
    return {
        "vson_size": vson_size,
        "json_size": json_size,
        "vson_size_formatted": format_size(vson_size),
        "json_size_formatted": format_size(json_size),
        "compression_ratio": ratio,
        "space_savings_percent": savings,
        "vson_vs_json": f"{ratio:.2f}x smaller",
    }


# =========================================================================
# VALIDATION AND CHECKING
# =========================================================================

def check_dependencies() -> Dict[str, bool]:
    """
    Check availability of optional dependencies
    
    Returns:
        Dictionary with dependency status
    
    Example:
        deps = check_dependencies()
        if deps['brotli']:
            print("brotli compression available")
    """
    dependencies = {}
    
    # Check gzip (builtin)
    try:
        import gzip
        dependencies['gzip'] = True
    except ImportError:
        dependencies['gzip'] = False
    
    # Check brotli (optional)
    try:
        import brotli
        dependencies['brotli'] = True
    except ImportError:
        dependencies['brotli'] = False
    
    # Check pandas (optional)
    try:
        import pandas
        dependencies['pandas'] = True
    except ImportError:
        dependencies['pandas'] = False
    
    # Check numpy (optional)
    try:
        import numpy
        dependencies['numpy'] = True
    except ImportError:
        dependencies['numpy'] = False
    
    return dependencies


def get_library_info() -> Dict[str, str]:
    """
    Get VSON library information
    
    Returns:
        Dictionary with library info
    """
    from . import __version__
    
    return {
        "library": "VSON",
        "version": __version__,
        "description": "Vesion Snapshot Object Notation",
        "format": "Text-based, CSV-like structure",
        "compression": "75-80% vs JSON",
        "performance": "30-40% faster parsing than JSON",
    }


# =========================================================================
# PROFILING AND BENCHMARKING
# =========================================================================

def profile_encode(data: Dict[str, Any], iterations: int = 10) -> Dict[str, float]:
    """
    Profile encoding performance
    
    Args:
        data: Data to encode
        iterations: Number of iterations
    
    Returns:
        Performance statistics
    """
    import time
    from .core import smart_encode
    
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        smart_encode(data)
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    return {
        "min_ms": min(times),
        "max_ms": max(times),
        "avg_ms": sum(times) / len(times),
        "total_ms": sum(times),
        "iterations": iterations,
    }


def profile_decode(vson_str: str, iterations: int = 10) -> Dict[str, float]:
    """
    Profile decoding performance
    
    Args:
        vson_str: VSON string to decode
        iterations: Number of iterations
    
    Returns:
        Performance statistics
    """
    import time
    from .core import smart_decode
    
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        smart_decode(vson_str)
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    return {
        "min_ms": min(times),
        "max_ms": max(times),
        "avg_ms": sum(times) / len(times),
        "total_ms": sum(times),
        "iterations": iterations,
    }
