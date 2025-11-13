# vson/cli/main.py
"""
VSON CLI Main Entry Point

Provides command-line interface for encoding, decoding, validation, and analysis.
"""

import argparse
import sys
from pathlib import Path


def main():
    """Main CLI entry point"""
    
    parser = argparse.ArgumentParser(
        description='VSON - Vesion Snapshot Object Notation',
        epilog='For more info, visit: https://github.com/yourusername/vson-format'
    )
    
    parser.add_argument('--version', action='version', version='VSON 1.0.0')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # =====================================================================
    # ENCODE COMMAND
    # =====================================================================
    encode_parser = subparsers.add_parser('encode', help='Encode JSON to VSON')
    encode_parser.add_argument('input', help='Input JSON file')
    encode_parser.add_argument('-o', '--output', help='Output VSON file')
    encode_parser.add_argument(
        '-m', '--mode',
        choices=['default', 'incremental_a', 'delta_b', 'depth_c'],
        default='default',
        help='Encoding mode'
    )
    encode_parser.add_argument(
        '-c', '--compression',
        choices=['gzip', 'brotli'],
        help='Compression method'
    )
    encode_parser.set_defaults(func=encode_command)
    
    # =====================================================================
    # DECODE COMMAND
    # =====================================================================
    decode_parser = subparsers.add_parser('decode', help='Decode VSON to JSON')
    decode_parser.add_argument('input', help='Input VSON file')
    decode_parser.add_argument('-o', '--output', help='Output JSON file')
    decode_parser.set_defaults(func=decode_command)
    
    # =====================================================================
    # VALIDATE COMMAND
    # =====================================================================
    validate_parser = subparsers.add_parser('validate', help='Validate VSON file')
    validate_parser.add_argument('input', help='VSON file to validate')
    validate_parser.add_argument('-s', '--strict', action='store_true', help='Strict validation')
    validate_parser.set_defaults(func=validate_command)
    
    # =====================================================================
    # STATS COMMAND
    # =====================================================================
    stats_parser = subparsers.add_parser('stats', help='File statistics')
    stats_parser.add_argument('input', help='VSON file')
    stats_parser.add_argument('-c', '--compare', help='Compare with JSON file')
    stats_parser.set_defaults(func=stats_command)
    
    # =====================================================================
    # SCHEMA COMMAND
    # =====================================================================
    schema_parser = subparsers.add_parser('schema', help='Extract/generate schema')
    schema_parser.add_argument('input', help='VSON file')
    schema_parser.add_argument('-o', '--output', help='Output schema file')
    schema_parser.add_argument('-i', '--infer', action='store_true', help='Infer schema')
    schema_parser.set_defaults(func=schema_command)
    
    # =====================================================================
    # MERGE COMMAND
    # =====================================================================
    merge_parser = subparsers.add_parser('merge', help='Merge multiple files')
    merge_parser.add_argument('inputs', nargs='+', help='Input VSON files')
    merge_parser.add_argument('-o', '--output', required=True, help='Output file')
    merge_parser.set_defaults(func=merge_command)
    
    # =====================================================================
    # SPLIT COMMAND
    # =====================================================================
    split_parser = subparsers.add_parser('split', help='Split large file')
    split_parser.add_argument('input', help='Input VSON file')
    split_parser.add_argument('-d', '--dir', default='chunks', help='Output directory')
    split_parser.add_argument('-s', '--size', type=int, default=10000, help='Chunk size')
    split_parser.set_defaults(func=split_command)
    
    # =====================================================================
    # BENCHMARK COMMAND
    # =====================================================================
    bench_parser = subparsers.add_parser('benchmark', help='Performance benchmark')
    bench_parser.add_argument('input', help='Input file')
    bench_parser.add_argument('-i', '--iterations', type=int, default=10, help='Iterations')
    bench_parser.set_defaults(func=benchmark_command)
    
    # =====================================================================
    # DIFF COMMAND
    # =====================================================================
    diff_parser = subparsers.add_parser('diff', help='Compare two files')
    diff_parser.add_argument('file1', help='First file')
    diff_parser.add_argument('file2', help='Second file')
    diff_parser.set_defaults(func=diff_command)
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        args.func(args)
    except Exception as e:
        print(f"âŒ Error: {e}", file=sys.stderr)
        sys.exit(1)


# =========================================================================
# COMMAND IMPLEMENTATIONS
# =========================================================================

def encode_command(args):
    """Encode JSON to VSON"""
    import json
    import vson
    
    input_path = Path(args.input)
    output_path = Path(args.output or input_path.stem + '.vson')
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Read JSON
    with open(input_path) as f:
        data = json.load(f)
    
    # Encode
    vson.smart_encode(
        data,
        output_path,
        mode=args.mode,
        compression=args.compression
    )
    
    # Show stats
    input_size = input_path.stat().st_size
    output_size = output_path.stat().st_size
    ratio = input_size / output_size if output_size > 0 else 0
    
    print(f"âœ… Encoded successfully")
    print(f"   Input:  {vson.utils.format_size(input_size)}")
    print(f"   Output: {vson.utils.format_size(output_size)}")
    print(f"   Ratio:  {ratio:.2f}x smaller")
    print(f"   Mode:   {args.mode}")


def decode_command(args):
    """Decode VSON to JSON"""
    import json
    import vson
    
    input_path = Path(args.input)
    output_path = Path(args.output or input_path.stem + '.json')
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Decode
    data = vson.smart_decode(input_path)
    
    # Write JSON
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"âœ… Decoded successfully")
    print(f"   Records: {len(data.get('snapshots', []))}")
    print(f"   Output:  {output_path}")


def validate_command(args):
    """Validate VSON file"""
    import vson
    
    input_path = Path(args.input)
    
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")
    
    try:
        data = vson.smart_decode(input_path)
        records = data.get('snapshots', [])
        
        print(f"âœ… Valid VSON file")
        print(f"   Records: {len(records)}")
        print(f"   Fields:  {list(records[0].keys()) if records else []}")
        
    except Exception as e:
        print(f"âŒ Invalid VSON file: {e}")
        sys.exit(1)


def stats_command(args):
    """Show file statistics"""
    import vson
    import json
    
    input_path = Path(args.input)
    
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")
    
    stats = vson.utils.get_file_stats(input_path)
    
    print(f"ðŸ“Š VSON File Statistics")
    print(f"   File:    {stats['file_path']}")
    print(f"   Size:    {stats['file_size_formatted']}")
    print(f"   Records: {stats['num_records']}")
    print(f"   Fields:  {stats['num_fields']}")
    
    # Compare with JSON if requested
    if args.compare:
        json_path = Path(args.compare)
        if json_path.exists():
            json_size = json_path.stat().st_size
            ratio = json_size / stats['file_size']
            savings = ((json_size - stats['file_size']) / json_size * 100)
            
            print(f"\n   ðŸ“ˆ vs JSON ({vson.utils.format_size(json_size)})")
            print(f"      Ratio:   {ratio:.2f}x smaller")
            print(f"      Savings: {savings:.1f}%")


def schema_command(args):
    """Extract/generate schema"""
    import vson
    import json
    
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else None
    
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")
    
    # Decode file
    data = vson.smart_decode(input_path)
    records = data.get('snapshots', [])
    
    if not records:
        print("âŒ No records found")
        return
    
    # Infer schema
    if args.infer:
        schema = vson.schema.VSONSchema()
        schema.infer_schema(records, auto_required=True)
        schema_dict = schema.export_schema()
    else:
        schema_dict = {
            'fields': {
                field: {'type': type(val).__name__}
                for field, val in records[0].items()
            }
        }
    
    # Output
    schema_json = json.dumps(schema_dict, indent=2)
    
    if output_path:
        output_path.write_text(schema_json)
        print(f"âœ… Schema exported to {output_path}")
    else:
        print(schema_json)


def merge_command(args):
    """Merge multiple VSON files"""
    import vson
    
    input_paths = [Path(f) for f in args.inputs]
    output_path = Path(args.output)
    
    # Check all files exist
    for p in input_paths:
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
    
    # Merge
    vson.utils.merge_files(input_paths, output_path, verbose=True)


def split_command(args):
    """Split large VSON file"""
    import vson
    
    input_path = Path(args.input)
    output_dir = Path(args.dir)
    
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")
    
    # Split
    files = vson.utils.split_file(
        input_path,
        output_dir,
        chunk_size=args.size,
        verbose=True
    )
    
    print(f"âœ… Split into {len(files)} chunks")


def benchmark_command(args):
    """Run performance benchmarks"""
    import vson
    import json
    
    input_path = Path(args.input)
    
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")
    
    # Read data
    with open(input_path) as f:
        if input_path.suffix == '.json':
            data = json.load(f)
        else:
            data = vson.smart_decode(input_path)
    
    # Benchmark encode
    encode_stats = vson.utils.profile_encode(data, args.iterations)
    
    # Benchmark decode
    vson_str = vson.smart_encode(data)
    decode_stats = vson.utils.profile_decode(vson_str, args.iterations)
    
    # Display
    print(f"âš¡ Performance Benchmarks ({args.iterations} iterations)")
    print(f"\n   Encoding:")
    print(f"      Min:  {encode_stats['min_ms']:.2f}ms")
    print(f"      Max:  {encode_stats['max_ms']:.2f}ms")
    print(f"      Avg:  {encode_stats['avg_ms']:.2f}ms")
    print(f"\n   Decoding:")
    print(f"      Min:  {decode_stats['min_ms']:.2f}ms")
    print(f"      Max:  {decode_stats['max_ms']:.2f}ms")
    print(f"      Avg:  {decode_stats['avg_ms']:.2f}ms")


def diff_command(args):
    """Compare two files"""
    import vson
    
    file1 = Path(args.file1)
    file2 = Path(args.file2)
    
    if not file1.exists():
        raise FileNotFoundError(f"File not found: {file1}")
    if not file2.exists():
        raise FileNotFoundError(f"File not found: {file2}")
    
    # Diff
    diff = vson.utils.diff_files(file1, file2, verbose=True)
    
    print(f"\nðŸ“Š Comparison Results")
    print(f"   File 1: {diff['records_file1']} records")
    print(f"   File 2: {diff['records_file2']} records")
    print(f"   Change: {diff['records_added'] - diff['records_removed']:+d} records")


if __name__ == '__main__':
    main()
