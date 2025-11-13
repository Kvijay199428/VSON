# vson/cli/commands.py
"""
CLI Command Utilities

Helper functions for CLI commands.
"""

import sys
from pathlib import Path
from typing import Any, Dict, Optional


class CLIFormatter:
    """Format output for CLI"""
    
    @staticmethod
    def success(message: str) -> None:
        """Print success message"""
        print(f"âœ… {message}")
    
    @staticmethod
    def error(message: str) -> None:
        """Print error message"""
        print(f"âŒ {message}", file=sys.stderr)
    
    @staticmethod
    def warning(message: str) -> None:
        """Print warning message"""
        print(f"âš ï¸  {message}")
    
    @staticmethod
    def info(message: str) -> None:
        """Print info message"""
        print(f"â„¹ï¸  {message}")
    
    @staticmethod
    def section(title: str) -> None:
        """Print section header"""
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}\n")
    
    @staticmethod
    def table(rows: list, headers: list = None) -> None:
        """Print formatted table"""
        if headers:
            print("  ".join(f"{h:<15}" for h in headers))
            print("  " + "-" * 50)
        
        for row in rows:
            print("  ".join(f"{str(v):<15}" for v in row))


class ProgressBar:
    """Simple progress bar"""
    
    def __init__(self, total: int, prefix: str = ""):
        self.total = total
        self.current = 0
        self.prefix = prefix
    
    def update(self, amount: int = 1) -> None:
        """Update progress"""
        self.current += amount
        percentage = (self.current / self.total) * 100
        bar = "â–ˆ" * int(percentage / 5) + "â–‘" * (20 - int(percentage / 5))
        print(f"{self.prefix} |{bar}| {percentage:.1f}%", end='\r')
        
        if self.current >= self.total:
            print()
