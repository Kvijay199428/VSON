#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
VSON Package Setup

Installation configuration for VSON library.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_path = Path(__file__).parent / "README.md"
long_description = ""
if readme_path.exists():
    long_description = readme_path.read_text(encoding="utf-8")

setup(
    name="vson",
    version="1.0.0",
    description="VSON - High-performance serialization format for market data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    
    author="HYDRA Labs",
    author_email="hydralabs@gmail.com",
    url="https://github.com/kvijay199428/vson",
    
    license="MIT",
    
    # Automatically find all packages (includes vson, vson.cli, vson.validators, etc.)
    packages=find_packages(exclude=["tests", "tests.*", "docs", "examples"]),
    
    python_requires=">=3.7",
    
    install_requires=[
        # No external dependencies required for core functionality
    ],
    
    extras_require={
        'dev': [
            'pytest>=6.0',
            'pytest-cov>=2.12.0',
            'black>=21.0',
            'flake8>=3.9',
            'mypy>=0.910',
            'isort>=5.9',
        ],
        'docs': [
            'sphinx>=4.0',
            'sphinx-rtd-theme>=1.0',
        ],
        'compression': [
            'brotli>=1.0',
        ],
    },
    
    entry_points={
        'console_scripts': [
            'vson=vson.cli.main:main',
        ],
    },
    
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Office/Business :: Financial :: Investment",
    ],
    
    keywords="vson serialization market-data trading ohlc compression json performance",
    
    project_urls={
        "Documentation": "https://github.com/kvijay199428/vson/wiki",
        "Source": "https://github.com/kvijay199428/vson",
        "Bug Tracker": "https://github.com/kvijay199428/vson/issues",
    },
    
    # Include package data
    include_package_data=True,
    zip_safe=False,
)
