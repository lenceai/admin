# GPFS Tools 1.5

## Overview
This directory contains tools for managing and analyzing GPFS (General Parallel File System) layouts, including data consolidation, cleaning, and distribution across storage systems.

## Features
- Load and consolidate data from multiple Excel sheets.
- Filter and clean data based on source systems.
- Calculate total storage and distribute data across Cohesity groups.
- Generate reports and logs for analysis.

## Installation
1. Ensure Python 3.8 or higher is installed.
2. Install dependencies using `pip install pandas` (based on required libraries; add more as needed).

## Usage
1. Navigate to this directory.
2. Run the main script, e.g., `python gpfs_layout.1.5.0.py` (if available).
3. Review output files in the `output/data` and `output/log` directories.

## Dependencies
- pandas for data manipulation.
- Standard libraries: os, datetime, logging.

## Contributing
Feel free to add improvements or report issues in the main project repository.

## License
This project is licensed under the terms specified in the root LICENSE file.

## gpfs_python_mount

### Description
This tool provides functionality for mounting GPFS (General Parallel File System) file systems using Python, allowing for automated setup and management in your environment.

### Usage
1. Ensure GPFS is installed and properly configured on your system.
2. Run the script with appropriate options, e.g., `python gpfs_python_mount.py --mount-point /path/to/mount`.
3. Refer to the script's help for additional parameters: `python gpfs_python_mount.py --help`. 