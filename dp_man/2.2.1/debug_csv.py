#!/usr/bin/env python
"""
Debug utility for CSV credential files 
"""

import sys
import csv
import os

def debug_csv(filename):
    """Read and debug a CSV file"""
    print(f"Debugging CSV file: {filename}")
    if not os.path.exists(filename):
        print(f"Error: File {filename} does not exist")
        return
        
    try:
        with open(filename, 'r') as csvfile:
            # First read as plain text to see raw content
            raw_content = csvfile.read(1000)  # Read first 1000 chars 
            print("\nRaw content preview:")
            print("-" * 80)
            print(raw_content)
            print("-" * 80)
            
            # Reset and read as CSV
            csvfile.seek(0)
            reader = csv.reader(csvfile)
            
            # Get header row
            try:
                header = next(reader)
                print("\nHeader row:")
                print(header)
            except StopIteration:
                print("CSV file is empty or has no header row")
                return
                
            # Read data rows
            print("\nData rows:")
            row_count = 0
            for row in reader:
                row_count += 1
                # Mask password if present
                display_row = row.copy()
                password_idx = header.index('password') if 'password' in header else -1
                if password_idx >= 0 and password_idx < len(display_row):
                    pwd = display_row[password_idx]
                    if pwd and len(pwd) > 2:
                        display_row[password_idx] = pwd[:1] + '*' * (len(pwd) - 2) + pwd[-1:]
                    else:
                        display_row[password_idx] = '***'
                        
                print(f"Row {row_count}: {display_row}")
                
                # Show actual column values with column names
                if len(row) >= len(header):
                    print("  Column details:")
                    for i, col_name in enumerate(header):
                        value = row[i]
                        if col_name == 'password' and value:
                            if len(value) > 2:
                                value = value[:1] + '*' * (len(value) - 2) + value[-1:]
                            else:
                                value = '***'
                        print(f"    {col_name}: {value}")
                else:
                    print(f"  Warning: Row has fewer columns ({len(row)}) than header ({len(header)})")
            
            print(f"\nTotal: {row_count} data rows")
            
    except UnicodeDecodeError as e:
        print(f"Error decoding file: {str(e)}")
        print("This might indicate the file is not a text file or uses a different encoding")
    except Exception as e:
        print(f"Error reading CSV: {str(e)}")
        
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python debug_csv.py <csv_file>")
        sys.exit(1)
        
    debug_csv(sys.argv[1]) 