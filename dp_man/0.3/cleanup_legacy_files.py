#!/usr/bin/env python3
"""
Cleanup script to organize legacy output files into the output directory.
This script moves any JSON reports and log files from the main directory to the output folder.
"""

import os
import shutil
import glob
from datetime import datetime

def cleanup_legacy_files():
    """Move legacy output files to the output directory."""
    
    print("🧹 Cleaning up legacy output files...")
    print("=" * 50)
    
    # Create output directory if it doesn't exist
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    moved_files = []
    
    # Find and move JSON report files
    json_files = glob.glob("cohesity_*.json")
    for json_file in json_files:
        try:
            dest_path = os.path.join(output_dir, json_file)
            if not os.path.exists(dest_path):
                shutil.move(json_file, dest_path)
                moved_files.append((json_file, dest_path))
                print(f"📄 Moved: {json_file} → {dest_path}")
            else:
                print(f"⚠️  Skipped: {json_file} (already exists in output)")
        except Exception as e:
            print(f"❌ Error moving {json_file}: {e}")
    
    # Find and move log files (but not the current one)
    log_files = glob.glob("*.log")
    for log_file in log_files:
        if log_file != "dp_man_v3.log":  # Skip if it's the current log
            try:
                dest_path = os.path.join(output_dir, log_file)
                if not os.path.exists(dest_path):
                    shutil.move(log_file, dest_path)
                    moved_files.append((log_file, dest_path))
                    print(f"📝 Moved: {log_file} → {dest_path}")
                else:
                    print(f"⚠️  Skipped: {log_file} (already exists in output)")
            except Exception as e:
                print(f"❌ Error moving {log_file}: {e}")
    
    # Find and move other common output files
    other_patterns = ["*_report_*.json", "*_inventory_*.json", "example_*.json"]
    for pattern in other_patterns:
        files = glob.glob(pattern)
        for file in files:
            try:
                dest_path = os.path.join(output_dir, file)
                if not os.path.exists(dest_path):
                    shutil.move(file, dest_path)
                    moved_files.append((file, dest_path))
                    print(f"📦 Moved: {file} → {dest_path}")
                else:
                    print(f"⚠️  Skipped: {file} (already exists in output)")
            except Exception as e:
                print(f"❌ Error moving {file}: {e}")
    
    # Summary
    print(f"\n📊 CLEANUP SUMMARY")
    print("-" * 30)
    print(f"Files moved: {len(moved_files)}")
    
    if moved_files:
        print(f"\n📁 All files are now organized in the output/ directory:")
        for old_path, new_path in moved_files:
            print(f"  ✅ {new_path}")
    else:
        print(f"  ✅ No files needed to be moved")
    
    print(f"\n🎯 Organization complete!")

if __name__ == "__main__":
    cleanup_legacy_files() 