#!/usr/bin/env python3
"""
GPFS Snapshot Mount Scripts - Python Version
Cohesity backup pre/post scripts for mounting GPFS snapshots without mmlssnapshot dependency

Warning: This code is provided on a best effort basis and is not officially supported.
"""

import os
import sys
import subprocess
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional


class GPFSSnapshotManager:
    """
    Manages GPFS snapshot discovery and mounting operations without requiring 
    GPFS management tools like mmlssnapshot.
    """
    
    def __init__(self, log_file: str = "/tmp/cohesity-python-script.log"):
        """Initialize the snapshot manager with logging configuration."""
        self.log_file = log_file
        self.setup_logging()
        
        # Get the backup entity from environment variable (set by Cohesity)
        self.backup_entity = os.environ.get('COHESITY_BACKUP_ENTITY', 'unknown')
        self.mount_prefix = f"Cohesity-{self.backup_entity}"
        
    def setup_logging(self):
        """Configure logging to write to both file and console."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',
            handlers=[
                logging.FileHandler(self.log_file, mode='a'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def find_fileset_path(self, filesystem: str, fileset: str) -> Optional[str]:
        """
        Find the actual mount path for a GPFS fileset.
        This replaces the mmlsfileset command by checking common GPFS mount patterns.
        
        Args:
            filesystem: Name of the GPFS filesystem
            fileset: Name of the fileset within the filesystem
            
        Returns:
            The full path to the fileset, or None if not found
        """
        # Common GPFS mount patterns to check
        possible_paths = [
            f"/gpfs/{filesystem}/{fileset}",
            f"/gpfs/{filesystem}",  # fileset might be at filesystem root
            f"/{filesystem}/{fileset}",
            f"/{filesystem}",
            f"/mnt/gpfs/{filesystem}/{fileset}",
            f"/mnt/{filesystem}/{fileset}"
        ]
        
        for path in possible_paths:
            if os.path.exists(path) and os.path.isdir(path):
                # Verify this looks like a GPFS fileset by checking for .snapshots
                snapshots_dir = os.path.join(path, '.snapshots')
                if os.path.exists(snapshots_dir):
                    self.logger.info(f"Found fileset path: {path}")
                    return path
                    
        self.logger.error(f"Could not locate fileset {filesystem}/{fileset}")
        return None
    
    def discover_snapshots(self, fileset_path: str) -> List[Tuple[str, datetime]]:
        """
        Discover available snapshots by examining the .snapshots directory.
        This replaces the mmlssnapshot command functionality.
        
        Args:
            fileset_path: Full path to the GPFS fileset
            
        Returns:
            List of tuples containing (snapshot_name, creation_time)
            sorted by creation time (newest first)
        """
        snapshots_dir = os.path.join(fileset_path, '.snapshots')
        
        if not os.path.exists(snapshots_dir):
            self.logger.error(f"No .snapshots directory found in {fileset_path}")
            return []
        
        snapshots = []
        
        try:
            # List all directories in .snapshots
            for item in os.listdir(snapshots_dir):
                item_path = os.path.join(snapshots_dir, item)
                
                # Only consider directories (actual snapshots)
                if not os.path.isdir(item_path):
                    continue
                    
                # Try to extract timestamp from snapshot name
                # Common patterns: snap_YYYYMMDD_HHMM, snap_YYYY-MM-DD_HH-MM-SS, etc.
                timestamp = self.parse_snapshot_timestamp(item)
                
                if timestamp:
                    # Verify snapshot is accessible (equivalent to "Valid" status)
                    if self.is_snapshot_valid(item_path):
                        snapshots.append((item, timestamp))
                        self.logger.info(f"Found valid snapshot: {item} ({timestamp})")
                    else:
                        self.logger.warning(f"Found invalid/corrupted snapshot: {item}")
                else:
                    self.logger.warning(f"Could not parse timestamp from snapshot name: {item}")
                    
        except PermissionError:
            self.logger.error(f"Permission denied accessing {snapshots_dir}")
            return []
        except Exception as e:
            self.logger.error(f"Error discovering snapshots: {e}")
            return []
        
        # Sort by timestamp, newest first
        snapshots.sort(key=lambda x: x[1], reverse=True)
        return snapshots
    
    def parse_snapshot_timestamp(self, snapshot_name: str) -> Optional[datetime]:
        """
        Parse timestamp from snapshot name using common GPFS snapshot naming patterns.
        
        Args:
            snapshot_name: Name of the snapshot directory
            
        Returns:
            Parsed datetime object, or None if parsing fails
        """
        # Common timestamp patterns in GPFS snapshots
        patterns = [
            r'snap_(\d{8})_(\d{4})',           # snap_20241128_0600
            r'snap_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})',  # snap_20241128_0600 (alternative)
            r'snap_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})',  # snap_2024-11-28_06-00-00
            r'(\d{8})_(\d{4})',                # 20241128_0600
            r'(\d{4})-(\d{2})-(\d{2})_(\d{2}):(\d{2}):(\d{2})',  # 2024-11-28_06:00:00
        ]
        
        for pattern in patterns:
            match = re.search(pattern, snapshot_name)
            if match:
                try:
                    groups = match.groups()
                    
                    if len(groups) == 2:  # YYYYMMDD_HHMM format
                        date_str, time_str = groups
                        if len(date_str) == 8 and len(time_str) == 4:
                            year = int(date_str[:4])
                            month = int(date_str[4:6])
                            day = int(date_str[6:8])
                            hour = int(time_str[:2])
                            minute = int(time_str[2:4])
                            return datetime(year, month, day, hour, minute)
                    
                    elif len(groups) == 5:  # YYYYMMDD_HHMM (split groups)
                        year, month, day, hour, minute = map(int, groups)
                        return datetime(year, month, day, hour, minute)
                    
                    elif len(groups) == 6:  # YYYY-MM-DD_HH-MM-SS format
                        year, month, day, hour, minute, second = map(int, groups)
                        return datetime(year, month, day, hour, minute, second)
                        
                except ValueError:
                    continue  # Try next pattern
        
        return None
    
    def is_snapshot_valid(self, snapshot_path: str) -> bool:
        """
        Check if a snapshot is valid and accessible.
        This replaces checking the "Valid" status from mmlssnapshot.
        
        Args:
            snapshot_path: Full path to the snapshot directory
            
        Returns:
            True if snapshot is accessible and appears valid
        """
        try:
            # Basic accessibility check
            if not os.path.exists(snapshot_path):
                return False
                
            # Try to list contents (this will fail for corrupted snapshots)
            os.listdir(snapshot_path)
            
            # Optional: Check if snapshot is not empty
            # Some organizations consider empty snapshots invalid
            return True
            
        except (PermissionError, OSError):
            return False
    
    def get_latest_snapshot(self, filesystem: str, fileset: str) -> Optional[str]:
        """
        Get the latest valid snapshot for a filesystem/fileset combination.
        This is the main replacement for the mmlssnapshot logic.
        
        Args:
            filesystem: GPFS filesystem name
            fileset: GPFS fileset name
            
        Returns:
            Name of the latest snapshot, or None if no valid snapshots found
        """
        self.logger.info(f"Finding latest snapshot for {filesystem}/{fileset}")
        
        # First, locate the fileset
        fileset_path = self.find_fileset_path(filesystem, fileset)
        if not fileset_path:
            return None
        
        # Discover all snapshots
        snapshots = self.discover_snapshots(fileset_path)
        if not snapshots:
            self.logger.error(f"No valid snapshots found for {filesystem}/{fileset}")
            return None
        
        # Return the latest (first in sorted list)
        latest_snapshot = snapshots[0][0]
        self.logger.info(f"Latest snapshot for {filesystem}/{fileset}: {latest_snapshot}")
        return latest_snapshot
    
    def create_mount_point(self, filesystem: str, fileset: str) -> str:
        """Create and return the standardized mount point path."""
        mount_path = f"/mnt/{self.mount_prefix}-{filesystem}-{fileset}"
        os.makedirs(mount_path, exist_ok=True)
        return mount_path
    
    def mount_snapshot(self, filesystem: str, fileset: str) -> bool:
        """
        Mount the latest snapshot for a filesystem/fileset to a consistent path.
        
        Args:
            filesystem: GPFS filesystem name
            fileset: GPFS fileset name
            
        Returns:
            True if mount successful, False otherwise
        """
        # Get the latest snapshot
        latest_snapshot = self.get_latest_snapshot(filesystem, fileset)
        if not latest_snapshot:
            return False
        
        # Find fileset path
        fileset_path = self.find_fileset_path(filesystem, fileset)
        if not fileset_path:
            return False
        
        # Create mount point
        mount_point = self.create_mount_point(filesystem, fileset)
        
        # Unmount any existing mount at this location
        self.logger.info(f"Unmounting any existing mount at {mount_point}")
        subprocess.run(['umount', mount_point], capture_output=True)
        
        # Build snapshot path
        snapshot_path = os.path.join(fileset_path, '.snapshots', latest_snapshot)
        
        # Perform bind mount
        self.logger.info(f"Mounting {snapshot_path} to {mount_point}")
        result = subprocess.run(
            ['mount', '--bind', snapshot_path, mount_point],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            self.logger.info(f"Successfully mounted {filesystem}/{fileset}")
            return True
        else:
            self.logger.error(f"Mount failed for {filesystem}/{fileset}: {result.stderr}")
            return False
    
    def unmount_snapshot(self, filesystem: str, fileset: str) -> bool:
        """
        Unmount a previously mounted snapshot.
        
        Args:
            filesystem: GPFS filesystem name
            fileset: GPFS fileset name
            
        Returns:
            True if unmount successful, False otherwise
        """
        mount_point = f"/mnt/{self.mount_prefix}-{filesystem}-{fileset}"
        
        self.logger.info(f"Unmounting {mount_point}")
        result = subprocess.run(
            ['umount', mount_point],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            self.logger.info(f"Successfully unmounted {mount_point}")
            return True
        else:
            self.logger.warning(f"Unmount failed for {mount_point}: {result.stderr}")
            # Don't treat unmount failures as critical errors
            return True


def run_prescript(filesets_param: str) -> int:
    """
    Pre-script logic: mount latest snapshots for specified filesets.
    
    Args:
        filesets_param: Comma-separated list of filesystem/fileset pairs
        
    Returns:
        0 for success, 1 for failure
    """
    manager = GPFSSnapshotManager("/tmp/cohesity-prescript.log")
    
    manager.logger.info("")
    manager.logger.info("=" * 63)
    manager.logger.info(f"***** {datetime.now()} : Pre-Script Started")
    manager.logger.info("=" * 63)
    manager.logger.info("")
    
    if not filesets_param:
        manager.logger.error("No script parameters provided")
        manager.logger.error("Script exiting in failure")
        return 1
    
    # Parse filesets parameter
    filesets = [fs.strip() for fs in filesets_param.split(',') if fs.strip()]
    mounted_filesets = []
    
    # Attempt to mount each fileset
    for fileset_spec in filesets:
        if '/' not in fileset_spec:
            manager.logger.error(f"Invalid fileset specification: {fileset_spec}")
            manager.logger.error("Expected format: filesystem/fileset")
            continue
            
        filesystem, fileset = fileset_spec.split('/', 1)
        
        if manager.mount_snapshot(filesystem, fileset):
            mounted_filesets.append((filesystem, fileset))
        else:
            # Mount failed - clean up any successful mounts
            manager.logger.error(f"Mount failed for {filesystem}/{fileset}")
            for fs, fset in mounted_filesets:
                manager.unmount_snapshot(fs, fset)
            
            manager.logger.error(f"{datetime.now()} : Pre-Script exiting in failure")
            return 1
    
    manager.logger.info(f"***** {datetime.now()} : Pre-Script completed successfully")
    manager.logger.info("")
    return 0


def run_postscript(filesets_param: str) -> int:
    """
    Post-script logic: unmount all mounted snapshots.
    
    Args:
        filesets_param: Comma-separated list of filesystem/fileset pairs
        
    Returns:
        0 for success, 1 for failure
    """
    manager = GPFSSnapshotManager("/tmp/cohesity-postscript.log")
    
    manager.logger.info("")
    manager.logger.info("=" * 63)
    manager.logger.info(f"***** {datetime.now()} : Post-Script Started")
    manager.logger.info("=" * 63)
    manager.logger.info("")
    
    if not filesets_param:
        manager.logger.error("No script parameters provided")
        manager.logger.error("Script exiting in failure")
        return 1
    
    # Parse filesets parameter
    filesets = [fs.strip() for fs in filesets_param.split(',') if fs.strip()]
    
    # Unmount each fileset
    for fileset_spec in filesets:
        if '/' not in fileset_spec:
            manager.logger.error(f"Invalid fileset specification: {fileset_spec}")
            continue
            
        filesystem, fileset = fileset_spec.split('/', 1)
        manager.unmount_snapshot(filesystem, fileset)
    
    manager.logger.info(f"***** {datetime.now()} : Post-Script completed successfully")
    manager.logger.info("")
    return 0


if __name__ == "__main__":
    # Determine script mode based on script name or command line argument
    script_name = os.path.basename(sys.argv[0])
    
    if len(sys.argv) < 2:
        print("Usage: python script.py <filesets_param>")
        print("Example: python script.py 'fs1/fileset1,fs2/fileset2'")
        sys.exit(1)
    
    filesets_param = sys.argv[1]
    
    # Run appropriate script based on name
    if 'pre' in script_name.lower():
        exit_code = run_prescript(filesets_param)
    elif 'post' in script_name.lower():
        exit_code = run_postscript(filesets_param)
    else:
        # Default behavior - you can modify this
        print("Script mode not determined from filename.")
        print("Rename script to include 'pre' or 'post' in filename,")
        print("or modify the main section to specify behavior.")
        exit_code = 1
    
    sys.exit(exit_code)
