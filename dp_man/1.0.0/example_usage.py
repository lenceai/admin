#!/usr/bin/env python3
"""
Example script showing how to use the Cohesity Manager programmatically
"""

from cohesity_manager import CohesityManager
import logging
import os
import json
from datetime import datetime
import numpy as np
import pandas as pd

# Ensure directories exist
os.makedirs("data", exist_ok=True)
os.makedirs("output", exist_ok=True)

# Set up logging to console and file
log_file = os.path.join("output", f"example_usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)

# Example 1: Upgrade clusters
def example_upgrade_clusters():
    print("="*50)
    print("Example 1: Upgrading Clusters")
    print("="*50)
    
    print("\nCommand-line equivalent:")
    print("  python cohesity_manager.py -u")
    print("  python cohesity_manager.py -u --target-version 7.1.2_u3")
    print("  python cohesity_manager.py -u --clusters custom_clusters.csv")
    
    # Create manager with debug mode enabled
    manager = CohesityManager(debug=True)
    
    # Load clusters from CSV
    csv_path = "data/clusters.csv"
    if not manager.load_clusters_from_csv(csv_path):
        print(f"Failed to load clusters from {csv_path}")
        return
    
    # Connect to clusters
    connected = manager.connect_to_clusters()
    if connected == 0:
        print("Failed to connect to any clusters")
        return
    
    # Start upgrades to the latest version
    print("\nStarting cluster upgrades...")
    results = manager.upgrade_all_clusters()
    
    print(f"\nUpgrade Results:")
    print(f"  Success: {results['success']}")
    print(f"  Failed: {results['failed']}")
    print(f"  Skipped: {results['skipped']}")
    
    # Save results to file
    results_file = os.path.join("output", f"upgrade_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to: {results_file}")

# Example 2: Create protection groups
def example_create_protection_groups():
    print("\n"+"="*50)
    print("Example 2: Creating Protection Groups")
    print("="*50)
    
    print("\nCommand-line equivalent:")
    print("  python cohesity_manager.py -c data/filesets.csv")
    print("  python cohesity_manager.py -c data/filesets.csv --policy Gold --group-name Linux_Daily_Backup")
    
    # Create manager
    manager = CohesityManager()
    
    # Load clusters from CSV
    csv_path = "data/clusters.csv"
    if not manager.load_clusters_from_csv(csv_path):
        print(f"Failed to load clusters from {csv_path}")
        return
    
    # Connect to clusters
    connected = manager.connect_to_clusters()
    if connected == 0:
        print("Failed to connect to any clusters")
        return
    
    # Create protection groups using filesets from CSV
    filesets_csv = "data/filesets.csv"
    group_name_template = "Linux_Daily_Backup"
    policy_name = "Gold"
    
    print(f"\nCreating protection groups using:")
    print(f"  Filesets CSV: {filesets_csv}")
    print(f"  Group name template: {group_name_template}")
    print(f"  Policy: {policy_name}")
    
    results = manager.create_protection_groups_on_all_clusters(
        filesets_csv=filesets_csv,
        group_name_template=group_name_template,
        policy_name=policy_name
    )
    
    print(f"\nProtection Group Creation Results:")
    print(f"  Success: {results['success']}")
    print(f"  Failed: {results['failed']}")
    
    # Save results to file
    results_file = os.path.join("output", f"protection_group_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to: {results_file}")

# Example 3: Check cluster status
def example_check_cluster_status():
    print("\n"+"="*50)
    print("Example 3: Checking Cluster Status")
    print("="*50)
    
    print("\nCommand-line equivalent:")
    print("  python cohesity_manager.py -s")
    print("  python cohesity_manager.py -s --details")
    
    # Create manager
    manager = CohesityManager()
    
    # Load clusters from CSV
    csv_path = "data/clusters.csv"
    if not manager.load_clusters_from_csv(csv_path):
        print(f"Failed to load clusters from {csv_path}")
        return
    
    # Connect to clusters
    connected = manager.connect_to_clusters()
    if connected == 0:
        print("Failed to connect to any clusters")
        return
    
    # Get basic status
    print("\nGetting basic status for all clusters:")
    statuses = manager.get_all_clusters_status(detailed=False)
    
    print(f"\nBasic Status Results:")
    print(f"  Retrieved status for {len(statuses)} clusters")
    
    # Convert to DataFrame for analysis
    if statuses:
        status_df = pd.DataFrame(statuses)
        
        # Calculate storage statistics with NumPy
        print("\nStorage Analysis:")
        storage_used = np.array([s.get('storage_used', 0) for s in statuses if s.get('storage_used')], dtype=np.float64)
        storage_total = np.array([s.get('storage_total', 0) for s in statuses if s.get('storage_total')], dtype=np.float64)
        
        if len(storage_used) > 0:
            total_used = np.sum(storage_used)
            print(f"  Total Storage Used: {manager._format_size(total_used)}")
            
        if len(storage_total) > 0:
            total_capacity = np.sum(storage_total)
            print(f"  Total Storage Capacity: {manager._format_size(total_capacity)}")
            
            if len(storage_used) > 0 and len(storage_total) > 0:
                usage_percentage = (np.sum(storage_used) / np.sum(storage_total)) * 100
                print(f"  Overall Usage: {usage_percentage:.2f}%")
                
                # Calculate standard deviation to show variation in cluster usage
                if len(storage_used) == len(storage_total) and len(storage_used) > 1:
                    usage_percentages = (storage_used / storage_total) * 100
                    std_dev = np.std(usage_percentages)
                    print(f"  Usage Standard Deviation: {std_dev:.2f}% (indicates variation between clusters)")
        
        # Analyze health status if available
        if 'health' in status_df.columns:
            health_counts = status_df['health'].value_counts()
            print("\nHealth Status Distribution:")
            for status, count in health_counts.items():
                print(f"  {status}: {count} clusters")
        
        # Analyze node counts
        if 'node_count' in status_df.columns:
            node_counts = status_df['node_count'].dropna()
            if not node_counts.empty:
                total_nodes = node_counts.sum()
                avg_nodes = node_counts.mean()
                print(f"\nNode Analysis:")
                print(f"  Total Nodes: {total_nodes}")
                print(f"  Average Nodes per Cluster: {avg_nodes:.2f}")
    
        # Get detailed status for the first cluster (as an example)
        print("\nGetting detailed status for the first cluster:")
        first_cluster_ip = statuses[0]["ip"]
        detailed_status = manager.get_cluster_status(first_cluster_ip, detailed=True)
    
    # Save all results to file (both JSON and CSV)
    if statuses:
        # Save as JSON
        results_file = os.path.join("output", f"cluster_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(results_file, 'w') as f:
            json.dump(statuses, f, indent=2)
        print(f"\nStatus results saved to: {results_file}")
        
        # Also save as CSV for easier data analysis
        csv_file = os.path.join("output", f"cluster_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        try:
            # Clean the DataFrame to handle nested data
            clean_df = status_df.copy()
            for col in clean_df.columns:
                # Convert non-scalar values to strings
                if clean_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    clean_df[col] = clean_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
                
                # Convert storage values to human readable format
                if col in ['storage_used', 'storage_total']:
                    clean_df[col] = clean_df[col].apply(lambda x: manager._format_size(x) if x is not None else "Unknown")
            
            # Save to CSV
            clean_df.to_csv(csv_file, index=False)
            print(f"Status results also written to CSV: {csv_file}")
        except Exception as e:
            print(f"Error writing status results to CSV: {str(e)}")

# Example 4: Custom cluster operations
def example_custom_operations():
    print("\n"+"="*50)
    print("Example 4: Custom Cluster Operations")
    print("="*50)
    
    # Create manager
    manager = CohesityManager()
    
    # Manually define clusters (alternative to CSV loading)
    clusters = [
        {
            "cluster_ip": "10.0.1.100",
            "username": "admin",
            "password": "Password123",
            "domain": "LOCAL"
        },
        {
            "cluster_ip": "10.0.1.101",
            "username": "admin",
            "password": "Password456",
            "domain": "LOCAL"
        }
    ]
    
    # Set the clusters list directly
    manager.clusters = clusters
    
    # Connect to clusters
    connected = manager.connect_to_clusters()
    if connected == 0:
        print("Failed to connect to any clusters")
        return
    
    # Perform custom operations on each connected cluster
    cluster_info = []
    for cluster_ip, cluster_data in manager.cluster_clients.items():
        client = cluster_data['client']
        info = cluster_data['info']
        
        print(f"\nCluster: {info['name']} ({cluster_ip})")
        print(f"  Version: {info['version']}")
        print(f"  ID: {info['id']}")
        
        cluster_details = {
            "name": info['name'],
            "ip": cluster_ip,
            "version": info['version'],
            "id": info['id'],
            "protection_groups": []
        }
        
        # Example: List protection groups
        try:
            protection_groups = client.protection_group.get_protection_groups()
            print(f"  Protection Groups: {len(protection_groups)}")
            for pg in protection_groups[:3]:  # Show first 3 groups
                print(f"    - {pg.name}")
                cluster_details["protection_groups"].append({"name": pg.name, "id": pg.id})
            if len(protection_groups) > 3:
                print(f"    - ... ({len(protection_groups) - 3} more)")
        except Exception as e:
            print(f"  Error listing protection groups: {str(e)}")
        
        cluster_info.append(cluster_details)
    
    # Save results to file
    results_file = os.path.join("output", f"cluster_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(results_file, 'w') as f:
        json.dump(cluster_info, f, indent=2)
    print(f"\nCluster information saved to: {results_file}")

if __name__ == "__main__":
    print("Cohesity Manager Examples")
    print("------------------------\n")
    
    try:
        print("Command-line usage examples:")
        print("  Show version:            python cohesity_manager.py -v")
        print("  Check status:            python cohesity_manager.py -s")
        print("  Detailed status:         python cohesity_manager.py -s --details")
        print("  Upgrade clusters:        python cohesity_manager.py -u")
        print("  Create protection groups: python cohesity_manager.py -c data/filesets.csv")
        print("\nRunning programmatic examples...\n")
        
        example_upgrade_clusters()
        example_create_protection_groups()
        example_check_cluster_status()
        example_custom_operations()
    except KeyboardInterrupt:
        print("\nExamples interrupted by user.")
    except Exception as e:
        print(f"\nError running examples: {str(e)}")
        
    print("\nExamples completed.") 