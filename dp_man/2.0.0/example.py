#!/usr/bin/env python
"""
Example script demonstrating the usage of the Cohesity Manager module.
"""

import sys
import os
import datetime
from time import sleep

# Import the cohesity_manager module
try:
    from cohesity_manager import cohesity_manager as cm
except ImportError:
    print("Error: cohesity_manager module not found.")
    print("Make sure cohesity_manager.py is in your Python path.")
    sys.exit(1)

def print_section(title):
    """Print a section title"""
    print("\n" + "=" * 80)
    print(" " * 30 + title)
    print("=" * 80)

def cluster_info_example():
    """Get and display cluster information"""
    print_section("CLUSTER INFORMATION")
    
    info = cm.get_cluster_info()
    if info:
        print(f"Cluster Name: {info['name']}")
        print(f"Cluster ID: {info['id']}")
        print(f"Cluster Software Version: {info['softwareVersion']}")
        print(f"Number of Nodes: {len(info['nodeConfigs'])}")
        
        # Storage information
        stats = cm.get_storage_stats()
        if stats:
            total_tb = stats['totalCapacityBytes'] / (1024**4)
            used_tb = stats['usedCapacityBytes'] / (1024**4)
            print(f"Total Storage: {total_tb:.2f} TB")
            print(f"Used Storage: {used_tb:.2f} TB ({used_tb/total_tb*100:.1f}%)")
    else:
        print("Unable to retrieve cluster information")

def list_protection_jobs_example():
    """List protection jobs"""
    print_section("PROTECTION JOBS")
    
    jobs = cm.list_protection_jobs()
    if jobs:
        print(f"Found {len(jobs)} active protection jobs:")
        print("{:<40} {:<15} {:<20} {:<10}".format("JOB NAME", "ENVIRONMENT", "LAST RUN STATUS", "PAUSED"))
        print("-" * 90)
        
        for job in jobs:
            # Get last run
            runs = cm.get_protection_job_runs(job_id=job['id'], num_runs=1)
            last_run_status = "Never run"
            
            if runs and len(runs) > 0:
                if 'backupRun' in runs[0]:
                    # v1 API format
                    last_run_status = runs[0]['backupRun']['status']
                elif 'localBackupInfo' in runs[0]:
                    # v2 API format
                    last_run_status = runs[0]['localBackupInfo']['status']
            
            print("{:<40} {:<15} {:<20} {:<10}".format(
                job['name'][:38],
                job['environment'][1:],  # Remove 'k' prefix from environment
                last_run_status,
                'Yes' if job.get('isPaused', False) else 'No'
            ))
    else:
        print("No protection jobs found or unable to retrieve jobs")

def list_views_example():
    """List views"""
    print_section("VIEWS")
    
    views = cm.list_views()
    if views:
        print(f"Found {len(views)} active views:")
        print("{:<40} {:<15} {:<15}".format("VIEW NAME", "PROTOCOL", "SIZE (GB)"))
        print("-" * 80)
        
        for view in views:
            protocols = []
            if view.get('protocolAccess') == 'kAll':
                protocols = ["SMB", "NFS", "S3"]
            elif view.get('smbAccess') == 'kEnabled':
                protocols.append("SMB")
            elif view.get('nfsAccess') == 'kEnabled':
                protocols.append("NFS")
            elif view.get('s3Access') == 'kEnabled':
                protocols.append("S3")
            
            size_bytes = view.get('logicalUsageBytes', 0)
            size_gb = size_bytes / (1024**3)
            
            print("{:<40} {:<15} {:<15.2f}".format(
                view['name'][:38],
                ", ".join(protocols),
                size_gb
            ))
    else:
        print("No views found or unable to retrieve views")

def recent_alerts_example():
    """Display recent alerts"""
    print_section("RECENT ALERTS")
    
    alerts = cm.get_active_alerts(max_alerts=10)
    if alerts:
        print(f"Found {len(alerts)} recent alerts:")
        print("{:<25} {:<15} {:<40}".format("DATE", "SEVERITY", "MESSAGE"))
        print("-" * 85)
        
        for alert in alerts:
            alert_time = cm.format_timestamp(alert['alertDocument'].get('firstTimestampUsecs', 0))
            severity = alert['alertDocument'].get('severity', '')[1:] # Remove 'k' prefix
            message = alert['alertDocument'].get('alertTitle', 'No message')
            
            print("{:<25} {:<15} {:<40}".format(
                alert_time,
                severity,
                message[:38] + ('...' if len(message) > 38 else '')
            ))
    else:
        print("No alerts found or unable to retrieve alerts")

def job_run_details_example(job_name):
    """Display details of a specific job's runs"""
    print_section(f"RUNS FOR JOB: {job_name}")
    
    job = cm.get_protection_job_by_name(job_name)
    if not job:
        print(f"Job '{job_name}' not found")
        return
    
    runs = cm.get_protection_job_runs(job_id=job['id'], num_runs=5)
    if runs:
        print(f"Found {len(runs)} recent runs for job '{job_name}':")
        print("{:<25} {:<15} {:<15} {:<15}".format("START TIME", "STATUS", "DURATION (min)", "DATA (GB)"))
        print("-" * 85)
        
        for run in runs:
            # Handle v1 vs v2 API differences
            if 'backupRun' in run:
                # v1 API format
                start_time = run['backupRun']['stats'].get('startTimeUsecs', 0)
                end_time = run['backupRun']['stats'].get('endTimeUsecs', 0)
                status = run['backupRun']['status']
                bytes_backed_up = run['backupRun']['stats'].get('totalBytesReadFromSource', 0)
            elif 'localBackupInfo' in run:
                # v2 API format
                start_time = run.get('startTimeUsecs', 0)
                end_time = run['localBackupInfo'].get('endTimeUsecs', 0)
                status = run['localBackupInfo']['status']
                bytes_backed_up = run['localBackupInfo'].get('bytesReadFromSource', 0)
            else:
                continue
            
            start_time_str = cm.format_timestamp(start_time)
            duration_mins = 0
            if end_time > 0 and start_time > 0:
                duration_mins = (end_time - start_time) / 60000000  # Convert usecs to minutes
            
            data_gb = bytes_backed_up / (1024**3)
            
            print("{:<25} {:<15} {:<15.1f} {:<15.2f}".format(
                start_time_str,
                status,
                duration_mins,
                data_gb
            ))
    else:
        print(f"No runs found for job '{job_name}' or unable to retrieve runs")

def protected_sources_example():
    """List protected sources"""
    print_section("PROTECTED SOURCES")
    
    # Get protection jobs and their associated sources
    jobs = cm.list_protection_jobs()
    if not jobs:
        print("No protection jobs found or unable to retrieve jobs")
        return
    
    print("Sources protected by jobs:")
    print("{:<40} {:<20} {:<20}".format("SOURCE", "TYPE", "PROTECTING JOB"))
    print("-" * 85)
    
    # Set to track unique sources we've seen
    seen_sources = set()
    
    for job in jobs:
        env_type = job['environment'][1:] # Remove 'k' prefix
        
        if 'sourceIds' in job:
            for source_id in job['sourceIds']:
                # Get source name - we'd need to query each source
                # For simplicity, use source ID in this example
                source_name = f"Source ID: {source_id}"
                
                # Avoid duplicates
                if source_name not in seen_sources:
                    seen_sources.add(source_name)
                    print("{:<40} {:<20} {:<20}".format(
                        source_name[:38],
                        env_type,
                        job['name'][:18]
                    ))

def main():
    """Main example routine"""
    # Connect to cluster
    cluster = input("Enter Cohesity cluster FQDN or IP (or 'helios' for Helios): ")
    username = input("Enter username: ")
    domain = input("Enter domain (default: local): ") or "local"
    
    # Determine if we're connecting to Helios
    if cluster.lower() == 'helios':
        print("\nConnecting to Helios...")
        if not cm.connect(username=username, domain=domain):
            print("Failed to connect to Helios")
            sys.exit(1)
        
        # List available clusters
        clusters = cm.list_helios_clusters()
        if not clusters:
            print("No clusters found connected to Helios")
            sys.exit(1)
        
        print("\nAvailable clusters:")
        for i, cluster in enumerate(clusters, 1):
            print(f"{i}. {cluster['name']}")
        
        # Select a cluster
        selected = int(input("\nSelect a cluster number: "))
        if selected < 1 or selected > len(clusters):
            print("Invalid selection")
            sys.exit(1)
        
        print(f"Connecting to {clusters[selected-1]['name']}...")
        if not cm.connect_to_helios_cluster(clusters[selected-1]['name']):
            print(f"Failed to connect to {clusters[selected-1]['name']}")
            sys.exit(1)
    else:
        # Connect directly to the cluster
        print(f"\nConnecting to {cluster}...")
        if not cm.connect(cluster=cluster, username=username, domain=domain):
            print(f"Failed to connect to {cluster}")
            sys.exit(1)
        
    print("Connected successfully!")
    
    # Run examples
    try:
        cluster_info_example()
        list_protection_jobs_example()
        list_views_example()
        recent_alerts_example()
        
        # Ask for a job name to show detailed runs
        jobs = cm.list_protection_jobs()
        if jobs:
            print("\nAvailable protection jobs:")
            for i, job in enumerate(jobs[:10], 1):  # Limit to first 10 for brevity
                print(f"{i}. {job['name']}")
            
            try:
                selected = int(input("\nSelect a job number to view detailed run info (or 0 to skip): "))
                if selected > 0 and selected <= len(jobs[:10]):
                    job_run_details_example(jobs[selected-1]['name'])
            except ValueError:
                print("Skipping job run details")
        
        protected_sources_example()
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"\nError during example execution: {e}")
    finally:
        # Disconnect
        print("\nDisconnecting...")
        cm.disconnect()
        print("Disconnected. Example completed.")

if __name__ == "__main__":
    main() 