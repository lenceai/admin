#!/usr/bin/env python3
"""
Community Examples for Cohesity SDK
Inspired by the Cohesity community automation samples and Brian Seltzer's scripts

This file contains example patterns commonly used in the Cohesity community.
References:
- https://github.com/cohesity/community-automation-samples
- https://github.com/bseltz-cohesity/scripts
- https://github.com/cohesity/cohesity-nagios-plugin
"""

import getpass
import json
import os
import urllib3
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Disable SSL warnings for self-signed certificates (common in Cohesity clusters)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def create_simple_cohesity_client(cluster_vip: str, username: str, password: str = None, domain: str = "LOCAL"):
    """
    Simple client creation pattern used in community scripts.
    
    Args:
        cluster_vip: Cluster IP or FQDN
        username: Username for authentication
        password: Password (if None, will prompt securely)
        domain: Authentication domain
    
    Returns:
        Connected ClusterClient instance
    """
    from cohesity_sdk.cluster.cluster_client import ClusterClient
    
    if not password:
        password = getpass.getpass(f"🔐 Enter password for {username}@{cluster_vip}: ")
    
    client = ClusterClient(
        cluster_vip=cluster_vip,
        username=username,
        password=password,
        domain=domain
    )
    
    return client


def get_cluster_basic_info(client):
    """
    Get basic cluster information - commonly used pattern.
    Inspired by Nagios plugin examples.
    """
    try:
        cluster_info = client.platform_api.get_cluster()
        
        return {
            "name": cluster_info.name,
            "id": cluster_info.id,
            "software_version": cluster_info.sw_version,
            "nodes": getattr(cluster_info, 'node_count', 0),
            "cluster_type": getattr(cluster_info, 'cluster_type', 'Unknown')
        }
    except Exception as e:
        print(f"❌ Error getting cluster info: {e}")
        return {}


def get_protection_jobs_summary(client):
    """
    Get protection jobs summary - common automation pattern.
    Based on community monitoring scripts.
    """
    try:
        groups_response = client.protection_group_api.get_protection_groups()
        
        if not hasattr(groups_response, 'protection_groups') or not groups_response.protection_groups:
            return {"total": 0, "active": 0, "paused": 0}
        
        groups = groups_response.protection_groups
        total = len(groups)
        active = sum(1 for group in groups if getattr(group, 'is_active', True))
        paused = total - active
        
        return {
            "total": total,
            "active": active,
            "paused": paused,
            "jobs": [
                {
                    "name": group.name,
                    "id": group.id,
                    "environment": getattr(group, 'environment', 'Unknown'),
                    "is_active": getattr(group, 'is_active', True),
                    "policy_id": getattr(group, 'policy_id', None)
                } for group in groups
            ]
        }
    except Exception as e:
        print(f"❌ Error getting protection jobs: {e}")
        return {"total": 0, "active": 0, "paused": 0}


def get_protection_sources_by_environment(client):
    """
    Get protection sources organized by environment.
    Common pattern in community scripts for source management.
    """
    try:
        sources_response = client.source_api.get_source_registrations()
        
        if not hasattr(sources_response, 'registrations') or not sources_response.registrations:
            return {}
        
        sources = sources_response.registrations
        sources_by_env = {}
        
        for source in sources:
            env = getattr(source, 'environment', 'Unknown')
            if env not in sources_by_env:
                sources_by_env[env] = []
            
            sources_by_env[env].append({
                "name": getattr(source, 'name', 'Unknown'),
                "id": getattr(source, 'id', None),
                "environment": env,
                "connection_id": getattr(source, 'connection_id', None),
                "status": getattr(source, 'registration_info', {}).get('registration_status', 'Unknown')
            })
        
        return sources_by_env
    except Exception as e:
        print(f"❌ Error getting protection sources: {e}")
        return {}


def create_monitoring_report(client):
    """
    Create a comprehensive monitoring report.
    Pattern inspired by Nagios monitoring scripts and community reporting tools.
    """
    report = {
        "timestamp": datetime.now().isoformat(),
        "report_type": "cluster_monitoring",
        "cluster": get_cluster_basic_info(client),
        "protection_jobs": get_protection_jobs_summary(client),
        "protection_sources": get_protection_sources_by_environment(client)
    }
    
    # Add summary statistics
    total_sources = sum(len(sources) for sources in report["protection_sources"].values())
    report["summary"] = {
        "total_protection_sources": total_sources,
        "total_protection_jobs": report["protection_jobs"]["total"],
        "active_protection_jobs": report["protection_jobs"]["active"],
        "environments_count": len(report["protection_sources"])
    }
    
    return report


def save_report_to_file(report: Dict, filename: str = None):
    """
    Save report to JSON file with timestamp in output directory.
    Standard pattern in community scripts.
    """
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cohesity_monitoring_report_{timestamp}.json"
    
    try:
        # Create output folder if it doesn't exist
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create full path to output file
        output_path = os.path.join(output_dir, filename)
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"📄 Report saved to: {output_path}")
        return output_path
    except Exception as e:
        print(f"❌ Error saving report: {e}")
        return None


def print_cluster_summary(report: Dict):
    """
    Print a formatted cluster summary.
    Pattern used in many community scripts for console output.
    """
    print("\n" + "="*60)
    print("🏢 COHESITY CLUSTER SUMMARY")
    print("="*60)
    
    cluster = report.get("cluster", {})
    print(f"📍 Cluster Name: {cluster.get('name', 'Unknown')}")
    print(f"🔗 Cluster ID: {cluster.get('id', 'Unknown')}")
    print(f"📦 Software Version: {cluster.get('software_version', 'Unknown')}")
    print(f"🖥️  Node Count: {cluster.get('nodes', 'Unknown')}")
    
    summary = report.get("summary", {})
    print(f"\n📊 PROTECTION SUMMARY")
    print(f"🛡️  Total Protection Jobs: {summary.get('total_protection_jobs', 0)}")
    print(f"✅ Active Jobs: {summary.get('active_protection_jobs', 0)}")
    print(f"📋 Total Sources: {summary.get('total_protection_sources', 0)}")
    print(f"🌍 Environments: {summary.get('environments_count', 0)}")
    
    # Show environment breakdown
    sources = report.get("protection_sources", {})
    if sources:
        print(f"\n🌍 SOURCES BY ENVIRONMENT")
        for env, env_sources in sources.items():
            print(f"  {env}: {len(env_sources)} sources")


def example_basic_monitoring():
    """
    Example of basic cluster monitoring - common community pattern.
    """
    print("🚀 Cohesity Basic Monitoring Example")
    print("=" * 50)
    
    # Connection parameters - typically from config file or environment
    cluster_vip = input("🔗 Enter cluster VIP/FQDN: ").strip()
    username = input(f"👤 Enter username: ").strip()
    
    try:
        # Create client using community pattern
        client = create_simple_cohesity_client(cluster_vip, username)
        
        # Generate monitoring report
        print("\n📊 Generating monitoring report...")
        report = create_monitoring_report(client)
        
        # Display summary
        print_cluster_summary(report)
        
        # Save to file
        filename = save_report_to_file(report)
        
        print(f"\n✅ Monitoring completed successfully!")
        return report
        
    except Exception as e:
        print(f"❌ Monitoring failed: {e}")
        return None


def example_source_inventory():
    """
    Example of source inventory - pattern used in community scripts for asset management.
    """
    print("🚀 Cohesity Source Inventory Example")
    print("=" * 50)
    
    cluster_vip = input("🔗 Enter cluster VIP/FQDN: ").strip()
    username = input(f"👤 Enter username: ").strip()
    
    try:
        client = create_simple_cohesity_client(cluster_vip, username)
        
        print("\n📋 Getting protection sources inventory...")
        sources_by_env = get_protection_sources_by_environment(client)
        
        print(f"\n📊 PROTECTION SOURCES INVENTORY")
        print("-" * 40)
        
        total_sources = 0
        for env, sources in sources_by_env.items():
            print(f"\n🌍 {env} ({len(sources)} sources):")
            total_sources += len(sources)
            
            for source in sources[:5]:  # Show first 5 sources per environment
                status_icon = "✅" if source.get("status") == "kRegistered" else "❌"
                print(f"  {status_icon} {source.get('name', 'Unknown')}")
            
            if len(sources) > 5:
                print(f"  ... and {len(sources) - 5} more sources")
        
        print(f"\n📈 Total Sources: {total_sources}")
        
        # Save detailed inventory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cohesity_source_inventory_{timestamp}.json"
        
        # Create output folder if it doesn't exist
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)
        
        with open(output_path, 'w') as f:
            json.dump(sources_by_env, f, indent=2, default=str)
        
        print(f"📄 Detailed inventory saved to: {output_path}")
        
    except Exception as e:
        print(f"❌ Inventory failed: {e}")


if __name__ == "__main__":
    print("🧪 Cohesity Community Examples")
    print("Choose an example to run:")
    print("1. Basic Monitoring Report")
    print("2. Source Inventory")
    
    choice = input("\nEnter choice (1-2): ").strip()
    
    if choice == "1":
        example_basic_monitoring()
    elif choice == "2":
        example_source_inventory()
    else:
        print("❌ Invalid choice") 