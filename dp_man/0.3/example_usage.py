#!/usr/bin/env python3
"""
Example usage of Data Protection Manager v0.3

This script shows how to use the CohesityManager class programmatically
instead of through the command line interface.
"""

import json
import os
from datetime import datetime
from dp_man_v3 import CohesityManager

def example_basic_usage():
    """Basic example of using CohesityManager."""
    
    print("🚀 Data Protection Manager v0.3 - Example Usage")
    print("=" * 50)
    
    # Configuration - replace with your cluster details
    config = {
        "cluster_vip": "your-cluster.example.com",
        "username": "admin",
        "password": "your-password",  # In real usage, use getpass or environment variables
        "domain": "LOCAL"
    }
    
    try:
        # Initialize the manager
        print("📡 Initializing Cohesity Manager...")
        cohesity_mgr = CohesityManager(
            cluster_vip=config["cluster_vip"],
            username=config["username"],
            password=config["password"],
            domain=config["domain"]
        )
        
        # Connect to the cluster
        print("🔌 Connecting to cluster...")
        if not cohesity_mgr.connect():
            print("❌ Failed to connect to cluster")
            return
        
        # Get cluster information
        print("\n📊 Cluster Information:")
        print("-" * 30)
        cluster_info = cohesity_mgr.get_cluster_info()
        for key, value in cluster_info.items():
            print(f"  {key}: {value}")
        
        # Get protection sources
        print("\n📋 Protection Sources:")
        print("-" * 30)
        sources = cohesity_mgr.get_protection_sources()
        print(f"  Found {len(sources)} protection sources")
        
        # Get protection groups
        print("\n🛡️  Protection Groups:")
        print("-" * 30)
        groups = cohesity_mgr.get_protection_groups()
        print(f"  Found {len(groups)} protection groups")
        
        # Generate full report
        print("\n📈 Generating comprehensive report...")
        report = cohesity_mgr.generate_report("both")
        
        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"example_report_{timestamp}.json"
        cohesity_mgr.export_report(report, output_file)
        
        # The export_report method now saves to output directory automatically
        output_path = os.path.join("output", output_file)
        print(f"✅ Report saved to: {output_path}")
        
        # Display report summary
        print(f"\n📄 Report Summary:")
        print(f"  Timestamp: {report['timestamp']}")
        print(f"  Cluster: {report['cluster'].get('name', 'Unknown')}")
        print(f"  Sources: {len(report.get('protection_sources', []))}")
        print(f"  Groups: {len(report.get('protection_groups', []))}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True


def example_sources_only():
    """Example showing how to get only protection sources."""
    
    print("\n" + "=" * 50)
    print("📋 Example: Protection Sources Only")
    print("=" * 50)
    
    # This example shows how to get only protection sources
    # You would use the same CohesityManager instance from above
    
    # Simulated for demonstration
    print("This example would:")
    print("1. Initialize CohesityManager")
    print("2. Connect to cluster")
    print("3. Call get_protection_sources()")
    print("4. Process and display source information")
    
    # Example of what the sources data might look like
    example_sources = [
        {"name": "VM-Server-01", "type": "VMware", "status": "Protected"},
        {"name": "DB-Server-02", "type": "Physical", "status": "Protected"},
        {"name": "File-Server-03", "type": "NAS", "status": "Unprotected"}
    ]
    
    print("\nExample sources format:")
    print(json.dumps(example_sources, indent=2))


def example_error_handling():
    """Example showing proper error handling."""
    
    print("\n" + "=" * 50)
    print("🚨 Example: Error Handling")
    print("=" * 50)
    
    print("Best practices for error handling:")
    print("1. Always check connection before making API calls")
    print("2. Handle SDK import errors gracefully")
    print("3. Use try-except blocks for API operations")
    print("4. Provide meaningful error messages to users")
    
    # Example error handling pattern
    example_code = '''
try:
    cohesity_mgr = CohesityManager(cluster_vip, username, password)
    if not cohesity_mgr.connect():
        print("Failed to connect - check credentials and cluster availability")
        return
    
    sources = cohesity_mgr.get_protection_sources()
    # Process sources...
    
except ImportError:
    print("cohesity_sdk not installed - run install_sdk.sh")
except Exception as e:
    print(f"Unexpected error: {e}")
    '''
    
    print(f"\nExample error handling code:\n{example_code}")


if __name__ == "__main__":
    print("📚 Data Protection Manager v0.3 Examples")
    print("This script demonstrates various usage patterns.")
    print("")
    print("⚠️  Note: Update the configuration in example_basic_usage()")
    print("   with your actual cluster details before running.")
    print("")
    
    # Run examples (basic usage commented out to avoid connection attempts)
    # example_basic_usage()
    example_sources_only()
    example_error_handling()
    
    print("\n✅ Examples completed!")
    print("\n💡 To run the actual script:")
    print("   python3 dp_man_v3.py -s your-cluster.com -u admin") 