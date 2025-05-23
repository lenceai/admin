#!/usr/bin/env python3
"""
Data Protection Manager (dp_man) v0.3
A clean, simple tool for managing Cohesity cluster protection using the modern cohesity_sdk.

This version uses the newer cohesity_sdk with V2 APIs for better performance and features.
"""

import argparse
import getpass
import json
import logging
import os
import sys
import urllib3
from datetime import datetime
from typing import Dict, List, Optional, Any

# Disable SSL warnings for self-signed certificates (common in Cohesity clusters)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Ensure output directory exists for logs
os.makedirs("output", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('output', 'dp_man_v3.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CohesityManager:
    """Main class for managing Cohesity cluster operations using the modern SDK."""
    
    def __init__(self, cluster_vip: str, username: str, password: str, domain: str = "LOCAL"):
        """Initialize the Cohesity manager with connection details."""
        self.cluster_vip = cluster_vip
        self.username = username
        self.password = password
        self.domain = domain
        self.client = None
        
    def connect(self) -> bool:
        """Connect to the Cohesity cluster using the modern SDK."""
        try:
            # Import the modern cohesity_sdk
            from cohesity_sdk.cluster.cluster_client import ClusterClient
            
            self.client = ClusterClient(
                cluster_vip=self.cluster_vip,
                username=self.username,
                password=self.password,
                domain=self.domain
            )
            
            # Test connection by getting cluster info
            cluster_info = self.client.platform_api.get_cluster()
            logger.info(f"✅ Connected to Cohesity cluster: {cluster_info.name}")
            logger.info(f"📦 Software version: {cluster_info.sw_version}")
            
            return True
            
        except ImportError:
            logger.error("❌ cohesity_sdk not found. Please install it:")
            logger.error("   pip install cohesity_sdk")
            logger.error("   OR clone from: https://github.com/cohesity/cohesity_sdk")
            return False
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to cluster: {e}")
            return False
    
    def get_protection_sources(self) -> List[Dict]:
        """Get all protection sources from the cluster."""
        try:
            if not self.client:
                raise Exception("Not connected to cluster")
            
            # Get source registrations using V2 API (this is the correct endpoint)
            sources_response = self.client.source_api.get_source_registrations()
            
            # Handle the response object properly
            if hasattr(sources_response, 'registrations') and sources_response.registrations:
                sources = sources_response.registrations
                logger.info(f"📋 Found {len(sources)} protection sources")
                return [source.__dict__ if hasattr(source, '__dict__') else source for source in sources]
            else:
                logger.info("📋 Found 0 protection sources")
                return []
            
        except Exception as e:
            logger.error(f"❌ Failed to get protection sources: {e}")
            return []
    
    def get_protection_groups(self) -> List[Dict]:
        """Get all protection groups (jobs) from the cluster."""
        try:
            if not self.client:
                raise Exception("Not connected to cluster")
            
            # Get protection groups using V2 API
            groups_response = self.client.protection_group_api.get_protection_groups()
            
            # Handle the response object properly
            if hasattr(groups_response, 'protection_groups') and groups_response.protection_groups:
                groups = groups_response.protection_groups
                logger.info(f"🛡️  Found {len(groups)} protection groups")
                return [group.__dict__ if hasattr(group, '__dict__') else group for group in groups]
            else:
                logger.info("🛡️  Found 0 protection groups")
                return []
            
        except Exception as e:
            logger.error(f"❌ Failed to get protection groups: {e}")
            return []
    
    def get_cluster_info(self) -> Dict:
        """Get basic cluster information."""
        try:
            if not self.client:
                raise Exception("Not connected to cluster")
            
            cluster_info = self.client.platform_api.get_cluster()
            
            info = {
                "name": cluster_info.name,
                "id": cluster_info.id,
                "software_version": cluster_info.sw_version,
                "cluster_type": getattr(cluster_info, 'cluster_type', 'Unknown'),
                "nodes_count": len(getattr(cluster_info, 'node_ips', [])),
                "timezone": getattr(cluster_info, 'timezone', 'Unknown')
            }
            
            return info
            
        except Exception as e:
            logger.error(f"❌ Failed to get cluster info: {e}")
            return {}
    
    def generate_report(self, report_type: str = "both") -> Dict:
        """Generate a comprehensive report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "cluster": self.get_cluster_info()
        }
        
        if report_type in ["sources", "both"]:
            report["protection_sources"] = self.get_protection_sources()
        
        if report_type in ["groups", "both"]:
            report["protection_groups"] = self.get_protection_groups()
        
        return report
    
    def export_report(self, report: Dict, filename: str):
        """Export report to JSON file in output folder."""
        try:
            # Create output folder if it doesn't exist
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            
            # Create full path to output file
            output_path = os.path.join(output_dir, filename)
            
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"📄 Report exported to: {output_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to export report: {e}")


def check_sdk_installation():
    """Check if the cohesity_sdk is properly installed."""
    try:
        import cohesity_sdk
        logger.info("✅ cohesity_sdk is installed")
        return True
    except ImportError:
        logger.error("❌ cohesity_sdk not found!")
        logger.error("")
        logger.error("📦 Installation instructions:")
        logger.error("   Method 1 - From source (recommended):")
        logger.error("   git clone https://github.com/cohesity/cohesity_sdk.git")
        logger.error("   cd cohesity_sdk")
        logger.error("   pip install -r requirements.txt")
        logger.error("   python setup.py install")
        logger.error("")
        logger.error("   Method 2 - Direct install (if available on PyPI):")
        logger.error("   pip install cohesity_sdk")
        logger.error("")
        return False


def main():
    """Main function to run the Data Protection Manager."""
    
    print("🚀 Cohesity Data Protection Manager v0.3")
    print("   Using modern cohesity_sdk with V2 APIs")
    print("")
    
    # Check SDK installation first
    if not check_sdk_installation():
        sys.exit(1)
    
    parser = argparse.ArgumentParser(
        description="Cohesity Data Protection Manager v0.3 - Modern SDK Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -s cluster.example.com -u admin
  %(prog)s -s 10.1.1.100 -u admin -a sources
  %(prog)s -s cluster.local -u backup_admin -d DOMAIN -o my_report.json
        """
    )
    
    parser.add_argument("-s", "--server", required=True, 
                       help="Cohesity cluster hostname or IP")
    parser.add_argument("-u", "--username", required=True, 
                       help="Username for authentication")
    parser.add_argument("-p", "--password", 
                       help="Password (recommended: omit for secure prompt)")
    parser.add_argument("-d", "--domain", default="LOCAL", 
                       help="Authentication domain (default: LOCAL)")
    parser.add_argument("-a", "--action", 
                       choices=["sources", "groups", "both"], default="both",
                       help="Report type: sources, groups, or both (default: both)")
    parser.add_argument("-o", "--output", 
                       help="Output filename (default: auto-generated)")
    parser.add_argument("-v", "--verbose", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Handle password securely
    password = args.password
    if not password:
        try:
            password = getpass.getpass(f"🔐 Enter password for {args.username}@{args.server}: ")
        except KeyboardInterrupt:
            print("\n❌ Operation cancelled by user.")
            sys.exit(1)
        
        if not password:
            logger.error("❌ Password cannot be empty")
            sys.exit(1)
    
    # Initialize and connect to Cohesity cluster
    cohesity_mgr = CohesityManager(
        cluster_vip=args.server,
        username=args.username,
        password=password,
        domain=args.domain
    )
    
    if not cohesity_mgr.connect():
        sys.exit(1)
    
    # Generate report
    logger.info(f"📊 Generating {args.action} report...")
    report = cohesity_mgr.generate_report(args.action)
    
    # Export report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = args.output or f"cohesity_report_{timestamp}.json"
    cohesity_mgr.export_report(report, output_file)
    
    # Get the full output path for display
    output_path = os.path.join("output", output_file)
    
    # Display summary
    print("")
    print("📈 REPORT SUMMARY")
    print("=" * 50)
    print(f"🏢 Cluster: {report['cluster'].get('name', 'Unknown')}")
    print(f"📦 Version: {report['cluster'].get('software_version', 'Unknown')}")
    print(f"🔢 Node Count: {report['cluster'].get('nodes_count', 'Unknown')}")
    
    if 'protection_sources' in report:
        print(f"📋 Protection Sources: {len(report['protection_sources'])}")
    
    if 'protection_groups' in report:
        print(f"🛡️  Protection Groups: {len(report['protection_groups'])}")
    
    print(f"📄 Full report saved to: {output_path}")
    print("")
    print("✅ Data Protection Manager completed successfully!")


if __name__ == "__main__":
    main() 