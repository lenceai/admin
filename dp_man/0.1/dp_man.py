#!/usr/bin/env python3
"""
Data Protection Manager (dp_man) v0.1
A tool for managing Cohesity cluster protection jobs and backup resources.

This version focuses on Physical Linux Servers with file-based protection sources.
"""

import json
import logging
import requests
import argparse
import sys
import getpass
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import urllib3

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dp_man.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class CohesityCluster:
    """Cohesity cluster connection configuration."""
    hostname: str
    username: str
    password: str
    domain: str = "LOCAL"
    port: int = 443
    api_version: str = "v2"


@dataclass
class PhysicalServer:
    """Physical server protection source model."""
    id: Optional[int] = None
    name: str = ""
    hostname: str = ""
    ip_address: str = ""
    os_type: str = "Linux"
    agent_version: str = ""
    connection_status: str = ""
    last_backup: Optional[str] = None
    protection_jobs: List[str] = None
    
    def __post_init__(self):
        if self.protection_jobs is None:
            self.protection_jobs = []


@dataclass
class ProtectionJob:
    """Protection job model for file-based backups."""
    id: Optional[int] = None
    name: str = ""
    description: str = ""
    policy_name: str = ""
    environment: str = "kPhysical"
    sources: List[PhysicalServer] = None
    backup_paths: List[str] = None
    exclude_paths: List[str] = None
    status: str = ""
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    
    def __post_init__(self):
        if self.sources is None:
            self.sources = []
        if self.backup_paths is None:
            self.backup_paths = []
        if self.exclude_paths is None:
            self.exclude_paths = []


class CohesityAPIClient:
    """Client for interacting with Cohesity REST API."""
    
    def __init__(self, cluster: CohesityCluster):
        self.cluster = cluster
        self.base_url = f"https://{cluster.hostname}:{cluster.port}/{cluster.api_version}"
        self.session = requests.Session()
        self.session.verify = False
        self.auth_token = None
        
    def authenticate(self) -> bool:
        """Authenticate with the Cohesity cluster."""
        try:
            auth_url = f"{self.base_url}/access-tokens"
            auth_data = {
                "domain": self.cluster.domain,
                "username": self.cluster.username,
                "password": self.cluster.password
            }
            
            response = self.session.post(auth_url, json=auth_data)
            response.raise_for_status()
            
            token_data = response.json()
            self.auth_token = token_data.get("accessToken")
            
            if self.auth_token:
                self.session.headers.update({"Authorization": f"Bearer {self.auth_token}"})
                logger.info(f"Successfully authenticated to Cohesity cluster: {self.cluster.hostname}")
                return True
            else:
                logger.error("Failed to retrieve access token")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {e}")
            return False
    
    def get_protection_sources(self, environment: str = "kPhysical") -> List[Dict]:
        """Get protection sources of specified environment type."""
        # Try v2 API first
        try:
            url = f"{self.base_url}/data-protect/sources"
            params = {"environments": environment}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"v2 API failed for protection sources: {e}")
            
            # Fallback to v1 API
            try:
                logger.info("Trying v1 API for protection sources...")
                v1_url = f"https://{self.cluster.hostname}:{self.cluster.port}/irisservices/api/v1/public/protectionSources"
                params = {"environment": environment}
                
                response = self.session.get(v1_url, params=params)
                response.raise_for_status()
                
                v1_data = response.json()
                logger.info("Successfully retrieved sources using v1 API")
                logger.debug(f"v1 API response: {v1_data}")
                return v1_data
                
            except requests.exceptions.RequestException as e2:
                logger.error(f"Both v2 and v1 API failed for protection sources: v2={e}, v1={e2}")
                return []
    
    def get_protection_jobs(self, environment: str = "kPhysical") -> List[Dict]:
        """Get protection jobs for specified environment."""
        try:
            url = f"{self.base_url}/data-protect/protection-groups"
            params = {"environments": environment}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get protection jobs: {e}")
            return []
    
    def get_protection_job_runs(self, job_id: int, num_runs: int = 10) -> List[Dict]:
        """Get recent runs for a specific protection job."""
        try:
            url = f"{self.base_url}/data-protect/protection-groups/{job_id}/runs"
            params = {
                "numRuns": num_runs
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            runs_data = response.json()
            logger.debug(f"Raw runs API response: {runs_data}")
            
            # Handle different response formats
            if isinstance(runs_data, dict):
                # v2 API might return {"runs": [...]} or similar
                if "runs" in runs_data:
                    return runs_data["runs"]
                elif "protectionRuns" in runs_data:
                    return runs_data["protectionRuns"]
                else:
                    logger.warning(f"Unexpected runs response format: {runs_data}")
                    return []
            elif isinstance(runs_data, list):
                return runs_data
            else:
                logger.warning(f"Unexpected runs response type: {type(runs_data)}")
                return []
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"v2 API failed for protection job runs: {e}")
            
            # Fallback to v1 API
            try:
                logger.info(f"Trying v1 API for job runs: {job_id}")
                v1_url = f"https://{self.cluster.hostname}:{self.cluster.port}/irisservices/api/v1/public/protectionRuns"
                params = {
                    "jobId": job_id,
                    "numRuns": num_runs
                }
                
                response = self.session.get(v1_url, params=params)
                response.raise_for_status()
                
                v1_runs = response.json()
                logger.info("Successfully retrieved runs using v1 API")
                logger.debug(f"v1 runs response: {v1_runs}")
                return v1_runs
                
            except requests.exceptions.RequestException as e2:
                logger.error(f"Both v2 and v1 API failed for protection job runs: v2={e}, v1={e2}")
                return []
    
    def get_feature_flags(self) -> Dict:
        """Get cluster feature flags."""
        try:
            url = f"{self.base_url}/clusters/feature-flag"
            
            response = self.session.get(url)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get feature flags: {e}")
            return {}


class PhysicalServerManager:
    """Manager for Physical Server protection sources."""
    
    def __init__(self, api_client: CohesityAPIClient):
        self.api_client = api_client
        
    def get_physical_servers(self) -> List[PhysicalServer]:
        """Get all physical servers registered with the cluster."""
        servers = []
        protection_sources = self.api_client.get_protection_sources("kPhysical")
        
        def extract_servers_from_hierarchy(source_list, level=0):
            """Recursively extract servers from protection source hierarchy."""
            for source in source_list:
                if isinstance(source, dict):
                    # Check if this is a physical server node
                    protection_source = source.get("protectionSource", {})
                    if protection_source.get("environment") == "kPhysical":
                        physical_source = protection_source.get("physicalProtectionSource", {})
                        
                        # If this has a specific hostname/IP, it's likely an actual server
                        hostname = physical_source.get("name", "")
                        if hostname and hostname != "Physical Servers" and hostname != protection_source.get("name", ""):
                            server = PhysicalServer(
                                id=protection_source.get("id"),
                                name=protection_source.get("name", ""),
                                hostname=hostname,
                                ip_address=physical_source.get("networkingInfo", {}).get("resourceVec", [{}])[0].get("ip", ""),
                                os_type=physical_source.get("osType", "Linux"),
                                agent_version=physical_source.get("agents", [{}])[0].get("version", ""),
                                connection_status=source.get("connectionStatus", "")
                            )
                            servers.append(server)
                        
                        # Also check for any child nodes
                        if "nodes" in source:
                            extract_servers_from_hierarchy(source["nodes"], level + 1)
                    
                    # Check for child nodes even if this isn't a physical source
                    elif "nodes" in source:
                        extract_servers_from_hierarchy(source["nodes"], level + 1)
        
        # Handle both list and dict responses
        if isinstance(protection_sources, list):
            extract_servers_from_hierarchy(protection_sources)
        elif isinstance(protection_sources, dict) and "protectionSources" in protection_sources:
            extract_servers_from_hierarchy(protection_sources["protectionSources"])
        
        logger.info(f"Found {len(servers)} physical servers")
        return servers
    
    def get_server_backup_status(self, server: PhysicalServer) -> Dict:
        """Get backup status for a specific physical server."""
        # This would typically involve checking protection job runs
        # for jobs that include this server
        status = {
            "server_name": server.name,
            "last_successful_backup": None,
            "last_failed_backup": None,
            "backup_size": 0,
            "status": "Unknown"
        }
        
        # Implementation would check recent job runs
        # This is a placeholder for the actual implementation
        return status


class ProtectionJobManager:
    """Manager for Protection Jobs."""
    
    def __init__(self, api_client: CohesityAPIClient):
        self.api_client = api_client
        
    def get_protection_jobs(self) -> List[ProtectionJob]:
        """Get all physical protection jobs."""
        jobs = []
        job_data = self.api_client.get_protection_jobs("kPhysical")
        
        # Handle different API response formats
        if isinstance(job_data, dict) and "protectionGroups" in job_data:
            # v2 API format: {"protectionGroups": [...]}
            protection_groups = job_data["protectionGroups"]
        elif isinstance(job_data, list):
            # Direct list format
            protection_groups = job_data
        else:
            logger.warning(f"Unexpected API response format: {type(job_data)}")
            protection_groups = []
        
        logger.debug(f"Found {len(protection_groups)} protection groups from API")
        
        for job in protection_groups:
            # Handle different response formats
            if isinstance(job, str):
                # If job is a string, create a basic protection job
                protection_job = ProtectionJob(
                    name=job,
                    environment="kPhysical"
                )
            elif isinstance(job, dict):
                # If job is a dictionary, extract all available fields
                # Extract protected servers from physicalParams
                protected_servers = []
                backup_paths = []
                if job.get("physicalParams", {}).get("fileProtectionTypeParams", {}).get("objects"):
                    for obj in job["physicalParams"]["fileProtectionTypeParams"]["objects"]:
                        protected_servers.append(obj.get("name", ""))
                        for file_path in obj.get("filePaths", []):
                            if file_path.get("includedPath"):
                                backup_paths.append(file_path["includedPath"])
                
                protection_job = ProtectionJob(
                    id=job.get("id"),
                    name=job.get("name", ""),
                    description=job.get("description", ""),
                    policy_name=job.get("policyId", ""),  # v2 API uses policyId
                    environment=job.get("environment", ""),
                    status=job.get("lastProtectionRunStatus", "Active" if job.get("isActive") else "Inactive"),
                    last_run=job.get("lastProtectionRunTimeUsecs"),
                    next_run=job.get("nextProtectionRunTimeUsecs")
                )
                
                # Add the extracted server and path information
                protection_job.backup_paths = backup_paths
                # Store server names in the sources list (simplified)
                for server_name in protected_servers:
                    server_obj = PhysicalServer(name=server_name)
                    protection_job.sources.append(server_obj)
            else:
                logger.warning(f"Unexpected job data type: {type(job)}, value: {job}")
                continue
            
            # Convert timestamps if present
            if protection_job.last_run:
                protection_job.last_run = datetime.fromtimestamp(
                    int(protection_job.last_run) / 1000000
                ).strftime("%Y-%m-%d %H:%M:%S")
                
            if protection_job.next_run:
                protection_job.next_run = datetime.fromtimestamp(
                    int(protection_job.next_run) / 1000000
                ).strftime("%Y-%m-%d %H:%M:%S")
            
            jobs.append(protection_job)
            
        logger.info(f"Found {len(jobs)} protection jobs")
        return jobs
    
    def get_job_details(self, job_id: int) -> Dict:
        """Get detailed information about a specific protection job."""
        runs = self.api_client.get_protection_job_runs(job_id)
        
        details = {
            "job_id": job_id,
            "recent_runs": [],
            "success_rate": 0,
            "avg_backup_size": 0
        }
        
        if runs:
            successful_runs = 0
            total_size = 0
            
            for run in runs:
                # Handle different run data formats
                if isinstance(run, str):
                    # Basic run info if it's just a string
                    run_info = {
                        "start_time": "Unknown",
                        "status": run,
                        "duration": 0
                    }
                elif isinstance(run, dict):
                    # Handle both v1 and v2 API formats
                    backup_run = run.get("backupRun", run)  # v1 might not have backupRun wrapper
                    stats = backup_run.get("stats", {})
                    
                    # Try different timestamp fields
                    start_time_usecs = (
                        stats.get("startTimeUsecs") or 
                        backup_run.get("startTimeUsecs") or
                        run.get("startTimeUsecs") or
                        0
                    )
                    
                    # Try different status fields
                    status = (
                        backup_run.get("status") or
                        run.get("status") or
                        "Unknown"
                    )
                    
                    run_info = {
                        "start_time": datetime.fromtimestamp(
                            int(start_time_usecs) / 1000000
                        ).strftime("%Y-%m-%d %H:%M:%S") if start_time_usecs else "Unknown",
                        "status": status,
                        "duration": stats.get("totalBytesReadFromSource", 0)
                    }
                    
                    if run_info["status"] == "kSuccess":
                        successful_runs += 1
                        
                    total_size += stats.get("totalBytesReadFromSource", 0)
                else:
                    logger.debug(f"Unexpected run data type: {type(run)}")
                    continue
                
                details["recent_runs"].append(run_info)
            
            details["success_rate"] = (successful_runs / len(runs)) * 100
            details["avg_backup_size"] = total_size / len(runs) if runs else 0
            
        return details


class DataProtectionManager:
    """Main Data Protection Manager class."""
    
    def __init__(self, cluster: CohesityCluster):
        self.cluster = cluster
        self.api_client = CohesityAPIClient(cluster)
        self.server_manager = PhysicalServerManager(self.api_client)
        self.job_manager = ProtectionJobManager(self.api_client)
        
    def connect(self) -> bool:
        """Connect and authenticate to the Cohesity cluster."""
        return self.api_client.authenticate()
    
    def generate_server_report(self) -> Dict:
        """Generate a comprehensive report of physical servers."""
        servers = self.server_manager.get_physical_servers()
        
        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_servers": len(servers),
            "servers": []
        }
        
        for server in servers:
            server_info = asdict(server)
            backup_status = self.server_manager.get_server_backup_status(server)
            server_info.update(backup_status)
            report["servers"].append(server_info)
            
        return report
    
    def generate_job_report(self) -> Dict:
        """Generate a comprehensive report of protection jobs."""
        jobs = self.job_manager.get_protection_jobs()
        
        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_jobs": len(jobs),
            "jobs": []
        }
        
        for job in jobs:
            job_info = asdict(job)
            if job.id:
                job_details = self.job_manager.get_job_details(job.id)
                job_info.update(job_details)
            report["jobs"].append(job_info)
            
        return report
    
    def export_report(self, report: Dict, filename: str):
        """Export report to JSON file."""
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Report exported to {filename}")
        except Exception as e:
            logger.error(f"Failed to export report: {e}")
    
    def generate_feature_flags_report(self) -> Dict:
        """Generate a report of cluster feature flags."""
        feature_flags = self.api_client.get_feature_flags()
        
        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cluster": self.cluster.hostname,
            "feature_flags": feature_flags
        }
        
        return report


def main():
    """Main function to run the Data Protection Manager."""
    parser = argparse.ArgumentParser(description="Cohesity Data Protection Manager v0.1")
    parser.add_argument("-s", "--server", required=True, help="Cohesity cluster hostname/IP")
    parser.add_argument("-u", "--username", required=True, help="Username for authentication")
    parser.add_argument("-p", "--password", help="Password for authentication (RECOMMENDED: omit to use secure prompt)")
    parser.add_argument("-d", "--domain", default="LOCAL", help="Authentication domain")
    parser.add_argument("-a", "--action", choices=["servers", "jobs", "both"], default="both",
                       help="Generate report for servers, jobs, or both")
    parser.add_argument("-o", "--output", help="Output filename (default: timestamp-based)")
    parser.add_argument("-g", "--gflags", action="store_true", 
                       help="Get and display cluster feature flags")
    
    args = parser.parse_args()
    
    # Handle password - prompt if not provided
    password = args.password
    
    if not password:
        try:
            password = getpass.getpass(f"Enter password for {args.username}@{args.server}: ")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error reading password: {e}")
            sys.exit(1)
        
        if not password:
            logger.error("Password cannot be empty")
            sys.exit(1)
    
    # Create cluster configuration
    cluster = CohesityCluster(
        hostname=args.server,
        username=args.username,
        password=password,
        domain=args.domain
    )
    
    # Initialize Data Protection Manager
    dp_manager = DataProtectionManager(cluster)
    
    # Connect to cluster with retry capability for password issues
    connected = False
    
    if not dp_manager.connect():
        logger.error("Failed to connect to Cohesity cluster")
        
        # Provide helpful guidance for password issues
        if args.password and ('!' in args.password or '$' in args.password or '`' in args.password):
            print("\n💡 TROUBLESHOOTING TIP:")
            print("   Your password contains special characters that may have been modified by the shell.")
            print("   Try one of these solutions:")
            print("   1. Use secure prompt: omit -p and enter password when prompted")
            print("   2. Quote your password: -p 'your!password'")
            print("   3. Escape special chars: -p your\\!password")
            print()
            print("   For security and convenience, the secure prompt method is recommended.")
            print()
            
            # Offer to retry with secure prompt
            try:
                response = input("   Would you like to retry with secure password prompt? (y/n): ").lower().strip()
                if response in ['y', 'yes']:
                    try:
                        retry_password = getpass.getpass(f"Enter password for {args.username}@{args.server}: ")
                        if retry_password:
                            # Update the cluster configuration with new password
                            cluster.password = retry_password
                            dp_manager = DataProtectionManager(cluster)
                            if dp_manager.connect():
                                print("✅ Authentication successful with corrected password!")
                                connected = True
                            else:
                                print("❌ Authentication still failed. Please check your credentials.")
                                sys.exit(1)
                        else:
                            print("No password entered.")
                            sys.exit(1)
                    except KeyboardInterrupt:
                        print("\nOperation cancelled.")
                        sys.exit(1)
                else:
                    sys.exit(1)
            except KeyboardInterrupt:
                print("\nOperation cancelled.")
                sys.exit(1)
        
        if not connected:
            sys.exit(1)
    else:
        connected = True
    
    # Handle feature flags request
    if args.gflags:
        logger.info("Generating feature flags report...")
        flags_report = dp_manager.generate_feature_flags_report()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = args.output or f"feature_flags_{timestamp}.json"
        dp_manager.export_report(flags_report, output_file)
        
        print(f"\nCluster Feature Flags:")
        print(f"Cluster: {flags_report['cluster']}")
        
        # Handle both dict and list format from API
        feature_flags = flags_report.get('feature_flags', {})
        if isinstance(feature_flags, dict):
            print(f"Total flags: {len(feature_flags)}")
            if feature_flags:
                print("\nKey Feature Flags:")
                for flag_name, flag_value in list(feature_flags.items())[:10]:
                    print(f"  - {flag_name}: {flag_value}")
                if len(feature_flags) > 10:
                    print(f"  ... and {len(feature_flags) - 10} more flags")
        elif isinstance(feature_flags, list):
            print(f"Total flags: {len(feature_flags)}")
            if feature_flags:
                print("\nFeature Flags:")
                for i, flag in enumerate(feature_flags[:10]):
                    if isinstance(flag, dict):
                        flag_name = flag.get('name', f'flag_{i}')
                        flag_enabled = flag.get('enabled', flag.get('isApproved', 'unknown'))
                        flag_ui = flag.get('isUiFeature', False)
                        flag_reason = flag.get('reason', '')
                        
                        status = "✓" if flag_enabled else "✗" if flag_enabled is False else "?"
                        ui_indicator = " [UI]" if flag_ui else ""
                        reason_text = f" - {flag_reason}" if flag_reason else ""
                        
                        print(f"  {status} {flag_name}{ui_indicator}{reason_text}")
                    else:
                        print(f"  - {flag}")
                if len(feature_flags) > 10:
                    print(f"  ... and {len(feature_flags) - 10} more flags")
        else:
            print("No feature flags data available or unexpected format")
        
        logger.info("Feature flags report completed successfully")
        return
    
    # Generate reports based on action
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if args.action in ["servers", "both"]:
        logger.info("Generating server report...")
        server_report = dp_manager.generate_server_report()
        
        output_file = args.output or f"server_report_{timestamp}.json"
        dp_manager.export_report(server_report, output_file)
        
        print(f"\nServer Summary:")
        print(f"Total Servers: {server_report['total_servers']}")
        for server in server_report['servers'][:5]:  # Show first 5
            print(f"  - {server['name']} ({server['hostname']}) - {server['connection_status']}")
        if len(server_report['servers']) > 5:
            print(f"  ... and {len(server_report['servers']) - 5} more")
    
    if args.action in ["jobs", "both"]:
        logger.info("Generating job report...")
        job_report = dp_manager.generate_job_report()
        
        output_file = args.output or f"job_report_{timestamp}.json"
        if args.action == "both":
            output_file = args.output or f"dp_report_{timestamp}.json"
            combined_report = {
                "servers": server_report,
                "jobs": job_report
            }
            dp_manager.export_report(combined_report, output_file)
        else:
            dp_manager.export_report(job_report, output_file)
        
        print(f"\nJob Summary:")
        print(f"Total Protection Jobs: {job_report['total_jobs']}")
        print("=" * 80)
        
        for i, job in enumerate(job_report['jobs'][:10], 1):  # Show first 10
            print(f"\n📋 Job #{i}: {job['name']}")
            print(f"   ID: {job['id']}")
            print(f"   Status: {job['status'] or 'Active'}")
            print(f"   Environment: {job['environment']}")
            
            # Show protected servers
            if job['sources']:
                servers = [s['name'] for s in job['sources'] if s['name']]
                if servers:
                    print(f"   Protected Servers: {', '.join(servers)}")
            
            # Show backup paths
            if job['backup_paths']:
                print(f"   Backup Paths: {', '.join(job['backup_paths'][:3])}")
                if len(job['backup_paths']) > 3:
                    print(f"                 ... and {len(job['backup_paths']) - 3} more paths")
            
            # Show timing information
            if job.get('last_run'):
                print(f"   Last Run: {job['last_run']}")
            if job.get('next_run'):
                print(f"   Next Run: {job['next_run']}")
            
            # Show recent run statistics if available
            if job.get('recent_runs'):
                recent_runs = job['recent_runs'][:3]
                print(f"   Recent Runs:")
                for run in recent_runs:
                    status_icon = "✅" if run['status'] == "kSuccess" else "❌" if run['status'] == "kFailure" else "⏳"
                    print(f"     {status_icon} {run['start_time']} - {run['status']}")
                
                if job.get('success_rate') is not None:
                    print(f"   Success Rate: {job['success_rate']:.1f}%")
            
            print("-" * 80)
        
        if len(job_report['jobs']) > 10:
            print(f"\n... and {len(job_report['jobs']) - 10} more jobs (see JSON report for full details)")
    
    logger.info("Data Protection Manager completed successfully")


if __name__ == "__main__":
    main()
