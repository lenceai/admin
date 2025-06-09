#!/usr/bin/env python3
"""
Cohesity Cluster Management Script

This script provides functionality to manage multiple Cohesity clusters, including:
- Upgrading clusters to the latest version of Cohesity software
- Creating protection groups for Linux file systems across multiple clusters
- Reading configuration from CSV files

Usage examples:
  python cohesity_manager.py --clusters data/clusters.csv --upgrade
  python cohesity_manager.py --clusters data/clusters.csv --create-protection-groups data/filesets.csv

Requires: cohesity_sdk, pandas, numpy
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
import pandas as pd
import numpy as np  # Add NumPy import

# Ensure data and output directories exist
os.makedirs("data", exist_ok=True)
os.makedirs("output", exist_ok=True)
os.makedirs("output/logs", exist_ok=True)

# Import Cohesity SDK
try:
    from cohesity_sdk.cluster.cluster_client import ClusterClient
    COHESITY_SDK_AVAILABLE = True
except ImportError:
    COHESITY_SDK_AVAILABLE = False
    print("Error: cohesity_sdk module not found.")
    print("Please install it using: pip install cohesity_sdk")

# Set up logging
LOG_FILE = os.path.join("output", "logs", f"cohesity_manager_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)

logger = logging.getLogger(__name__)

# Helper function for handling API responses
def _parse_api_response(response):
    """Parse API response to handle different SDK versions
    
    Args:
        response: API response object
        
    Returns:
        dict or None: Parsed JSON data or None if parsing failed
    """
    if response is None:
        return None
    
    try:
        # Modern SDK with json() method
        if hasattr(response, 'json'):
            try:
                return response.json()
            except Exception:
                pass
                
        # Parse JSON from text
        if hasattr(response, 'text') and response.text:
            try:
                return json.loads(response.text)
            except json.JSONDecodeError:
                pass
                
        # Last resort - if response itself is a dict or list
        if isinstance(response, (dict, list)):
            return response
                
        return None
    except Exception as e:
        logger.debug(f"Error parsing API response: {str(e)}")
        return None

class CohesityManager:
    """Main class for managing Cohesity clusters"""
    
    def __init__(self, debug=False, verbose_debug=False):
        """Initialize the manager"""
        self.debug = debug
        self.verbose_debug = verbose_debug
        self.clusters = []
        self.cluster_clients = {}
        
        if self.debug:
            logger.setLevel(logging.DEBUG)
        
        if self.verbose_debug:
            # Enable verbose debug for API calls
            import http.client as http_client
            http_client.HTTPConnection.debuglevel = 1
            requests_log = logging.getLogger("requests.packages.urllib3")
            requests_log.setLevel(logging.DEBUG)
            requests_log.propagate = True
            
        if not COHESITY_SDK_AVAILABLE:
            logger.error("Cohesity SDK not available. Please install it using 'pip install cohesity_sdk'")
            sys.exit(1)
            
        logger.debug("CohesityManager initialized")
    
    def load_clusters_from_csv(self, csv_file_path):
        """Load cluster information from a CSV file
        
        Args:
            csv_file_path (str): Path to CSV file with cluster information
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read CSV file with pandas for better handling of different formats
            df = pd.read_csv(csv_file_path)
            
            # Check for required columns
            required_columns = ['cluster_ip', 'username', 'password']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Missing required columns in CSV: {', '.join(missing_columns)}")
                logger.error(f"CSV file must contain columns: {', '.join(required_columns)}")
                return False
            
            # Convert DataFrame to list of dictionaries
            self.clusters = df.to_dict('records')
            
            # Validate cluster data
            valid_clusters = []
            for cluster in self.clusters:
                if not cluster.get('cluster_ip') or not cluster.get('username') or not cluster.get('password'):
                    logger.warning(f"Skipping invalid cluster configuration: {cluster.get('cluster_ip', 'unknown')}")
                    continue
                
                # Add domain if not present
                if 'domain' not in cluster or pd.isna(cluster['domain']):
                    cluster['domain'] = "LOCAL"  # Use LOCAL as default domain
                
                valid_clusters.append(cluster)
            
            self.clusters = valid_clusters
            logger.info(f"Loaded {len(self.clusters)} valid clusters from {csv_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading clusters from CSV: {str(e)}")
            return False
    
    def connect_to_clusters(self):
        """Connect to all loaded clusters
        
        Returns:
            int: Number of successful connections
        """
        successful_connections = 0
        
        for cluster in self.clusters:
            cluster_ip = cluster['cluster_ip']
            try:
                logger.info(f"Connecting to cluster {cluster_ip}...")
                
                # Create ClusterClient instance
                try:
                    client = ClusterClient(
                        cluster_vip=cluster_ip,
                        username=cluster['username'],
                        password=cluster['password'],
                        domain=cluster['domain']
                    )
                    
                    # Log available attributes for debugging
                    if self.debug:
                        logger.debug(f"Client attributes: {dir(client)}")
                    
                    # Verify connection by getting cluster info
                    # Check if we can access cluster information
                    # The SDK might have different structures in different versions
                    cluster_info = None
                    cluster_version = "Unknown"
                    cluster_name = f"Cluster-{cluster_ip}"
                    cluster_id = "Unknown"
                    
                    # Try different API patterns to find the right one
                    if hasattr(client, 'platform'):
                        # Try the pattern used in our original code
                        try:
                            if self.debug:
                                logger.debug(f"Trying platform API: {dir(client.platform)}")
                            cluster_info = client.platform.get_cluster()
                            cluster_version = cluster_info.sw_version if hasattr(cluster_info, 'sw_version') else "Unknown"
                            cluster_name = cluster_info.name if hasattr(cluster_info, 'name') else f"Cluster-{cluster_ip}"
                            cluster_id = cluster_info.id if hasattr(cluster_info, 'id') else "Unknown"
                            logger.debug(f"Connected using client.platform.get_cluster() method")
                        except Exception as e:
                            logger.debug(f"Error with platform.get_cluster(): {str(e)}")
                    
                    # Try alternative pattern (common in some SDK versions)
                    if not cluster_info and hasattr(client, 'cluster'):
                        try:
                            if self.debug:
                                logger.debug(f"Trying cluster API: {dir(client.cluster)}")
                            cluster_info = client.cluster.get_basic_cluster_info()
                            cluster_version = cluster_info.cluster_software_version if hasattr(cluster_info, 'cluster_software_version') else "Unknown"
                            cluster_name = cluster_info.name if hasattr(cluster_info, 'name') else f"Cluster-{cluster_ip}"
                            cluster_id = cluster_info.id if hasattr(cluster_info, 'id') else "Unknown"
                            logger.debug(f"Connected using client.cluster.get_basic_cluster_info() method")
                        except Exception as e:
                            logger.debug(f"Error with cluster.get_basic_cluster_info(): {str(e)}")
                    
                    # Try another pattern
                    if not cluster_info and hasattr(client, 'cluster_info'):
                        try:
                            if self.debug:
                                logger.debug(f"Trying cluster_info API: {dir(client.cluster_info)}")
                            cluster_info = client.cluster_info.get()
                            cluster_version = cluster_info.version if hasattr(cluster_info, 'version') else "Unknown"
                            cluster_name = cluster_info.name if hasattr(cluster_info, 'name') else f"Cluster-{cluster_ip}"
                            cluster_id = cluster_info.id if hasattr(cluster_info, 'id') else "Unknown"
                            logger.debug(f"Connected using client.cluster_info.get() method")
                        except Exception as e:
                            logger.debug(f"Error with cluster_info.get(): {str(e)}")
                    
                    # If we still don't have cluster info, try a raw API call
                    if not cluster_info:
                        try:
                            # Use a direct API call method if available
                            if hasattr(client, 'get'):
                                logger.debug(f"Trying direct API call")
                                response = client.get('/irisservices/api/v1/public/basicClusterInfo')
                                
                                # Handle different response patterns
                                data = None
                                if hasattr(response, 'json'):
                                    # Modern SDK with json() method
                                    data = response.json()
                                elif hasattr(response, 'text'):
                                    # Parse JSON from text
                                    import json
                                    try:
                                        data = json.loads(response.text)
                                    except json.JSONDecodeError:
                                        data = None
                                
                                if data:
                                    cluster_version = data.get('clusterSoftwareVersion', 'Unknown')
                                    cluster_name = data.get('name', f"Cluster-{cluster_ip}")
                                    cluster_id = data.get('id', 'Unknown')
                                    logger.debug(f"Connected using direct API call to /irisservices/api/v1/public/basicClusterInfo")
                                else:
                                    logger.debug(f"Direct API call returned: {response}")
                        except Exception as e:
                            logger.debug(f"Error with direct API call: {str(e)}")
                    
                    # Try more API paths if we still don't have info
                    if self.debug and cluster_version == "Unknown":
                        logger.debug("Testing additional API paths for version discovery...")
                        self._test_api_endpoints(client, cluster_ip)
                    
                    # Store client in dictionary for later use
                    self.cluster_clients[cluster_ip] = {
                        'client': client,
                        'info': {
                            'name': cluster_name,
                            'id': cluster_id,
                            'version': cluster_version
                        },
                        'api_structure': {
                            'has_platform': hasattr(client, 'platform'),
                            'has_cluster': hasattr(client, 'cluster'),
                            'has_cluster_info': hasattr(client, 'cluster_info'),
                            'has_get': hasattr(client, 'get'),
                            'has_post': hasattr(client, 'post')
                        }
                    }
                    
                    logger.info(f"Successfully connected to {cluster_ip}, name: {cluster_name}, version: {cluster_version}")
                    successful_connections += 1
                except Exception as client_error:
                    logger.error(f"Error creating client for {cluster_ip}: {str(client_error)}")
                    raise  # Re-raise to be caught by the outer exception handler
                
            except Exception as e:
                logger.error(f"Failed to connect to cluster {cluster_ip}: {str(e)}")
        
        logger.info(f"Successfully connected to {successful_connections}/{len(self.clusters)} clusters")
        return successful_connections
    
    def _test_api_endpoints(self, client, cluster_ip):
        """Test various API endpoints to discover the API structure
        
        Args:
            client: The Cohesity SDK client
            cluster_ip (str): IP address of the cluster
        """
        logger.debug(f"Testing API endpoints for {cluster_ip}...")
        
        # Test platform API
        if hasattr(client, 'platform'):
            try:
                cluster_info = client.platform.get_cluster()
                logger.debug(f"Platform API: SUCCESS - Cluster info retrieved")
                if hasattr(cluster_info, 'sw_version'):
                    logger.debug(f"  Cluster version: {cluster_info.sw_version}")
            except Exception as e:
                logger.debug(f"Platform API: ERROR - {str(e)}")
        
        # Test cluster API
        if hasattr(client, 'cluster'):
            try:
                cluster_info = client.cluster.get_basic_cluster_info()
                logger.debug(f"Cluster API: SUCCESS - Basic cluster info retrieved")
                if hasattr(cluster_info, 'cluster_software_version'):
                    logger.debug(f"  Cluster version: {cluster_info.cluster_software_version}")
            except Exception as e:
                logger.debug(f"Cluster API: ERROR - {str(e)}")
        
        # Test stats API
        if hasattr(client, 'stats'):
            try:
                stats = client.stats.get_cluster_stats()
                logger.debug(f"Stats API: SUCCESS - Cluster stats retrieved")
            except Exception as e:
                logger.debug(f"Stats API: ERROR - {str(e)}")
        
        # Test protection group API
        if hasattr(client, 'protection_group'):
            try:
                groups = client.protection_group.get_protection_groups()
                logger.debug(f"Protection Group API: SUCCESS - Retrieved {len(groups) if groups else 0} groups")
            except Exception as e:
                logger.debug(f"Protection Group API: ERROR - {str(e)}")
        
        # Test available methods for each API
        for attr_name in ['platform', 'cluster', 'stats', 'protection_group']:
            if hasattr(client, attr_name):
                attr = getattr(client, attr_name)
                methods = [m for m in dir(attr) if not m.startswith('_') and callable(getattr(attr, m))]
                logger.debug(f"Available methods for client.{attr_name}: {methods}")
    
    def get_upgrade_status(self, cluster_ip):
        """Check upgrade status for a cluster
        
        Args:
            cluster_ip (str): IP address of the cluster
            
        Returns:
            dict: Upgrade status information or None if error
        """
        if cluster_ip not in self.cluster_clients:
            logger.error(f"No connection to cluster {cluster_ip}")
            return None
        
        try:
            client = self.cluster_clients[cluster_ip]['client']
            api_structure = self.cluster_clients[cluster_ip]['api_structure']
            
            # Try different API patterns
            upgrade_status = None
            
            # Try platform API
            if api_structure.get('has_platform'):
                try:
                    upgrade_status = client.platform.get_upgrade_status()
                    logger.debug(f"Got upgrade status using platform API")
                    return upgrade_status
                except Exception as e:
                    logger.debug(f"Error getting upgrade status using platform API: {str(e)}")
            
            # Try cluster API if platform didn't work
            if upgrade_status is None and api_structure.get('has_cluster'):
                try:
                    upgrade_status = client.cluster.get_upgrade_status()
                    logger.debug(f"Got upgrade status using cluster API")
                    return upgrade_status
                except Exception as e:
                    logger.debug(f"Error getting upgrade status using cluster API: {str(e)}")
            
            # Try direct API call as last resort
            if upgrade_status is None and hasattr(client, 'get'):
                try:
                    response = client.get('/irisservices/api/v1/public/clusters/software/upgrade/status')
                    # Handle different response patterns
                    if hasattr(response, 'json'):
                        # Modern SDK with json() method
                        data = response.json()
                    elif hasattr(response, 'text'):
                        # Parse JSON from text
                        import json
                        data = json.loads(response.text)
                    else:
                        data = None
                        
                    if data:
                        # Create a simple status object
                        class UpgradeStatus:
                            def __init__(self, status_data):
                                self.in_progress = status_data.get('inProgress', False)
                                self.status = status_data.get('status', 'Unknown')
                                self.target_version = status_data.get('targetVersion', 'Unknown')
                                self.error_message = status_data.get('errorMsg', '')
                        
                        upgrade_status = UpgradeStatus(data)
                        logger.debug(f"Got upgrade status using direct API call")
                        return upgrade_status
                except Exception as e:
                    logger.debug(f"Error getting upgrade status using direct API: {str(e)}")
            
            logger.warning(f"Unable to get upgrade status for {cluster_ip} using available API methods")
            return None
            
        except Exception as e:
            logger.error(f"Error getting upgrade status for {cluster_ip}: {str(e)}")
            return None
    
    def start_cluster_upgrade(self, cluster_ip, version=None):
        """Start upgrade on a specific cluster
        
        Args:
            cluster_ip (str): IP address of the cluster
            version (str, optional): Version to upgrade to. If None, uses latest available.
            
        Returns:
            bool: True if upgrade started successfully, False otherwise
        """
        if cluster_ip not in self.cluster_clients:
            logger.error(f"No connection to cluster {cluster_ip}")
            return False
        
        try:
            client = self.cluster_clients[cluster_ip]['client']
            current_version = self.cluster_clients[cluster_ip]['info']['version']
            api_structure = self.cluster_clients[cluster_ip]['api_structure']
            
            # Check available software packages using different API patterns
            available_packages = None
            
            # Try using platform API
            if api_structure.get('has_platform'):
                try:
                    available_packages = client.platform.get_available_software_packages()
                    logger.debug(f"Got available packages using platform API")
                except Exception as e:
                    logger.debug(f"Error getting available packages using platform API: {str(e)}")
            
            # Try cluster API if platform didn't work
            if available_packages is None and api_structure.get('has_cluster'):
                try:
                    available_packages = client.cluster.get_available_software_packages()
                    logger.debug(f"Got available packages using cluster API")
                except Exception as e:
                    logger.debug(f"Error getting available packages using cluster API: {str(e)}")
            
            # Try direct API call as last resort
            if available_packages is None and hasattr(client, 'get'):
                try:
                    response = client.get('/irisservices/api/v1/public/packages/available')
                    
                    # Handle different response patterns
                    data = None
                    if hasattr(response, 'json'):
                        # Modern SDK with json() method
                        data = response.json()
                    elif hasattr(response, 'text'):
                        # Parse JSON from text
                        import json
                        try:
                            data = json.loads(response.text)
                        except json.JSONDecodeError:
                            data = None
                    
                    if data and isinstance(data, list):
                        # Create package objects similar to SDK output
                        available_packages = []
                        for pkg in data:
                            # Create a simple object with expected attributes
                            class Package:
                                def __init__(self, pkg_data):
                                    self.id = pkg_data.get('id')
                                    self.version = pkg_data.get('version')
                                    self.release_date = pkg_data.get('releaseDate')
                            
                            available_packages.append(Package(pkg))
                        logger.debug(f"Got available packages using direct API call")
                except Exception as e:
                    logger.debug(f"Error getting available packages using direct API: {str(e)}")
            
            if not available_packages:
                logger.error(f"No upgrade packages available for {cluster_ip}")
                return False
            
            # Sort packages by release date to find the latest
            if hasattr(available_packages[0], 'release_date'):
                available_packages.sort(key=lambda x: x.release_date, reverse=True)
            
            # If version is specified, find that specific package
            target_package = None
            if version:
                for package in available_packages:
                    if package.version == version:
                        target_package = package
                        break
                
                if not target_package:
                    logger.error(f"Version {version} not found in available packages for {cluster_ip}")
                    return False
            else:
                # Use the latest package
                target_package = available_packages[0]
            
            # Compare versions to see if upgrade is needed
            if current_version == target_package.version:
                logger.info(f"Cluster {cluster_ip} is already on version {current_version}")
                return True
            
            logger.info(f"Starting upgrade on {cluster_ip} from {current_version} to {target_package.version}")
            
            # Start upgrade based on available API patterns
            upgrade_result = None
            
            # Try platform API
            if api_structure.get('has_platform'):
                try:
                    upgrade_result = client.platform.create_cluster_software_upgrade(
                        package_id=target_package.id
                    )
                    logger.debug(f"Started upgrade using platform API")
                except Exception as e:
                    logger.debug(f"Error starting upgrade using platform API: {str(e)}")
            
            # Try cluster API if platform didn't work
            if upgrade_result is None and api_structure.get('has_cluster'):
                try:
                    upgrade_result = client.cluster.create_software_upgrade(
                        package_id=target_package.id
                    )
                    logger.debug(f"Started upgrade using cluster API")
                except Exception as e:
                    logger.debug(f"Error starting upgrade using cluster API: {str(e)}")
            
            # Try direct API call as last resort
            if upgrade_result is None and hasattr(client, 'post'):
                try:
                    upgrade_data = {"packageId": target_package.id}
                    response = client.post('/irisservices/api/v1/public/clusters/software/upgrade', 
                                         body=upgrade_data)
                    
                    # Check if successful
                    success = False
                    if hasattr(response, 'status_code'):
                        success = response.status_code in [200, 201, 202]
                    elif hasattr(response, 'status'):
                        success = response.status in [200, 201, 202]
                    else:
                        success = True  # Assume success if we can't determine status
                    
                    if success:
                        # Create a simple result object
                        class UpgradeResult:
                            def __init__(self, task_id):
                                self.task_id = task_id
                        
                        # Try to extract task ID from response
                        task_id = "Unknown"
                        
                        # Handle different response patterns
                        data = None
                        if hasattr(response, 'json'):
                            # Modern SDK with json() method
                            data = response.json()
                        elif hasattr(response, 'text'):
                            # Parse JSON from text
                            import json
                            try:
                                data = json.loads(response.text)
                            except json.JSONDecodeError:
                                data = None
                        
                        if data:
                            task_id = data.get('taskId', 'Unknown')
                        
                        upgrade_result = UpgradeResult(task_id)
                        logger.debug(f"Started upgrade using direct API call")
                except Exception as e:
                    logger.debug(f"Error starting upgrade using direct API: {str(e)}")
            
            if upgrade_result:
                task_id = getattr(upgrade_result, 'task_id', 'Unknown')
                logger.info(f"Upgrade initiated on {cluster_ip}. Task ID: {task_id}")
                return True
            else:
                logger.error(f"Failed to start upgrade on {cluster_ip} using available API methods")
                return False
            
        except Exception as e:
            logger.error(f"Error starting upgrade on {cluster_ip}: {str(e)}")
            return False
    
    def upgrade_all_clusters(self, version=None):
        """Upgrade all connected clusters
        
        Args:
            version (str, optional): Version to upgrade to. If None, uses latest available.
            
        Returns:
            dict: Results of upgrade attempts
        """
        if not self.cluster_clients:
            logger.error("No connected clusters")
            return {"success": 0, "failed": 0, "skipped": 0}
        
        results = {"success": 0, "failed": 0, "skipped": 0}
        
        for cluster_ip in self.cluster_clients:
            logger.info(f"Processing upgrade for cluster {cluster_ip}")
            
            # Check current status before attempting upgrade
            status = self.get_upgrade_status(cluster_ip)
            
            if status and status.in_progress:
                logger.info(f"Upgrade already in progress for {cluster_ip}. Skipping.")
                results["skipped"] += 1
                continue
            
            # Start the upgrade
            success = self.start_cluster_upgrade(cluster_ip, version)
            
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1
        
        logger.info(f"Upgrade summary: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped")
        return results
    
    def load_filesets_from_csv(self, csv_file_path):
        """Load fileset information from a CSV file
        
        Args:
            csv_file_path (str): Path to CSV file with fileset information
            
        Returns:
            list: List of fileset configurations
        """
        try:
            # Read CSV file with pandas for better handling of different formats
            df = pd.read_csv(csv_file_path)
            
            # Check for required columns
            required_columns = ['server_name', 'path']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Missing required columns in CSV: {', '.join(missing_columns)}")
                logger.error(f"CSV file must contain columns: {', '.join(required_columns)}")
                return []
            
            # Group by server name to organize paths by server
            filesets = []
            grouped = df.groupby('server_name')
            
            for server_name, group in grouped:
                fileset = {
                    'server_name': server_name,
                    'paths': group['path'].tolist()
                }
                
                # Add optional include/exclude patterns if present in the CSV
                if 'include_patterns' in group.columns:
                    includes = group['include_patterns'].dropna().tolist()
                    if includes:
                        fileset['include_patterns'] = includes
                
                if 'exclude_patterns' in group.columns:
                    excludes = group['exclude_patterns'].dropna().tolist()
                    if excludes:
                        fileset['exclude_patterns'] = excludes
                
                filesets.append(fileset)
            
            logger.info(f"Loaded {len(filesets)} filesets from {csv_file_path}")
            return filesets
            
        except Exception as e:
            logger.error(f"Error loading filesets from CSV: {str(e)}")
            return []
    
    def create_protection_group(self, cluster_ip, group_name, policy_name, filesets, description=None):
        """Create a protection group for Linux physical servers
        
        Args:
            cluster_ip (str): IP address of the cluster
            group_name (str): Name for the protection group
            policy_name (str): Name of the protection policy to use
            filesets (list): List of fileset configurations
            description (str, optional): Description for the protection group
            
        Returns:
            bool: True if successful, False otherwise
        """
        if cluster_ip not in self.cluster_clients:
            logger.error(f"No connection to cluster {cluster_ip}")
            return False
        
        try:
            client = self.cluster_clients[cluster_ip]['client']
            api_structure = self.cluster_clients[cluster_ip]['api_structure']
            
            # Verify policy exists using different API patterns
            policies = None
            policy_id = None
            
            # Try platform API first
            if api_structure.get('has_platform') and hasattr(client.platform, 'protection_policy'):
                try:
                    policies = client.platform.protection_policy.get_protection_policies()
                    logger.debug(f"Got policies using platform.protection_policy API")
                except Exception as e:
                    logger.debug(f"Error getting policies using platform.protection_policy API: {str(e)}")
            
            # Try cluster API if platform didn't work
            if not policies and api_structure.get('has_cluster') and hasattr(client.cluster, 'protection_policies'):
                try:
                    policies = client.cluster.protection_policies.get_protection_policies()
                    logger.debug(f"Got policies using cluster.protection_policies API")
                except Exception as e:
                    logger.debug(f"Error getting policies using cluster.protection_policies API: {str(e)}")
            
            # Try direct API call as last resort
            if not policies and hasattr(client, 'get'):
                try:
                    response = client.get('/irisservices/api/v1/public/protectionPolicies')
                    if hasattr(response, 'json'):
                        data = response.json()
                        if isinstance(data, list):
                            # Create policy objects similar to SDK output
                            policies = []
                            for pol in data:
                                # Create a simple object with expected attributes
                                class Policy:
                                    def __init__(self, pol_data):
                                        self.id = pol_data.get('id')
                                        self.name = pol_data.get('name')
                            
                            policies.append(Policy(pol))
                        logger.debug(f"Got policies using direct API call")
                except Exception as e:
                    logger.debug(f"Error getting policies using direct API: {str(e)}")
            
            # Find the policy by name
            if policies:
                for policy in policies:
                    if policy.name == policy_name:
                        policy_id = policy.id
                        break
            
            if not policy_id:
                logger.error(f"Protection policy '{policy_name}' not found on cluster {cluster_ip}")
                return False
            
            # Find physical servers for the filesets using different API patterns
            physical_sources = {}
            physical_server_sources = None
            
            # Try using platform API
            if api_structure.get('has_platform') and hasattr(client.platform, 'protection_source'):
                try:
                    physical_server_sources = client.platform.protection_source.list_protection_sources(
                        environment="kPhysical"
                    )
                    logger.debug(f"Got physical sources using platform.protection_source API")
                except Exception as e:
                    logger.debug(f"Error getting physical sources using platform.protection_source API: {str(e)}")
            
            # Try cluster API if platform didn't work
            if not physical_server_sources and api_structure.get('has_cluster') and hasattr(client.cluster, 'protection_sources'):
                try:
                    physical_server_sources = client.cluster.protection_sources.list_protection_sources(
                        environment="kPhysical"
                    )
                    logger.debug(f"Got physical sources using cluster.protection_sources API")
                except Exception as e:
                    logger.debug(f"Error getting physical sources using cluster.protection_sources API: {str(e)}")
            
            # Try direct API call as last resort
            if not physical_server_sources and hasattr(client, 'get'):
                try:
                    response = client.get('/irisservices/api/v1/public/protectionSources?environments=kPhysical')
                    if hasattr(response, 'json'):
                        data = response.json()
                        # Create source objects similar to SDK output
                        
                        # This is a simplified structure - you may need to adapt based on the actual API response
                        class ProtectionSource:
                            def __init__(self, source_data):
                                self.nodes = []
                                if 'nodes' in source_data:
                                    for node in source_data.get('nodes', []):
                                        self.nodes.append(SourceNode(node))
                        
                        class SourceNode:
                            def __init__(self, node_data):
                                self.name = node_data.get('name')
                                self.id = node_data.get('id')
                        
                        physical_server_sources = [ProtectionSource(data)]
                        logger.debug(f"Got physical sources using direct API call")
                except Exception as e:
                    logger.debug(f"Error getting physical sources using direct API: {str(e)}")
            
            # Build a map of server names to source IDs
            if physical_server_sources:
                for node in physical_server_sources:
                    # Navigate the node structure to find physical servers
                    if hasattr(node, 'nodes') and node.nodes:
                        for physical_server in node.nodes:
                            if hasattr(physical_server, 'name') and physical_server.name:
                                physical_sources[physical_server.name] = physical_server.id
            
            # Check if we found all the servers
            missing_servers = []
            for fileset in filesets:
                if fileset['server_name'] not in physical_sources:
                    missing_servers.append(fileset['server_name'])
            
            if missing_servers:
                logger.error(f"Could not find the following servers on cluster {cluster_ip}: {', '.join(missing_servers)}")
                return False
            
            # Prepare protection group sources with filesets
            sources = []
            for fileset in filesets:
                server_name = fileset['server_name']
                source_id = physical_sources[server_name]
                
                # Create fileset for each server
                source = {
                    "id": source_id,
                    "name": server_name,
                    "include_paths": fileset['paths']
                }
                
                # Add include/exclude patterns if specified
                if 'include_patterns' in fileset:
                    source["include_patterns"] = fileset['include_patterns']
                
                if 'exclude_patterns' in fileset:
                    source["exclude_patterns"] = fileset['exclude_patterns']
                
                sources.append(source)
            
            # Create the protection group using different API patterns
            protection_group = {
                "name": group_name,
                "policy_id": policy_id,
                "environment": "kPhysical",
                "sources": sources
            }
            
            if description:
                protection_group["description"] = description
            
            # Try platform API first
            success = False
            if api_structure.get('has_platform') and hasattr(client.platform, 'protection_group'):
                try:
                    result = client.platform.protection_group.create_protection_group(
                        body=protection_group
                    )
                    logger.debug(f"Created protection group using platform.protection_group API")
                    success = True
                except Exception as e:
                    logger.debug(f"Error creating protection group using platform.protection_group API: {str(e)}")
            
            # Try cluster API if platform didn't work
            if not success and api_structure.get('has_cluster') and hasattr(client.cluster, 'protection_groups'):
                try:
                    result = client.cluster.protection_groups.create_protection_group(
                        body=protection_group
                    )
                    logger.debug(f"Created protection group using cluster.protection_groups API")
                    success = True
                except Exception as e:
                    logger.debug(f"Error creating protection group using cluster.protection_groups API: {str(e)}")
            
            # Try direct API call as last resort
            if not success and hasattr(client, 'post'):
                try:
                    response = client.post('/irisservices/api/v1/public/protectionGroups', 
                                         body=protection_group)
                    if response.status_code in [200, 201, 202]:
                        logger.debug(f"Created protection group using direct API call")
                        success = True
                except Exception as e:
                    logger.debug(f"Error creating protection group using direct API: {str(e)}")
            
            if success:
                logger.info(f"Created protection group '{group_name}' on cluster {cluster_ip}")
                return True
            else:
                logger.error(f"Failed to create protection group '{group_name}' on cluster {cluster_ip}")
                return False
            
        except Exception as e:
            logger.error(f"Error creating protection group on {cluster_ip}: {str(e)}")
            return False
    
    def create_protection_groups_on_all_clusters(self, filesets_csv, group_name_template, policy_name):
        """Create protection groups on all connected clusters
        
        Args:
            filesets_csv (str): Path to CSV file with fileset information
            group_name_template (str): Template for protection group names (cluster name will be added)
            policy_name (str): Name of the protection policy to use
            
        Returns:
            dict: Results of protection group creation attempts
        """
        if not self.cluster_clients:
            logger.error("No connected clusters")
            return {"success": 0, "failed": 0}
        
        # Load filesets from CSV
        filesets = self.load_filesets_from_csv(filesets_csv)
        
        if not filesets:
            logger.error("No valid filesets found in CSV")
            return {"success": 0, "failed": 0}
        
        results = {"success": 0, "failed": 0}
        
        for cluster_ip, cluster_data in self.cluster_clients.items():
            cluster_name = cluster_data['info']['name']
            group_name = f"{group_name_template}_{cluster_name}"
            
            logger.info(f"Creating protection group '{group_name}' on cluster {cluster_ip}")
            
            success = self.create_protection_group(
                cluster_ip=cluster_ip,
                group_name=group_name,
                policy_name=policy_name,
                filesets=filesets,
                description=f"Linux file backup group created by automated script on {datetime.now().strftime('%Y-%m-%d')}"
            )
            
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1
        
        logger.info(f"Protection group creation summary: {results['success']} successful, {results['failed']} failed")
        return results
    
    def get_cluster_status(self, cluster_ip, detailed=False):
        """Get status of a Cohesity cluster
        
        Args:
            cluster_ip (str): IP address of the cluster
            detailed (bool): Whether to display detailed information
            
        Returns:
            dict: Status information or None if error
        """
        if cluster_ip not in self.cluster_clients:
            logger.error(f"No connection to cluster {cluster_ip}")
            return None
        
        status = {
            "ip": cluster_ip,
            "name": None,
            "version": None,
            "node_count": None,
            "health": None,
            "storage_used": None,
            "storage_total": None,
            "protection_groups": None,
            "error": None
        }
        
        try:
            client = self.cluster_clients[cluster_ip]['client']
            api_structure = self.cluster_clients[cluster_ip]['api_structure']
            
            # Basic info from the cached cluster_clients dict
            status["name"] = self.cluster_clients[cluster_ip]['info']['name']
            status["version"] = self.cluster_clients[cluster_ip]['info']['version']
            
            # Get node count based on available API patterns
            if api_structure.get('has_platform'):
                try:
                    basic_info = client.platform.get_cluster()
                    status["node_count"] = len(basic_info.nodes) if hasattr(basic_info, 'nodes') and basic_info.nodes else 0
                    logger.debug(f"Got node count using platform API")
                except Exception as e:
                    logger.debug(f"Error getting node count using platform API: {str(e)}")
            
            if status["node_count"] is None and api_structure.get('has_cluster'):
                try:
                    basic_info = client.cluster.get_basic_cluster_info()
                    status["node_count"] = basic_info.node_count if hasattr(basic_info, 'node_count') else 0
                    logger.debug(f"Got node count using cluster API")
                except Exception as e:
                    logger.debug(f"Error getting node count using cluster API: {str(e)}")
            
            # Try direct API call if other methods failed
            if status["node_count"] is None and hasattr(client, 'get'):
                try:
                    response = client.get('/irisservices/api/v1/public/nodes')
                    if hasattr(response, 'json'):
                        data = response.json()
                        if isinstance(data, list):
                            status["node_count"] = len(data)
                            logger.debug(f"Got node count using direct API call")
                except Exception as e:
                    logger.debug(f"Error getting node count using direct API: {str(e)}")
            
            # Get cluster health
            try:
                health = None
                if api_structure.get('has_platform'):
                    try:
                        health = client.platform.get_cluster_health()
                        status["health"] = health.entity_health_state if hasattr(health, 'entity_health_state') else "Unknown"
                        logger.debug(f"Got health using platform API")
                    except Exception as e:
                        logger.debug(f"Error getting health using platform API: {str(e)}")
                
                if status["health"] is None and api_structure.get('has_cluster'):
                    try:
                        health = client.cluster.get_cluster_health()
                        status["health"] = health.state if hasattr(health, 'state') else "Unknown"
                        logger.debug(f"Got health using cluster API")
                    except Exception as e:
                        logger.debug(f"Error getting health using cluster API: {str(e)}")
                    
                # Try direct API call if other methods failed
                if status["health"] is None and hasattr(client, 'get'):
                    try:
                        response = client.get('/irisservices/api/v1/public/clusterHealth')
                        if hasattr(response, 'json'):
                            data = response.json()
                            status["health"] = data.get('healthStatus', 'Unknown')
                            logger.debug(f"Got health using direct API call")
                    except Exception as e:
                        logger.debug(f"Error getting health using direct API: {str(e)}")
                    
                if status["health"] is None:
                    status["health"] = "Unknown"
            except Exception as e:
                logger.debug(f"Error checking cluster health for {cluster_ip}: {str(e)}")
                status["health"] = "Unknown"
            
            # Get storage stats
            try:
                if api_structure.get('has_platform') and hasattr(client.platform, 'stats'):
                    try:
                        storage_stats = client.platform.stats.get_storage_stats()
                        if storage_stats:
                            status["storage_used"] = storage_stats.used_bytes
                            status["storage_total"] = storage_stats.total_bytes
                            logger.debug(f"Got storage stats using platform.stats API")
                    except Exception as e:
                        logger.debug(f"Error getting storage stats using platform.stats API: {str(e)}")
                
                if status["storage_used"] is None and api_structure.get('has_cluster') and hasattr(client.cluster, 'stats'):
                    try:
                        storage_stats = client.cluster.stats.get_storage_stats()
                        if storage_stats:
                            status["storage_used"] = storage_stats.used_bytes
                            status["storage_total"] = storage_stats.total_bytes
                            logger.debug(f"Got storage stats using cluster.stats API")
                    except Exception as e:
                        logger.debug(f"Error getting storage stats using cluster.stats API: {str(e)}")
                
                # Try direct API call if other methods failed
                if status["storage_used"] is None and hasattr(client, 'get'):
                    try:
                        response = client.get('/irisservices/api/v1/public/stats/storage')
                        if hasattr(response, 'json'):
                            data = response.json()
                            status["storage_used"] = data.get('usageBytes', None)
                            status["storage_total"] = data.get('totalCapacityBytes', None)
                            logger.debug(f"Got storage stats using direct API call")
                    except Exception as e:
                        logger.debug(f"Error getting storage stats using direct API: {str(e)}")
                    
            except Exception as e:
                logger.debug(f"Error getting storage stats for {cluster_ip}: {str(e)}")
            
            # Get protection group count
            try:
                if api_structure.get('has_platform') and hasattr(client.platform, 'protection_group'):
                    try:
                        protection_groups = client.platform.protection_group.get_protection_groups()
                        status["protection_groups"] = len(protection_groups) if protection_groups else 0
                        logger.debug(f"Got protection groups using platform.protection_group API")
                    except Exception as e:
                        logger.debug(f"Error getting protection groups using platform.protection_group API: {str(e)}")
                
                if status["protection_groups"] is None and api_structure.get('has_cluster') and hasattr(client.cluster, 'protection_groups'):
                    try:
                        protection_groups = client.cluster.protection_groups.get_groups()
                        status["protection_groups"] = len(protection_groups) if protection_groups else 0
                        logger.debug(f"Got protection groups using cluster.protection_groups API")
                    except Exception as e:
                        logger.debug(f"Error getting protection groups using cluster.protection_groups API: {str(e)}")
                
                # Try direct API call if other methods failed
                if status["protection_groups"] is None and hasattr(client, 'get'):
                    try:
                        response = client.get('/irisservices/api/v1/public/protectionGroups')
                        if hasattr(response, 'json'):
                            data = response.json()
                            if isinstance(data, list):
                                status["protection_groups"] = len(data)
                                logger.debug(f"Got protection groups using direct API call")
                    except Exception as e:
                        logger.debug(f"Error getting protection groups using direct API: {str(e)}")
                    
            except Exception as e:
                logger.debug(f"Error getting protection groups for {cluster_ip}: {str(e)}")
            
            # Format the output
            storage_used_str = self._format_size(status["storage_used"]) if status["storage_used"] is not None else "Unknown"
            storage_total_str = self._format_size(status["storage_total"]) if status["storage_total"] is not None else "Unknown"
            storage_str = f"{storage_used_str} / {storage_total_str}"
            
            if detailed:
                logger.info(f"Detailed status for {cluster_ip} [{status['name']}]:")
                logger.info(f"  Version: {status['version']}")
                logger.info(f"  Nodes: {status['node_count']}")
                logger.info(f"  Health: {status['health']}")
                logger.info(f"  Storage: {storage_str}")
                logger.info(f"  Protection Groups: {status['protection_groups']}")
            else:
                # Basic status line format similar to ilo_power
                logger.info(f"{cluster_ip} | {status['name']} | v{status['version']} | Nodes: {status['node_count']} | Health: {status['health']} | Storage: {storage_str} | PGs: {status['protection_groups']}")
            
            return status
            
        except Exception as e:
            error_msg = f"Error getting status for {cluster_ip}: {str(e)}"
            logger.error(error_msg)
            status["error"] = error_msg
            return status
    
    def get_all_clusters_status(self, detailed=False):
        """Get status for all connected clusters using pandas for data handling
        
        Args:
            detailed (bool): Whether to display detailed information
            
        Returns:
            list: List of status dictionaries
        """
        if not self.cluster_clients:
            logger.error("No connected clusters")
            return []
        
        logger.info(f"Fetching status for {len(self.cluster_clients)} clusters...")
        
        # Collect status data for each cluster
        statuses = []
        for cluster_ip in self.cluster_clients:
            status = self.get_cluster_status(cluster_ip, detailed)
            if status:
                statuses.append(status)
        
        # Use pandas DataFrame for better data analysis and summary
        if statuses:
            # Convert to DataFrame
            status_df = pd.DataFrame(statuses)
            
            # Calculate storage statistics using NumPy - safely handle None values
            storage_used = np.array([float(s.get('storage_used') or 0) for s in statuses], dtype=np.float64)
            storage_used = storage_used[storage_used > 0]  # Filter out zeros
            
            storage_total = np.array([float(s.get('storage_total') or 0) for s in statuses], dtype=np.float64)
            storage_total = storage_total[storage_total > 0]  # Filter out zeros
            
            # Get health status distribution
            health_counts = status_df['health'].value_counts().to_dict() if 'health' in status_df.columns else {}
            
            # Add summary information
            logger.info("\nCluster Status Summary:")
            logger.info(f"  Total Clusters: {len(statuses)}")
            
            if len(storage_used) > 0:
                total_used = np.sum(storage_used)
                logger.info(f"  Total Storage Used: {self._format_size(total_used)}")
            else:
                logger.info("  Total Storage Used: Unknown (No data available)")
            
            if len(storage_total) > 0:
                total_capacity = np.sum(storage_total)
                logger.info(f"  Total Storage Capacity: {self._format_size(total_capacity)}")
                
                if len(storage_used) > 0:
                    usage_percentage = (np.sum(storage_used) / np.sum(storage_total)) * 100
                    logger.info(f"  Overall Storage Usage: {usage_percentage:.2f}%")
            else:
                logger.info("  Total Storage Capacity: Unknown (No data available)")
            
            # Report on health status
            if health_counts:
                logger.info("  Health Status Distribution:")
                for status, count in health_counts.items():
                    logger.info(f"    {status}: {count} clusters")
            
            # Get protection group count statistics - safely handle None values
            # Convert None to 0 before creating NumPy array
            pg_counts = np.array([int(s.get('protection_groups') or 0) for s in statuses], dtype=np.int32)
            
            if np.any(pg_counts > 0):  # Check if we have any non-zero values
                total_pgs = np.sum(pg_counts)
                avg_pgs = np.mean(pg_counts)
                logger.info(f"  Total Protection Groups: {total_pgs}")
                logger.info(f"  Average Protection Groups per Cluster: {avg_pgs:.2f}")
            else:
                logger.info("  Protection Groups: Unknown (No data available)")
        
        return statuses
    
    def _format_size(self, size_bytes):
        """Format bytes into human-readable format using NumPy for better precision
        
        Args:
            size_bytes (int): Size in bytes
            
        Returns:
            str: Formatted size string
        """
        if size_bytes is None:
            return "Unknown"
            
        # Define size units
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        
        # Use NumPy for calculations
        if size_bytes == 0:
            return "0 B"
        
        # Convert to log base 1024 to determine the appropriate unit
        power = np.min([np.floor(np.log(np.abs(size_bytes)) / np.log(1024)), 5])
        size = size_bytes / np.power(1024, power)
        
        # Return formatted string with appropriate unit
        return f"{size:.2f} {units[int(power)]}"

def main():
    """Main function to parse arguments and run the manager"""
    parser = argparse.ArgumentParser(description="Cohesity Cluster Management Script")
    
    # Optional arguments
    parser.add_argument("--clusters", default="data/clusters.csv", 
                        help="Path to CSV file with cluster information (default: data/clusters.csv)")
    parser.add_argument("--policy", default="Bronze", 
                        help="Protection policy name to use (default: Bronze)")
    parser.add_argument("--group-name", default="Linux_Filesystems",
                       help="Template for protection group names (default: Linux_Filesystems)")
    parser.add_argument("--debug", action="store_true", 
                        help="Enable debug logging")
    parser.add_argument("--verbose-debug", action="store_true", 
                        help="Enable verbose debug with HTTP request/response details (use for API troubleshooting)")
    parser.add_argument("--output-dir", default="output/logs", 
                        help="Directory for output files (default: output/logs)")
    parser.add_argument("--details", action="store_true", 
                        help="Show detailed status information")
    parser.add_argument("--target-version", 
                        help="Specific version to upgrade to (if not provided, latest will be used)")
    parser.add_argument("-v", "--version", action="store_true",
                        help="Show script version and exit")
    parser.add_argument("--test-api", action="store_true",
                        help="Test API endpoints to discover structure (useful for troubleshooting)")
    
    # Action arguments in a mutually exclusive group
    action_group = parser.add_mutually_exclusive_group(required=False)
    action_group.add_argument("-u", "--upgrade", action="store_true", 
                             help="Upgrade clusters to the latest version")
    action_group.add_argument("-c", "--create-protection-groups", metavar="FILESETS_CSV",
                             help="Create protection groups using filesets from CSV")
    action_group.add_argument("-s", "--status", action="store_true", 
                             help="Check status of all clusters")
    
    args = parser.parse_args()
    
    # Show version and exit if requested
    if args.version:
        print("Cohesity Cluster Management Script v1.0.0")
        return 0
    
    # Ensure at least one action is specified
    if not (args.upgrade or args.create_protection_groups or args.status or args.test_api):
        parser.print_help()
        print("\nError: You must specify an action (--status, --upgrade, --create-protection-groups, or --test-api)")
        return 1
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Create manager instance
    manager = CohesityManager(debug=args.debug, verbose_debug=args.verbose_debug)
    
    # Load clusters from CSV
    if not manager.load_clusters_from_csv(args.clusters):
        logger.error(f"Failed to load clusters from {args.clusters}. Exiting.")
        return 1
    
    # Connect to clusters
    connected = manager.connect_to_clusters()
    if connected == 0:
        logger.error("Failed to connect to any clusters. Exiting.")
        return 1
    
    # Test API if requested
    if args.test_api:
        logger.info("Testing API endpoints...")
        for cluster_ip, cluster_data in manager.cluster_clients.items():
            logger.info(f"Testing API for cluster {cluster_ip}...")
            manager._test_api_endpoints(cluster_data['client'], cluster_ip)
        logger.info("API testing complete.")
        return 0
    
    # Perform requested action
    if args.upgrade:
        logger.info("Starting cluster upgrades...")
        results = manager.upgrade_all_clusters(version=args.target_version)
        
        # Write results to JSON file in output directory
        results_file = os.path.join(args.output_dir, f"upgrade_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        try:
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Upgrade results written to {results_file}")
        except Exception as e:
            logger.error(f"Error writing results to file: {str(e)}")
        
        if results["success"] > 0:
            logger.info(f"Successfully initiated upgrades on {results['success']} clusters")
        else:
            logger.error("Failed to initiate upgrades on any clusters")
            return 1
    
    elif args.create_protection_groups:
        logger.info("Creating protection groups...")
        results = manager.create_protection_groups_on_all_clusters(
            filesets_csv=args.create_protection_groups,
            group_name_template=args.group_name,
            policy_name=args.policy
        )
        
        # Write results to JSON file in output directory
        results_file = os.path.join(args.output_dir, f"protection_group_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        try:
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Protection group results written to {results_file}")
        except Exception as e:
            logger.error(f"Error writing results to file: {str(e)}")
        
        if results["success"] > 0:
            logger.info(f"Successfully created protection groups on {results['success']} clusters")
        else:
            logger.error("Failed to create protection groups on any clusters")
            return 1
    
    elif args.status:
        logger.info("Checking cluster status...")
        results = manager.get_all_clusters_status(detailed=args.details)
        
        # Save status results to JSON file in output directory
        if results:
            # Convert the results to a pandas DataFrame for additional analysis
            status_df = pd.DataFrame(results)
            
            # Save as JSON for complete data
            results_file = os.path.join(args.output_dir, f"cluster_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            try:
                with open(results_file, 'w') as f:
                    json.dump(results, f, indent=2)
                logger.info(f"Status results written to {results_file}")
            except Exception as e:
                logger.error(f"Error writing status results to file: {str(e)}")
                
            # Also save as CSV for easier data analysis
            csv_file = os.path.join(args.output_dir, f"cluster_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            try:
                # Clean the DataFrame to handle nested data that can't be directly serialized to CSV
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
                logger.info(f"Status results also written to CSV: {csv_file}")
            except Exception as e:
                logger.error(f"Error writing status results to CSV: {str(e)}")
                
        logger.info(f"Status check complete. Retrieved status for {len(results)}/{connected} clusters.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 