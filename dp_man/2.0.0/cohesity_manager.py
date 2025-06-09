#!/usr/bin/env python
"""Cohesity Manager Module - Version 2.0.0"""

##########################################################################################
# Cohesity Manager Module
# ====================
# 
# This module provides a simplified interface to the Cohesity REST API by leveraging
# the pyhesity module. It includes functions for common operations like:
#
# - Authentication and session management
# - Protection job operations
# - Backup and restore operations
# - Reporting and analytics
# - Cluster management
#
##########################################################################################

import os
import json
import datetime
import time
import sys
import argparse
import textwrap
import getpass
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('cohesity_manager')

# Import pyhesity module
try:
    from pyhesity import *
except ImportError:
    print("Error: pyhesity module not found. Please install it using:")
    print("curl -O https://raw.githubusercontent.com/cohesity/community-automation-samples/main/python/pyhesity/pyhesity.py")
    sys.exit(1)

__version__ = '2.0.0'

class CohesityManager:
    """
    CohesityManager class provides simplified access to Cohesity cluster operations
    by leveraging the pyhesity module
    """
    
    def __init__(self):
        """Initialize the CohesityManager instance"""
        self.connected = False
        self.cluster_info = None
        self.current_tenant = None
    
    def connect(self, cluster=None, username=None, domain='local', password=None, 
                tenant=None, update_password=False, use_api_key=False, mfa_code=None, 
                email_mfa=False, quiet=False):
        """
        Connect to Cohesity cluster or Helios
        
        Args:
            cluster (str): Cohesity cluster FQDN or IP (default is helios.cohesity.com for Helios)
            username (str): Username for authentication
            domain (str): User domain (default is 'local')
            password (str): Password (not recommended to specify, will prompt if not stored)
            tenant (str): Tenant to impersonate (optional)
            update_password (bool): Update stored password
            use_api_key (bool): Use API key authentication
            mfa_code (str): MFA code for authentication
            email_mfa (bool): Use email MFA authentication
            quiet (bool): Suppress connection messages
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Default to Helios if no cluster specified
            if not cluster:
                cluster = 'helios.cohesity.com'
                is_helios = True
            else:
                is_helios = cluster.lower() in ['helios.cohesity.com', 'helios.gov-cohesity.com']
            
            # Debug logging
            logger.debug(f"Connecting to cluster: {cluster}")
            logger.debug(f"Username: {username}")
            logger.debug(f"Domain: {domain}")
            logger.debug(f"Update password: {update_password}")
            logger.debug(f"Use API key: {use_api_key}")
            
            # Set environment variable for password storage
            os.environ['PYHESITY_PASSWORD_FILE'] = os.path.expanduser('~/.pyhesity/passwords')
            
            # Authenticate to cluster or Helios using pyhesity's built-in password management
            apiauth(vip=cluster, 
                    username=username, 
                    domain=domain, 
                    password=password, 
                    updatepw=update_password,  # This will update stored password if True
                    useApiKey=use_api_key,
                    helios=is_helios,
                    mfaCode=mfa_code,
                    emailMfaCode=email_mfa,
                    quiet=quiet)
            
            # Check if connection was successful
            self.connected = apiconnected()
            
            if self.connected:
                logger.debug("Successfully connected to cluster")
                
                # Impersonate tenant if specified
                if tenant and not is_helios:
                    impersonate(tenant)
                    self.current_tenant = tenant
                
                # Get cluster info (if not Helios)
                if not is_helios:
                    self.cluster_info = self.get_cluster_info()
            else:
                logger.error("Failed to connect to cluster")
            
            return self.connected
            
        except Exception as e:
            logger.error(f"Error connecting to cluster: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from the current session"""
        apidrop()
        self.connected = False
        self.cluster_info = None
        self.current_tenant = None
    
    def check_connection(self):
        """Check if currently connected to a Cohesity cluster"""
        return self.connected and apiconnected()
    
    def get_cluster_info(self):
        """
        Get basic cluster information
        
        Returns:
            dict: Cluster information
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        try:
            # Get basic cluster information with additional details
            # Use query parameters to get more comprehensive information
            cluster_info = api('get', 'cluster?fetchStats=true&fetchTimeSeriesSchema=true&includeMinimumNodesInfo=true')
            logger.debug(f"Retrieved cluster info with additional parameters")
            
            # Extract software version directly from cluster info if available
            if 'clusterSoftwareVersion' in cluster_info:
                cluster_info['softwareVersion'] = cluster_info['clusterSoftwareVersion']
                logger.debug("Found version in cluster info: clusterSoftwareVersion")
            
            # Add default empty node configs if not present
            if 'nodeConfigs' not in cluster_info:
                cluster_info['nodeConfigs'] = []
            
            # If we have nodeCount but no node details, try specific API endpoints for this version
            if cluster_info.get('nodeCount', 0) > 0 and len(cluster_info.get('nodeConfigs', [])) == 0:
                try:
                    # Try with 'nodes' endpoint
                    nodes = api('get', 'nodes')
                    if nodes and 'nodes' in nodes:
                        cluster_info['nodeConfigs'] = nodes['nodes']
                        logger.debug("Successfully retrieved nodes from 'nodes' endpoint")
                except Exception as e:
                    logger.debug(f"Error getting nodes from 'nodes' endpoint: {str(e)}")
                    
                    try:
                        # Try with 'cluster/nodes' endpoint
                        nodes = api('get', 'cluster/nodes')
                        if nodes and 'nodes' in nodes:
                            cluster_info['nodeConfigs'] = nodes['nodes']
                            logger.debug("Successfully retrieved nodes from 'cluster/nodes' endpoint")
                    except Exception as e1:
                        logger.debug(f"Error getting nodes from 'cluster/nodes' endpoint: {str(e1)}")
                        
                        try:
                            # Try with 'v1/nodes' endpoint
                            nodes = api('get', 'v1/nodes')
                            if isinstance(nodes, list):
                                cluster_info['nodeConfigs'] = nodes
                                logger.debug("Successfully retrieved nodes from 'v1/nodes' endpoint")
                        except Exception as e2:
                            logger.debug(f"Error getting nodes from 'v1/nodes' endpoint: {str(e2)}")
                            
                            # For Cohesity 7.x, try 'clusterConfiguration/node' endpoint
                            try:
                                nodes_7x = api('get', 'clusterConfiguration/node')
                                if nodes_7x:
                                    # Convert to expected format
                                    if isinstance(nodes_7x, list):
                                        cluster_info['nodeConfigs'] = nodes_7x
                                        logger.debug("Successfully retrieved nodes from 'clusterConfiguration/node' endpoint")
                                    elif 'nodes' in nodes_7x:
                                        cluster_info['nodeConfigs'] = nodes_7x['nodes']
                                        logger.debug("Successfully retrieved nodes from 'clusterConfiguration/node' endpoint (nodes field)")
                            except Exception as e3:
                                logger.debug(f"Error getting nodes from 'clusterConfiguration/node' endpoint: {str(e3)}")
            
            # Handle special field mappings
            if 'nodes' in cluster_info and not cluster_info.get('nodeConfigs') and isinstance(cluster_info['nodes'], list):
                cluster_info['nodeConfigs'] = cluster_info['nodes']
                logger.debug("Used nodes field from cluster info")
            
            # If we still have no node configs but have nodeIps, create placeholder nodes
            if len(cluster_info.get('nodeConfigs', [])) == 0 and 'nodeIps' in cluster_info:
                node_ips = cluster_info['nodeIps'].split(',')
                placeholder_nodes = []
                for i, ip in enumerate(node_ips):
                    placeholder_nodes.append({
                        'nodeIp': ip.strip(),
                        'nodeId': i+1,
                        'status': 'Unknown (placeholder)',
                        'role': 'Unknown (placeholder)'
                    })
                
                if placeholder_nodes:
                    cluster_info['nodeConfigs'] = placeholder_nodes
                    logger.debug(f"Created {len(placeholder_nodes)} placeholder nodes from nodeIps field")
            
            # Get node count for hardware info check
            node_count = len(cluster_info.get('nodeConfigs', []))
            
            # Try getting hardware info which might have additional details
            try:
                hardware_info = api('get', 'hardware')
                if hardware_info:
                    # Add hardware info to cluster info
                    cluster_info['hardwareDetails'] = hardware_info
                    logger.debug("Successfully retrieved hardware info")
                    
                    # Extract version if needed
                    if ('softwareVersion' not in cluster_info or not cluster_info['softwareVersion']) and 'softwareVersion' in hardware_info:
                        cluster_info['softwareVersion'] = hardware_info['softwareVersion']
                        logger.debug("Successfully retrieved version from hardware info")
            except Exception as hw_e:
                logger.debug(f"Error getting hardware info: {str(hw_e)}")
            
            # Set default version if all attempts failed
            if 'softwareVersion' not in cluster_info or not cluster_info['softwareVersion']:
                logger.warning("Could not determine software version from any API endpoint")
                cluster_info['softwareVersion'] = "Unknown (API endpoints not available)"
            
            return cluster_info
        except Exception as e:
            logger.error(f"Error getting cluster information: {str(e)}")
            return None
    
    def list_protection_jobs(self, include_inactive=False, job_type=None):
        """
        List protection jobs
        
        Args:
            include_inactive (bool): Include inactive jobs
            job_type (str): Filter by job type (e.g., 'kVMware', 'kPhysical', 'kView')
            
        Returns:
            list: List of protection jobs
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Build query parameters
        params = "?isActive=true" if not include_inactive else ""
        if job_type:
            params += f"&environments={job_type}"
        
        # Get protection jobs
        jobs = api('get', f'protectionJobs{params}')
        
        return jobs
    
    def get_protection_job_by_name(self, job_name):
        """
        Get protection job by name
        
        Args:
            job_name (str): Protection job name
            
        Returns:
            dict: Protection job information
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        jobs = self.list_protection_jobs(include_inactive=True)
        
        if jobs:
            for job in jobs:
                if job['name'].lower() == job_name.lower():
                    return job
        
        return None
    
    def run_protection_job(self, job_name=None, job_id=None, source_ids=None, full_backup=False):
        """
        Run a protection job
        
        Args:
            job_name (str): Protection job name
            job_id (int): Protection job ID (alternative to job_name)
            source_ids (list): List of source IDs to include in the run
            full_backup (bool): Force a full backup
            
        Returns:
            dict: Job run response
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Get job ID if name provided
        if job_name and not job_id:
            job = self.get_protection_job_by_name(job_name)
            if not job:
                print(f"Protection job '{job_name}' not found")
                return None
            job_id = job['id']
        
        # Build request body
        run_params = {}
        if source_ids:
            run_params['sourceIds'] = source_ids
        if full_backup:
            run_params['runType'] = 'kFull'
        
        # Run job
        return api('post', f'protectionJobs/run/{job_id}', run_params)
    
    def get_protection_job_runs(self, job_name=None, job_id=None, num_runs=10, include_object_details=False):
        """
        Get protection job runs
        
        Args:
            job_name (str): Protection job name
            job_id (int): Protection job ID (alternative to job_name)
            num_runs (int): Number of runs to retrieve
            include_object_details (bool): Include object details
            
        Returns:
            list: List of job runs
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Get job ID if name provided
        if job_name and not job_id:
            job = self.get_protection_job_by_name(job_name)
            if not job:
                print(f"Protection job '{job_name}' not found")
                return None
            job_id = job['id']
        
        # Get job runs using the getRuns helper function
        return getRuns(job_id, numRuns=num_runs, includeObjectDetails=include_object_details)
    
    def list_sources(self, source_type=None):
        """
        List registered sources
        
        Args:
            source_type (str): Filter by source type (e.g., 'kVMware', 'kPhysical')
            
        Returns:
            list: List of sources
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Build query parameters
        params = ""
        if source_type:
            params = f"?environments={source_type}"
        
        # Get sources
        return api('get', f'protectionSources{params}')
    
    def list_views(self, include_inactive=False):
        """
        List views
        
        Args:
            include_inactive (bool): Include inactive views
            
        Returns:
            list: List of views
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Build query parameters
        params = ""
        if not include_inactive:
            params = "?isActive=true"
        
        # Get views
        views = api('get', f'views{params}')
        
        # Return views array if available
        if views and 'views' in views:
            return views['views']
        
        return []
    
    def create_view(self, view_name, policy_id=None, storage_quota_gb=None, description=None):
        """
        Create a new view
        
        Args:
            view_name (str): View name
            policy_id (str): Protection policy ID
            storage_quota_gb (int): Storage quota in GB
            description (str): View description
            
        Returns:
            dict: Created view information
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Build view creation parameters
        view_params = {
            'name': view_name,
            'protocolAccess': 'kAll'
        }
        
        if description:
            view_params['description'] = description
        
        if policy_id:
            view_params['policyId'] = policy_id
        
        if storage_quota_gb:
            view_params['qos'] = {
                'principalName': view_name,
                'principalId': 'None',
                'usageLimitOverrides': [
                    {
                        'limitType': 'kStorageQuota',
                        'limitBytes': storage_quota_gb * 1024 * 1024 * 1024
                    }
                ]
            }
        
        # Create view
        return api('post', 'views', view_params)
    
    def delete_view(self, view_name):
        """
        Delete a view
        
        Args:
            view_name (str): View name
            
        Returns:
            dict: API response
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Get views with this name
        views = self.list_views(include_inactive=True)
        view_id = None
        
        for view in views:
            if view['name'].lower() == view_name.lower():
                view_id = view['viewId']
                break
        
        if not view_id:
            print(f"View '{view_name}' not found")
            return None
        
        # Delete view
        return api('delete', f'views/{view_id}')
    
    def search_objects(self, search_term, object_type=None, job_id=None, registered_source_id=None):
        """
        Search for objects
        
        Args:
            search_term (str): Search term
            object_type (str): Object type to search (e.g., 'kVMware', 'kPhysical')
            job_id (int): Filter by protection job ID
            registered_source_id (int): Filter by registered source ID
            
        Returns:
            list: Search results
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Build search parameters
        search_params = {
            'search': search_term
        }
        
        if object_type:
            search_params['environments'] = [object_type]
        
        if job_id:
            search_params['jobIds'] = [job_id]
        
        if registered_source_id:
            search_params['registeredSourceIds'] = [registered_source_id]
        
        # Perform search
        search_results = api('post', 'searchvms', search_params)
        
        if search_results and 'vms' in search_results:
            return search_results['vms']
        
        return []
    
    def restore_vm(self, vm_doc_id, target_vm_name=None, target_location=None):
        """
        Restore a VM
        
        Args:
            vm_doc_id (str): VM document ID to restore
            target_vm_name (str): Target VM name
            target_location (str): Target restore location
            
        Returns:
            dict: Restore task information
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Get VM details
        vm_details = api('get', f'restore/objects/{vm_doc_id}')
        
        if not vm_details:
            print(f"VM with ID '{vm_doc_id}' not found")
            return None
        
        # Build restore parameters
        restore_params = {
            'name': 'Restore-' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
            'objects': [
                {
                    'jobId': vm_details['objectSnapshot']['jobId'],
                    'jobRunId': vm_details['objectSnapshot']['jobRunId'],
                    'startTimeUsecs': vm_details['objectSnapshot']['startTimeUsecs'],
                    'entity': vm_details['entity']
                }
            ],
            'type': 'kRecoverVMs'
        }
        
        # Add target VM name if specified
        if target_vm_name:
            restore_params['newNameSuffix'] = target_vm_name
        
        # Add target location if specified
        if target_location:
            restore_params['restoreParentSource'] = target_location
        
        # Perform restore
        return api('post', 'restore/recover', restore_params)
    
    def get_active_alerts(self, max_alerts=100, alert_severity=None):
        """
        Get active alerts
        
        Args:
            max_alerts (int): Maximum number of alerts to retrieve
            alert_severity (str): Filter by severity (e.g., 'kCritical', 'kWarning', 'kInfo')
            
        Returns:
            list: List of active alerts
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # Build query parameters
        params = f"?maxAlerts={max_alerts}"
        
        if alert_severity:
            params += f"&alertCategoryList={alert_severity}"
        
        # Get alerts
        alerts = api('get', f'alerts{params}')
        
        if alerts and 'alerts' in alerts:
            return alerts['alerts']
        
        return []
    
    def get_task_status(self, task_id):
        """
        Get status of a specific task
        
        Args:
            task_id (str): Task ID
            
        Returns:
            dict: Task status information
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        return api('get', f'restoretasks/{task_id}')
    
    def cancel_task(self, task_id):
        """
        Cancel a running task
        
        Args:
            task_id (str): Task ID
            
        Returns:
            dict: API response
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        return api('delete', f'restoretasks/{task_id}')
    
    def list_helios_clusters(self):
        """
        List Helios connected clusters
        
        Returns:
            list: List of connected clusters
        """
        if not self.check_connection():
            print("Not connected to Helios")
            return None
        
        return heliosClusters()
    
    def connect_to_helios_cluster(self, cluster_name):
        """
        Connect to a specific cluster via Helios
        
        Args:
            cluster_name (str): Cluster name
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.check_connection():
            print("Not connected to Helios")
            return False
        
        try:
            heliosCluster(cluster_name, verbose=True)
            self.cluster_info = self.get_cluster_info()
            return True
        except Exception as e:
            print(f"Error connecting to cluster {cluster_name}: {e}")
            return False
    
    def format_timestamp(self, usecs, fmt='%Y-%m-%d %H:%M:%S'):
        """
        Format timestamp in usecs to human-readable date
        
        Args:
            usecs (int): Timestamp in microseconds
            fmt (str): Date format
            
        Returns:
            str: Formatted date string
        """
        return usecsToDate(usecs, fmt)
    
    def get_timestamp_usecs(self, date_time=None):
        """
        Get timestamp in usecs for a given date or current time
        
        Args:
            date_time (str or datetime): Date or datetime object
            
        Returns:
            int: Timestamp in microseconds
        """
        return dateToUsecs(date_time if date_time else datetime.datetime.now())
    
    def time_ago(self, value, unit):
        """
        Get timestamp in usecs for a time in the past
        
        Args:
            value (int): Time value
            unit (str): Time unit (seconds, minutes, hours, days, weeks, months, years)
            
        Returns:
            int: Timestamp in microseconds
        """
        return timeAgo(value, unit)
    
    def get_storage_stats(self):
        """
        Get cluster storage statistics
        
        Returns:
            dict: Storage statistics
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        try:
            # Try multiple approaches to get storage stats
            stats = None
            
            # Try with 'stats/storage' endpoint
            try:
                stats = api('get', 'stats/storage')
                if stats:
                    logger.debug("Successfully retrieved storage stats from 'stats/storage' endpoint")
                    return stats
            except Exception as e:
                logger.debug(f"Error getting storage stats from 'stats/storage' endpoint: {str(e)}")
            
            # Try with 'statistics/cluster' endpoint
            try:
                stats = api('get', 'statistics/cluster?fetchTimeSeriesStats=false')
                if stats:
                    logger.debug("Successfully retrieved storage stats from 'statistics/cluster' endpoint")
                    return stats
            except Exception as e:
                logger.debug(f"Error getting storage stats from 'statistics/cluster' endpoint: {str(e)}")
            
            # Try with 'v1/cluster/stats' endpoint
            try:
                stats = api('get', 'v1/cluster/stats')
                if stats:
                    logger.debug("Successfully retrieved storage stats from 'v1/cluster/stats' endpoint")
                    return stats
            except Exception as e:
                logger.debug(f"Error getting storage stats from 'v1/cluster/stats' endpoint: {str(e)}")
            
            # Try with 'cluster/stats' endpoint
            try:
                stats = api('get', 'cluster/stats')
                if stats:
                    logger.debug("Successfully retrieved storage stats from 'cluster/stats' endpoint")
                    return stats
            except Exception as e:
                logger.debug(f"Error getting storage stats from 'cluster/stats' endpoint: {str(e)}")
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get storage stats: {str(e)}")
            return None
    
    def list_protection_policies(self):
        """
        List protection policies
        
        Returns:
            list: List of protection policies
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        return api('get', 'protectionPolicies')
    
    def list_protection_runs_by_job_and_time(self, job_id, start_time_usecs=None, end_time_usecs=None):
        """
        List protection runs for a job within a specific time range
        
        Args:
            job_id (int): Protection job ID
            start_time_usecs (int): Start time in microseconds
            end_time_usecs (int): End time in microseconds
            
        Returns:
            list: List of protection runs
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        # If no end time provided, use current time
        if not end_time_usecs:
            end_time_usecs = self.get_timestamp_usecs()
        
        # Use the getRuns helper function
        return getRuns(job_id, startTimeUsecs=start_time_usecs, endTimeUsecs=end_time_usecs)
    
    def file_download(self, api_path, target_file_path):
        """
        Download file from Cohesity cluster
        
        Args:
            api_path (str): API path to the file
            target_file_path (str): Local file path to save the file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return False
        
        try:
            fileDownload(api_path, target_file_path)
            return True
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False
    
    def file_upload(self, api_path, source_file_path):
        """
        Upload file to Cohesity cluster
        
        Args:
            api_path (str): API path for the upload
            source_file_path (str): Local file path to upload
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return False
        
        try:
            fileUpload(api_path, source_file_path)
            return True
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False

    def get_tenant_info(self, tenant_name=None):
        """
        Get information about tenants
        
        Args:
            tenant_name (str): Name of specific tenant to get info for
            
        Returns:
            dict or list: Tenant information
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return None
        
        tenants = api('get', 'tenants')
        
        if tenant_name and tenants:
            for tenant in tenants:
                if tenant['name'].lower() == tenant_name.lower():
                    return tenant
            return None
        
        return tenants
    
    def impersonate_tenant(self, tenant_name):
        """
        Impersonate a tenant
        
        Args:
            tenant_name (str): Tenant name to impersonate
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return False
        
        try:
            impersonate(tenant_name)
            self.current_tenant = tenant_name
            return True
        except Exception as e:
            print(f"Error impersonating tenant {tenant_name}: {e}")
            return False
    
    def stop_tenant_impersonation(self):
        """
        Stop tenant impersonation
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return False
        
        try:
            switchback()
            self.current_tenant = None
            return True
        except Exception as e:
            print(f"Error stopping tenant impersonation: {e}")
            return False

    def print_cluster_info(self):
        """
        Print cluster information in a formatted way
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.check_connection():
            print("Not connected to a Cohesity cluster")
            return False
            
        try:
            # Get basic cluster information
            cluster_info = self.get_cluster_info()
            if not cluster_info:
                print("Failed to retrieve cluster information")
                return False
                
            # Get storage statistics
            storage_stats = cluster_info.get('stats', self.get_storage_stats())
            if not storage_stats:
                print("Failed to retrieve storage statistics")
                
            # Print cluster information
            print("\n" + "=" * 80)
            print(f" CLUSTER INFORMATION: {cluster_info['name']}")
            print("=" * 80)
            print(f"Cluster Name: {cluster_info['name']}")
            print(f"Cluster ID: {cluster_info['id']}")
            print(f"Cluster Software Version: {cluster_info.get('softwareVersion', 'Unknown')}")
            print(f"Cluster Domain: {cluster_info.get('domainNames', ['Unknown'])[0]}")
            
            # Print additional cluster info if available
            if 'clusterType' in cluster_info:
                print(f"Cluster Type: {cluster_info['clusterType']}")
            if 'clusterSize' in cluster_info:
                print(f"Cluster Size: {cluster_info['clusterSize']}")
            
            # Print detailed node information
            nodes = cluster_info.get('nodeConfigs', [])
            node_count = cluster_info.get('nodeCount', len(nodes))
            
            print(f"\nNode Information: {node_count} nodes")
            
            # If we have node IPs but no detailed config
            if not nodes and 'nodeIps' in cluster_info:
                node_ips = cluster_info['nodeIps'].split(',')
                print(f"  Node IPs: {', '.join(node_ips)}")
            
            if nodes:
                for i, node in enumerate(nodes, 1):
                    print(f"\n  Node {i}:")
                    
                    # Print IP address - different possible field names
                    ip_address = node.get('nodeIp', node.get('ip', 'Unknown'))
                    print(f"    IP Address: {ip_address}")
                    
                    # Print Node ID - different possible field names
                    node_id = node.get('nodeId', node.get('id', 'Unknown'))
                    print(f"    Node ID: {node_id}")
                    
                    # Print Status - different possible field names
                    status = node.get('status', node.get('state', node.get('health', 'Unknown')))
                    print(f"    Status: {status}")
                    
                    # Print Role - different possible field names
                    role = node.get('role', node.get('nodeType', 'Unknown'))
                    print(f"    Role: {role}")
                    
                    # Print Hardware info if available
                    hw_info = node.get('hardwareInfo', node.get('hardware', {}))
                    if hw_info:
                        print(f"    Hardware:")
                        
                        # Model - different possible field names
                        model = hw_info.get('model', hw_info.get('hardwareModel', 'Unknown'))
                        print(f"      Model: {model}")
                        
                        # CPU - different possible field names
                        cpu = hw_info.get('cpuModel', hw_info.get('cpu', 'Unknown'))
                        print(f"      CPU: {cpu}")
                        
                        # Memory - could be in different formats
                        memory_bytes = hw_info.get('memorySizeBytes', hw_info.get('memory', 0))
                        if isinstance(memory_bytes, str) and 'GB' in memory_bytes:
                            print(f"      Memory: {memory_bytes}")
                        else:
                            memory_gb = memory_bytes / (1024**3) if isinstance(memory_bytes, (int, float)) else 0
                            print(f"      Memory: {memory_gb:.1f} GB")
                        
                        # Disks - could be in different formats
                        if 'disks' in hw_info:
                            disk_count = len(hw_info['disks'])
                            print(f"      Disks: {disk_count}")
                        elif 'diskCount' in hw_info:
                            print(f"      Disks: {hw_info['diskCount']}")
            
            # Print storage information if available
            if storage_stats:
                # Try different possible field names for capacity and usage
                if 'usagePerfStats' in storage_stats:
                    perf_stats = storage_stats['usagePerfStats']
                    total_bytes = perf_stats.get('physicalCapacityBytes', 0)
                    used_bytes = perf_stats.get('totalPhysicalUsageBytes', 0)
                else:
                    # Try direct fields
                    total_bytes = (storage_stats.get('totalCapacityBytes') or 
                                storage_stats.get('totalBytes') or 
                                storage_stats.get('totalCapacity') or 0)
                    
                    used_bytes = (storage_stats.get('usedCapacityBytes') or 
                                storage_stats.get('usedBytes') or 
                                storage_stats.get('usedCapacity') or 0)
                
                # Convert to human-readable format
                total_tb = total_bytes / (1024.0 ** 4)
                used_tb = used_bytes / (1024.0 ** 4)
                usage_percent = (used_bytes / total_bytes * 100) if total_bytes > 0 else 0
                
                print("\nStorage Information:")
                print(f"  Total Capacity: {total_tb:.2f} TB")
                print(f"  Used Capacity: {used_tb:.2f} TB")
                print(f"  Usage Percentage: {usage_percent:.1f}%")
                
                # If we have data protection stats, print them
                if 'dataUsageStats' in storage_stats:
                    data_stats = storage_stats['dataUsageStats']
                    if 'dataProtectLogicalUsageBytes' in data_stats:
                        protect_bytes = data_stats['dataProtectLogicalUsageBytes']
                        protect_tb = protect_bytes / (1024.0 ** 4)
                        print(f"  Data Protection Logical Usage: {protect_tb:.2f} TB")
                    
                    if 'fileServicesLogicalUsageBytes' in data_stats:
                        file_bytes = data_stats['fileServicesLogicalUsageBytes']
                        file_tb = file_bytes / (1024.0 ** 4)
                        print(f"  File Services Logical Usage: {file_tb:.2f} TB")
                
            # Print protection job summary if available
            protection_jobs = self.list_protection_jobs()
            if protection_jobs:
                print(f"\nProtection Jobs Summary: {len(protection_jobs)} active jobs")
                
                # Count jobs by environment
                env_counts = {}
                for job in protection_jobs:
                    env = job.get('environment', 'Unknown')[1:]  # Remove 'k' prefix
                    env_counts[env] = env_counts.get(env, 0) + 1
                
                for env, count in env_counts.items():
                    print(f"  {env}: {count} jobs")
            
            # Print views summary if available
            views = self.list_views()
            if views:
                total_logical_usage = sum(view.get('logicalUsageBytes', 0) for view in views)
                total_logical_usage_tb = total_logical_usage / (1024.0 ** 4)
                
                print(f"\nViews Summary: {len(views)} active views")
                print(f"  Total Logical Usage: {total_logical_usage_tb:.2f} TB")
                
            return True
            
        except Exception as e:
            print(f"Error retrieving cluster information: {e}")
            return False


# Create a singleton instance for direct import use
cohesity_manager = CohesityManager()

# For backward compatibility with older scripts
def connect(*args, **kwargs):
    """Wrapper function for backward compatibility"""
    return cohesity_manager.connect(*args, **kwargs)

def disconnect():
    """Wrapper function for backward compatibility"""
    return cohesity_manager.disconnect()

def get_cluster_info():
    """Wrapper function for backward compatibility"""
    return cohesity_manager.get_cluster_info()

def print_version():
    """Print the version of the cohesity_manager module"""
    print(f"Cohesity Manager version {__version__}")
    
def main():
    """Main function when running as a script"""
    parser = argparse.ArgumentParser(
        description='Cohesity Manager - Command-line interface for managing Cohesity clusters',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
        Examples:
          python cohesity_manager.py --info -s cluster.example.com -u admin
          python cohesity_manager.py --version
          python cohesity_manager.py --info -s cluster.example.com -u admin --update-password
        ''')
    )
    
    # Add arguments
    parser.add_argument('--version', action='store_true', help='Print version information')
    parser.add_argument('--info', action='store_true', help='Display cluster information')
    
    # Connection parameters
    conn_group = parser.add_argument_group('Connection Options')
    conn_group.add_argument('-s', '--server', help='Cohesity cluster IP or hostname')
    conn_group.add_argument('-u', '--username', help='Username for authentication')
    conn_group.add_argument('-d', '--domain', default='local', help='Domain for authentication (default: local)')
    conn_group.add_argument('-p', '--password', help='Password (not recommended, will prompt if not specified)')
    conn_group.add_argument('-t', '--tenant', help='Tenant to impersonate')
    conn_group.add_argument('--api-key', action='store_true', help='Use API key authentication')
    conn_group.add_argument('-m', '--mfa-code', help='MFA code for authentication')
    conn_group.add_argument('--update-password', action='store_true', help='Update stored password')
    conn_group.add_argument('-q', '--quiet', action='store_true', help='Suppress connection messages')
    
    args = parser.parse_args()
    
    # Just print version and exit if --version specified
    if args.version:
        print_version()
        return 0
    
    # Check if we need to connect to a cluster
    if args.info:
        if not args.server:
            parser.error("--info requires --server")
        
        # Prompt for username if not provided
        username = args.username
        if not username:
            username = input("Enter username: ")
        
        # Connect to the cluster
        if not cohesity_manager.connect(
            cluster=args.server,
            username=username,
            domain=args.domain,
            password=args.password,
            tenant=args.tenant,
            update_password=args.update_password,
            use_api_key=args.api_key,
            mfa_code=args.mfa_code,
            quiet=args.quiet
        ):
            print(f"Failed to connect to cluster {args.server}")
            return 1
        
        # Print cluster information
        cohesity_manager.print_cluster_info()
        
        # Disconnect
        cohesity_manager.disconnect()
        return 0
    
    # If no action specified, print help
    if not any([args.version, args.info]):
        parser.print_help()
        return 0
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 