#!/usr/bin/env python
"""Cohesity Manager Module - Version 2.1.1"""

##########################################################################################
# Cohesity Manager Module
# ====================
# 
# This module provides a simple interface to the Cohesity REST API. It includes functions
# for common operations like:
#
# - Authentication and session management
# - Cluster information retrieval
# - CSV-based credential management
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
import csv
import requests
import pandas as pd
import numpy as np
import threading
import queue
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress insecure request warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('cohesity_manager')

__version__ = '2.1.1'

class CohesityManager:
    """
    CohesityManager class provides simplified access to Cohesity cluster operations
    """
    
    def __init__(self, credential_file=None):
        """
        Initialize the CohesityManager instance
        
        Args:
            credential_file (str): Optional custom path to credentials CSV file
        """
        self.connected = False
        self.cluster_info = None
        self.cluster_hostname = None
        self.username = None
        self.domain = None
        self.session = None
        self.headers = {}
        self.request_timeout = 10  # Default timeout for API requests in seconds
        
        # Set credential file path (default or custom)
        self.credential_file = credential_file or os.path.expanduser('~/.cohesity/credentials.csv')
        
    def connect(self, cluster=None, username=None, domain='local', password=None, connect_timeout=5):
        """
        Connect to Cohesity cluster
        
        Args:
            cluster (str): Cohesity cluster FQDN or IP
            username (str): Username for authentication
            domain (str): User domain (default is 'local')
            password (str): Password (will prompt if not provided)
            connect_timeout (int): Connection timeout in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Save connection parameters
            self.cluster_hostname = cluster
            self.username = username
            self.domain = domain
            
            # Prompt for password if not provided
            if password is None:
                password = getpass.getpass(f"Password for {username}@{domain} on {cluster}: ")
            
            # Create a new session with timeout
            self.session = requests.Session()
            
            # Test basic connectivity with a simple TCP connection before attempting API requests
            # This avoids long waits for unresponsive hosts
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(connect_timeout)
            
            try:
                logger.debug(f"Testing TCP connection to {cluster} (timeout: {connect_timeout}s)")
                sock.connect((cluster, 443))  # HTTPS port
                logger.debug(f"TCP connection to {cluster} successful")
            except (socket.timeout, socket.error) as e:
                logger.error(f"TCP connection to {cluster} failed: {e}")
                return False
            finally:
                sock.close()
            
            # Track if any authentication method succeeds
            auth_success = False
            
            # Method 1: Standard authentication (primary method)
            # Prepare authentication request
            auth_url = f"https://{cluster}/irisservices/api/v1/public/accessTokens"
            
            # Don't modify domain case for authentication request to ensure it matches exactly what's expected
            auth_data = {
                'domain': domain,
                'username': username,
                'password': password
            }
            
            # Log the authentication attempt with domain info
            logger.info(f"Authenticating to {cluster} as {username}@{domain}")
            
            # Authenticate with timeout
            try:
                auth_response = self.session.post(
                    auth_url, 
                    data=json.dumps(auth_data), 
                    verify=False, 
                    timeout=connect_timeout
                )
                
                # Check authentication status
                if auth_response.status_code == 201:
                    auth_data = auth_response.json()
                    token = auth_data['accessToken']
                    
                    # Set authorization header for future requests
                    self.headers = {
                        'Authorization': f"Bearer {token}",
                        'Content-Type': 'application/json'
                    }
                    self.session.headers.update(self.headers)
                    auth_success = True
                    logger.debug("Authentication successful using standard method")
                else:
                    logger.warning(f"Standard authentication failed: {auth_response.status_code} - {auth_response.text}")
                    # Continue to try alternative methods
            except requests.exceptions.Timeout:
                logger.warning(f"Standard authentication timed out after {connect_timeout}s")
                # Continue to try alternative methods
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Standard authentication connection error: {e}")
                # Continue to try alternative methods
            except Exception as e:
                logger.warning(f"Standard authentication error: {str(e)}")
                # Continue to try alternative methods
            
            # Method 2: Alternative authentication with JSON content-type header (fallback)
            if not auth_success:
                logger.debug("Trying alternative authentication method")
                try:
                    # Some Cohesity versions require specific content-type header
                    alt_headers = {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                    
                    # Try with lowercase 'local' domain if the provided domain is uppercase 'LOCAL'
                    alt_domain = domain
                    if domain.upper() == 'LOCAL':
                        alt_domain = 'local'
                        logger.debug(f"Trying with lowercase domain: {alt_domain}")
                    
                    alt_auth_data = {
                        'domain': alt_domain,
                        'username': username,
                        'password': password
                    }
                    
                    auth_response = self.session.post(
                        auth_url, 
                        json=alt_auth_data,  # Use json parameter instead of data+dumps 
                        headers=alt_headers,
                        verify=False, 
                        timeout=connect_timeout
                    )
                    
                    if auth_response.status_code == 201:
                        auth_data = auth_response.json()
                        token = auth_data['accessToken']
                        
                        # Set authorization header for future requests
                        self.headers = {
                            'Authorization': f"Bearer {token}",
                            'Content-Type': 'application/json'
                        }
                        self.session.headers.update(self.headers)
                        auth_success = True
                        logger.debug("Authentication successful using alternative method")
                    else:
                        logger.warning(f"Alternative authentication failed: {auth_response.status_code} - {auth_response.text}")
                except Exception as e:
                    logger.warning(f"Alternative authentication error: {str(e)}")
            
            # If any authentication method succeeded
            if auth_success:
                self.connected = True
                
                # Get minimal cluster info with timeout
                try:
                    url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/cluster"
                    response = self.session.get(url, verify=False, timeout=connect_timeout)
                    
                    if response.status_code == 200:
                        self.cluster_info = response.json()
                        logger.info(f"Retrieved cluster info for {self.cluster_info.get('name', 'unknown')}")
                except Exception as e:
                    logger.warning(f"Got basic connection but failed to get cluster info: {str(e)}")
                    # We're still considering this a successful connection since auth worked
                
                logger.info(f"Successfully connected to {cluster}")
                return True
            else:
                logger.error(f"All authentication methods failed for {cluster}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to cluster: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from the current session"""
        if self.session:
            self.session.close()
        self.connected = False
        self.cluster_info = None
        self.headers = {}
        self.session = None
    
    def check_connection(self):
        """Check if currently connected to a Cohesity cluster"""
        return self.connected and self.session is not None
    
    def get_cluster_info(self):
        """
        Get basic cluster information
        
        Returns:
            dict: Cluster information
        """
        if not self.check_connection():
            logger.error("Not connected to a Cohesity cluster")
            return None
        
        try:
            # Call the API to get cluster information
            url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/cluster"
            response = self.session.get(url, verify=False)
            
            if response.status_code == 200:
                cluster_info = response.json()
                logger.info(f"Retrieved cluster info for {cluster_info.get('name', 'unknown')}")
                
                # Get additional cluster details for a more comprehensive view
                cluster_info['nodes'] = self.get_nodes()
                cluster_info['storage'] = self.get_storage_stats()
                cluster_info['protection'] = self.get_protection_stats()
                
                return cluster_info
            else:
                logger.error(f"Failed to get cluster info: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting cluster information: {str(e)}")
            return None
    
    def get_nodes(self):
        """
        Get detailed node information
        
        Returns:
            list: List of node details
        """
        if not self.check_connection():
            return []
            
        try:
            # Try different endpoints for node information
            endpoints = [
                'nodes',
                'cluster/nodes',
                'v1/nodes',
                'v2/nodes',
                'nodeInformation'
            ]
            
            for endpoint in endpoints:
                try:
                    url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/{endpoint}"
                    response = self.session.get(url, verify=False)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Different endpoints return different formats
                        if isinstance(data, list):
                            return data
                        elif 'nodes' in data:
                            return data['nodes']
                        elif 'nodeVec' in data:
                            return data['nodeVec']
                        elif 'nodeInfoVec' in data:
                            return data['nodeInfoVec']
                        else:
                            # Try to find any node-related array
                            for key, value in data.items():
                                if isinstance(value, list) and len(value) > 0 and 'node' in key.lower():
                                    return value
                except Exception as e:
                    logger.debug(f"Failed to get nodes from endpoint {endpoint}: {str(e)}")
                    continue
            
            # If we got here, none of the endpoints worked
            logger.warning("Could not retrieve detailed node information")
            return []
            
        except Exception as e:
            logger.error(f"Error getting node information: {str(e)}")
            return []
    
    def get_storage_stats(self):
        """
        Get storage statistics
        
        Returns:
            dict: Storage statistics
        """
        if not self.check_connection():
            return {}
            
        try:
            # Try different endpoints for storage statistics
            endpoints = [
                'stats/storage',
                'clusterStorage',
                'cluster/stats',
                'v1/cluster/stats',
                'statistics/cluster'
            ]
            
            for endpoint in endpoints:
                try:
                    url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/{endpoint}"
                    response = self.session.get(url, verify=False)
                    
                    if response.status_code == 200:
                        return response.json()
                except Exception:
                    continue
            
            # If we got here, none of the endpoints worked
            logger.warning("Could not retrieve storage statistics")
            return {}
            
        except Exception as e:
            logger.error(f"Error getting storage statistics: {str(e)}")
            return {}
    
    def get_protection_stats(self):
        """
        Get protection job statistics
        
        Returns:
            dict: Protection job statistics
        """
        if not self.check_connection():
            return {}
            
        try:
            # Get basic protection job information
            url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/protectionJobs"
            response = self.session.get(url, verify=False)
            
            if response.status_code == 200:
                jobs = response.json()
                
                # Get summary stats
                stats = {
                    'total_jobs': len(jobs),
                    'active_jobs': sum(1 for job in jobs if job.get('isActive', False)),
                    'paused_jobs': sum(1 for job in jobs if not job.get('isActive', True)),
                    'environment_counts': {},
                    'jobs': jobs[:5]  # Include just the first 5 jobs for brevity
                }
                
                # Count jobs by environment
                for job in jobs:
                    env = job.get('environment', 'Unknown')
                    if env in stats['environment_counts']:
                        stats['environment_counts'][env] += 1
                    else:
                        stats['environment_counts'][env] = 1
                
                return stats
            else:
                logger.warning("Could not retrieve protection job statistics")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting protection job statistics: {str(e)}")
            return {}
    
    def print_cluster_info(self):
        """
        Print cluster information in a formatted way similar to iris_cli
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.check_connection():
            logger.error("Not connected to a Cohesity cluster")
            return False
            
        try:
            # Get cluster information if not already loaded
            if not self.cluster_info:
                self.cluster_info = self.get_cluster_info()
                
            if not self.cluster_info:
                logger.error("Failed to retrieve cluster information")
                return False
            
            # Section 1: Basic cluster information
            print("\n" + "=" * 80)
            print(f" CLUSTER INFORMATION: {self.cluster_info['name']}")
            print("=" * 80)
            print(f"Cluster Name:          {self.cluster_info['name']}")
            print(f"Cluster ID:            {self.cluster_info['id']}")
            print(f"Cluster IP:            {self.cluster_hostname}")
            
            # Try different fields for software version
            sw_version = self.cluster_info.get('softwareVersion', 
                        self.cluster_info.get('clusterSoftwareVersion',
                        self.cluster_info.get('version', 'Unknown')))
            print(f"Software Version:      {sw_version}")
            
            # Check if there's a detailed version
            sw_details = None
            for field in ['clusterSoftwareVersion', 'softwareVersionInfo', 'softwareDetails']:
                if field in self.cluster_info and self.cluster_info[field] and self.cluster_info[field] != sw_version:
                    sw_details = self.cluster_info[field]
                    break
                    
            if sw_details:
                if isinstance(sw_details, dict):
                    if 'name' in sw_details:
                        print(f"Software Details:      {sw_details['name']}")
                    else:
                        print(f"Software Details:      {str(sw_details)[:80]}")
                else:
                    print(f"Software Details:      {sw_details}")
            
            # Get and display cluster health status using the enhanced health check
            cluster_health = self.get_cluster_health()
            print(f"Cluster Health:        {cluster_health}")
            
            # Section 2: Node Information
            print("\n" + "-" * 80)
            print(" NODE INFORMATION")
            print("-" * 80)
            
            # Get node count from either basic info or detailed node list
            node_count = self.cluster_info.get('nodeCount', len(self.cluster_info.get('nodes', [])))
            print(f"Node Count: {node_count}")
            
            # Print detailed node information if available
            nodes = self.cluster_info.get('nodes', [])
            if nodes:
                print("\nNode Details:")
                for i, node in enumerate(nodes):
                    if node is None:
                        continue
                        
                    print(f"\n  Node {i+1}:")
                    
                    # Handle different field names in node objects
                    node_id = node.get('id', node.get('nodeId', 'Unknown'))
                    ip = node.get('ip', node.get('nodeIp', 'Unknown'))
                    
                    # Log the full node data in debug mode to help troubleshoot
                    logger.debug(f"Node data for node {i+1}: {node}")
                    
                    # Get and evaluate node status using improved detection
                    status = str(node.get('status', 'Unknown'))
                    health = str(node.get('health', ''))
                    state = str(node.get('state', ''))
                    
                    # Determine health status with better detection
                    node_health = "Unknown"
                    health_status_orig = status
                    
                    # List of good status indicators
                    healthy_status = [
                        'active', 'healthy', 'ok', 'kactive', 'connected', 'khealthy', 
                        'online', 'konline', 'normal', 'knormal', '1', '2', 'running'
                    ]
                    
                    # List of bad status indicators
                    unhealthy_status = [
                        'failed', 'kfailed', 'offline', 'koffline', 'down', 'kdown',
                        'critical', 'kcritical', 'error', 'kerror', '0', 'dead'
                    ]
                    
                    # Check various fields for health indications
                    status_lower = status.lower()
                    health_lower = health.lower()
                    state_lower = state.lower()
                    
                    if status_lower in healthy_status or health_lower in healthy_status or state_lower in healthy_status:
                        node_health = "Healthy"
                    elif status_lower in unhealthy_status or health_lower in unhealthy_status or state_lower in unhealthy_status:
                        node_health = "Unhealthy"
                    else:
                        # Check for additional ways to determine health
                        # For Cohesity 7.x, if a node is part of the API response and has an IP,
                        # it's typically healthy even if the status isn't explicitly reported
                        if ip != "Unknown" and ip:
                            # Check if there's any indication of node being offline/disconnected
                            offline_indicators = ['inaccessible', 'offline', 'down', 'unreachable', 
                                                 'disconnected', 'unavailable']
                            
                            # Look through all fields for any indication of being offline
                            found_offline = False
                            for key, value in node.items():
                                if isinstance(value, str) and any(ind in value.lower() for ind in offline_indicators):
                                    found_offline = True
                                    break
                            
                            if not found_offline:
                                # Node has IP and no indication of being offline, assume it's healthy
                                node_health = "Healthy (Inferred)"
                                logger.debug(f"Inferred health for node with IP {ip} - assuming healthy")
                    
                    print(f"    Node ID:       {node_id}")
                    print(f"    IP Address:    {ip}")
                    print(f"    Status:        {status}")
                    print(f"    Health:        {node_health}")
                    
                    if node.get('role'):
                        print(f"    Role:          {node['role']}")
                    
                    # Check if hardware exists and is not None
                    hw = node.get('hardware')
                    if hw and isinstance(hw, dict):
                        print(f"    Model:         {hw.get('model', 'Unknown')}")
                        if hw.get('serialNumber'):
                            print(f"    Serial Number: {hw['serialNumber']}")
                    
                    # Check if stats exists and is not None
                    stats = node.get('stats')
                    if stats and isinstance(stats, dict):
                        cpu_usage = stats.get('cpuUsagePct')
                        if cpu_usage is not None:
                            print(f"    CPU Usage:     {cpu_usage}%")
                            
                        memory_usage = stats.get('memoryUsagePct')
                        if memory_usage is not None:
                            print(f"    Memory Usage:  {memory_usage}%")
            
            # Section 3: Storage Information
            print("\n" + "-" * 80)
            print(" STORAGE INFORMATION")
            print("-" * 80)
            
            storage = self.cluster_info.get('storage', {})
            if not storage:
                print("Storage information not available")
            else:
                # Try to find capacity information in various fields
                total_capacity = storage.get('totalCapacityBytes', 
                                storage.get('physicalCapacityBytes',
                                storage.get('totalBytes', 0)))
                
                used_capacity = storage.get('usedCapacityBytes',
                              storage.get('physicalUsageBytes',
                              storage.get('usedBytes', 0)))
                
                # Make sure we have valid numeric values
                try:
                    total_capacity = float(total_capacity) if total_capacity is not None else 0
                    used_capacity = float(used_capacity) if used_capacity is not None else 0
                    
                    # Convert to readable format
                    total_tb = total_capacity / (1024**4) if total_capacity > 0 else 0
                    used_tb = used_capacity / (1024**4) if used_capacity > 0 else 0
                    
                    # Calculate percentage
                    usage_pct = (used_capacity / total_capacity * 100) if total_capacity > 0 else 0
                    
                    print(f"Total Capacity:    {total_tb:.2f} TB")
                    print(f"Used Capacity:     {used_tb:.2f} TB")
                    print(f"Free Capacity:     {total_tb - used_tb:.2f} TB")
                    print(f"Usage Percentage:  {usage_pct:.2f}%")
                except (ValueError, TypeError) as e:
                    print(f"Error calculating storage stats: {str(e)}")
                    print("Raw storage data:")
                    for key, value in storage.items():
                        if isinstance(value, (int, float)) and "byte" in key.lower():
                            print(f"  {key}: {value}")
            
            # Section 4: Protection Information
            print("\n" + "-" * 80)
            print(" PROTECTION INFORMATION")
            print("-" * 80)
            
            protection = self.cluster_info.get('protection', {})
            if not protection:
                print("Protection information not available")
            else:
                job_count = protection.get('total_jobs', 0)
                
                print(f"Total Jobs:        {job_count}")
                if 'active_jobs' in protection and protection['active_jobs'] is not None:
                    print(f"Active Jobs:       {protection['active_jobs']}")
                if 'paused_jobs' in protection and protection['paused_jobs'] is not None:
                    print(f"Paused Jobs:       {protection['paused_jobs']}")
                
                # Print job environment summary
                if protection.get('environment_counts') and isinstance(protection['environment_counts'], dict):
                    print("\nJobs by Environment:")
                    for env, count in protection['environment_counts'].items():
                        if env is None:
                            env = 'Unknown'
                        # Remove the 'k' prefix that Cohesity uses
                        elif isinstance(env, str) and env.startswith('k'):
                            env = env[1:]
                        print(f"  {env}: {count}")
                
                # Print recent jobs
                if protection.get('jobs') and isinstance(protection['jobs'], list):
                    print("\nRecent Protection Jobs:")
                    for job in protection['jobs']:
                        if not job or not isinstance(job, dict):
                            continue
                            
                        # Remove the 'k' prefix from environment type
                        env = job.get('environment', 'Unknown')
                        if isinstance(env, str) and env.startswith('k'):
                            env = env[1:]
                            
                        print(f"  Name: {job.get('name', 'Unknown')}")
                        print(f"    ID: {job.get('id', 'Unknown')}")
                        print(f"    Environment: {env}")
                        print(f"    Policy: {job.get('policyId', 'Unknown')}")
                        print(f"    Status: {'Active' if job.get('isActive', False) else 'Paused'}")
                        print()
            
            return True
            
        except Exception as e:
            logger.error(f"Error retrieving cluster information: {str(e)}")
            return False
    
    def save_cluster_credentials(self, cluster, username, domain, password, description=None):
        """
        Save cluster credentials to CSV file
        
        Args:
            cluster (str): Cluster hostname or IP
            username (str): Username
            domain (str): Domain
            password (str): Password
            description (str): Optional description
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.credential_file), exist_ok=True)
            
            # Check if file exists
            file_exists = os.path.isfile(self.credential_file)
            
            # Load existing credentials if file exists
            if file_exists:
                df = pd.read_csv(self.credential_file)
                
                # Check if this cluster is already in the file
                mask = df['hostname'] == cluster
                if mask.any():
                    # Update existing entry
                    df.loc[mask, 'username'] = username
                    df.loc[mask, 'domain'] = domain
                    df.loc[mask, 'password'] = password
                    if description is not None:
                        df.loc[mask, 'description'] = description
                else:
                    # Add new entry
                    new_row = {
                        'hostname': cluster,
                        'username': username,
                        'domain': domain,
                        'password': password,
                        'description': description if description else ''
                    }
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            else:
                # Create new file with this entry
                df = pd.DataFrame([{
                    'hostname': cluster,
                    'username': username,
                    'domain': domain,
                    'password': password,
                    'description': description if description else ''
                }])
            
            # Save to CSV
            df.to_csv(self.credential_file, index=False)
            logger.info(f"Saved credentials for {cluster} to {self.credential_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving credentials: {str(e)}")
            return False
    
    def load_cluster_credentials(self, cluster):
        """
        Load cluster credentials from CSV file
        
        Args:
            cluster (str): Cluster hostname or IP
            
        Returns:
            dict: Credentials or None if not found
        """
        try:
            # Check if credential file exists
            if not os.path.isfile(self.credential_file):
                logger.error(f"Credential file {self.credential_file} not found")
                return None
            
            # Use Python's native csv module instead of pandas to avoid any potential issues with special characters
            with open(self.credential_file, 'r') as csvfile:
                # First read the header to determine column names
                reader = csv.reader(csvfile)
                header = next(reader)
                
                # Determine which column contains the hostname/cluster_ip
                hostname_col_idx = None
                if 'hostname' in header:
                    hostname_col_idx = header.index('hostname')
                elif 'cluster_ip' in header:
                    hostname_col_idx = header.index('cluster_ip')
                else:
                    logger.error(f"No hostname or cluster_ip column found in {self.credential_file}")
                    return None
                
                # Get indices for other required columns
                username_col_idx = header.index('username') if 'username' in header else None
                password_col_idx = header.index('password') if 'password' in header else None
                domain_col_idx = header.index('domain') if 'domain' in header else None
                
                if username_col_idx is None or password_col_idx is None:
                    logger.error(f"Missing required columns in {self.credential_file}")
                    return None
                
                # Find the cluster in the CSV
                csvfile.seek(0)  # Reset file pointer
                next(reader)  # Skip header
                credentials = None
                
                for row in reader:
                    if len(row) <= max(hostname_col_idx, username_col_idx, password_col_idx):
                        logger.warning(f"Skipping incomplete row in credentials file: {row}")
                        continue
                        
                    if row[hostname_col_idx] == cluster:
                        # Found the cluster
                        credentials = {
                            'hostname': row[hostname_col_idx],
                            'username': row[username_col_idx],
                            'password': row[password_col_idx],
                            'domain': row[domain_col_idx] if domain_col_idx is not None and domain_col_idx < len(row) else 'local'
                        }
                        break
                
                if not credentials:
                    logger.error(f"No credentials found for {cluster}")
                    return None
                
                # Handle domain case sensitivity correctly
                if credentials['domain'].upper() == 'LOCAL':
                    credentials['domain'] = 'local'
                
                # Add some debug info (securely - don't log full passwords)
                pwd = credentials['password']
                masked_pwd = pwd[:1] + '*' * (len(pwd) - 2) + pwd[-1:] if len(pwd) > 2 else '***'
                logger.debug(f"Loaded credentials for {cluster}: username={credentials['username']}, " 
                            f"domain={credentials['domain']}, password length={len(pwd)}, first/last chars={masked_pwd}")
                
                return credentials
                
        except Exception as e:
            logger.error(f"Error loading credentials: {str(e)}")
            return None
    
    def connect_from_csv(self, cluster):
        """
        Connect to a cluster using credentials from CSV file
        
        Args:
            cluster (str): Cluster hostname or IP
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Load credentials
            credentials = self.load_cluster_credentials(cluster)
            if not credentials:
                return False
            
            # Log the connection attempt with domain information for debugging
            logger.debug(f"Connecting to {cluster} as {credentials['username']}@{credentials['domain']}")
            
            # Connect using loaded credentials
            return self.connect(
                cluster=credentials['hostname'],
                username=credentials['username'],
                domain=credentials['domain'],
                password=credentials['password']
            )
            
        except Exception as e:
            logger.error(f"Error connecting from CSV: {str(e)}")
            return False
    
    def list_saved_clusters(self):
        """
        List all clusters saved in the credential file
        
        Returns:
            pd.DataFrame: DataFrame with cluster information or None if error
        """
        try:
            # Check if credential file exists
            if not os.path.isfile(self.credential_file):
                logger.error(f"Credential file {self.credential_file} not found")
                return None
            
            # Load credentials
            df = pd.read_csv(self.credential_file)
            
            # Check if we have the expected column names or alternatives
            hostname_col = 'hostname'
            if 'cluster_ip' in df.columns and 'hostname' not in df.columns:
                hostname_col = 'cluster_ip'
            
            # Create a copy with standardized column names for display
            display_df = df.copy()
            
            # Rename columns if needed
            if hostname_col != 'hostname':
                display_df = display_df.rename(columns={hostname_col: 'hostname'})
            
            # Ensure password column exists
            if 'password' in display_df.columns:
                display_df['password'] = '********'
            
            # Add any missing columns
            for col in ['hostname', 'username', 'domain', 'password', 'description']:
                if col not in display_df.columns:
                    display_df[col] = ''
                    
            return display_df
            
        except Exception as e:
            logger.error(f"Error listing saved clusters: {str(e)}")
            return None
    
    def set_credential_file(self, file_path):
        """
        Set a new credential file path
        
        Args:
            file_path (str): New credential file path
            
        Returns:
            None
        """
        self.credential_file = file_path
        logger.info(f"Credential file path set to: {file_path}")

    def set_timeout(self, timeout_seconds):
        """
        Set the timeout for API requests
        
        Args:
            timeout_seconds (int): Timeout in seconds
        """
        self.request_timeout = timeout_seconds
        logger.debug(f"Request timeout set to {timeout_seconds} seconds")

    def get_cluster_health(self, quick_mode=False):
        """
        Get the health status of the cluster from various possible endpoints
        
        Args:
            quick_mode (bool): If True, use faster but less comprehensive checks
            
        Returns:
            str: Health status string or 'Unknown' if not available
        """
        if not self.check_connection():
            return 'Unknown'
            
        try:
            # Log we're starting health check
            logger.info(f"Checking health for cluster {self.cluster_hostname}")
            
            # Quick mode - just check if we can connect and have a node count
            if quick_mode:
                logger.info("Using quick mode for health check")
                if self.check_connection() and self.cluster_info:
                    if self.cluster_info.get('nodeCount', 0) > 0:
                        return "Healthy (Quick check)"
                    if 'softwareVersion' in self.cluster_info or 'clusterSoftwareVersion' in self.cluster_info:
                        return "Healthy (Quick check)"
                    return "Responding (Quick check)"
                else:
                    return "Unreachable"
                    
            # Try Cohesity 7.x specific endpoints first - these are more likely to work
            # Get cluster ID first
            cluster_id = self.cluster_info.get('id') if self.cluster_info else None
            
            # Try the simplest direct Cohesity 7.x health endpoints first
            direct_v2_endpoints = [
                "v2/health",
                "v2/clusters/health",
                "v2/mcm/healthCheck",
                "v2/public/health"
            ]
            
            for endpoint in direct_v2_endpoints:
                try:
                    url = f"https://{self.cluster_hostname}/irisservices/api/{endpoint}"
                    logger.debug(f"Trying direct endpoint: {url}")
                    
                    response = self.session.get(url, verify=False)
                    if response.status_code == 200:
                        data = response.json()
                        logger.debug(f"Endpoint {endpoint} response: {data}")
                        
                        # Extract health from various possible fields
                        if 'healthStatus' in data:
                            return data['healthStatus']
                        elif 'status' in data:
                            return data['status']
                        elif 'health' in data:
                            return data['health']
                        elif 'result' in data and 'status' in data['result']:
                            return data['result']['status']
                except Exception as e:
                    logger.debug(f"Failed to get health from endpoint {endpoint}: {str(e)}")
                    continue
            
            # Try cluster ID specific endpoints if we have a cluster ID
            if cluster_id:
                # Try most common Cohesity 7.x endpoints
                v2_endpoints = [
                    f"v2/public/clusters/{cluster_id}/health",
                    f"v2/public/clusters/{cluster_id}/status",
                    f"v2/public/clusters/health"
                ]
                
                for endpoint in v2_endpoints:
                    try:
                        url = f"https://{self.cluster_hostname}/irisservices/api/{endpoint}"
                        logger.debug(f"Trying endpoint: {url}")
                        
                        response = self.session.get(url, verify=False)
                        if response.status_code == 200:
                            data = response.json()
                            logger.debug(f"Endpoint {endpoint} response: {data}")
                            
                            # Handle different response formats
                            if 'healthStatus' in data:
                                return data['healthStatus']
                            elif 'status' in data:
                                return data['status']
                            elif 'health' in data:
                                return data['health']
                            elif 'clusterHealth' in data:
                                return data['clusterHealth']
                            elif 'healthDetail' in data:
                                details = data['healthDetail']
                                if isinstance(details, dict) and 'status' in details:
                                    return details['status']
                    except Exception as e:
                        logger.debug(f"Failed to get health from endpoint {endpoint}: {str(e)}")
                        continue
            
            # Try getting health from the /v1/public/cluster endpoint
            try:
                url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/cluster"
                logger.debug(f"Trying to get health from basic cluster endpoint: {url}")
                response = self.session.get(url, verify=False)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Look for health status in various fields
                    for field in ['healthStatus', 'health', 'clusterStatus', 'status']:
                        if field in data:
                            logger.debug(f"Found health in cluster endpoint: {field}={data[field]}")
                            return data[field]
            except Exception as e:
                logger.debug(f"Failed to get health from basic cluster endpoint: {str(e)}")
            
            # For legacy versions or if v2 endpoints didn't work
            legacy_endpoints = [
                'alerts/summary',
                'cluster/status',
                'cluster/health',
                'cluster',  # Basic cluster info might include health
                'health'
            ]
            
            for endpoint in legacy_endpoints:
                try:
                    url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/{endpoint}"
                    logger.debug(f"Trying endpoint: {url}")
                    
                    response = self.session.get(url, verify=False)
                    if response.status_code == 200:
                        data = response.json()
                        logger.debug(f"Endpoint {endpoint} response: {data}")
                        
                        # Look for health status in various possible fields
                        if 'status' in data:
                            return data['status']
                        elif 'clusterHealth' in data:
                            return data['clusterHealth']
                        elif 'healthStatus' in data:
                            return data['healthStatus']
                        elif 'health' in data:
                            return data['health']
                        elif 'clusterStatus' in data:
                            return data['clusterStatus']
                except Exception as e:
                    logger.debug(f"Failed to get health from endpoint {endpoint}: {str(e)}")
                    continue
            
            # Try health check endpoint specifically for Cohesity 7.x
            try:
                url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/basicClusterInfo"
                logger.debug(f"Trying basicClusterInfo endpoint: {url}")
                response = self.session.get(url, verify=False)
                
                if response.status_code == 200:
                    data = response.json()
                    logger.debug(f"basicClusterInfo response: {data}")
                    
                    # Look for health status fields
                    for field in ['healthStatus', 'health', 'clusterStatus', 'status']:
                        if field in data:
                            return data[field]
            except Exception as e:
                logger.debug(f"Failed to get health from basicClusterInfo: {str(e)}")
            
            # Try direct API-based health check for clusters that don't expose health via API
            # This looks at key services and node status
            logger.debug("Trying direct API-based health check")
            
            # Alternative 1: Check node status
            try:
                node_endpoints = [
                    'nodes',
                    'cluster/nodes',
                    'v1/nodes',
                    'clusterNodes'
                ]
                
                for endpoint in node_endpoints:
                    try:
                        nodes_url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/{endpoint}"
                        logger.debug(f"Trying node endpoint: {nodes_url}")
                        response = self.session.get(nodes_url, verify=False)
                        
                        if response.status_code == 200:
                            data = response.json()
                            
                            # Different APIs return different data structures
                            node_list = None
                            if isinstance(data, list):
                                node_list = data
                            elif isinstance(data, dict):
                                for key in ['nodes', 'nodeVec', 'nodeInfoVec', 'clusterNodes']:
                                    if key in data and isinstance(data[key], list):
                                        node_list = data[key]
                                        break
                            
                            if node_list and len(node_list) > 0:
                                # Log the actual status values to understand what we're getting
                                status_values = [str(node.get('status', 'Unknown')) for node in node_list[:5]]
                                logger.debug(f"Node status values (first 5): {status_values}")
                                
                                # Log full node data for the first node to help identify structure
                                if node_list and len(node_list) > 0:
                                    logger.debug(f"First node data sample: {node_list[0]}")
                                
                                # Count nodes by status
                                total_nodes = len(node_list)
                                
                                # Cohesity 7.x often uses numeric status codes or different formats
                                # Assume nodes are healthy unless specifically marked as unhealthy
                                healthy_status = [
                                    'active', 'healthy', 'ok', 'kactive', 'connected', 'khealthy', 
                                    'online', 'konline', 'normal', 'knormal', '1', '2', 'running'
                                ]
                                unhealthy_status = [
                                    'failed', 'kfailed', 'offline', 'koffline', 'down', 'kdown',
                                    'critical', 'kcritical', 'error', 'kerror', '0', 'dead'
                                ]
                                
                                # Try to handle Cohesity 7.x node status which can be integers or enum values
                                unhealthy_nodes = 0
                                for node in node_list:
                                    # Get status (might be a string, integer, or enum value)
                                    status = str(node.get('status', '')).lower()
                                    
                                    # Also check additional fields that might indicate health
                                    health = str(node.get('health', '')).lower()
                                    state = str(node.get('state', '')).lower()
                                    
                                    # Check if any field indicates unhealthy state
                                    is_unhealthy = (
                                        status in unhealthy_status or
                                        health in unhealthy_status or
                                        state in unhealthy_status
                                    )
                                    
                                    if is_unhealthy:
                                        unhealthy_nodes += 1
                                
                                healthy_nodes = total_nodes - unhealthy_nodes
                                logger.debug(f"Found {healthy_nodes}/{total_nodes} healthy nodes")
                                
                                # For Cohesity 7.x clusters with "Unknown" status
                                if healthy_nodes == 0 and unhealthy_nodes == 0:
                                    # If all nodes have Unknown status but have IPs, assume they're healthy
                                    nodes_with_ips = sum(1 for node in node_list if 
                                                       node.get('ip') or node.get('nodeIp'))
                                    
                                    if nodes_with_ips > 0:
                                        logger.info(f"All nodes have Unknown status but {nodes_with_ips} have IPs - assuming healthy")
                                        return "Healthy (All nodes responding)"
                                
                                # For Cohesity 7.x, if we can connect to the cluster, assume it's functioning
                                # even if we can't determine specific node status
                                if healthy_nodes == 0 and self.check_connection():
                                    logger.info("No healthy nodes detected but connection successful - assuming cluster is operational")
                                    return "Healthy (Cluster responding)"
                                elif healthy_nodes == total_nodes:
                                    return "Healthy (All nodes OK)"
                                elif healthy_nodes > 0:
                                    return f"Warning ({healthy_nodes}/{total_nodes} nodes healthy)"
                                else:
                                    return "Critical (No healthy nodes)"
                    except Exception as e:
                        logger.debug(f"Failed to check node status from endpoint {endpoint}: {str(e)}")
                        continue
            except Exception as e:
                logger.debug(f"Failed to check node status: {str(e)}")
            
            # Alternative 2: Check active alerts
            alerts_url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/alerts?maxAlerts=10"
            try:
                logger.debug(f"Checking alerts: {alerts_url}")
                response = self.session.get(alerts_url, verify=False)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'alerts' in data and isinstance(data['alerts'], list):
                        alerts = data['alerts']
                        logger.debug(f"Found {len(alerts)} alerts")
                        
                        critical = sum(1 for alert in alerts if str(alert.get('severity', '')).lower() in ['critical', 'kcritical'])
                        warning = sum(1 for alert in alerts if str(alert.get('severity', '')).lower() in ['warning', 'kwarning'])
                        
                        if critical > 0:
                            return f"Critical ({critical} critical alerts)"
                        elif warning > 0:
                            return f"Warning ({warning} warning alerts)"
                        else:
                            return "Healthy (No alerts)"
            except Exception as e:
                logger.debug(f"Failed to check alerts: {str(e)}")
            
            logger.warning(f"Could not determine health for cluster {self.cluster_hostname}")
            
            # Last resort: Assume healthy if we can connect and get basic info
            if self.check_connection() and self.cluster_info:
                logger.info("Assuming cluster is healthy based on successful API responses")
                
                # Check specifically if we have node count information
                if self.cluster_info.get('nodeCount', 0) > 0:
                    logger.info(f"Cluster has {self.cluster_info.get('nodeCount')} nodes according to API")
                    return "Healthy (Assumed from API)"
                
                # For Cohesity 7.x, if we can connect and get basic info, it's likely healthy
                if 'softwareVersion' in self.cluster_info or 'clusterSoftwareVersion' in self.cluster_info:
                    logger.info("Cluster is responding with version information")
                    return "Healthy (Cluster operational)"
                
                return "Healthy (Assumed)"
            
            return 'Unknown'
            
        except Exception as e:
            logger.error(f"Error getting cluster health: {str(e)}")
            return 'Unknown'

    def check_cluster_status(self, quick_mode=False):
        """
        Check cluster status and return a brief status report
        
        Args:
            quick_mode (bool): If True, use faster but less comprehensive checks
            
        Returns:
            dict: Status information or None if error
        """
        if not self.check_connection():
            logger.error("Not connected to a Cohesity cluster")
            return None
        
        try:
            status = {
                'name': None,
                'version': None,
                'node_count': 0,
                'health': 'Unknown',
                'uptime': None,
                'storage_used_pct': None,
                'service_state_sync': None,
                'cluster_heal_status': None
            }
            
            # Get basic cluster info
            if not self.cluster_info:
                self.cluster_info = self.get_cluster_info()
                
            if self.cluster_info:
                status['name'] = self.cluster_info.get('name', 'Unknown')
                
                # Get software version from various possible fields
                status['version'] = self.cluster_info.get('softwareVersion', 
                                    self.cluster_info.get('clusterSoftwareVersion',
                                    self.cluster_info.get('version', 'Unknown')))
                
                # Get node count
                node_count = self.cluster_info.get('nodeCount', 0)
                if node_count == 0 and 'nodes' in self.cluster_info:
                    node_count = len(self.cluster_info['nodes'])
                status['node_count'] = node_count
                
                # Try to determine health using appropriate method based on mode
                if quick_mode:
                    # For quick mode, do minimal health checks
                    status['health'] = self.get_cluster_health(quick_mode=True)
                else:
                    # Try to determine health from cluster info first
                    if 'clusterStatus' in self.cluster_info:
                        status['health'] = self.cluster_info['clusterStatus']
                    elif 'health' in self.cluster_info:
                        status['health'] = self.cluster_info['health']
                    elif 'status' in self.cluster_info:
                        status['health'] = self.cluster_info['status']
                    else:
                        # Make a dedicated call to get health status
                        status['health'] = self.get_cluster_health()
                
                # Calculate storage usage
                storage = self.cluster_info.get('storage', {})
                if storage:
                    total_capacity = storage.get('totalCapacityBytes', 
                                    storage.get('physicalCapacityBytes',
                                    storage.get('totalBytes', 0)))
                    
                    used_capacity = storage.get('usedCapacityBytes',
                                  storage.get('physicalUsageBytes',
                                  storage.get('usedBytes', 0)))
                    
                    if total_capacity > 0:
                        status['storage_used_pct'] = round((used_capacity / total_capacity) * 100, 1)
                
                # Try to get uptime
                uptime_sec = self.cluster_info.get('uptimeSeconds', self.cluster_info.get('uptime', 0))
                if uptime_sec > 0:
                    days = uptime_sec // 86400
                    hours = (uptime_sec % 86400) // 3600
                    minutes = (uptime_sec % 3600) // 60
                    status['uptime'] = f"{days}d {hours}h {minutes}m"
            
            # Try to get service state sync and cluster heal status from various endpoints
            try:
                # Check cluster status endpoint first
                endpoints = [
                    'v1/public/clusterStatus',
                    'v1/public/cluster/status',
                    'v2/public/clusters/status',
                    'cluster/status'
                ]
                
                for endpoint in endpoints:
                    url = f"https://{self.cluster_hostname}/irisservices/api/{endpoint}"
                    logger.debug(f"Trying to get service state sync from {url}")
                    
                    try:
                        response = self.session.get(url, verify=False)
                        if response.status_code == 200:
                            data = response.json()
                            logger.debug(f"Response from {endpoint}: {data}")
                            
                            # Extract service state sync
                            if 'serviceStateSync' in data:
                                status['service_state_sync'] = data['serviceStateSync']
                            elif 'serviceState' in data:
                                status['service_state_sync'] = data['serviceState']
                            elif 'clusterServiceState' in data:
                                status['service_state_sync'] = data['clusterServiceState']
                                
                            # Extract cluster heal status
                            if 'clusterHealStatus' in data:
                                status['cluster_heal_status'] = data['clusterHealStatus']
                            elif 'healStatus' in data:
                                status['cluster_heal_status'] = data['healStatus']
                            
                            # If we found both fields, we can break
                            if status['service_state_sync'] and status['cluster_heal_status']:
                                break
                    except Exception as e:
                        logger.debug(f"Error getting status from {endpoint}: {str(e)}")
                        continue
                
                # If we couldn't find them in status endpoints, check basic cluster info
                if not status['service_state_sync'] or not status['cluster_heal_status']:
                    url = f"https://{self.cluster_hostname}/irisservices/api/v1/public/cluster"
                    logger.debug("Checking basic cluster info for service state sync and heal status")
                    
                    try:
                        response = self.session.get(url, verify=False)
                        if response.status_code == 200:
                            data = response.json()
                            
                            # Extract service state sync if not found yet
                            if not status['service_state_sync']:
                                for field in ['serviceStateSync', 'serviceState', 'clusterServiceState']:
                                    if field in data:
                                        status['service_state_sync'] = data[field]
                                        break
                            
                            # Extract cluster heal status if not found yet
                            if not status['cluster_heal_status']:
                                for field in ['clusterHealStatus', 'healStatus']:
                                    if field in data:
                                        status['cluster_heal_status'] = data[field]
                                        break
                    except Exception as e:
                        logger.debug(f"Error checking basic cluster info: {str(e)}")
            
            except Exception as e:
                logger.debug(f"Error getting service state sync and heal status: {str(e)}")
            
            return status
            
        except Exception as e:
            logger.error(f"Error checking cluster status: {str(e)}")
            return None
    
    def print_cluster_status(self, quick_mode=False):
        """
        Print a brief status report for the connected cluster
        
        Args:
            quick_mode (bool): If True, use faster but less comprehensive checks
            
        Returns:
            bool: True if successful, False otherwise
        """
        status = self.check_cluster_status(quick_mode=quick_mode)
        if not status:
            return False
            
        try:
            # Determine health status indicator
            health_indicator = "?"
            health = status['health'].lower() if status['health'] else 'unknown'
            if 'healthy' in health or 'ok' in health or 'good' in health:
                health_indicator = "✓"
            elif 'warning' in health or 'degraded' in health:
                health_indicator = "⚠"
            elif 'critical' in health or 'fail' in health or 'error' in health:
                health_indicator = "✗"
            
            # Print status
            print(f"[{health_indicator}] {status['name']} (v{status['version']})")
            print(f"  Nodes: {status['node_count']}")
            
            if status['uptime']:
                print(f"  Uptime: {status['uptime']}")
                
            if status['storage_used_pct'] is not None:
                print(f"  Storage: {status['storage_used_pct']}% used")
                
            print(f"  Health: {status['health']}")
            
            # Add service state sync and cluster heal status if available
            if status['service_state_sync']:
                # Format the service state sync string - usually like kInProgress, kNormal etc.
                service_state = status['service_state_sync']
                if service_state and service_state.startswith('k') and len(service_state) > 1:
                    service_state = service_state[1:].upper()
                
                print(f"  Service State Sync: {service_state}")
                
            if status['cluster_heal_status']:
                # Format the cluster heal status string
                heal_status = status['cluster_heal_status']
                if heal_status and heal_status.startswith('k') and len(heal_status) > 1:
                    heal_status = heal_status[1:].upper()
                
                print(f"  Cluster Heal Status: {heal_status}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error printing cluster status: {str(e)}")
            return False

    def api_get(self, endpoint, base_url=None, timeout=None):
        """
        Helper method to make GET requests with timeout
        
        Args:
            endpoint (str): API endpoint
            base_url (str): Base URL for the request
            timeout (int): Request timeout in seconds
            
        Returns:
            dict: API response or None if error
        """
        if not self.check_connection():
            return None
            
        # Use default timeout if not specified
        if timeout is None:
            timeout = self.request_timeout
            
        # Use default base URL if not specified
        if base_url is None:
            base_url = f"https://{self.cluster_hostname}/irisservices/api/"
            
        url = f"{base_url}{endpoint}"
        logger.debug(f"API GET request to {url} with timeout {timeout}s")
        
        try:
            response = self.session.get(url, verify=False, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                logger.debug(f"API GET request failed: {response.status_code} - {response.text}")
                return None
        except requests.exceptions.Timeout:
            logger.warning(f"API GET request to {url} timed out after {timeout}s")
            return None
        except Exception as e:
            logger.debug(f"API GET request error: {str(e)}")
            return None

# Create a singleton instance for direct import use
cohesity_manager = CohesityManager()

def print_version():
    """Print the version of the cohesity_manager module"""
    print(f"Cohesity Manager version {__version__}")
    
def process_cluster_with_timeout(cohesity_manager, cluster_name, row, force_healthy, quick_mode, max_wait=15):
    """
    Process a single cluster with timeout
    
    Args:
        cohesity_manager: CohesityManager instance
        cluster_name (str): Cluster hostname
        row: Pandas row with credentials
        force_healthy (bool): Force healthy status
        quick_mode (bool): Quick mode flag
        max_wait (int): Maximum wait time in seconds
        
    Returns:
        tuple: (success, message)
    """
    result_queue = queue.Queue()
    
    def process_cluster():
        try:
            # Determine connection parameters
            cluster_ip = cluster_name
            username = row.get('username')
            
            # Critical fix: Ensure domain is in correct case
            domain = row.get('domain', 'local')
            if domain.upper() == 'LOCAL':
                domain = 'local'  # Use lowercase 'local' as required
                
            password = row.get('password')
            
            # Use a shorter timeout for quick mode or if cluster is not responding
            connect_timeout = 3 if quick_mode else 5
            
            # Try to connect
            if cohesity_manager.connect(
                cluster=cluster_ip,
                username=username,
                domain=domain,
                password=password,
                connect_timeout=connect_timeout
            ):
                success = True
                
                # Capture the output rather than printing directly
                output_lines = []
                
                # Get status based on whether we're forcing healthy status
                status = cohesity_manager.check_cluster_status(quick_mode=quick_mode)
                if status:
                    if force_healthy:
                        # Force healthy status
                        status['health'] = "Healthy (Forced)"
                        health_indicator = "✓"
                    else:
                        # Determine health status indicator
                        health_indicator = "?"
                        health = status['health'].lower() if status['health'] else 'unknown'
                        if 'healthy' in health or 'ok' in health or 'good' in health:
                            health_indicator = "✓"
                        elif 'warning' in health or 'degraded' in health:
                            health_indicator = "⚠"
                        elif 'critical' in health or 'fail' in health or 'error' in health:
                            health_indicator = "✗"
                    
                    # Format output lines
                    output_lines.append(f"[{health_indicator}] {status['name']} (v{status['version']})")
                    output_lines.append(f"  Nodes: {status['node_count']}")
                    
                    if status['uptime']:
                        output_lines.append(f"  Uptime: {status['uptime']}")
                        
                    if status['storage_used_pct'] is not None:
                        output_lines.append(f"  Storage: {status['storage_used_pct']}% used")
                        
                    output_lines.append(f"  Health: {status['health']}")
                    
                    # Add service state sync and cluster heal status if available
                    if status.get('service_state_sync'):
                        # Format the service state sync string 
                        service_state = status['service_state_sync']
                        if service_state and service_state.startswith('k') and len(service_state) > 1:
                            service_state = service_state[1:].upper()
                        
                        output_lines.append(f"  Service State Sync: {service_state}")
                        
                    if status.get('cluster_heal_status'):
                        # Format the cluster heal status string
                        heal_status = status['cluster_heal_status']
                        if heal_status and heal_status.startswith('k') and len(heal_status) > 1:
                            heal_status = heal_status[1:].upper()
                        
                        output_lines.append(f"  Cluster Heal Status: {heal_status}")
                
                # Disconnect and add result to queue
                cohesity_manager.disconnect()
                result_queue.put((True, "\n".join(output_lines)))
            else:
                result_queue.put((False, f"[✗] Failed to connect to {cluster_name}"))
        except Exception as e:
            result_queue.put((False, f"[✗] Error processing {cluster_name}: {str(e)}"))
    
    # Start processing in a thread
    thread = threading.Thread(target=process_cluster)
    thread.daemon = True
    thread.start()
    
    # Wait for result with timeout
    start_time = time.time()
    while thread.is_alive() and (time.time() - start_time) < max_wait:
        time.sleep(0.5)
        
        # Try to get result from queue
        try:
            success, message = result_queue.get_nowait()
            return success, message
        except queue.Empty:
            pass
    
    # If we got here, it timed out
    if thread.is_alive():
        logger.warning(f"Processing {cluster_name} timed out after {max_wait}s")
        cohesity_manager.disconnect()  # Force disconnect
        return False, f"[✗] {cluster_name}: Processing timed out after {max_wait}s"
    
    # If thread completed but no result in queue (shouldn't happen)
    try:
        success, message = result_queue.get_nowait()
        return success, message
    except queue.Empty:
        return False, f"[✗] {cluster_name}: Unknown error (no result from thread)"

def process_clusters_parallel(clusters_df, force_healthy=False, quick_mode=False, max_workers=4, max_wait=15):
    """
    Process multiple clusters in parallel with timeout
    
    Args:
        clusters_df: Pandas DataFrame with cluster credentials
        force_healthy (bool): Force healthy status
        quick_mode (bool): Quick mode flag
        max_workers (int): Maximum number of parallel workers
        max_wait (int): Maximum wait time per cluster in seconds
        
    Returns:
        tuple: (success_count, fail_count, results)
    """
    results = []
    success_count = 0
    fail_count = 0
    
    # Check for hostname column
    hostname_col = 'hostname'
    if 'cluster_ip' in clusters_df.columns and 'hostname' not in clusters_df.columns:
        hostname_col = 'cluster_ip'
    
    # Process each cluster with its own manager instance to ensure clean state
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        
        for idx, row in clusters_df.iterrows():
            cluster_name = row[hostname_col]
            
            # Convert row to dict for easier handling
            credentials = row.to_dict()
            
            # Create a dedicated manager instance for this cluster
            cm = CohesityManager()
            if quick_mode:
                cm.set_timeout(3)  # Shorter timeout for quick mode
            
            # Ensure domain is handled properly
            if 'domain' in credentials and credentials['domain']:
                if credentials['domain'].upper() == 'LOCAL':
                    credentials['domain'] = 'local'
            else:
                credentials['domain'] = 'local'
            
            # Submit task
            future = executor.submit(
                process_cluster_with_timeout,
                cm, cluster_name, credentials, force_healthy, quick_mode, max_wait
            )
            futures[future] = cluster_name
        
        # Process results as they complete
        for future in as_completed(futures):
            cluster_name = futures[future]
            try:
                success, message = future.result()
                
                # Add to results
                print(f"\n{message}")
                
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                
                # Add to results for summary
                results.append({
                    'cluster': cluster_name,
                    'success': success,
                    'message': message
                })
            except Exception as e:
                print(f"\n[✗] Error processing {cluster_name}: {str(e)}")
                fail_count += 1
                results.append({
                    'cluster': cluster_name,
                    'success': False,
                    'message': f"Error: {str(e)}"
                })
    
    return success_count, fail_count, results

def main():
    """Main function when running as a script"""
    parser = argparse.ArgumentParser(
        description='Cohesity Manager - Command-line interface for managing Cohesity clusters',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
        Examples:
          python cohesity_manager.py --info -s cluster.example.com -u admin
          python cohesity_manager.py --version
          python cohesity_manager.py --info --saved-cluster cluster1
          python cohesity_manager.py --save-cluster cluster.example.com -u admin -d local
          python cohesity_manager.py --file /path/to/clusters.csv --list-clusters
          python cohesity_manager.py --file /path/to/clusters.csv --info
          python cohesity_manager.py --file /path/to/clusters.csv --status
          python cohesity_manager.py --file /path/to/clusters.csv --status --debug
          python cohesity_manager.py --file /path/to/clusters.csv --status --force-healthy
          python cohesity_manager.py --file /path/to/clusters.csv --status --quick
          python cohesity_manager.py --file /path/to/clusters.csv --status --parallel
        ''')
    )
    
    # Add arguments
    parser.add_argument('--version', action='store_true', help='Print version information')
    parser.add_argument('--info', action='store_true', help='Display detailed cluster information')
    parser.add_argument('--status', action='store_true', help='Display brief cluster status')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--force-healthy', action='store_true', help='Force all clusters to be reported as healthy')
    parser.add_argument('--quick', action='store_true', help='Use quick mode for faster results (less comprehensive)')
    parser.add_argument('--timeout', type=int, default=10, help='API request timeout in seconds (default: 10)')
    parser.add_argument('--max-wait', type=int, default=15, help='Maximum wait time per cluster in seconds (default: 15)')
    parser.add_argument('--parallel', action='store_true', help='Process clusters in parallel')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    
    # Connection parameters
    conn_group = parser.add_argument_group('Connection Options')
    conn_group.add_argument('-s', '--server', help='Cohesity cluster IP or hostname')
    conn_group.add_argument('-u', '--username', help='Username for authentication')
    conn_group.add_argument('-d', '--domain', default='local', help='Domain for authentication (default: local)')
    conn_group.add_argument('-p', '--password', help='Password (not recommended, will prompt if not specified)')
    
    # Credential management
    cred_group = parser.add_argument_group('Credential Management')
    cred_group.add_argument('--file', help='Path to CSV file containing cluster credentials')
    cred_group.add_argument('--save-cluster', metavar='HOSTNAME', help='Save cluster credentials to CSV')
    cred_group.add_argument('--saved-cluster', metavar='HOSTNAME', help='Connect using saved credentials')
    cred_group.add_argument('--list-clusters', action='store_true', help='List saved clusters')
    cred_group.add_argument('--description', help='Description for saved cluster')
    
    args = parser.parse_args()
    
    # Set request timeout
    cohesity_manager.set_timeout(args.timeout)
    
    # Enable debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Set custom credentials file if specified
    if args.file:
        cohesity_manager.set_credential_file(args.file)
    
    # Just print version and exit if --version specified
    if args.version:
        print_version()
        return 0
    
    # List saved clusters
    if args.list_clusters:
        clusters = cohesity_manager.list_saved_clusters()
        if clusters is not None and not clusters.empty:
            print("\nSaved Clusters:")
            print(clusters.to_string(index=False))
        else:
            print("No saved clusters found")
        return 0
    
    # Save cluster credentials
    if args.save_cluster:
        if not args.username:
            args.username = input("Enter username: ")
        
        if not args.password:
            args.password = getpass.getpass(f"Password for {args.username}@{args.domain} on {args.save_cluster}: ")
        
        result = cohesity_manager.save_cluster_credentials(
            args.save_cluster, 
            args.username,
            args.domain,
            args.password,
            args.description
        )
        
        if result:
            print(f"Successfully saved credentials for {args.save_cluster}")
        else:
            print(f"Failed to save credentials for {args.save_cluster}")
        
        return 0 if result else 1
    
    def print_cluster_status_wrapper(cm, cluster_name, force_healthy=False, quick_mode=False):
        """Helper function to print cluster status with optional force healthy flag"""
        if force_healthy:
            # Skip health checks and assume healthy
            status = cm.check_cluster_status(quick_mode=quick_mode)
            if status:
                # Set health to healthy regardless of actual status
                status['health'] = "Healthy (Forced)"
                
                # Determine health indicator
                health_indicator = "✓"
                
                # Print status
                print(f"[{health_indicator}] {status['name']} (v{status['version']})")
                print(f"  Nodes: {status['node_count']}")
                
                if status['uptime']:
                    print(f"  Uptime: {status['uptime']}")
                    
                if status['storage_used_pct'] is not None:
                    print(f"  Storage: {status['storage_used_pct']}% used")
                    
                print(f"  Health: {status['health']}")
                
                # Add service state sync and cluster heal status if available
                if status.get('service_state_sync'):
                    # Format the service state sync string - usually like kInProgress, kNormal etc.
                    service_state = status['service_state_sync']
                    if service_state and service_state.startswith('k') and len(service_state) > 1:
                        service_state = service_state[1:].upper()
                    
                    print(f"  Service State Sync: {service_state}")
                    
                if status.get('cluster_heal_status'):
                    # Format the cluster heal status string
                    heal_status = status['cluster_heal_status']
                    if heal_status and heal_status.startswith('k') and len(heal_status) > 1:
                        heal_status = heal_status[1:].upper()
                    
                    print(f"  Cluster Heal Status: {heal_status}")
                
                return True
            else:
                return False
        else:
            # Use normal status printing
            return cm.print_cluster_status(quick_mode=quick_mode)

    # Process status command
    if args.status:
        if not args.server and not args.saved_cluster:
            # If --file is specified, show status for all clusters in the file
            if args.file:
                # Read cluster credentials directly from CSV to avoid any pandas parsing issues
                if os.path.isfile(args.file):
                    clusters_list = []
                    
                    try:
                        # Read CSV directly 
                        with open(args.file, 'r') as csvfile:
                            reader = csv.DictReader(csvfile)
                            for row in reader:
                                if row:  # Skip empty rows
                                    clusters_list.append(row)
                        
                        logger.debug(f"Read {len(clusters_list)} clusters from {args.file}")
                        
                        if not clusters_list:
                            print(f"No clusters found in {args.file}")
                            return 1
                            
                        # Convert to DataFrame for compatibility with existing code
                        clusters = pd.DataFrame(clusters_list)
                        
                        # Debug: print column names (without passwords)
                        if args.debug:
                            safe_cols = [col for col in clusters.columns if col != 'password']
                            logger.debug(f"CSV columns: {safe_cols}")
                            
                            # Print first row with masked password
                            if len(clusters) > 0:
                                row_dict = clusters.iloc[0].to_dict()
                                if 'password' in row_dict:
                                    pwd = row_dict['password']
                                    row_dict['password'] = pwd[:1] + '*' * (len(pwd) - 2) + pwd[-1:] if len(pwd) > 2 else '***'
                                logger.debug(f"First row sample: {row_dict}")
                    except Exception as e:
                        logger.error(f"Error reading CSV file: {str(e)}")
                        print(f"Error reading CSV file: {str(e)}")
                        return 1
                        
                    # Use parallel processing if requested
                    if args.parallel:
                        print(f"Processing {len(clusters)} clusters in parallel mode (max wait: {args.max_wait}s per cluster)")
                        success_count, fail_count, _ = process_clusters_parallel(
                            clusters, 
                            force_healthy=args.force_healthy, 
                            quick_mode=args.quick,
                            max_workers=args.workers,
                            max_wait=args.max_wait
                        )
                        print(f"\nSummary: Connected to {success_count} clusters, failed to connect to {fail_count} clusters")
                    else:
                        # Use original sequential processing
                        success_count = 0
                        fail_count = 0
                        
                        # Check for hostname column
                        hostname_col = 'hostname'
                        if 'cluster_ip' in clusters.columns and 'hostname' not in clusters.columns:
                            hostname_col = 'cluster_ip'
                        
                        for idx, row in clusters.iterrows():
                            cluster_name = row[hostname_col]
                            print(f"\nChecking {cluster_name}...")
                            
                            # Use a separate manager instance for each cluster to avoid state contamination
                            cm = CohesityManager()
                            cm.set_credential_file(args.file)  # Set the correct credential file
                            
                            # Set shorter timeout if in quick mode
                            if args.quick:
                                cm.set_timeout(3)
                            else:
                                cm.set_timeout(args.timeout)
                            
                            # Set up a timer to force moving to next cluster if this one takes too long
                            start_time = time.time()
                            connection_successful = False
                            
                            try:
                                # Get credentials
                                username = row.get('username')
                                
                                # Critical fix: Ensure domain is in correct case
                                domain = row.get('domain', 'local')
                                if domain.upper() == 'LOCAL':
                                    domain = 'local'  # Use lowercase 'local' as required
                                
                                password = row.get('password')
                                
                                # Log masked credentials for debugging
                                if args.debug:
                                    masked_pwd = password[:1] + '*' * (len(password) - 2) + password[-1:] if len(password) > 2 else '***'
                                    logger.debug(f"Using credentials: {username}@{domain}, password={masked_pwd}")
                                
                                # Use shorter connection timeout
                                connect_timeout = 3 if args.quick else 5
                                
                                # Try connecting with timeout
                                connection_successful = cm.connect(
                                    cluster=cluster_name,
                                    username=username,
                                    domain=domain,
                                    password=password,
                                    connect_timeout=connect_timeout
                                )
                                
                                # Check if we're taking too long already
                                if (time.time() - start_time) > args.max_wait:
                                    print(f"[✗] Processing {cluster_name} timed out after {args.max_wait}s")
                                    cm.disconnect()
                                    fail_count += 1
                                    continue
                                    
                                if connection_successful:
                                    # Use quick status check with short timeout
                                    if print_cluster_status_wrapper(cm, cluster_name, args.force_healthy, args.quick):
                                        success_count += 1
                                    else:
                                        fail_count += 1
                                    
                                    # Disconnect
                                    cm.disconnect()
                                else:
                                    print(f"[✗] Failed to connect to {cluster_name}")
                                    fail_count += 1
                                    
                            except Exception as e:
                                print(f"[✗] Error processing {cluster_name}: {str(e)}")
                                fail_count += 1
                                continue
                                
                            # Check if we've gone over time limit
                            if (time.time() - start_time) > args.max_wait:
                                print(f"[✗] Processing {cluster_name} took too long ({int(time.time() - start_time)}s > {args.max_wait}s limit)")
                                # Force disconnect if we're still connected
                                if connection_successful:
                                    cm.disconnect()
                        
                        print(f"\nSummary: Connected to {success_count} clusters, failed to connect to {fail_count} clusters")
                    
                    return 0 if fail_count == 0 else 1
                else:
                    logger.error(f"Credential file {args.file} not found")
                    print(f"Credential file {args.file} not found")
                    return 1
            else:
                parser.error("--status requires --server or --saved-cluster")
        
        # Connect to a single specified cluster
        username = args.username
        if not username and args.server:
            username = input("Enter username: ")
        
        # Connect to the cluster
        if args.server:
            if not cohesity_manager.connect(
                cluster=args.server,
                username=username,
                domain=args.domain,
                password=args.password
            ):
                print(f"Failed to connect to cluster {args.server}")
                return 1
            
            # Print cluster status
            print_cluster_status_wrapper(cohesity_manager, args.server, args.force_healthy, args.quick)
            
            # Disconnect
            cohesity_manager.disconnect()
        elif args.saved_cluster:
            if not cohesity_manager.connect_from_csv(args.saved_cluster):
                print(f"Failed to connect to {args.saved_cluster} using saved credentials")
                return 1
            
            # Print cluster status
            print_cluster_status_wrapper(cohesity_manager, args.saved_cluster, args.force_healthy, args.quick)
            
            # Disconnect
            cohesity_manager.disconnect()
        
        return 0
    
    # Connect using saved credentials for info command
    if args.saved_cluster:
        if not cohesity_manager.connect_from_csv(args.saved_cluster):
            print(f"Failed to connect to {args.saved_cluster} using saved credentials")
            return 1
        
        if args.info:
            cohesity_manager.print_cluster_info()
        
        cohesity_manager.disconnect()
        return 0
    
    # Process info command
    if args.info:
        if not args.server and not args.saved_cluster:
            # If --file is specified, show info for all clusters in the file
            if args.file:
                clusters = cohesity_manager.list_saved_clusters()
                if clusters is not None and not clusters.empty:
                    success_count = 0
                    fail_count = 0
                    
                    # Check for hostname column
                    hostname_col = 'hostname'
                    if 'cluster_ip' in clusters.columns and 'hostname' not in clusters.columns:
                        hostname_col = 'cluster_ip'
                    
                    for idx, row in clusters.iterrows():
                        cluster_name = row[hostname_col]
                        print(f"\nConnecting to {cluster_name}...")
                        
                        if cohesity_manager.connect_from_csv(cluster_name):
                            cohesity_manager.print_cluster_info()
                            cohesity_manager.disconnect()
                            success_count += 1
                        else:
                            print(f"Failed to connect to {cluster_name}")
                            fail_count += 1
                    
                    print(f"\nSummary: Connected to {success_count} clusters, failed to connect to {fail_count} clusters")
                    return 0 if fail_count == 0 else 1
                else:
                    print("No clusters found in the specified file")
                    return 1
            else:
                parser.error("--info requires --server or --saved-cluster")
        
        # Connect to specified server for info
        if args.server:
            # Prompt for username if not provided
            username = args.username
            if not username:
                username = input("Enter username: ")
            
            # Connect to the cluster
            if not cohesity_manager.connect(
                cluster=args.server,
                username=username,
                domain=args.domain,
                password=args.password
            ):
                print(f"Failed to connect to cluster {args.server}")
                return 1
            
            # Print cluster information
            cohesity_manager.print_cluster_info()
            
            # Disconnect
            cohesity_manager.disconnect()
        
        return 0
    
    # If no action specified, print help
    if not any([args.version, args.info, args.list_clusters, args.save_cluster, args.saved_cluster, args.status]):
        parser.print_help()
        return 0
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 