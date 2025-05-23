#!/usr/bin/env python3
"""
HPE iLO Power Management Script

This script provides functionality to monitor and manage power states 
of HPE servers through their iLO interfaces via the Redfish API.

Features:
- Check basic and detailed status of servers
- Monitor power consumption periodically
- Dedicated CPU utilization collection for newer iLO versions
- Get and set server power management policies
- Power on/off servers (graceful or forced)
- Periodically collect and save monitoring data to CSV files
- Support for both single server and batch (CSV) operations

Usage examples:
  python ilo_power_0.18.py -i 10.0.0.100 -u Administrator -s
  python ilo_power_0.18.py -f servers.csv -s
  python ilo_power_0.18.py -f servers.csv -s --details
  python ilo_power_0.18.py -f servers.csv --power-watts
  python ilo_power_0.18.py -f servers.csv --monitor-power --interval 15
  python ilo_power_0.18.py -f servers.csv --get-cpu
  python ilo_power_0.18.py -f servers.csv --get-power-policy
  python ilo_power_0.18.py -f servers.csv --set-power-policy "Dynamic Power Savings"
  python ilo_power_0.18.py -f servers.csv --power-on
  python ilo_power_0.18.py -f servers.csv --power-off

Version: 0.18
"""

# Import all necessary libraries
import requests
import json
import urllib3
import pandas as pd
import argparse
from concurrent.futures import ThreadPoolExecutor
import sys
import getpass
import os
import csv
import time
import datetime
import traceback

# Disable SSL warnings (for environments with self-signed certificates)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def read_ilo_systems_from_csv(csv_file_path, debug=False):
    """
    Read iLO system details from a CSV file using pandas with improved column detection.
    
    Args:
        csv_file_path (str): Path to the CSV file containing iLO system details
        debug (bool): Whether to print debug information during parsing
        
    Returns:
        list: A list of dictionaries containing iLO system details (ip, username, password)
    """
    try:
        if debug:
            print(f"Reading CSV file: {csv_file_path}")
        
        # Try to read the first few lines of the file to inspect it
        with open(csv_file_path, 'rb') as f:
            sample = f.read(4096)
        
        if debug:
            print(f"Sample of file content (first 100 bytes):\n{sample[:100]}")
        
        # Try different approaches to read the file
        try:
            # First attempt: standard read_csv
            df = pd.read_csv(csv_file_path)
            if debug:
                print("Successfully read CSV file with standard method")
        except Exception as e1:
            if debug:
                print(f"Standard read failed: {e1}")
            try:
                # Second attempt: with encoding detection
                df = pd.read_csv(csv_file_path, encoding='utf-16')
                if debug:
                    print("Successfully read CSV file with UTF-16 encoding")
            except Exception as e2:
                if debug:
                    print(f"UTF-16 read failed: {e2}")
                try:
                    # Third attempt: with encoding detection and different separator
                    df = pd.read_csv(csv_file_path, encoding='utf-16', sep=';')
                    if debug:
                        print("Successfully read CSV file with UTF-16 encoding and semicolon separator")
                except Exception as e3:
                    if debug:
                        print(f"UTF-16 with semicolon separator failed: {e3}")
                    # Fourth attempt: low-level approach
                    if debug:
                        print("Trying low-level file reading approach...")
                    return read_ilo_systems_manually(csv_file_path, debug)
        
        if debug:
            print(f"Detected columns: {df.columns.tolist()}")
        
        # Try to map column names to expected fields
        col_mapping = {}
        required_fields = ['ip', 'username', 'password']
        
        for col in df.columns:
            # Try different string transformations to match required fields
            col_clean = col.strip().strip('"').lower()
            for field in required_fields:
                if field in col_clean or col_clean in field:
                    col_mapping[field] = col
                    break
        
        if debug:
            print(f"Column mapping found: {col_mapping}")
        
        # Check if all required fields were mapped
        missing_fields = [field for field in required_fields if field not in col_mapping]
        if missing_fields:
            if debug:
                print(f"Could not map these required fields: {missing_fields}")
            # Try to guess based on position
            if len(df.columns) >= 3:
                if debug:
                    print("Trying to map by position...")
                # Assume first column is IP, second is username, third is password
                col_mapping = {
                    'ip': df.columns[0],
                    'username': df.columns[1],
                    'password': df.columns[2]
                }
                if debug:
                    print(f"Mapped by position: {col_mapping}")
        
        # Extract data using the mapping
        ilo_systems = []
        for _, row in df.iterrows():
            system = {}
            for field, col in col_mapping.items():
                value = str(row[col]).strip().strip('"')
                if value and value.lower() not in ['nan', 'none']:
                    system[field] = value
            
            # Only include complete systems
            if all(field in system for field in required_fields):
                ilo_systems.append(system)
        
        if ilo_systems:
            print(f"Loaded {len(ilo_systems)} iLO systems from CSV file.")
            return ilo_systems
        else:
            print("No valid systems found with column mapping.")
            return []
            
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        if debug:
            print(traceback.format_exc())
        return []

def read_ilo_systems_manually(csv_file_path, debug=False):
    """
    Manual fallback method to read the CSV file when standard methods fail.
    
    Args:
        csv_file_path (str): Path to the CSV file containing iLO system details
        debug (bool): Whether to print debug information during parsing
        
    Returns:
        list: A list of dictionaries containing iLO system details (ip, username, password)
    """
    ilo_systems = []
    
    # Try different encodings
    for encoding in ['utf-8', 'utf-16', 'utf-16-le', 'latin-1']:
        try:
            if debug:
                print(f"Trying manual read with {encoding} encoding...")
            with open(csv_file_path, 'r', encoding=encoding) as file:
                lines = file.readlines()
                
                # Skip empty lines
                lines = [line.strip() for line in lines if line.strip()]
                
                if not lines:
                    continue
                
                # Determine separator (comma or semicolon)
                separator = ','
                for sep in [',', ';']:
                    # Check if using this separator gives us at least 3 fields in the first line
                    if len(lines[0].split(sep)) >= 3:
                        separator = sep
                        if debug:
                            print(f"Using separator: '{sep}'")
                        break
                
                # Process headers and determine column indices
                headers = lines[0].split(separator)
                headers = [h.strip().strip('"').lower() for h in headers]
                
                if debug:
                    print(f"Manual read headers: {headers}")
                
                # Find indices for required fields using fuzzy matching
                ip_idx = username_idx = password_idx = None
                
                for i, header in enumerate(headers):
                    if 'ip' in header:
                        ip_idx = i
                    elif 'user' in header or 'name' in header:
                        username_idx = i
                    elif 'pass' in header:
                        password_idx = i
                
                # If we couldn't find all required fields, try position-based approach
                if ip_idx is None or username_idx is None or password_idx is None:
                    if len(headers) >= 3:
                        ip_idx, username_idx, password_idx = 0, 1, 2
                    else:
                        continue
                
                if debug:
                    print(f"Using column indices: IP={ip_idx}, Username={username_idx}, Password={password_idx}")
                
                # Process data rows
                for i in range(1, len(lines)):
                    fields = lines[i].split(separator)
                    fields = [f.strip().strip('"') for f in fields]
                    
                    if len(fields) > max(ip_idx, username_idx, password_idx):
                        ip = fields[ip_idx]
                        username = fields[username_idx]
                        password = fields[password_idx]
                        
                        if ip and username and password:
                            ilo_systems.append({
                                'ip': ip,
                                'username': username,
                                'password': password
                            })
                
                if ilo_systems:
                    print(f"Successfully parsed {len(ilo_systems)} systems manually.")
                    return ilo_systems
                    
        except Exception as e:
            if debug:
                print(f"Manual read with {encoding} failed: {str(e)}")
            continue
    
    print("Failed to read systems manually with any encoding.")
    return []

def save_power_data_to_csv(csv_path, timestamp, total_watts, avg_watts, avg_cpu_load, valid_readings, total_servers):
    """
    Save power consumption and CPU load data to a CSV file with timestamp.
    
    Args:
        csv_path (str): Path to the CSV file to save data to
        timestamp (str): Formatted timestamp for the current reading
        total_watts (float): Total power consumption in watts
        avg_watts (float): Average power consumption per server in watts
        avg_cpu_load (float or None): Average CPU load percentage, or None if not available
        valid_readings (int): Number of servers with valid power readings
        total_servers (int): Total number of servers in the list
        
    Returns:
        str: Path to the CSV file
    """
    try:
        # Check if file exists to determine if we need to write headers
        file_exists = os.path.isfile(csv_path)
        
        # Open file in append mode
        with open(csv_path, 'a', newline='') as csvfile:
            fieldnames = ['timestamp', 'total_power_watts', 'avg_power_watts', 'avg_cpu_load', 'valid_readings', 'total_servers']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            # Write data row
            writer.writerow({
                'timestamp': timestamp,
                'total_power_watts': f"{total_watts:.2f}",
                'avg_power_watts': f"{avg_watts:.2f}",
                'avg_cpu_load': f"{avg_cpu_load:.2f}" if avg_cpu_load is not None else "Unknown",
                'valid_readings': valid_readings,
                'total_servers': total_servers
            })
        
        return csv_path
    except Exception as e:
        print(f"Error saving power data to CSV: {str(e)}")
        return None

def get_system_power_watts(system, max_retries=3, retry_delay=2):
    """
    Get just the power consumption in watts for a system, with retries.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        max_retries (int): Maximum number of retry attempts for failed operations
        retry_delay (int): Delay in seconds between retry attempts
        
    Returns:
        float or None: Power consumption in watts if successful, None otherwise
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    # Create a session for reusing the same connection
    session = requests.Session()
    session.auth = (username, password)
    session.verify = False  # Ignore SSL verification
    
    # For identification in output
    identifier = ""
    try:
        # Get system info for identification
        sys_url = f"https://{ip}/redfish/v1/Systems/1"
        sys_response = session.get(sys_url, timeout=30)
        if sys_response.status_code == 200:
            sys_data = sys_response.json()
            asset_tag = sys_data.get("AssetTag", "")
            serial_number = sys_data.get("SerialNumber", "")
            identifier = asset_tag if asset_tag else serial_number
    except Exception:
        pass
    
    # Get server identifier for output messages
    id_str = f" ({identifier})" if identifier else ""
    
    # Try to get power data with retries
    watts = None
    retry_count = 0
    last_error = None
    
    while watts is None and retry_count <= max_retries:
        if retry_count > 0:
            # Only show retry message after first attempt
            print(f"Retry {retry_count}/{max_retries} for server at {ip}{id_str}...")
            time.sleep(retry_delay)  # Wait before retry
            
        try:
            # First try the standard Chassis power endpoint
            power_url = f"https://{ip}/redfish/v1/Chassis/1/Power"
            power_response = session.get(power_url, timeout=30)
            
            if power_response.status_code == 200:
                power_data = power_response.json()
                if "PowerControl" in power_data and len(power_data["PowerControl"]) > 0:
                    power_control = power_data["PowerControl"][0]
                    watts = power_control.get("PowerConsumedWatts")
                    
            if watts is None:
                # Try alternative endpoint
                power_url = f"https://{ip}/redfish/v1/Chassis/1/PowerSubsystem/PowerMetrics"
                power_response = session.get(power_url, timeout=30)
                
                if power_response.status_code == 200:
                    power_data = power_response.json()
                    watts = power_data.get("PowerWatts")
        except Exception as e:
            last_error = str(e)
        
        retry_count += 1
    
    # Print result and return the value
    if watts is not None:
        print(f"Server at {ip}{id_str}: {watts}W")
        return watts
    else:
        error_msg = f": {last_error}" if last_error else ""
        print(f"Server at {ip}{id_str}: Unable to retrieve power consumption after {max_retries} retries{error_msg}")
        return None

def get_system_power_and_cpu(system, max_retries=3, retry_delay=2, debug=False):
    """
    Get power consumption in watts and CPU load percentage for a system.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        max_retries (int): Maximum number of retry attempts for failed operations
        retry_delay (int): Delay in seconds between retry attempts
        debug (bool): Whether to print debug information
        
    Returns:
        dict: Updated system dictionary with watts and cpu_load values added
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    # Create a session for reusing the same connection
    session = requests.Session()
    session.auth = (username, password)
    session.verify = False  # Ignore SSL verification
    
    # For identification in output
    identifier = ""
    try:
        # Get system info for identification
        sys_url = f"https://{ip}/redfish/v1/Systems/1"
        sys_response = session.get(sys_url, timeout=30)
        if sys_response.status_code == 200:
            sys_data = sys_response.json()
            asset_tag = sys_data.get("AssetTag", "")
            serial_number = sys_data.get("SerialNumber", "")
            identifier = asset_tag if asset_tag else serial_number
            
            # First, try to get CPU utilization directly from OEM extensions
            if "Oem" in sys_data:
                oem_data = sys_data.get("Oem", {})
                # Check for HPE or HP specific extensions
                hpe_data = None
                if "Hpe" in oem_data:
                    hpe_data = oem_data.get("Hpe", {})
                elif "Hp" in oem_data:
                    hpe_data = oem_data.get("Hp", {})
                
                if hpe_data and "ProcessorUtilization" in hpe_data:
                    cpu_load = hpe_data.get("ProcessorUtilization")
                    # Store in the system dict for return
                    system["cpu_load"] = cpu_load
    except Exception:
        pass
    
    # Get server identifier for output messages
    id_str = f" ({identifier})" if identifier else ""
    
    # Try to get power data with retries
    watts = None
    retry_count = 0
    last_error = None
    
    while watts is None and retry_count <= max_retries:
        if retry_count > 0:
            # Only show retry message after first attempt
            print(f"Retry {retry_count}/{max_retries} for server at {ip}{id_str}...")
            time.sleep(retry_delay)  # Wait before retry
            
        try:
            # First try the standard Chassis power endpoint
            power_url = f"https://{ip}/redfish/v1/Chassis/1/Power"
            power_response = session.get(power_url, timeout=30)
            
            if power_response.status_code == 200:
                power_data = power_response.json()
                if "PowerControl" in power_data and len(power_data["PowerControl"]) > 0:
                    power_control = power_data["PowerControl"][0]
                    watts = power_control.get("PowerConsumedWatts")
                    
            if watts is None:
                # Try alternative endpoint
                power_url = f"https://{ip}/redfish/v1/Chassis/1/PowerSubsystem/PowerMetrics"
                power_response = session.get(power_url, timeout=30)
                
                if power_response.status_code == 200:
                    power_data = power_response.json()
                    watts = power_data.get("PowerWatts")
        except Exception as e:
            last_error = str(e)
        
        retry_count += 1
    
    # If CPU load wasn't found in the main system info, try other approaches
    if "cpu_load" not in system or system["cpu_load"] is None:
        cpu_load = None
        retry_count = 0
        
        while cpu_load is None and retry_count <= max_retries:
            if retry_count > 0:
                # Only show retry message after first attempt
                print(f"Retry {retry_count}/{max_retries} for CPU load at {ip}{id_str}...")
                time.sleep(retry_delay)  # Wait before retry
            
            try:
                # Try performance metrics endpoint
                perf_url = f"https://{ip}/redfish/v1/Systems/1/Metrics"
                if debug:
                    print(f"Attempting to retrieve CPU data from {ip} via endpoint: {perf_url}")
                perf_response = session.get(perf_url, timeout=30)
                if perf_response.status_code == 200:
                    perf_data = perf_response.json()
                    if "CPUUtilization" in perf_data:
                        cpu_load = perf_data.get("CPUUtilization")
                        if debug:
                            print(f"Found CPU utilization via Systems/Metrics: {cpu_load}%")
                        break
                elif debug:
                    print(f"Failed with status code: {perf_response.status_code}")
            except Exception as e:
                if debug:
                    print(f"Error retrieving CPU data from Metrics endpoint: {str(e)}")
                pass
            
            # Try newer iLO firmware ProcessorMetrics endpoint
            try:
                metrics_url = f"https://{ip}/redfish/v1/Systems/1/ProcessorSummary/ProcessorMetrics"
                if debug:
                    print(f"Attempting to retrieve CPU data from {ip} via endpoint: {metrics_url}")
                metrics_response = session.get(metrics_url, timeout=30)
                if metrics_response.status_code == 200:
                    metrics_data = metrics_response.json()
                    if "TotalCorePercent" in metrics_data:
                        cpu_load = metrics_data.get("TotalCorePercent")
                        if debug:
                            print(f"Found CPU utilization via ProcessorMetrics: {cpu_load}%")
                        break
                elif debug:
                    print(f"Failed with status code: {metrics_response.status_code}")
            except Exception as e:
                if debug:
                    print(f"Error retrieving CPU data from ProcessorMetrics endpoint: {str(e)}")
                pass
            
            # Try HPE ProLiant specific endpoints
            try:
                proliant_url = f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/ProcessorCollection"
                if debug:
                    print(f"Attempting to retrieve CPU data from {ip} via HPE endpoint: {proliant_url}")
                proliant_response = session.get(proliant_url, timeout=30)
                if proliant_response.status_code == 200:
                    proliant_data = proliant_response.json()
                    if "Members" in proliant_data and len(proliant_data["Members"]) > 0:
                        cpu_loads = []
                        for cpu in proliant_data["Members"]:
                            if "ProcessorUtilization" in cpu:
                                cpu_loads.append(cpu.get("ProcessorUtilization"))
                        if cpu_loads:
                            cpu_load = sum(cpu_loads) / len(cpu_loads)
                            if debug:
                                print(f"Found CPU utilization via ProLiant endpoint: {cpu_load}%")
                            break
                elif debug:
                    print(f"Failed with status code: {proliant_response.status_code}")
            except Exception as e:
                if debug:
                    print(f"Error retrieving CPU data from ProLiant endpoint: {str(e)}")
                pass
            
            try:
                # Check processor collection for individual processor data
                processors_url = f"https://{ip}/redfish/v1/Systems/1/Processors"
                if debug:
                    print(f"Attempting to retrieve CPU data from {ip} via endpoint: {processors_url}")
                proc_response = session.get(processors_url, timeout=30)
                if proc_response.status_code == 200:
                    proc_collection = proc_response.json()
                    if "Members" in proc_collection:
                        # Collect all CPU loads to calculate average
                        cpu_loads = []
                        for member in proc_collection.get("Members", []):
                            if "@odata.id" in member:
                                proc_url = member["@odata.id"]
                                try:
                                    proc_detail_response = session.get(f"https://{ip}{proc_url}", timeout=30)
                                    if proc_detail_response.status_code == 200:
                                        proc_detail = proc_detail_response.json()
                                        # Check for utilization in various possible locations
                                        if "Oem" in proc_detail:
                                            oem_proc = proc_detail.get("Oem", {})
                                            if "Hpe" in oem_proc and "CurrentUtilization" in oem_proc["Hpe"]:
                                                cpu_loads.append(oem_proc["Hpe"]["CurrentUtilization"])
                                                if debug:
                                                    print(f"Found CPU utilization in HPE processor data")
                                            elif "Hp" in oem_proc and "CurrentUtilization" in oem_proc["Hp"]:
                                                cpu_loads.append(oem_proc["Hp"]["CurrentUtilization"])
                                                if debug:
                                                    print(f"Found CPU utilization in HP processor data")
                                except Exception as e:
                                    if debug:
                                        print(f"Error retrieving individual processor data: {str(e)}")
                                    pass
                        
                        # Calculate average CPU load if we collected any
                        if cpu_loads:
                            cpu_load = sum(cpu_loads) / len(cpu_loads)
                            if debug:
                                print(f"Calculated average CPU load from {len(cpu_loads)} processors: {cpu_load}%")
                            break
                elif debug:
                    print(f"Failed with status code: {proc_response.status_code}")
            except Exception as e:
                if debug:
                    print(f"Error retrieving processor collection data: {str(e)}")
                pass
                
            retry_count += 1
        
        # Store CPU load in system dict
        if cpu_load is not None:
            system["cpu_load"] = cpu_load
        else:
            system["cpu_load"] = None
    
    # Print result and return the system with updated values
    system["watts"] = watts
    
    if watts is not None and system["cpu_load"] is not None:
        print(f"Server at {ip}{id_str}: {watts}W, CPU: {system['cpu_load']:.1f}%")
    elif watts is not None:
        print(f"Server at {ip}{id_str}: {watts}W, CPU: Unknown")
    elif system["cpu_load"] is not None:
        print(f"Server at {ip}{id_str}: Power unknown, CPU: {system['cpu_load']:.1f}%")
    else:
        error_msg = f": {last_error}" if last_error else ""
        print(f"Server at {ip}{id_str}: Unable to retrieve power consumption or CPU load after {max_retries} retries{error_msg}")
    
    return system

def get_cpu_utilization(system, max_retries=3, retry_delay=2, debug=False):
    """
    Get detailed CPU utilization metrics using multiple methods for newer iLO versions.
    Uses all available endpoints to try to retrieve CPU information.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        max_retries (int): Maximum number of retry attempts for failed operations
        retry_delay (int): Delay in seconds between retry attempts
        debug (bool): Whether to print debug information
        
    Returns:
        dict: Dictionary with CPU utilization metrics
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    # Create a session for reusing the same connection
    session = requests.Session()
    session.auth = (username, password)
    session.verify = False  # Ignore SSL verification
    session.headers.update({"Accept": "application/json"})
    
    # Longer timeout for slow responses
    timeout = 60
    
    # For identification in output
    identifier = ""
    model = "Unknown"
    try:
        # Get system info for identification
        sys_url = f"https://{ip}/redfish/v1/Systems/1"
        if debug:
            print(f"[{ip}] Getting basic system information...")
        sys_response = session.get(sys_url, timeout=timeout)
        if sys_response.status_code == 200:
            sys_data = sys_response.json()
            asset_tag = sys_data.get("AssetTag", "")
            serial_number = sys_data.get("SerialNumber", "")
            model = sys_data.get("Model", "Unknown")
            identifier = asset_tag if asset_tag else serial_number
            if debug:
                print(f"[{ip}] System identified: Model={model}, SN={serial_number}")
    except Exception as e:
        if debug:
            print(f"[{ip}] Error getting system info: {str(e)}")
    
    # Get server identifier for output messages
    id_str = f" ({identifier})" if identifier else ""
    
    # Initialize results dictionary
    result = {
        'ip': ip,
        'identifier': identifier,
        'model': model,
        'cpu_methods_tried': [],
        'cpu_load': None,
        'cpu_details': {},
        'iLO_version': "Unknown"
    }
    
    # Try to get iLO version which helps with debugging
    try:
        manager_url = f"https://{ip}/redfish/v1/Managers/1"
        if debug:
            print(f"[{ip}] Getting iLO firmware version...")
        manager_response = session.get(manager_url, timeout=timeout)
        if manager_response.status_code == 200:
            manager_data = manager_response.json()
            if "FirmwareVersion" in manager_data:
                ilo_version = manager_data.get("FirmwareVersion", "Unknown")
                result['iLO_version'] = ilo_version
                if debug:
                    print(f"[{ip}] iLO firmware version: {ilo_version}")
    except Exception as e:
        if debug:
            print(f"[{ip}] Error getting iLO version: {str(e)}")
    
    print(f"Checking CPU utilization for system at {ip}{id_str}...")
    print(f"  iLO firmware version: {result['iLO_version']}")
    
    # Method 1: iLO 5/6 primary system OEM data
    try:
        if debug:
            print(f"[{ip}] Trying Method 1: System OEM data")
        
        sys_url = f"https://{ip}/redfish/v1/Systems/1"
        sys_response = session.get(sys_url, timeout=timeout)
        
        if sys_response.status_code == 200:
            sys_data = sys_response.json()
            
            # First, try to get CPU utilization directly from OEM extensions
            if "Oem" in sys_data:
                oem_data = sys_data.get("Oem", {})
                # Check for HPE or HP specific extensions
                hpe_data = None
                
                if "Hpe" in oem_data:
                    hpe_data = oem_data.get("Hpe", {})
                    result['cpu_methods_tried'].append("System OEM Hpe")
                elif "Hp" in oem_data:
                    hpe_data = oem_data.get("Hp", {})
                    result['cpu_methods_tried'].append("System OEM Hp")
                
                if hpe_data:
                    # Process Utilization
                    if "ProcessorUtilization" in hpe_data:
                        result['cpu_load'] = hpe_data.get("ProcessorUtilization")
                        result['cpu_details']['processor_utilization'] = result['cpu_load']
                        print(f"  Found CPU utilization via System OEM: {result['cpu_load']:.1f}%")
                    
                    # Memory Utilization 
                    if "MemoryUtilization" in hpe_data:
                        result['cpu_details']['memory_utilization'] = hpe_data.get("MemoryUtilization")
                        print(f"  Memory utilization: {result['cpu_details']['memory_utilization']:.1f}%")
    except Exception as e:
        if debug:
            print(f"[{ip}] Method 1 error: {str(e)}")
    
    # Method 2: iLO 6 Metrics endpoint (newer firmware)
    if result['cpu_load'] is None:
        try:
            if debug:
                print(f"[{ip}] Trying Method 2: System Metrics endpoint")
            
            result['cpu_methods_tried'].append("System Metrics")
            metrics_url = f"https://{ip}/redfish/v1/Systems/1/Metrics"
            metrics_response = session.get(metrics_url, timeout=timeout)
            
            if metrics_response.status_code == 200:
                metrics_data = metrics_response.json()
                if "CPUUtilization" in metrics_data:
                    result['cpu_load'] = metrics_data.get("CPUUtilization")
                    result['cpu_details']['cpu_utilization'] = result['cpu_load']
                    print(f"  Found CPU utilization via System Metrics: {result['cpu_load']:.1f}%")
                
                # Extract other useful metrics
                useful_metrics = ["AverageFrequencyMHz", "CPUUtil", "MemoryBusUtilization"]
                for metric in useful_metrics:
                    if metric in metrics_data:
                        result['cpu_details'][metric.lower()] = metrics_data.get(metric)
                        print(f"  {metric}: {metrics_data.get(metric)}")
            elif debug:
                print(f"[{ip}] System Metrics HTTP status: {metrics_response.status_code}")
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 2 error: {str(e)}")
    
    # Method 3: iLO 6 ProcessorSummary (newer firmware)
    if result['cpu_load'] is None:
        try:
            if debug:
                print(f"[{ip}] Trying Method 3: Processor Summary Metrics")
            
            result['cpu_methods_tried'].append("Processor Summary")
            metrics_url = f"https://{ip}/redfish/v1/Systems/1/ProcessorSummary/ProcessorMetrics"
            metrics_response = session.get(metrics_url, timeout=timeout)
            
            if metrics_response.status_code == 200:
                metrics_data = metrics_response.json()
                
                if "TotalCorePercent" in metrics_data:
                    result['cpu_load'] = metrics_data.get("TotalCorePercent")
                    result['cpu_details']['total_core_percent'] = result['cpu_load']
                    print(f"  Found CPU utilization via ProcessorMetrics: {result['cpu_load']:.1f}%")
                
                # Get other useful processor metrics
                useful_metrics = ["AverageFrequencyMHz", "TotalThreadPercent", "CorePercent"]
                for metric in useful_metrics:
                    if metric in metrics_data:
                        result['cpu_details'][metric.lower()] = metrics_data.get(metric)
                        print(f"  {metric}: {metrics_data.get(metric)}")
            elif debug:
                print(f"[{ip}] Processor Summary HTTP status: {metrics_response.status_code}")
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 3 error: {str(e)}")
    
    # Method 4: iLO 5+ processor collection (newer firmware)
    if result['cpu_load'] is None:
        try:
            if debug:
                print(f"[{ip}] Trying Method 4: HPE Processor Collection")
            
            result['cpu_methods_tried'].append("HPE Processor Collection")
            proliant_url = f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/ProcessorCollection"
            proliant_response = session.get(proliant_url, timeout=timeout)
            
            if proliant_response.status_code == 200:
                proliant_data = proliant_response.json()
                if "Members" in proliant_data and len(proliant_data["Members"]) > 0:
                    cpu_loads = []
                    cpu_details = []
                    
                    for cpu in proliant_data["Members"]:
                        if "ProcessorUtilization" in cpu:
                            cpu_loads.append(cpu.get("ProcessorUtilization"))
                            cpu_details.append({
                                'processor': cpu.get("Name", "Unknown"),
                                'utilization': cpu.get("ProcessorUtilization"),
                                'frequency': cpu.get("FrequencyMHz", 0)
                            })
                    
                    if cpu_loads:
                        result['cpu_load'] = sum(cpu_loads) / len(cpu_loads)
                        result['cpu_details']['processor_collection'] = cpu_details
                        print(f"  Found CPU utilization via ProLiant collection: {result['cpu_load']:.1f}%")
                        print(f"  Individual processors: {len(cpu_loads)}")
            elif debug:
                print(f"[{ip}] HPE Processor Collection HTTP status: {proliant_response.status_code}")
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 4 error: {str(e)}")
    
    # Method 5: Standard Redfish individual processors
    if result['cpu_load'] is None:
        try:
            if debug:
                print(f"[{ip}] Trying Method 5: Processors collection")
            
            result['cpu_methods_tried'].append("Processors Collection")
            processors_url = f"https://{ip}/redfish/v1/Systems/1/Processors"
            proc_response = session.get(processors_url, timeout=timeout)
            
            if proc_response.status_code == 200:
                proc_collection = proc_response.json()
                if "Members" in proc_collection:
                    cpu_loads = []
                    cpu_details = []
                    
                    for member in proc_collection.get("Members", []):
                        if "@odata.id" in member:
                            proc_url = member["@odata.id"]
                            try:
                                proc_detail_response = session.get(f"https://{ip}{proc_url}", timeout=timeout)
                                if proc_detail_response.status_code == 200:
                                    proc_detail = proc_detail_response.json()
                                    processor_name = proc_detail.get("Name", "Unknown")
                                    
                                    # Check for utilization in various possible locations
                                    if "Oem" in proc_detail:
                                        oem_proc = proc_detail.get("Oem", {})
                                        utilization = None
                                        
                                        if "Hpe" in oem_proc and "CurrentUtilization" in oem_proc["Hpe"]:
                                            utilization = oem_proc["Hpe"]["CurrentUtilization"]
                                            if debug:
                                                print(f"  Found CPU {processor_name} utilization in HPE data: {utilization}%")
                                        elif "Hp" in oem_proc and "CurrentUtilization" in oem_proc["Hp"]:
                                            utilization = oem_proc["Hp"]["CurrentUtilization"]
                                            if debug:
                                                print(f"  Found CPU {processor_name} utilization in HP data: {utilization}%")
                                        
                                        if utilization is not None:
                                            cpu_loads.append(utilization)
                                            cpu_details.append({
                                                'processor': processor_name,
                                                'utilization': utilization
                                            })
                            except Exception as e:
                                if debug:
                                    print(f"  Error retrieving processor details: {str(e)}")
                    
                    # Calculate average CPU load if we collected any
                    if cpu_loads:
                        result['cpu_load'] = sum(cpu_loads) / len(cpu_loads)
                        result['cpu_details']['processors'] = cpu_details
                        print(f"  Found CPU utilization from {len(cpu_loads)} processors: {result['cpu_load']:.1f}%")
            elif debug:
                print(f"[{ip}] Processors collection HTTP status: {proc_response.status_code}")
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 5 error: {str(e)}")
    
    # Method 6: iLO 5/6 processor metrics for each processor
    if result['cpu_load'] is None:
        try:
            if debug:
                print(f"[{ip}] Trying Method 6: Individual processor metrics")
            
            result['cpu_methods_tried'].append("Individual Processor Metrics")
            processors_url = f"https://{ip}/redfish/v1/Systems/1/Processors"
            proc_response = session.get(processors_url, timeout=timeout)
            
            if proc_response.status_code == 200:
                proc_collection = proc_response.json()
                if "Members" in proc_collection:
                    cpu_loads = []
                    cpu_details = []
                    
                    for member in proc_collection.get("Members", []):
                        if "@odata.id" in member:
                            proc_url = member["@odata.id"]
                            
                            # Try to get processor metrics
                            metrics_url = f"{proc_url}/ProcessorMetrics"
                            try:
                                metrics_response = session.get(f"https://{ip}{metrics_url}", timeout=timeout)
                                if metrics_response.status_code == 200:
                                    metrics_data = metrics_response.json()
                                    
                                    # Different HPE iLO versions use different field names
                                    utilization = None
                                    processor_name = f"CPU {len(cpu_loads) + 1}"
                                    
                                    if "CorePercent" in metrics_data:
                                        utilization = metrics_data.get("CorePercent")
                                    elif "AverageFrequencyMHz" in metrics_data:
                                        # Store frequency data even if utilization isn't available
                                        result['cpu_details']['cpu_frequency_mhz'] = metrics_data.get("AverageFrequencyMHz")
                                    
                                    if utilization is not None:
                                        cpu_loads.append(utilization)
                                        cpu_details.append({
                                            'processor': processor_name,
                                            'utilization': utilization
                                        })
                                        if debug:
                                            print(f"  Found {processor_name} utilization: {utilization}%")
                            except Exception as e:
                                if debug:
                                    print(f"  Error retrieving processor metrics: {str(e)}")
                    
                    # Calculate average CPU load if we collected any
                    if cpu_loads:
                        result['cpu_load'] = sum(cpu_loads) / len(cpu_loads)
                        result['cpu_details']['processor_metrics'] = cpu_details
                        print(f"  Found CPU utilization from processor metrics: {result['cpu_load']:.1f}%")
            elif debug:
                print(f"[{ip}] Processor metrics HTTP status: {proc_response.status_code}")
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 6 error: {str(e)}")
    
    # Method 7: iLO 5+ alternative OEM paths
    if result['cpu_load'] is None:
        try:
            if debug:
                print(f"[{ip}] Trying Method 7: HPE alternative OEM CPU data paths")
            
            result['cpu_methods_tried'].append("HPE Alt OEM Paths")
            
            # Try different OEM endpoints that might contain CPU data
            oem_urls = [
                f"https://{ip}/redfish/v1/Systems/1/Oem/Hp/CpuMetrics",
                f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/CpuMetrics",
                f"https://{ip}/redfish/v1/Systems/1/Oem/Hp/ProcessorInfo",
                f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/ProcessorInfo"
            ]
            
            for oem_url in oem_urls:
                try:
                    if debug:
                        print(f"  Checking {oem_url}")
                    oem_response = session.get(oem_url, timeout=timeout)
                    if oem_response.status_code == 200:
                        oem_data = oem_response.json()
                        
                        # Search for CPU utilization in various possible field names
                        cpu_fields = ["ProcessorUtilization", "CpuUtilization", "Utilization", 
                                    "CoreUtilization", "AverageUtilization", "SystemUtilization"]
                        
                        for field in cpu_fields:
                            if field in oem_data:
                                utilization = oem_data.get(field)
                                if isinstance(utilization, (int, float)) and utilization >= 0:
                                    result['cpu_load'] = utilization
                                    result['cpu_details']['alt_oem_utilization'] = utilization
                                    print(f"  Found CPU utilization via alt OEM path: {result['cpu_load']:.1f}%")
                                    break
                        
                        # If we found utilization, no need to check other URLs
                        if result['cpu_load'] is not None:
                            break
                except Exception as e:
                    if debug:
                        print(f"  Error with OEM URL {oem_url}: {str(e)}")
                    continue
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 7 error: {str(e)}")

    # Method 8: Try to get CPU data from Health Information
    if result['cpu_load'] is None:
        try:
            if debug:
                print(f"[{ip}] Trying Method 8: System health data")
            
            result['cpu_methods_tried'].append("System Health")
            health_url = f"https://{ip}/redfish/v1/Systems/1/ProcessorHealth"
            health_response = session.get(health_url, timeout=timeout)
            
            if health_response.status_code == 200:
                health_data = health_response.json()
                
                # Look for any CPU utilization data in the health response
                for key, value in health_data.items():
                    if "utilization" in key.lower() and isinstance(value, (int, float)) and value >= 0:
                        result['cpu_load'] = value
                        result['cpu_details']['health_utilization'] = value
                        print(f"  Found CPU utilization via system health: {result['cpu_load']:.1f}%")
                        break
            elif debug:
                # Try alternate URL
                health_url = f"https://{ip}/redfish/v1/Systems/1/Health"
                try:
                    health_response = session.get(health_url, timeout=timeout)
                    if health_response.status_code == 200:
                        health_data = health_response.json()
                        
                        # Look for any CPU utilization data in the health response
                        for key, value in health_data.items():
                            if "utilization" in key.lower() and isinstance(value, (int, float)) and value >= 0:
                                result['cpu_load'] = value
                                result['cpu_details']['health_utilization'] = value
                                print(f"  Found CPU utilization via alternate health: {result['cpu_load']:.1f}%")
                                break
                except Exception:
                    pass
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 8 error: {str(e)}")
    
    # Summarize findings
    if result['cpu_load'] is not None:
        print(f"System at {ip}{id_str} CPU utilization: {result['cpu_load']:.1f}%")
        print(f"  Methods used: {', '.join(result['cpu_methods_tried'])}")
    else:
        print(f"System at {ip}{id_str}: Unable to retrieve CPU utilization after trying multiple methods")
        print(f"  Methods tried: {', '.join(result['cpu_methods_tried']) if result['cpu_methods_tried'] else 'None'}")
        if result['iLO_version'] != "Unknown":
            print(f"  iLO version: {result['iLO_version']} (may not support CPU metrics)")
    
    return result

def get_system_basic_status(system, max_retries=3, retry_delay=2):
    """
    Get just the basic power status, BIOS version, and serial number in one line.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        max_retries (int): Maximum number of retry attempts for failed operations
        retry_delay (int): Delay in seconds between retry attempts
        
    Returns:
        bool: True if status was retrieved successfully, False otherwise
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    try:
        # Create a session for reusing the same connection
        session = requests.Session()
        session.auth = (username, password)
        session.verify = False  # Ignore SSL verification
        
        # Initialize values
        power_state = "Unknown"
        serial_number = "Unknown"
        bios_version = "Unknown"
        watts = "Unknown"
        
        # Get basic system info
        sys_url = f"https://{ip}/redfish/v1/Systems/1"
        sys_response = session.get(sys_url, timeout=30)
        
        if sys_response.status_code == 200:
            sys_data = sys_response.json()
            power_state = sys_data.get("PowerState", "Unknown")
            serial_number = sys_data.get("SerialNumber", "Unknown")
            bios_version = sys_data.get("BiosVersion", "Unknown")
            
            # Try to get power information
            retry_count = 0
            while retry_count <= max_retries:
                try:
                    # First try the standard Chassis power endpoint
                    power_url = f"https://{ip}/redfish/v1/Chassis/1/Power"
                    power_response = session.get(power_url, timeout=30)
                    
                    if power_response.status_code == 200:
                        power_data = power_response.json()
                        if "PowerControl" in power_data and len(power_data["PowerControl"]) > 0:
                            power_control = power_data["PowerControl"][0]
                            power_watts = power_control.get("PowerConsumedWatts")
                            if power_watts is not None:
                                watts = f"{power_watts}W"
                                break
                    
                    # If first attempt failed, try alternative endpoint
                    if watts == "Unknown":
                        power_url = f"https://{ip}/redfish/v1/Chassis/1/PowerSubsystem/PowerMetrics"
                        power_response = session.get(power_url, timeout=30)
                        
                        if power_response.status_code == 200:
                            power_data = power_response.json()
                            power_watts = power_data.get("PowerWatts")
                            if power_watts is not None:
                                watts = f"{power_watts}W"
                                break
                except Exception:
                    pass
                
                retry_count += 1
                if retry_count <= max_retries:
                    time.sleep(retry_delay)
        
        # Print the results in a single line
        print(f"{ip} | Power: {power_state} | BIOS: {bios_version} | S/N: {serial_number} | Power Usage: {watts}")
        
        return True
    except Exception as e:
        print(f"Error getting basic status for system at {ip}: {str(e)}")
        return False

def get_system_power_status(system):
    """
    Get detailed system metrics including power, CPU, memory, storage, network and temperature.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        
    Returns:
        str: The power state of the system, or "Unknown" if it could not be determined
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    # Initialize values
    power_state = "Unknown"
    power_usage = "Unknown"
    cpu_load = "Unknown"
    temperature = "Unknown"
    memory_usage = "Unknown"
    storage_status = "Unknown"
    network_info = "Unknown"
    system_uptime = "Unknown"
    firmware_version = "Unknown"
    health_summary = "Unknown"
    asset_tag = ""
    serial_number = ""
    model = ""
    hostname = ""
    ilo_version = "Unknown"
    
    try:
        # Create a session for reusing the same connection
        session = requests.Session()
        session.auth = (username, password)
        session.verify = False  # Ignore SSL verification
        session.headers.update({"Accept": "application/json"})
        
        # Longer timeout for slow responses
        timeout = 60
        
        # Try to get iLO version which helps with troubleshooting
        try:
            manager_url = f"https://{ip}/redfish/v1/Managers/1"
            manager_response = session.get(manager_url, timeout=timeout)
            if manager_response.status_code == 200:
                manager_data = manager_response.json()
                if "FirmwareVersion" in manager_data:
                    ilo_version = manager_data.get("FirmwareVersion", "Unknown")
        except Exception:
            pass
        
        # Build the URL to get system info
        sys_url = f"https://{ip}/redfish/v1/Systems/1"
        
        # Send the GET request for system info
        sys_response = session.get(sys_url, timeout=timeout)
        
        if sys_response.status_code == 200:
            sys_data = sys_response.json()
            power_state = sys_data.get("PowerState", "Unknown")
            
            # Get identifying information
            asset_tag = sys_data.get("AssetTag", "")
            serial_number = sys_data.get("SerialNumber", "")
            model = sys_data.get("Model", "")
            hostname = sys_data.get("HostName", "")
            
            # Get overall health status
            if "Status" in sys_data:
                status = sys_data.get("Status", {})
                health = status.get("Health", "Unknown")
                state = status.get("State", "")
                health_summary = f"{health}" if not state else f"{health} ({state})"

            # Get firmware/BIOS version
            if "BiosVersion" in sys_data:
                firmware_version = sys_data.get("BiosVersion", "Unknown")
            
            # Get system uptime (available in some iLO versions)
            try:
                if "Oem" in sys_data:
                    oem_data = sys_data.get("Oem", {})
                    # Check for HPE or HP specific extensions
                    hpe_data = None
                    if "Hpe" in oem_data:
                        hpe_data = oem_data.get("Hpe", {})
                    elif "Hp" in oem_data:
                        hpe_data = oem_data.get("Hp", {})
                    
                    if hpe_data and "CurrentPowerOnTimeSeconds" in hpe_data:
                        uptime_seconds = hpe_data.get("CurrentPowerOnTimeSeconds", 0)
                        # Convert to days, hours, minutes
                        days, remainder = divmod(uptime_seconds, 86400)
                        hours, remainder = divmod(remainder, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        
                        if days > 0:
                            system_uptime = f"{days}d {hours}h {minutes}m"
                        else:
                            system_uptime = f"{hours}h {minutes}m"
            except Exception:
                pass
                
            # Get memory usage info
            if "MemorySummary" in sys_data:
                memory_summary = sys_data.get("MemorySummary", {})
                total_memory_gb = memory_summary.get("TotalSystemMemoryGiB", 0)
                if total_memory_gb > 0:
                    memory_status = ""
                    if "Status" in memory_summary:
                        memory_status = f" ({memory_summary['Status'].get('Health', '')})"
                    
                    # Try to get memory usage percentage - available in some OEM extensions
                    memory_used_pct = None
                    if "Oem" in sys_data:
                        oem_data = sys_data.get("Oem", {})
                        if "Hpe" in oem_data and "MemoryUtilization" in oem_data["Hpe"]:
                            memory_used_pct = oem_data["Hpe"].get("MemoryUtilization")
                        elif "Hp" in oem_data and "MemoryUtilization" in oem_data["Hp"]:
                            memory_used_pct = oem_data["Hp"].get("MemoryUtilization")
                    
                    if memory_used_pct is not None:
                        memory_usage = f"{total_memory_gb} GiB ({memory_used_pct}% used){memory_status}"
                    else:
                        memory_usage = f"{total_memory_gb} GiB{memory_status}"
            
            # Enhanced CPU information collection
            # Method 1: From OEM data in System resource
            if "Oem" in sys_data:
                oem_data = sys_data.get("Oem", {})
                # Check for HPE or HP specific extensions
                hpe_data = None
                if "Hpe" in oem_data:
                    hpe_data = oem_data.get("Hpe", {})
                elif "Hp" in oem_data:
                    hpe_data = oem_data.get("Hp", {})
                
                if hpe_data and "ProcessorUtilization" in hpe_data:
                    # Some HPE servers expose CPU utilization in Oem data
                    cpu_load = f"{hpe_data.get('ProcessorUtilization', 'Unknown')}%"
            
            # Method 2: System Metrics endpoint (newer iLO versions)
            if cpu_load == "Unknown":
                try:
                    perf_url = f"https://{ip}/redfish/v1/Systems/1/Metrics"
                    perf_response = session.get(perf_url, timeout=timeout)
                    if perf_response.status_code == 200:
                        perf_data = perf_response.json()
                        if "CPUUtilization" in perf_data:
                            cpu_load = f"{perf_data.get('CPUUtilization', 'Unknown')}%"
                except Exception:
                    pass
            
            # Method 3: ProcessorSummary endpoint (newer iLO versions)
            if cpu_load == "Unknown":
                try:
                    metrics_url = f"https://{ip}/redfish/v1/Systems/1/ProcessorSummary/ProcessorMetrics"
                    metrics_response = session.get(metrics_url, timeout=timeout)
                    if metrics_response.status_code == 200:
                        metrics_data = metrics_response.json()
                        if "TotalCorePercent" in metrics_data:
                            cpu_load = f"{metrics_data.get('TotalCorePercent', 'Unknown')}%"
                except Exception:
                    pass
            
            # Method 4: Check processor collection for individual processor data
            if cpu_load == "Unknown":
                try:
                    processors_url = f"https://{ip}/redfish/v1/Systems/1/Processors"
                    proc_response = session.get(processors_url, timeout=timeout)
                    if proc_response.status_code == 200:
                        proc_collection = proc_response.json()
                        if "Members" in proc_collection:
                            # Collect all CPU loads to calculate average
                            cpu_loads = []
                            for member in proc_collection.get("Members", []):
                                if "@odata.id" in member:
                                    proc_url = member["@odata.id"]
                                    try:
                                        proc_detail_response = session.get(f"https://{ip}{proc_url}", timeout=timeout)
                                        if proc_detail_response.status_code == 200:
                                            proc_detail = proc_detail_response.json()
                                            # Check for utilization in various possible locations
                                            if "Oem" in proc_detail:
                                                oem_proc = proc_detail.get("Oem", {})
                                                if "Hpe" in oem_proc and "CurrentUtilization" in oem_proc["Hpe"]:
                                                    cpu_loads.append(oem_proc["Hpe"]["CurrentUtilization"])
                                                elif "Hp" in oem_proc and "CurrentUtilization" in oem_proc["Hp"]:
                                                    cpu_loads.append(oem_proc["Hp"]["CurrentUtilization"])
                                    except Exception:
                                        pass
                            
                            # Calculate average CPU load if we collected any
                            if cpu_loads:
                                avg_load = sum(cpu_loads) / len(cpu_loads)
                                cpu_load = f"{avg_load:.1f}%"
                except Exception:
                    pass
            
            # Method 5: HPE Processor Collection (newer firmware)
            if cpu_load == "Unknown":
                try:
                    proliant_url = f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/ProcessorCollection"
                    proliant_response = session.get(proliant_url, timeout=timeout)
                    if proliant_response.status_code == 200:
                        proliant_data = proliant_response.json()
                        if "Members" in proliant_data and len(proliant_data["Members"]) > 0:
                            cpu_loads = []
                            for cpu in proliant_data["Members"]:
                                if "ProcessorUtilization" in cpu:
                                    cpu_loads.append(cpu.get("ProcessorUtilization"))
                            
                            if cpu_loads:
                                avg_load = sum(cpu_loads) / len(cpu_loads)
                                cpu_load = f"{avg_load:.1f}%"
                except Exception:
                    pass
            
            # Method 6: Alternative OEM paths
            if cpu_load == "Unknown":
                try:
                    oem_urls = [
                        f"https://{ip}/redfish/v1/Systems/1/Oem/Hp/CpuMetrics",
                        f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/CpuMetrics",
                        f"https://{ip}/redfish/v1/Systems/1/Oem/Hp/ProcessorInfo",
                        f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/ProcessorInfo"
                    ]
                    
                    for oem_url in oem_urls:
                        try:
                            oem_response = session.get(oem_url, timeout=timeout)
                            if oem_response.status_code == 200:
                                oem_data = oem_response.json()
                                
                                # Search for CPU utilization in various possible field names
                                cpu_fields = ["ProcessorUtilization", "CpuUtilization", "Utilization", 
                                            "CoreUtilization", "AverageUtilization", "SystemUtilization"]
                                
                                for field in cpu_fields:
                                    if field in oem_data:
                                        utilization = oem_data.get(field)
                                        if isinstance(utilization, (int, float)) and utilization >= 0:
                                            cpu_load = f"{utilization:.1f}%"
                                            break
                                
                                # If we found utilization, no need to check other URLs
                                if cpu_load != "Unknown":
                                    break
                        except Exception:
                            continue
                except Exception:
                    pass
            
            # Get storage health status
            try:
                storage_url = f"https://{ip}/redfish/v1/Systems/1/Storage"
                storage_response = session.get(storage_url, timeout=timeout)
                
                if storage_response.status_code == 200:
                    storage_collection = storage_response.json()
                    if "Members" in storage_collection:
                        storage_statuses = []
                        storage_capacities = []
                        
                        for member in storage_collection.get("Members", [])[:2]:  # Limit to first 2 controllers
                            if "@odata.id" in member:
                                controller_url = member["@odata.id"]
                                try:
                                    controller_response = session.get(f"https://{ip}{controller_url}", timeout=timeout)
                                    if controller_response.status_code == 200:
                                        controller_data = controller_response.json()
                                        
                                        # Get controller health
                                        if "Status" in controller_data:
                                            controller_health = controller_data["Status"].get("Health", "")
                                            controller_name = controller_data.get("Name", "Storage Controller")
                                            if controller_health:
                                                storage_statuses.append(f"{controller_name}: {controller_health}")
                                        
                                        # Try to get volumes/drives information
                                        if "Drives" in controller_data and "@odata.id" in controller_data["Drives"]:
                                            drives_url = controller_data["Drives"]["@odata.id"]
                                            try:
                                                drives_response = session.get(f"https://{ip}{drives_url}", timeout=timeout)
                                                if drives_response.status_code == 200:
                                                    drives_data = drives_response.json()
                                                    if "Members" in drives_data:
                                                        drive_count = len(drives_data["Members"])
                                                        if drive_count > 0:
                                                            storage_capacities.append(f"{drive_count} drives")
                                            except Exception:
                                                pass
                                except Exception:
                                    pass
                        
                        # Combine storage information
                        if storage_statuses:
                            storage_status = ", ".join(storage_statuses)
                            if storage_capacities:
                                storage_status += f" ({', '.join(storage_capacities)})"
            except Exception:
                pass
            
            # Get network information
            try:
                net_url = f"https://{ip}/redfish/v1/Systems/1/EthernetInterfaces"
                net_response = session.get(net_url, timeout=timeout)
                
                if net_response.status_code == 200:
                    net_collection = net_response.json()
                    if "Members" in net_collection:
                        net_info = []
                        active_nics = 0
                        
                        for member in net_collection.get("Members", [])[:3]:  # Limit to first 3 interfaces
                            if "@odata.id" in member:
                                nic_url = member["@odata.id"]
                                try:
                                    nic_response = session.get(f"https://{ip}{nic_url}", timeout=timeout)
                                    if nic_response.status_code == 200:
                                        nic_data = nic_response.json()
                                        
                                        # Check if interface is enabled/active
                                        status = nic_data.get("Status", {})
                                        if status.get("State") == "Enabled":
                                            active_nics += 1
                                            
                                            # Get speed if available
                                            speed = nic_data.get("SpeedMbps", 0)
                                            mac = nic_data.get("MACAddress", "")
                                            name = nic_data.get("Name", "NIC")
                                            
                                            if speed and mac:
                                                net_info.append(f"{name}: {speed}Mbps ({mac})")
                                            elif mac:
                                                net_info.append(f"{name}: {mac}")
                                except Exception:
                                    pass
                        
                        # Combine network information
                        if active_nics > 0:
                            if net_info:
                                network_info = f"{active_nics} active NICs - " + ", ".join(net_info)
                            else:
                                network_info = f"{active_nics} active NICs"
            except Exception:
                pass
            
            # Now get power metrics
            # First try the Chassis power endpoint
            try:
                power_url = f"https://{ip}/redfish/v1/Chassis/1/Power"
                power_response = session.get(power_url, timeout=timeout)
                
                if power_response.status_code == 200:
                    power_data = power_response.json()
                    if "PowerControl" in power_data and len(power_data["PowerControl"]) > 0:
                        # Get the first power control entry
                        power_control = power_data["PowerControl"][0]
                        power_usage_watts = power_control.get("PowerConsumedWatts", "Unknown")
                        power_capacity = power_control.get("PowerCapacityWatts", "Unknown")
                        if power_usage_watts != "Unknown":
                            if power_capacity != "Unknown":
                                # Calculate percentage if capacity is known
                                try:
                                    power_percentage = (power_usage_watts / power_capacity) * 100
                                    power_usage = f"{power_usage_watts}W ({power_percentage:.1f}% of {power_capacity}W)"
                                except:
                                    power_usage = f"{power_usage_watts}W"
                            else:
                                power_usage = f"{power_usage_watts}W"
            except Exception:
                # Try alternative power endpoint for newer iLO versions
                try:
                    power_url = f"https://{ip}/redfish/v1/Chassis/1/PowerSubsystem/PowerMetrics"
                    power_response = session.get(power_url, timeout=timeout)
                    
                    if power_response.status_code == 200:
                        power_data = power_response.json()
                        power_usage_watts = power_data.get("PowerWatts", "Unknown")
                        if power_usage_watts != "Unknown":
                            power_usage = f"{power_usage_watts}W"
                except Exception:
                    pass
            
            # Get temperature information
            # Check thermal endpoint for temperature data
            try:
                thermal_url = f"https://{ip}/redfish/v1/Chassis/1/Thermal"
                thermal_response = session.get(thermal_url, timeout=timeout)
                
                if thermal_response.status_code == 200:
                    thermal_data = thermal_response.json()
                    if "Temperatures" in thermal_data and thermal_data["Temperatures"]:
                        # Get a list of all temperature readings
                        temp_readings = []
                        for temp_sensor in thermal_data["Temperatures"]:
                            # Look for important sensors like CPU or System
                            sensor_name = temp_sensor.get("Name", "").lower()
                            reading = temp_sensor.get("ReadingCelsius")
                            if reading is not None:
                                if any(important in sensor_name for important in ["cpu", "system", "inlet", "ambient", "exhaust"]):
                                    temp_readings.append((sensor_name, reading))
                        
                        # If we have readings, include the most relevant ones
                        if temp_readings:
                            # Sort by name to prioritize CPU, then System, then others
                            temp_readings.sort(key=lambda x: (0 if "cpu" in x[0] else (1 if "system" in x[0] else 2), x[0]))
                            # Format temperature information
                            temp_strings = [f"{name.capitalize()}: {reading}°C" for name, reading in temp_readings[:3]]
                            temperature = ", ".join(temp_strings)
            except Exception:
                # Try alternative thermal endpoint for newer iLO versions
                try:
                    thermal_url = f"https://{ip}/redfish/v1/Chassis/1/ThermalSubsystem/ThermalMetrics"
                    thermal_response = session.get(thermal_url, timeout=timeout)
                    
                    if thermal_response.status_code == 200:
                        thermal_data = thermal_response.json()
                        if "TemperatureSummary" in thermal_data:
                            temp_summary = thermal_data["TemperatureSummary"]
                            internal_temp = temp_summary.get("InternalTemperatureCelsius")
                            if internal_temp is not None:
                                temperature = f"Internal: {internal_temp}°C"
                except Exception:
                    pass
            
            # Build an identifier with available information (prioritizing asset tag)
            identifier = asset_tag if asset_tag else serial_number
            
            # Add additional info if we have it and if the primary identifier is not empty
            id_parts = []
            if identifier:
                id_parts.append(f"AssetTag/SN: {identifier}")
            if model:
                id_parts.append(f"Model: {model}")
            if hostname:
                id_parts.append(f"Host: {hostname}")
                
            # Final identifier string
            id_string = ", ".join(id_parts) if id_parts else "No identifier available"
            
            # Print the system status with all collected information
            print(f"System at {ip} ({id_string})")
            print(f"  Overall Health: {health_summary}")
            print(f"  Power State: {power_state}")
            print(f"  Power Usage: {power_usage}")
            print(f"  CPU Load: {cpu_load}")
            print(f"  Memory: {memory_usage}")
            print(f"  Temperature: {temperature}")
            print(f"  Storage: {storage_status}")
            print(f"  Network: {network_info}")
            if system_uptime != "Unknown":
                print(f"  System Uptime: {system_uptime}")
            if firmware_version != "Unknown":
                print(f"  Firmware/BIOS: {firmware_version}")
            print(f"  iLO Version: {ilo_version}")
            
            return power_state
        else:
            print(f"Failed to get status for system at {ip}. Status code: {sys_response.status_code}")
            return "Unknown"
            
    except Exception as e:
        print(f"Error getting status for system at {ip}: {str(e)}")
        return "Unknown"

def power_on_system(system):
    """
    Power on a single iLO system using Redfish API, but only if it's not already on.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        
    Returns:
        bool: True if powered on successfully or already on, False if operation failed
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    # First check current power state
    power_state = get_system_power_status(system)
    
    # If already on, don't try to power on again
    if power_state == "On":
        print(f"System at {ip} is already powered on. Skipping.")
        return True
    
    print(f"Attempting to power on system at {ip}")
    
    # Build the URL for the power action
    url = f"https://{ip}/redfish/v1/Systems/1/Actions/ComputerSystem.Reset"
    
    # Define the headers and payload
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # The payload for power on operation
    payload = {
        "ResetType": "On"
    }
    
    try:
        # Send the POST request to power on the system
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(payload),
            auth=(username, password),
            verify=False,  # Ignore SSL verification
            timeout=30     # Set a reasonable timeout
        )
        
        if response.status_code == 200:
            print(f"Successfully powered on system at {ip}")
            return True
        else:
            print(f"Failed to power on system at {ip}. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error powering on system at {ip}: {str(e)}")
        return False

def power_off_system(system, force=False):
    """
    Power off a single iLO system using Redfish API, but only if it's not already off.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        force (bool): Whether to force power off (like holding the power button) or
                      perform a graceful shutdown (OS initiated)
        
    Returns:
        bool: True if powered off successfully or already off, False if operation failed
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    # First check current power state
    power_state = get_system_power_status(system)
    
    # If already off, don't try to power off again
    if power_state == "Off":
        print(f"System at {ip} is already powered off. Skipping.")
        return True
    
    # Determine shutdown type based on force parameter
    reset_type = "ForceOff" if force else "GracefulShutdown"
    shutdown_desc = "forced" if force else "graceful"
    
    print(f"Attempting to power off system at {ip} ({shutdown_desc} shutdown)")
    
    # Build the URL for the power action
    url = f"https://{ip}/redfish/v1/Systems/1/Actions/ComputerSystem.Reset"
    
    # Define the headers and payload
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # The payload for power off operation
    payload = {
        "ResetType": reset_type
    }
    
    try:
        # Send the POST request to power off the system
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(payload),
            auth=(username, password),
            verify=False,  # Ignore SSL verification
            timeout=30     # Set a reasonable timeout
        )
        
        if response.status_code == 200:
            print(f"Successfully initiated {shutdown_desc} shutdown for system at {ip}")
            return True
        else:
            print(f"Failed to power off system at {ip}. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error powering off system at {ip}: {str(e)}")
        return False

def get_power_policy(system, debug=False):
    """
    Get the current power policy settings for an HPE iLO system.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        debug (bool): Whether to print debug information
        
    Returns:
        dict: Power policy information including current policy and available options
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    # Create a session for reusing the same connection
    session = requests.Session()
    session.auth = (username, password)
    session.verify = False  # Ignore SSL verification
    
    # Initialize result
    result = {
        'ip': ip,
        'current_policy': None,
        'available_policies': [],
        'power_settings': {},
        'raw_data': {},
        'source': None
    }
    
    # For identification in output
    identifier = ""
    try:
        # Get system info for identification
        sys_url = f"https://{ip}/redfish/v1/Systems/1"
        sys_response = session.get(sys_url, timeout=30)
        if sys_response.status_code == 200:
            sys_data = sys_response.json()
            asset_tag = sys_data.get("AssetTag", "")
            serial_number = sys_data.get("SerialNumber", "")
            model = sys_data.get("Model", "")
            identifier = asset_tag if asset_tag else serial_number
            result['identifier'] = identifier
            result['model'] = model
    except Exception as e:
        if debug:
            print(f"Error getting system info: {str(e)}")
    
    # Get server identifier for output messages
    id_str = f" ({identifier})" if identifier else ""
    
    print(f"Checking power policy for system at {ip}{id_str}...")
    
    # Method 1: Try BIOS settings
    try:
        if debug:
            print(f"[{ip}] Trying Method 1: BIOS Settings")
        
        bios_url = f"https://{ip}/redfish/v1/Systems/1/Bios"
        bios_response = session.get(bios_url, timeout=30)
        
        if bios_response.status_code == 200:
            bios_data = bios_response.json()
            result['raw_data']['bios'] = bios_data
            
            # Get current values
            if "@Redfish.Settings" in bios_data and "SettingsObject" in bios_data["@Redfish.Settings"]:
                settings_url = bios_data["@Redfish.Settings"]["SettingsObject"]["@odata.id"]
                settings_response = session.get(f"https://{ip}{settings_url}", timeout=30)
                if settings_response.status_code == 200:
                    settings_data = settings_response.json()
                    
                    # Check for power-related settings
                    if "Attributes" in settings_data:
                        power_attrs = {k: v for k, v in settings_data["Attributes"].items() 
                                    if any(term in k.lower() for term in 
                                        ["power", "performance", "energy", "cooling", "thermal"])}
                        
                        if power_attrs:
                            result['power_settings'].update(power_attrs)
                            # Try to identify the power policy
                            for key, value in power_attrs.items():
                                if "powerregulator" in key.lower() or "powerprofile" in key.lower():
                                    result['current_policy'] = value
                                    result['source'] = f"BIOS:{key}"
                                    break
            
            # If we didn't find the settings in @Redfish.Settings, try directly
            if not result['current_policy'] and "Attributes" in bios_data:
                power_attrs = {k: v for k, v in bios_data["Attributes"].items() 
                            if any(term in k.lower() for term in 
                                ["power", "performance", "energy", "cooling", "thermal"])}
                
                if power_attrs:
                    result['power_settings'].update(power_attrs)
                    # Try to identify the power policy
                    for key, value in power_attrs.items():
                        if "powerregulator" in key.lower() or "powerprofile" in key.lower():
                            result['current_policy'] = value
                            result['source'] = f"BIOS:{key}"
                            break
            
            # Try to get registry info for available options
            if "SettingsResult" in bios_data and "@odata.id" in bios_data["SettingsResult"]:
                registry_url = bios_data["SettingsResult"]["@odata.id"]
                registry_response = session.get(f"https://{ip}{registry_url}", timeout=30)
                if registry_response.status_code == 200:
                    registry_data = registry_response.json()
                    result['raw_data']['registry'] = registry_data
                    
                    # Extract available power modes if present
                    for key, value in registry_data.get("AvailableOptions", {}).items():
                        if "powerregulator" in key.lower() or "powerprofile" in key.lower():
                            result['available_policies'] = value
                            break
    except Exception as e:
        if debug:
            print(f"[{ip}] Method 1 error: {str(e)}")
    
    # Method 2: Try Power/PowerManagement endpoint
    if not result['current_policy']:
        try:
            if debug:
                print(f"[{ip}] Trying Method 2: Power Management Endpoint")
            
            power_mgmt_urls = [
                f"https://{ip}/redfish/v1/Systems/1/PowerManagement",
                f"https://{ip}/redfish/v1/Chassis/1/Power",
                f"https://{ip}/redfish/v1/Chassis/1/PowerSubsystem"
            ]
            
            for power_url in power_mgmt_urls:
                power_response = session.get(power_url, timeout=30)
                if power_response.status_code == 200:
                    power_data = power_response.json()
                    result['raw_data']['power'] = power_data
                    
                    # Look for power policy information
                    # This varies by iLO version, but try some common paths
                    if "PowerControl" in power_data and len(power_data["PowerControl"]) > 0:
                        power_control = power_data["PowerControl"][0]
                        
                        # Look for power mode/policy fields
                        policy_fields = ["PowerMode", "PowerProfile", "PowerLimit", "PowerRegulator", "PowerPolicy"]
                        for field in policy_fields:
                            if field in power_control:
                                result['current_policy'] = power_control[field]
                                result['source'] = f"PowerControl:{field}"
                                break
                        
                        # Look for power settings
                        power_settings = {k: v for k, v in power_control.items() 
                                    if k not in ["@odata.id", "MemberId", "Name", "Status"]}
                        if power_settings:
                            result['power_settings'].update(power_settings)
                    
                    # Look in Oem data
                    if "Oem" in power_data:
                        oem_data = power_data.get("Oem", {})
                        
                        # Check for HPE/HP specific extensions
                        if "Hpe" in oem_data:
                            hpe_data = oem_data["Hpe"]
                            # Look for power policy-related fields
                            if "PowerMode" in hpe_data:
                                result['current_policy'] = hpe_data["PowerMode"]
                                result['source'] = "Oem.Hpe.PowerMode"
                            elif "PowerRegulator" in hpe_data:
                                result['current_policy'] = hpe_data["PowerRegulator"]
                                result['source'] = "Oem.Hpe.PowerRegulator"
                            
                            # Check for available options
                            if "PowerRegulatorModes" in hpe_data:
                                result['available_policies'] = hpe_data["PowerRegulatorModes"]
                        elif "Hp" in oem_data:
                            hp_data = oem_data["Hp"]
                            # Similar checks for older iLO versions
                            if "PowerMode" in hp_data:
                                result['current_policy'] = hp_data["PowerMode"]
                                result['source'] = "Oem.Hp.PowerMode"
                            elif "PowerRegulator" in hp_data:
                                result['current_policy'] = hp_data["PowerRegulator"]
                                result['source'] = "Oem.Hp.PowerRegulator"
                            
                            # Check for available options
                            if "PowerRegulatorModes" in hp_data:
                                result['available_policies'] = hp_data["PowerRegulatorModes"]
                    
                    # If we found policy info, break the loop
                    if result['current_policy']:
                        break
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 2 error: {str(e)}")
    
    # Method 3: Try looking for power cap settings
    if not result['current_policy']:
        try:
            if debug:
                print(f"[{ip}] Trying Method 3: Power Cap Settings")
            
            # Try different power cap endpoints
            cap_urls = [
                f"https://{ip}/redfish/v1/Chassis/1/Power/PowerControl",
                f"https://{ip}/redfish/v1/Chassis/1/PowerManagement",
                f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/PowerManagement"
            ]
            
            for cap_url in cap_urls:
                cap_response = session.get(cap_url, timeout=30)
                if cap_response.status_code == 200:
                    cap_data = cap_response.json()
                    result['raw_data']['power_cap'] = cap_data
                    
                    # Check for power cap information
                    if "PowerLimit" in cap_data:
                        result['power_settings']['PowerLimit'] = cap_data["PowerLimit"]
                        
                    # Look for power mode information
                    policy_fields = ["PowerMode", "PowerProfile", "PowerAllocation", "PowerRegulator"]
                    for field in policy_fields:
                        if field in cap_data:
                            result['current_policy'] = cap_data[field]
                            result['source'] = f"PowerCap:{field}"
                            break
                    
                    # Check for OEM specific data
                    if "Oem" in cap_data:
                        oem_data = cap_data.get("Oem", {})
                        if "Hpe" in oem_data and "PowerRegulator" in oem_data["Hpe"]:
                            result['current_policy'] = oem_data["Hpe"]["PowerRegulator"]
                            result['source'] = "PowerCap:Oem.Hpe.PowerRegulator"
                        elif "Hp" in oem_data and "PowerRegulator" in oem_data["Hp"]:
                            result['current_policy'] = oem_data["Hp"]["PowerRegulator"]
                            result['source'] = "PowerCap:Oem.Hp.PowerRegulator"
                    
                    # If we found policy info, break the loop
                    if result['current_policy']:
                        break
        except Exception as e:
            if debug:
                print(f"[{ip}] Method 3 error: {str(e)}")
    
    # If we still haven't found the power policy but have power settings, try to infer it
    if not result['current_policy'] and result['power_settings']:
        try:
            # Look for common power policy indicators in the collected settings
            for key, value in result['power_settings'].items():
                if any(term in key.lower() for term in ["powermode", "powerpolicy", "powerregulator", "powerprofile"]):
                    result['current_policy'] = value
                    result['source'] = f"Inferred:{key}"
                    break
        except Exception as e:
            if debug:
                print(f"Error inferring power policy: {str(e)}")
    
    # Define common power policies if none found
    if not result['available_policies']:
        result['available_policies'] = [
            "Static Low Power",
            "Dynamic Power Savings",
            "Static High Performance",
            "OS Control",
            "Dynamic Power Savings Mode",
            "Static Low Power Mode", 
            "Static High Performance Mode",
            "OS Control Mode"
        ]
    
    # Print the result
    if result['current_policy']:
        policy_str = result['current_policy']
        if isinstance(policy_str, (int, float)):
            # Try to map numeric values to common policy names
            policy_map = {
                1: "Static Low Power Mode",
                2: "Dynamic Power Savings Mode", 
                3: "Static High Performance Mode",
                4: "OS Control Mode",
                0: "Unknown"
            }
            if policy_str in policy_map:
                policy_str = f"{policy_str} ({policy_map[policy_str]})"
        
        print(f"System at {ip}{id_str}: Current power policy: {policy_str}")
        if result['source']:
            print(f"  Source: {result['source']}")
        
        if result['available_policies']:
            print(f"  Available policies: {', '.join(str(p) for p in result['available_policies'])}")
    else:
        print(f"System at {ip}{id_str}: Unable to determine current power policy")
        if result['power_settings']:
            print("  Found power-related settings, but could not identify the policy")
            if debug:
                for key, value in result['power_settings'].items():
                    print(f"  {key}: {value}")
    
    return result

def set_power_policy(system, policy, debug=False):
    """
    Set the power policy for an HPE iLO system.
    
    Args:
        system (dict): Dictionary containing server connection details (ip, username, password)
        policy (str): The power policy to set
        debug (bool): Whether to print debug information
        
    Returns:
        bool: True if the policy was set successfully, False otherwise
    """
    ip = system["ip"]
    username = system["username"]
    password = system["password"]
    
    # Create a session for reusing the same connection
    session = requests.Session()
    session.auth = (username, password)
    session.verify = False  # Ignore SSL verification
    
    # For identification in output
    identifier = ""
    try:
        # Get system info for identification
        sys_url = f"https://{ip}/redfish/v1/Systems/1"
        sys_response = session.get(sys_url, timeout=30)
        if sys_response.status_code == 200:
            sys_data = sys_response.json()
            asset_tag = sys_data.get("AssetTag", "")
            serial_number = sys_data.get("SerialNumber", "")
            identifier = asset_tag if asset_tag else serial_number
    except Exception as e:
        if debug:
            print(f"Error getting system info: {str(e)}")
    
    # Get server identifier for output messages
    id_str = f" ({identifier})" if identifier else ""
    
    print(f"Setting power policy to '{policy}' for system at {ip}{id_str}...")
    
    # First, get the current power policy to identify the right endpoint and setting name
    current_policy_info = get_power_policy(system, debug=debug)
    
    if not current_policy_info['source']:
        print(f"Cannot set power policy for system at {ip}{id_str}: Unable to determine where power policy is controlled")
        return False
    
    # Extract source info to know where to update
    source_parts = current_policy_info['source'].split(':')
    source_type = source_parts[0]
    setting_name = source_parts[1] if len(source_parts) > 1 else None
    
    # Handle numeric policy values
    policy_value = policy
    if any(num in policy.lower() for num in ["static low", "dynamic", "static high", "os control"]):
        # Map policy names to numeric values
        policy_map = {
            "static low power": 1,
            "dynamic power savings": 2,
            "static high performance": 3,
            "os control": 4
        }
        # Find the closest match
        for key, value in policy_map.items():
            if key in policy.lower():
                policy_value = value
                break
    
    # Method 1: Update via BIOS Settings
    if source_type == "BIOS":
        try:
            if debug:
                print(f"[{ip}] Updating policy via BIOS Settings")
            
            bios_url = f"https://{ip}/redfish/v1/Systems/1/Bios/Settings"
            
            # Prepare the payload - this will vary depending on the setting name
            payload = {
                "Attributes": {
                    setting_name: policy_value
                }
            }
            
            # Send PATCH request to update the setting
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = session.patch(
                bios_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=30
            )
            
            if response.status_code in [200, 202, 204]:
                print(f"Successfully set power policy for system at {ip}{id_str}")
                print("  Note: Some changes may require a server reboot to take effect")
                return True
            else:
                print(f"Failed to set power policy for system at {ip}{id_str}. Status code: {response.status_code}")
                if debug:
                    print(f"Response: {response.text}")
                return False
        except Exception as e:
            print(f"Error setting power policy via BIOS for system at {ip}{id_str}: {str(e)}")
            return False
    
    # Method 2: Update via Power/PowerManagement endpoint
    elif source_type in ["PowerControl", "Oem.Hpe", "Oem.Hp"]:
        try:
            if debug:
                print(f"[{ip}] Updating policy via Power Management")
            
            # Determine the right endpoint and payload structure based on source info
            if "Oem.Hpe" in source_type:
                power_url = f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/PowerManagement"
                payload = {
                    "Oem": {
                        "Hpe": {
                            "PowerRegulator": policy_value
                        }
                    }
                }
            elif "Oem.Hp" in source_type:
                power_url = f"https://{ip}/redfish/v1/Systems/1/Oem/Hp/PowerManagement"
                payload = {
                    "Oem": {
                        "Hp": {
                            "PowerRegulator": policy_value
                        }
                    }
                }
            else:
                power_url = f"https://{ip}/redfish/v1/Chassis/1/Power"
                payload = {
                    "PowerControl": [
                        {
                            "MemberId": "0",
                            setting_name: policy_value
                        }
                    ]
                }
            
            # Send PATCH request to update the setting
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = session.patch(
                power_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=30
            )
            
            if response.status_code in [200, 202, 204]:
                print(f"Successfully set power policy for system at {ip}{id_str}")
                return True
            else:
                print(f"Failed to set power policy for system at {ip}{id_str}. Status code: {response.status_code}")
                if debug:
                    print(f"Response: {response.text}")
                return False
        except Exception as e:
            print(f"Error setting power policy via Power Management for system at {ip}{id_str}: {str(e)}")
            return False
    
    # Method 3: Update via Power Cap endpoint
    elif source_type == "PowerCap":
        try:
            if debug:
                print(f"[{ip}] Updating policy via Power Cap Settings")
            
            # Try different power cap endpoints
            if "Oem.Hpe" in setting_name:
                cap_url = f"https://{ip}/redfish/v1/Systems/1/Oem/Hpe/PowerManagement"
                payload = {
                    "Oem": {
                        "Hpe": {
                            "PowerRegulator": policy_value
                        }
                    }
                }
            elif "Oem.Hp" in setting_name:
                cap_url = f"https://{ip}/redfish/v1/Systems/1/Oem/Hp/PowerManagement"
                payload = {
                    "Oem": {
                        "Hp": {
                            "PowerRegulator": policy_value
                        }
                    }
                }
            else:
                cap_url = f"https://{ip}/redfish/v1/Chassis/1/Power/PowerControl"
                payload = {
                    setting_name: policy_value
                }
            
            # Send PATCH request to update the setting
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = session.patch(
                cap_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=30
            )
            
            if response.status_code in [200, 202, 204]:
                print(f"Successfully set power policy for system at {ip}{id_str}")
                return True
            else:
                print(f"Failed to set power policy for system at {ip}{id_str}. Status code: {response.status_code}")
                if debug:
                    print(f"Response: {response.text}")
                return False
        except Exception as e:
            print(f"Error setting power policy via Power Cap for system at {ip}{id_str}: {str(e)}")
            return False
    
    # If we don't recognize the source type
    else:
        print(f"Cannot set power policy for system at {ip}{id_str}: Unsupported power policy location: {current_policy_info['source']}")
        return False

def monitor_power_periodically(interval_minutes, ilo_systems, workers, output_csv, max_retries=3, retry_delay=2, debug=False, iterations=None):
    """
    Monitor power consumption and CPU utilization periodically and save to CSV.
    
    Args:
        interval_minutes (int): Time interval between checks in minutes
        ilo_systems (list): List of dictionaries containing server connection details
        workers (int): Number of parallel worker threads
        output_csv (str): Path to the CSV file to save data to
        max_retries (int): Maximum number of retry attempts for failed operations
        retry_delay (int): Delay in seconds between retry attempts
        debug (bool): Whether to print debug information
        iterations (int or None): Number of monitoring iterations, or None for indefinite monitoring
        
    Returns:
        None
    """
    print(f"Starting periodic monitoring every {interval_minutes} minutes")
    print(f"Using {max_retries} retries with {retry_delay}s delay between retries")
    print(f"Saving data to: {output_csv}")
    print(f"Collecting both power and CPU metrics when available")
    
    iteration = 0
    try:
        while iterations is None or iteration < iterations:
            iteration += 1
            
            # Get current timestamp
            current_time = datetime.datetime.now()
            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"\n[{timestamp}] Checking power and CPU metrics of {len(ilo_systems)} systems...")
            total_watts = 0
            total_cpu = 0
            valid_power_readings = 0
            valid_cpu_readings = 0
            
            # Define a function that includes retry parameters to get both power and CPU
            def get_metrics_with_retries(system):
                return get_system_power_and_cpu(system, max_retries=max_retries, retry_delay=retry_delay, debug=debug)
            
            # Use ThreadPoolExecutor for parallel execution
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(get_metrics_with_retries, ilo_systems))
            
            # Calculate totals
            for i, result in enumerate(results):
                if isinstance(result, dict):
                    # Process power data
                    if result.get("watts") is not None:
                        total_watts += result["watts"]
                        valid_power_readings += 1
                    
                    # Process CPU data
                    if result.get("cpu_load") is not None:
                        total_cpu += result["cpu_load"]
                        valid_cpu_readings += 1
            
            # Calculate averages
            avg_watts = 0
            avg_cpu = None
            if valid_power_readings > 0:
                avg_watts = total_watts / valid_power_readings
            if valid_cpu_readings > 0:
                avg_cpu = total_cpu / valid_cpu_readings
            
            # Print summary
            print("\nSystem Metrics Summary:")
            print(f"  Total power consumption: {total_watts:.2f}W")
            print(f"  Average per server: {avg_watts:.2f}W")
            print(f"  Power readings from {valid_power_readings} out of {len(ilo_systems)} servers")
            
            if valid_cpu_readings > 0:
                print(f"  Average CPU utilization: {avg_cpu:.2f}%")
                print(f"  CPU readings from {valid_cpu_readings} out of {len(ilo_systems)} servers")
            else:
                print("  No CPU utilization data available")
            
            # Save to CSV
            save_power_data_to_csv(output_csv, timestamp, total_watts, avg_watts, 
                                avg_cpu, valid_power_readings, len(ilo_systems))
            print(f"  Data saved to {output_csv}")
            
            # If this is a one-off run or we've hit our iteration limit, exit
            if iterations is not None and iteration >= iterations:
                break
                
            # Wait for the next interval
            next_time = current_time + datetime.timedelta(minutes=interval_minutes)
            print(f"Next check scheduled for: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Sleep until next interval
            time.sleep(interval_minutes * 60)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
    except Exception as e:
        print(f"Error during monitoring: {str(e)}")
        if debug:
            print(traceback.format_exc())

def main():
    """
    Main function that processes command-line arguments and executes operations.
    """
    try:
        print(f"Starting HPE iLO Power Management Script v0.18")
        
        # Set up argument parser
        parser = argparse.ArgumentParser(
            description='Manage power state of HPE iLO systems from a CSV file or individual IP',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        
        # Create a mutually exclusive group for input source
        input_group = parser.add_mutually_exclusive_group(required=True)
        input_group.add_argument(
            '-f', '--file',
            help='Path to CSV file containing iLO system details (columns: ip, username, password)'
        )
        input_group.add_argument(
            '-i', '--ip',
            help='IP address of a single iLO system'
        )
        
        # Optional arguments for single IP mode
        parser.add_argument(
            '-u', '--username',
            default='Administrator',
            help='Username for iLO login (used with --ip)'
        )
        parser.add_argument(
            '-p', '--password',
            help='Password for iLO login (used with --ip)'
        )
        
        parser.add_argument(
            '-w', '--workers',
            type=int,
            default=11,
            help='Number of parallel workers for operations (CSV mode only)'
        )
        
        parser.add_argument(
            '-y', '--yes',
            action='store_true',
            help='Skip confirmation prompt'
        )
        
        parser.add_argument(
            '-d', '--debug',
            action='store_true',
            help='Print detailed debug information'
        )
        
        # Status detail level option
        parser.add_argument(
            '--details',
            action='store_true',
            help='Show detailed status information (used with --status)'
        )
        
        # Power monitoring specific options
        parser.add_argument(
            '--interval',
            type=int,
            default=20,
            help='Interval in minutes between power consumption checks (for --monitor-power)'
        )
        
        parser.add_argument(
            '--output-csv',
            help='CSV file to save power monitoring data (default: inputfile_power_history.csv)'
        )
        
        parser.add_argument(
            '--retries',
            type=int,
            default=3,
            help='Number of retries for failed power readings'
        )
        
        parser.add_argument(
            '--retry-delay',
            type=int,
            default=2,
            help='Delay in seconds between retries'
        )
        
        # Create a mutually exclusive group for actions
        action_group = parser.add_mutually_exclusive_group(required=True)
        action_group.add_argument(
            '-s', '--status',
            action='store_true',
            help='Check system status (basic by default, use --details for comprehensive info)'
        )
        action_group.add_argument(
            '-watts', '--power-watts',
            action='store_true',
            help='Only check power consumption in watts and total for all servers (one time)'
        )
        action_group.add_argument(
            '-monitor', '--monitor-power',
            action='store_true', 
            help='Monitor power consumption periodically and save to CSV file'
        )
        action_group.add_argument(
            '-cpu', '--get-cpu',
            action='store_true',
            help='Try to get CPU utilization using multiple methods (for newer iLO versions)'
        )
        action_group.add_argument(
            '-pp', '--get-power-policy',
            action='store_true',
            help='Get the current power policy settings for the server(s)'
        )
        action_group.add_argument(
            '-spp', '--set-power-policy',
            metavar='POLICY',
            help='Set power policy for the server(s) - examples: "Static Low Power", "Dynamic Power Savings"'
        )
        action_group.add_argument(
            '-on', '--power-on',
            action='store_true',
            help='Power on systems'
        )
        action_group.add_argument(
            '-off', '--power-off',
            action='store_true',
            help='Power off systems gracefully (OS-initiated shutdown)'
        )
        action_group.add_argument(
            '-force-off', '--force-power-off',
            action='store_true',
            help='Force power off systems immediately (like holding the power button)'
        )
        
        # Parse arguments
        args = parser.parse_args()
        
        # Enable debug mode if requested
        if args.debug:
            print("Debug mode enabled")
        
        # Determine if we're using a CSV file or single IP
        if args.file:
            # CSV file mode
            if not os.path.exists(args.file):
                print(f"Error: File '{args.file}' not found")
                return
            
            print(f"Reading systems from CSV file: {args.file}")
            ilo_systems = read_ilo_systems_from_csv(args.file, args.debug)
            
            if not ilo_systems:
                print("No valid iLO systems found. Exiting.")
                return
                
            # Display the detected systems
            print("\nDetected iLO systems:")
            for i, system in enumerate(ilo_systems, 1):
                print(f"{i}. IP: {system['ip']}, Username: {system['username']}")
                
            # Set up default output CSV filename if not specified
            if args.monitor_power and not args.output_csv:
                # Get the base filename without extension
                base_name = os.path.splitext(args.file)[0]
                args.output_csv = f"{base_name}_power_history.csv"
        else:
            # Single IP mode
            if not args.password:
                # Prompt for password if not provided
                password = getpass.getpass(f"Enter password for {args.username}@{args.ip}: ")
            else:
                password = args.password
                
            ilo_systems = [{
                'ip': args.ip,
                'username': args.username,
                'password': password
            }]
            print(f"\nUsing single system: IP: {args.ip}, Username: {args.username}")
            
            # Set default output CSV for single IP mode
            if args.monitor_power and not args.output_csv:
                args.output_csv = f"{args.ip}_power_history.csv"
        
        # Determine operation to perform
        if args.status:
            if args.details:
                operation = get_system_power_status
                operation_name = "detailed status check"
            else:
                operation = get_system_basic_status
                operation_name = "basic status check"
            operation_args = {}
        elif args.power_watts:
            operation = get_system_power_watts
            operation_name = "power metrics check"
            operation_args = {}
        elif args.monitor_power:
            operation = get_system_power_watts
            operation_name = "power monitoring"
            operation_args = {}
        elif args.get_cpu:
            operation = get_cpu_utilization
            operation_name = "CPU utilization check"
            operation_args = {'debug': args.debug}
        elif args.get_power_policy:
            operation = get_power_policy
            operation_name = "power policy check"
            operation_args = {'debug': args.debug}
        elif args.set_power_policy:
            operation = set_power_policy
            operation_name = "set power policy"
            operation_args = {'policy': args.set_power_policy, 'debug': args.debug}
        elif args.power_on:
            operation = power_on_system
            operation_name = "power on"
            operation_args = {}
        elif args.power_off or args.force_power_off:
            operation = power_off_system
            force_mode = args.force_power_off
            operation_name = "force power off" if force_mode else "graceful power off"
            operation_args = {'force': force_mode}
        
        # If status mode, just run it
        if args.status:
            print(f"\nChecking {operation_name} of {'all systems' if args.file else 'system'}...")
            if args.file:
                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    list(executor.map(operation, ilo_systems))
            else:
                operation(ilo_systems[0])
            return
        
        # If power-watts mode, collect and sum total power (one time)
        if args.power_watts:
            print(f"\nChecking power consumption of {'all systems' if args.file else 'system'}...")
            total_watts = 0
            valid_readings = 0
            
            # Define a function that includes retry parameters
            def get_power_with_retries(system):
                return get_system_power_watts(system, max_retries=args.retries, retry_delay=args.retry_delay)
                
            if args.file:
                # Use ThreadPoolExecutor for parallel execution
                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    results = list(executor.map(get_power_with_retries, ilo_systems))
                    
                # Calculate total
                for result in results:
                    if result is not None:
                        total_watts += result
                        valid_readings += 1
            else:
                # Single system
                watts = get_power_with_retries(ilo_systems[0])
                if watts is not None:
                    total_watts += watts
                    valid_readings += 1
            
            # Print summary
            if valid_readings > 0:
                print("\nPower Consumption Summary:")
                print(f"  Total power consumption: {total_watts:.2f}W")
                avg_watts = total_watts / valid_readings
                print(f"  Average per server: {avg_watts:.2f}W")
                print(f"  Readings from {valid_readings} out of {len(ilo_systems)} servers")
                
                # Save to CSV if output file specified
                if args.output_csv:
                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    csv_path = save_power_data_to_csv(args.output_csv, timestamp, total_watts, avg_watts, 
                                                   None, valid_readings, len(ilo_systems))
                    if csv_path:
                        print(f"  Data saved to {csv_path}")
            else:
                print("\nUnable to retrieve power consumption from any server")
            
            return
            
        # If monitor-power mode, start periodic checking and CSV logging
        if args.monitor_power:
            # Start monitoring
            monitor_power_periodically(
                interval_minutes=args.interval,
                ilo_systems=ilo_systems,
                workers=args.workers,
                output_csv=args.output_csv,
                max_retries=args.retries,
                retry_delay=args.retry_delay,
                debug=args.debug
            )
            return
        
        # If get-cpu mode
        if args.get_cpu:
            print(f"\nChecking CPU utilization of {'all systems' if args.file else 'system'}...")
            cpu_results = []
            
            # Define a function that includes retry parameters
            def get_cpu_with_retries(system):
                return get_cpu_utilization(system, max_retries=args.retries, retry_delay=args.retry_delay, debug=args.debug)
                
            if args.file:
                # Use ThreadPoolExecutor for parallel execution
                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    cpu_results = list(executor.map(get_cpu_with_retries, ilo_systems))
                    
                # Calculate total
                valid_readings = 0
                total_cpu = 0
                for result in cpu_results:
                    if result['cpu_load'] is not None:
                        total_cpu += result['cpu_load']
                        valid_readings += 1
                
                # Print summary
                if valid_readings > 0:
                    print("\nCPU Utilization Summary:")
                    print(f"  Average CPU utilization: {total_cpu / valid_readings:.2f}%")
                    print(f"  Readings from {valid_readings} out of {len(ilo_systems)} servers")
                else:
                    print("\nUnable to retrieve CPU utilization from any server")
                
                # Save to CSV if output file specified
                if args.output_csv:
                    try:
                        # Create a DataFrame with results and save to CSV
                        import pandas as pd
                        
                        data = []
                        for result in cpu_results:
                            row = {
                                'ip': result['ip'],
                                'identifier': result['identifier'],
                                'model': result['model'],
                                'cpu_load': result['cpu_load'] if result['cpu_load'] is not None else 'Unknown',
                                'methods_tried': ', '.join(result['cpu_methods_tried'])
                            }
                            data.append(row)
                        
                        df = pd.DataFrame(data)
                        df.to_csv(args.output_csv, index=False)
                        print(f"  Detailed CPU data saved to {args.output_csv}")
                    except Exception as e:
                        print(f"  Error saving CPU data to CSV: {str(e)}")
            else:
                # Single system
                cpu_result = get_cpu_with_retries(ilo_systems[0])
                
                # Save to CSV if output file specified and we have a result
                if args.output_csv and cpu_result['cpu_load'] is not None:
                    try:
                        # Save detailed CPU info to CSV
                        with open(args.output_csv, 'w', newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(['Metric', 'Value'])
                            writer.writerow(['IP', cpu_result['ip']])
                            writer.writerow(['Identifier', cpu_result['identifier']])
                            writer.writerow(['Model', cpu_result['model']])
                            writer.writerow(['CPU Load', f"{cpu_result['cpu_load']:.2f}%"])
                            writer.writerow(['Methods Tried', ', '.join(cpu_result['cpu_methods_tried'])])
                            
                            # Add CPU details
                            writer.writerow(['', ''])
                            writer.writerow(['Detail Metrics', ''])
                            for key, value in cpu_result['cpu_details'].items():
                                if not isinstance(value, (list, dict)):
                                    writer.writerow([key, value])
                            
                        print(f"  Detailed CPU data saved to {args.output_csv}")
                    except Exception as e:
                        print(f"  Error saving CPU data to CSV: {str(e)}")
            
            return
        
        # If get-power-policy mode
        if args.get_power_policy:
            print(f"\nChecking power policy of {'all systems' if args.file else 'system'}...")
            policy_results = []
            
            if args.file:
                # Use ThreadPoolExecutor for parallel execution
                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    policy_results = list(executor.map(lambda s: get_power_policy(s, debug=args.debug), ilo_systems))
                    
                # Summarize policies
                policies = {}
                for result in policy_results:
                    if result['current_policy'] is not None:
                        policy = result['current_policy']
                        if isinstance(policy, (int, float)):
                            # Map numeric values if possible
                            policy_map = {
                                1: "Static Low Power Mode",
                                2: "Dynamic Power Savings Mode", 
                                3: "Static High Performance Mode",
                                4: "OS Control Mode"
                            }
                            if policy in policy_map:
                                policy_str = f"{policy} ({policy_map[policy]})"
                            else:
                                policy_str = str(policy)
                        else:
                            policy_str = str(policy)
                        
                        if policy_str in policies:
                            policies[policy_str] += 1
                        else:
                            policies[policy_str] = 1
                
                # Print summary
                if policies:
                    print("\nPower Policy Summary:")
                    for policy, count in policies.items():
                        print(f"  {policy}: {count} system(s)")
                    print(f"  Unknown/Not found: {len(ilo_systems) - sum(policies.values())} system(s)")
                else:
                    print("\nUnable to retrieve power policy from any server")
                
                # Save to CSV if output file specified
                if args.output_csv:
                    try:
                        # Create a DataFrame with results and save to CSV
                        import pandas as pd
                        
                        data = []
                        for result in policy_results:
                            if result.get('current_policy') is not None:
                                policy = result['current_policy']
                                if isinstance(policy, (int, float)):
                                    policy_map = {
                                        1: "Static Low Power Mode",
                                        2: "Dynamic Power Savings Mode", 
                                        3: "Static High Performance Mode",
                                        4: "OS Control Mode"
                                    }
                                    if policy in policy_map:
                                        policy_desc = policy_map[policy]
                                    else:
                                        policy_desc = "Unknown"
                                else:
                                    policy_desc = "N/A"
                            else:
                                policy_desc = "Unknown"
                            
                            row = {
                                'ip': result.get('ip', "Unknown"),
                                'identifier': result.get('identifier', ""),
                                'model': result.get('model', "Unknown"),
                                'power_policy': result.get('current_policy', "Unknown"),
                                'policy_description': policy_desc,
                                'source': result.get('source', "Unknown")
                            }
                            data.append(row)
                        
                        df = pd.DataFrame(data)
                        df.to_csv(args.output_csv, index=False)
                        print(f"  Power policy data saved to {args.output_csv}")
                    except Exception as e:
                        print(f"  Error saving power policy data to CSV: {str(e)}")
            else:
                # Single system
                get_power_policy(ilo_systems[0], debug=args.debug)
            
            return
        
        # If set-power-policy mode
        if args.set_power_policy:
            print(f"\nSetting power policy to '{args.set_power_policy}' for {'all systems' if args.file else 'system'}...")
            policy_results = []
            
            if args.file:
                # Use ThreadPoolExecutor for parallel execution
                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    policy_results = list(executor.map(
                        lambda s: set_power_policy(s, args.set_power_policy, debug=args.debug), 
                        ilo_systems
                    ))
                    
                # Print summary
                success_count = policy_results.count(True)
                print(f"\nSet power policy operation completed:")
                print(f"  Successfully set power policy on {success_count} out of {len(ilo_systems)} systems")
            else:
                # Single system
                result = set_power_policy(ilo_systems[0], args.set_power_policy, debug=args.debug)
                print(f"\nSet power policy operation {'completed successfully' if result else 'failed'}.")
            
            return
        
        # Confirm before proceeding with power actions, unless --yes flag is used
        if not args.yes:
            confirm = input(f"\n{operation_name.capitalize()} {len(ilo_systems)} iLO {'systems' if len(ilo_systems) > 1 else 'system'}? (y/n): ")
            if confirm.lower() != 'y':
                print("Operation cancelled.")
                return
        
        # Perform the operation
        if args.file:
            # Use ThreadPoolExecutor for CSV mode
            max_workers = min(args.workers, len(ilo_systems))  # Don't create more workers than needed
            
            # If we have operation args, we need a different approach with executor
            if operation_args:
                results = []
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    for system in ilo_systems:
                        future = executor.submit(operation, system, **operation_args)
                        futures.append(future)
                    
                    for future in futures:
                        results.append(future.result())
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    results = list(executor.map(operation, ilo_systems))
                
            # Print summary
            success_count = results.count(True)
            print(f"\n{operation_name.capitalize()} operation completed.")
            print(f"Successfully performed {operation_name} on {success_count} out of {len(ilo_systems)} systems.")
        else:
            # Just run directly for single IP mode
            if operation_args:
                result = operation(ilo_systems[0], **operation_args)
            else:
                result = operation(ilo_systems[0])
            print(f"\n{operation_name.capitalize()} operation {'completed successfully' if result else 'failed'}.")
    
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        if args.debug if 'args' in locals() else False:
            print(traceback.format_exc())
        return
        
# Entry point
if __name__ == "__main__":
    main()