#!/usr/bin/env python3
"""
HPE iLO Power Management Script (using redfish library)

This script provides functionality to monitor and manage power states 
of HPE servers through their iLO interfaces via the Redfish API.

Features:
- Check basic and detailed status of servers
- Monitor power consumption periodically
- Get CPU utilization data
- Get and set server power management policies
- Power on/off servers (graceful or forced)
- Periodically collect and save monitoring data to CSV files
- Support for both single server and batch (CSV) operations
- Sort status output by IP address
- Display cluster information when available in CSV

Usage examples:
  python ilo_power_1.0.4.py -i 10.0.0.100 -u Administrator -s
  python ilo_power_1.0.4.py -f servers.csv -s
  python ilo_power_1.0.4.py -f servers.csv --power-watts
  python ilo_power_1.0.4.py -f servers.csv --monitor-power --interval 15
  python ilo_power_1.0.4.py -f servers.csv --power-on
  python ilo_power_1.0.4.py -f servers.csv --status --sort

Version: 1.0.4 (redfish based)

Requires: pip install redfish pandas numpy
"""

# Import standard libraries
import json
import pandas as pd
import numpy as np  # Add NumPy for numerical calculations
import argparse
from concurrent.futures import ThreadPoolExecutor
import sys
import getpass
import os
import csv
import time
import datetime
import traceback
import inspect
from pathlib import Path  # Add pathlib for better path handling
import warnings  # For pandas warnings suppression

# Suppress pandas warnings which are not critical for this application
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=np.VisibleDeprecationWarning)  # Suppress NumPy warnings too

# Import redfish library
try:
    import redfish
    REDFISH_AVAILABLE = True
    print(f"Redfish module found. Version: {getattr(redfish, '__version__', 'unknown')}")
except ImportError:
    REDFISH_AVAILABLE = False
    print("Error: redfish module not found.")
    print("Please install it using: pip install redfish")

# Disable SSL warnings (keeps code cleaner)
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

# Redfish API Path Constants
REDFISH_SYSTEM_PATH = "/redfish/v1/Systems/1"
REDFISH_CHASSIS_PATH = "/redfish/v1/Chassis/1"
REDFISH_MANAGER_PATH = "/redfish/v1/Managers/1"
REDFISH_POWER_PATH = f"{REDFISH_CHASSIS_PATH}/Power"
REDFISH_POWER_SUBSYSTEM_METRICS_PATH = f"{REDFISH_CHASSIS_PATH}/PowerSubsystem/PowerMetrics"
REDFISH_BIOS_PATH = f"{REDFISH_SYSTEM_PATH}/Bios"
REDFISH_BIOS_SETTINGS_PATH = f"{REDFISH_BIOS_PATH}/Settings"
REDFISH_SYSTEM_METRICS_PATH = f"{REDFISH_SYSTEM_PATH}/Metrics"
REDFISH_PROCESSOR_SUMMARY_METRICS_PATH = f"{REDFISH_SYSTEM_PATH}/ProcessorSummary/ProcessorMetrics"
REDFISH_PROCESSOR_COLLECTION_PATH = f"{REDFISH_SYSTEM_PATH}/Processors"
REDFISH_HPE_PROCESSOR_COLLECTION_PATH = f"{REDFISH_SYSTEM_PATH}/Oem/Hpe/Processors"
REDFISH_RESET_ACTION_PATH = f"{REDFISH_SYSTEM_PATH}/Actions/ComputerSystem.Reset"
REDFISH_HPE_POWER_MANAGEMENT_PATH = f"{REDFISH_SYSTEM_PATH}/Oem/Hpe/PowerManagement" # Used experimentally in set_power_policy

# Simple connection exception classes
class ConnectionError(Exception):
    pass

class AuthenticationError(Exception):
    pass

# Helper function for safe JSON parsing
def _safe_get_json(response, ip="N/A", debug=False, context=""):
    """Safely parse JSON from a Redfish response object."""
    caller_name = inspect.stack()[1].function # Get caller function name for context
    full_context = f"{caller_name}{f': {context}' if context else ''}"
    
    if not response:
        if debug: print(f"DEBUG [{ip}] {full_context}: Received None response object.")
        return None
    
    if response.status != 200:
        if debug: print(f"DEBUG [{ip}] {full_context}: Received non-200 status: {response.status}. Text: {getattr(response, 'text', 'N/A')[:100]}...")
        return None
        
    if not response.text:
        if debug: print(f"DEBUG [{ip}] {full_context}: Received empty response text.")
        return None

    try:
        data = json.loads(response.text)
        return data if data else {} # Return empty dict if JSON is null/empty
    except json.JSONDecodeError as e:
        if debug: print(f"DEBUG [{ip}] {full_context}: JSON parse error: {e}. Text: {getattr(response, 'text', 'N/A')[:100]}...")
        return None
    except Exception as e:
        if debug: print(f"DEBUG [{ip}] {full_context}: Unexpected error during JSON parsing: {e}")
        return None

# CSV functions
def read_ilo_systems_from_csv(csv_file_path, debug=False):
    """Read iLO system details from a CSV file with improved error handling and flexible format support.
    
    Args:
        csv_file_path (str): Path to the CSV file containing iLO systems information
        debug (bool): Whether to print debug information during parsing
        
    Returns:
        list: A list of dictionaries containing iLO system details
    """
    try:
        if debug:
            print(f"Reading CSV file: {csv_file_path}")
        
        # Convert to Path object for better handling
        csv_path = Path(csv_file_path)
        
        if not csv_path.exists():
            print(f"Error: File '{csv_file_path}' not found")
            return []
        
        # Try multiple approaches to read the file
        encodings = ['utf-8', 'utf-16', 'latin-1']
        separators = [',', ';', '\t']
        
        # Sample the file to determine if it's binary (helps with encoding detection)
        is_binary = False
        try:
            with open(csv_path, 'rb') as f:
                sample = f.read(4096)
                # Look for null bytes which would indicate binary/utf-16
                if b'\x00' in sample:
                    is_binary = True
                    if debug:
                        print("File appears to be binary/UTF-16 encoded")
        except Exception as e:
            if debug:
                print(f"Error sampling file: {e}")
        
        # Prioritize UTF-16 if file appears to be binary
        if is_binary:
            encodings = ['utf-16', 'utf-16-le', 'utf-8', 'latin-1']
        
        # Try different combinations of encoding and separator
        df = None
        error_messages = []
        
        for encoding in encodings:
            for sep in separators:
                try:
                    # Use pandas with specified encoding and separator
                    df = pd.read_csv(
                        csv_path, 
                        encoding=encoding, 
                        sep=sep,
                        engine='python',  # More flexible but slower engine
                        on_bad_lines='warn',  # Don't fail on problematic lines
                        dtype=str  # Treat all columns as strings to avoid type conversion issues
                    )
                    
                    if debug:
                        print(f"Successfully read CSV with encoding={encoding}, separator='{sep}'")
                        print(f"Found columns: {df.columns.tolist()}")
                    
                    # If we reach here, reading was successful
                    break
                except Exception as e:
                    error_messages.append(f"Failed with encoding={encoding}, sep='{sep}': {str(e)}")
                    continue
            
            # Break the outer loop if we've successfully read the file
            if df is not None:
                break
        
        # If all methods failed, fall back to manual reading
        if df is None:
            if debug:
                print("All pandas read attempts failed, falling back to manual reading")
                for msg in error_messages:
                    print(f"  {msg}")
            return read_ilo_systems_manually(csv_file_path, debug)
        
        # Map column names to expected fields
        col_mapping = {}
        required_fields = ['ip', 'username', 'password']
        optional_fields = ['cluster']  # Add cluster as an optional field
        
        # Find best column matches - try multiple approaches
        for field in required_fields + optional_fields:
            # Exact match
            if field in df.columns:
                col_mapping[field] = field
                continue
                
            # Case-insensitive match
            col_lower = [col.lower() for col in df.columns]
            if field.lower() in col_lower:
                idx = col_lower.index(field.lower())
                col_mapping[field] = df.columns[idx]
                continue
                
            # Substring match (e.g., "IP_Address" for "ip")
            matches = [col for col in df.columns if field.lower() in col.lower()]
            if matches:
                col_mapping[field] = matches[0]
                continue
        
        # If mapping is incomplete, try position-based approach for a standard 3-column CSV
        missing_fields = [field for field in required_fields if field not in col_mapping]
        if missing_fields and len(df.columns) >= 3:
            if debug:
                print(f"Could not find columns for: {missing_fields}, trying position-based mapping")
            # Map by position, assuming standard order
            if 'ip' not in col_mapping:
                col_mapping['ip'] = df.columns[0]
            if 'username' not in col_mapping and len(df.columns) > 1:
                col_mapping['username'] = df.columns[1]
            if 'password' not in col_mapping and len(df.columns) > 2:
                col_mapping['password'] = df.columns[2]
        
        if debug:
            print(f"Final column mapping: {col_mapping}")
        
        # Check if we have all required fields
        if not all(field in col_mapping for field in required_fields):
            missing = [field for field in required_fields if field not in col_mapping]
            print(f"Error: Could not find columns for required fields: {missing}")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        # Extract data using the mapping
        ilo_systems = []
        for _, row in df.iterrows():
            system = {}
            
            # Extract each field using the mapping
            for field, col in col_mapping.items():
                # Convert to string, handle NaN and None values
                value = row[col]
                if pd.isna(value) or value is None:
                    value = ""
                else:
                    value = str(value).strip()
                
                if value and value.lower() not in ['nan', 'none', 'null', '']:
                    system[field] = value
            
            # Only include systems with all required fields
            if all(field in system for field in required_fields):
                ilo_systems.append(system)
        
        if ilo_systems:
            print(f"Loaded {len(ilo_systems)} iLO systems from CSV file.")
            # Print if cluster information was found
            if 'cluster' in col_mapping:
                print(f"Found cluster information in the CSV file.")
            return ilo_systems
        else:
            print("No valid systems found in CSV file (all fields must be present).")
            return []
            
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        if debug:
            print(traceback.format_exc())
        return []

def read_ilo_systems_manually(csv_file_path, debug=False):
    """Manual fallback method to read CSV files."""
    ilo_systems = []
    
    # Try different encodings
    for encoding in ['utf-8', 'utf-16', 'utf-16-le', 'latin-1']:
        try:
            if debug:
                print(f"Trying manual read with {encoding} encoding...")
                
            with open(csv_file_path, 'r', encoding=encoding) as file:
                lines = file.readlines()
                lines = [line.strip() for line in lines if line.strip()]
                
                if not lines:
                    continue
                
                # Determine separator
                separator = ',' if len(lines[0].split(',')) >= 3 else ';'
                
                # Process headers
                headers = lines[0].split(separator)
                headers = [h.strip().strip('"').lower() for h in headers]
                
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
    
    print("Failed to read systems with any method.")
    return []

def save_power_data_to_csv(csv_path, timestamp, total_watts, avg_watts, avg_cpu_load, valid_readings, total_servers):
    """Save power consumption and CPU load data to a CSV file with improved pandas handling.
    
    Args:
        csv_path (str): Path to the CSV file to save data to
        timestamp (str): Formatted timestamp for the current reading
        total_watts (float): Total power consumption in watts
        avg_watts (float): Average power consumption per server in watts
        avg_cpu_load (float or None): Average CPU load percentage, or None if not available
        valid_readings (int): Number of servers with valid power readings
        total_servers (int): Total number of servers in the list
        
    Returns:
        str: Path to the CSV file if successful, None otherwise
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = Path(csv_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Format values appropriately
        avg_cpu_str = f"{avg_cpu_load:.2f}" if avg_cpu_load is not None else "Unknown"
        
        # Create a DataFrame with a single row of data
        data = {
            'timestamp': [timestamp],
            'total_power_watts': [f"{total_watts:.2f}"],
            'avg_power_watts': [f"{avg_watts:.2f}"],
            'avg_cpu_load': [avg_cpu_str],
            'valid_readings': [valid_readings],
            'total_servers': [total_servers]
        }
        df = pd.DataFrame(data)
        
        # Check if file exists to determine if we need to write headers
        file_exists = Path(csv_path).exists()
        
        # Write to CSV file - append if exists, create if doesn't
        if file_exists:
            df.to_csv(csv_path, mode='a', header=False, index=False)
        else:
            df.to_csv(csv_path, index=False)
        
        return csv_path
    except Exception as e:
        print(f"Error saving power data to CSV: {str(e)}")
        return None

def save_full_monitor_data_to_csv(csv_path, timestamp, system_metrics):
    """Save comprehensive monitoring data to a CSV file with improved pandas handling.
    
    Args:
        csv_path (str): Path to the CSV file to save data to
        timestamp (str): Formatted timestamp for the current reading
        system_metrics (list): List of dictionaries containing system metrics
        
    Returns:
        str: Path to the CSV file if successful, None otherwise
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = Path(csv_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create field names from the first system with data
        valid_system = None
        for s in system_metrics:
            if s and s.get('system_usage'):
                valid_system = s
                break
                
        if not valid_system:
            print("No valid system metrics found for CSV headers.")
            return None
            
        # Prepare data rows for DataFrame
        rows = []
        for system in system_metrics:
            if system:
                # Base row with timestamp and IP
                row = {'timestamp': timestamp, 'ip': system.get('ip')}
                
                # Add power and CPU data
                row['power_watts'] = f"{system.get('watts', 0):.2f}" if system.get('watts') is not None else "Unknown"
                row['cpu_load'] = f"{system.get('cpu_load', 0):.2f}" if system.get('cpu_load') is not None else "Unknown"
                
                # Add all SystemUsage metrics if available
                if system.get('system_usage'):
                    for key, value in system.get('system_usage', {}).items():
                        # Create a snake_case column name
                        col_name = key.lower()
                        # Format numeric values 
                        if isinstance(value, (int, float)):
                            row[col_name] = f"{value:.2f}" if isinstance(value, float) else str(value)
                        else:
                            row[col_name] = str(value) if value is not None else ""
                            
                rows.append(row)
        
        # Create DataFrame from rows
        if not rows:
            print("No valid data rows to save.")
            return None
            
        df = pd.DataFrame(rows)
        
        # Check if file exists
        file_exists = Path(csv_path).exists()
        
        # Write to CSV file
        if file_exists:
            # If file exists, append without headers
            df.to_csv(csv_path, mode='a', header=False, index=False)
        else:
            # If new file, write with headers
            df.to_csv(csv_path, index=False)
        
        return csv_path
    except Exception as e:
        print(f"Error saving full monitoring data to CSV: {str(e)}")
        traceback.print_exc()
        return None

def save_status_to_csv(csv_path, results):
    """Save server status information to a CSV file.
    
    Args:
        csv_path (str): Path to the CSV file to save data to
        results (list): List of dictionaries containing server status information
        
    Returns:
        str: Path to the CSV file if successful, None otherwise
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = Path(csv_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create DataFrame from results
        # Convert selected numeric fields
        for r in results:
            # Convert watts to float if it's not None
            if r.get('watts') is not None:
                r['watts'] = float(r['watts'])
            # Convert cpu_load to float if it's not None
            if r.get('cpu_load') is not None:
                r['cpu_load'] = float(r['cpu_load'])
            # Convert memory_gib to float if it's not None
            if r.get('memory_gib') is not None:
                r['memory_gib'] = float(r['memory_gib'])
        
        # Create DataFrame
        df = pd.DataFrame(results)
        
        # Reorder columns with key information first
        ordered_cols = ['ip', 'cluster', 'hostname', 'identifier', 'model', 'power_state', 
                        'health', 'watts', 'cpu_load', 'ilo_version', 'bios_version', 
                        'memory_gib', 'processor_summary', 'error']
        
        # Only use columns that exist in the DataFrame
        available_cols = [col for col in ordered_cols if col in df.columns]
        remaining_cols = [col for col in df.columns if col not in ordered_cols]
        final_cols = available_cols + remaining_cols
        
        # Reorder columns
        df = df[final_cols]
        
        # Write to CSV file
        df.to_csv(csv_path, index=False)
        
        return csv_path
    except Exception as e:
        print(f"Error saving status data to CSV: {str(e)}")
        traceback.print_exc()
        return None

# Improved Redfish client class with retry logic
class RedfishSession:
    """Context manager for Redfish client sessions with improved error handling and retry logic"""
    
    def __init__(self, system_info, max_retries=3, retry_delay=2):
        self.ip = system_info['ip']
        self.username = system_info['username']
        self.password = system_info['password']
        self.client = None
        self.debug = False  # Add a debug flag
        self.max_retries = max_retries  # Maximum number of retry attempts
        self.retry_delay = retry_delay  # Delay between retries in seconds

    def __enter__(self):
        retries = 0
        last_error = None
        
        while retries <= self.max_retries:
            try:
                # Create redfish client
                self.client = redfish.redfish_client(
                    base_url=f"https://{self.ip}",
                    username=self.username,
                    password=self.password,
                    default_prefix='/redfish/v1'
                )

                # Disable SSL verification
                if hasattr(self.client, 'session'):
                    self.client.session.verify = False

                # Try login with session auth first, then basic if needed
                try:
                    self.client.login(auth="session")
                    if self.debug and retries > 0:
                        print(f"DEBUG [{self.ip}] Connected successfully after {retries} retries")
                    return self.client
                except Exception as e:
                    if self.debug: 
                        print(f"DEBUG [{self.ip}] Session auth failed: {e}, trying basic auth...")
                    try:
                        self.client.login(auth="basic")
                        if self.debug and retries > 0:
                            print(f"DEBUG [{self.ip}] Connected with basic auth after {retries} retries")
                        return self.client
                    except Exception as basic_e:
                        print(f"Login failed for {self.ip} with both session and basic auth.")
                        if self.debug:
                            print(f"  Session Error: {e}")
                            print(f"  Basic Error: {basic_e}")
                        if self.client:
                            try: self.client.logout()
                            except: pass
                        self.client = None
                        last_error = basic_e
                        raise AuthenticationError(f"Login failed for {self.ip}") from basic_e

            except redfish.rest.connections.ConnectionError as conn_err:
                self.client = None
                last_error = conn_err
                if retries >= self.max_retries:
                    print(f"Connection Error connecting to {self.ip}: {conn_err} (after {retries} retries)")
                    raise ConnectionError(f"Connection failed for {self.ip}") from conn_err
                else:
                    if self.debug:
                        print(f"DEBUG [{self.ip}] Connection attempt {retries+1} failed: {conn_err}, retrying...")
                    time.sleep(self.retry_delay)  # Wait before retry
                    retries += 1
                    continue
                    
            except AuthenticationError:
                # Don't retry auth errors - they're unlikely to succeed on retry
                raise
                
            except Exception as e:
                self.client = None
                last_error = e
                if retries >= self.max_retries:
                    print(f"Generic Error connecting to {self.ip}: {e} (after {retries} retries)")
                    raise ConnectionError(f"Unhandled exception during connection to {self.ip}") from e
                else:
                    if self.debug:
                        print(f"DEBUG [{self.ip}] Connection attempt {retries+1} failed with unhandled exception: {e}, retrying...")
                    time.sleep(self.retry_delay)
                    retries += 1
                    continue
        
        # If we get here, all retries failed
        error_msg = f"Failed to connect to {self.ip} after {self.max_retries} attempts"
        print(error_msg)
        if self.debug and last_error:
            print(f"DEBUG [{self.ip}] Last error: {last_error}")
        raise ConnectionError(error_msg)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            try:
                self.client.logout()
            except Exception as e:
                if self.debug:
                    print(f"DEBUG [{self.ip}] Error during logout: {e}")
        self.client = None

# Server functions using Redfish
def get_system_identifier(client, debug=False):
    """Get server identifier (serial number or asset tag) and model"""
    identifier, model = "Unknown", "Unknown"
    ip = client.get_base_url().split('//')[-1] # Get IP from client base URL for debug
    try:
        resp = client.get(REDFISH_SYSTEM_PATH)
        data = _safe_get_json(resp, ip, debug)

        if data:
            asset_tag = data.get("AssetTag", "")
            serial_number = data.get("SerialNumber", "")
            model = data.get("Model", "Unknown")
            # Prioritize Asset Tag if available, otherwise use Serial Number
            identifier = asset_tag if asset_tag and asset_tag.strip() else serial_number
        # Debug message handled within _safe_get_json if parsing fails
        
    except Exception as e:
        if debug: print(f"DEBUG [{ip}] Error in get_system_identifier GET request: {e}")
        # Identifier and model remain "Unknown"
        
    return identifier, model


def get_power_watts(client, ip, identifier, debug=False, timeout=30):
    """Get power consumption in watts with multiple attempts and improved error handling"""
    watts = None
    endpoints_tried = []
    
    try:
        # Method 1: Standard Chassis power endpoint
        endpoints_tried.append(REDFISH_POWER_PATH)
        try:
            resp = client.get(endpoints_tried[-1], timeout=timeout)
            data = _safe_get_json(resp, ip, debug, context="Method 1")
            
            if data:
                if debug: print(f"DEBUG [{ip}] Power Response 1 (JSON): {data}")
                if "PowerControl" in data and isinstance(data["PowerControl"], list) and len(data["PowerControl"]) > 0:
                    for pc in data["PowerControl"]:
                        if "PowerConsumedWatts" in pc:
                            watts = pc.get("PowerConsumedWatts")
                            if watts is not None:
                                if debug: print(f"DEBUG [{ip}] Power found via PowerControl: {watts}W")
                                return watts # Return first valid reading
        except redfish.rest.connections.ConnectionError as ce:
            if debug: print(f"DEBUG [{ip}] Connection error in Method 1: {ce}")
        except Exception as e:
            if debug: print(f"DEBUG [{ip}] Error in Method 1: {e}")
        # Error logging handled by _safe_get_json

        # Method 2: Alternative endpoint (iLO 5/6 common)
        if watts is None:
            endpoints_tried.append(REDFISH_POWER_SUBSYSTEM_METRICS_PATH)
            try:
                resp_alt = client.get(endpoints_tried[-1], timeout=timeout)
                data_alt = _safe_get_json(resp_alt, ip, debug, context="Method 2")
                
                if data_alt:
                    if debug: print(f"DEBUG [{ip}] Power Response 2 (JSON): {data_alt}")
                    if "PowerMetrics" in data_alt and isinstance(data_alt["PowerMetrics"], dict) and "PowerConsumedWatts" in data_alt["PowerMetrics"]:
                         watts = data_alt["PowerMetrics"].get("PowerConsumedWatts") # Check nested structure
                    elif "PowerConsumedWatts" in data_alt: # Check direct key
                        watts = data_alt.get("PowerConsumedWatts")
                    elif "PowerWatts" in data_alt: # Check alternative key
                         watts = data_alt.get("PowerWatts")

                    if watts is not None:
                        if debug: print(f"DEBUG [{ip}] Power found via PowerSubsystem/PowerMetrics: {watts}W")
                        return watts
            except redfish.rest.connections.ConnectionError as ce:
                if debug: print(f"DEBUG [{ip}] Connection error in Method 2: {ce}")
            except Exception as e:
                if debug: print(f"DEBUG [{ip}] Error in Method 2: {e}")
            # Error logging handled by _safe_get_json

        # Method 3: Check System OEM data (less common for power, but worth a try)
        if watts is None:
             endpoints_tried.append(f"{REDFISH_SYSTEM_PATH} (OEM Check)")
             try:
                 resp_sys = client.get(REDFISH_SYSTEM_PATH, timeout=timeout)
                 sys_data = _safe_get_json(resp_sys, ip, debug, context="Method 3 OEM")
                 
                 if sys_data and "Oem" in sys_data:
                     oem_data = sys_data.get("Oem", {})
                     hpe_data = oem_data.get("Hpe", oem_data.get("Hp", {}))
                     if "PowerConsumedWatts" in hpe_data:
                         watts = hpe_data.get("PowerConsumedWatts")
                         if watts is not None:
                             if debug: print(f"DEBUG [{ip}] Power found via System OEM: {watts}W")
                             return watts
             except redfish.rest.connections.ConnectionError as ce:
                 if debug: print(f"DEBUG [{ip}] Connection error in Method 3: {ce}")
             except Exception as e:
                 if debug: print(f"DEBUG [{ip}] Error in Method 3: {e}")
             # Error logging handled by _safe_get_json

        # If still not found after all methods
        if watts is None:
            if debug: print(f"DEBUG [{ip}] Power not found. Endpoints tried: {endpoints_tried}")
            return None
        else:
             # Should have returned earlier if found, but handle case just in case
             if debug: print(f"DEBUG [{ip}] Power found (at end): {watts}W")
             return watts

    except Exception as e:
        print(f"Error getting power for {ip} ({identifier}): {e}")
        if debug:
            print(f"DEBUG [{ip}] Endpoints tried before error: {endpoints_tried}")
            traceback.print_exc()
        return None

def get_cpu_utilization(client, ip, identifier, debug=False, timeout=60):
    """Get CPU utilization using various methods common in HPE iLO with improved error handling"""
    cpu_load = None
    methods_tried = []

    # Method 1: System OEM data (Often present in iLO 5/6)
    try:
        methods_tried.append("System OEM Hpe/Hp")
        try:
            resp = client.get(REDFISH_SYSTEM_PATH, timeout=timeout)
            data = _safe_get_json(resp, ip, debug, context="Method 1 OEM")
            
            if data and "Oem" in data:
                oem_data = data.get("Oem", {})
                hpe_data = oem_data.get("Hpe", oem_data.get("Hp", {}))
                if debug: print(f"DEBUG [{ip}] System OEM Data: {hpe_data}")

                # First check common ProcessorUtilization key
                if hpe_data and "ProcessorUtilization" in hpe_data:
                    cpu_load = hpe_data.get("ProcessorUtilization")
                    if cpu_load is not None:
                        if debug: print(f"DEBUG [{ip}] CPU via System OEM (ProcessorUtilization): {cpu_load}%")
                        return cpu_load

                # Check SystemUsage -> CPUUtil key
                elif hpe_data and "SystemUsage" in hpe_data and isinstance(hpe_data["SystemUsage"], dict):
                     system_usage = hpe_data["SystemUsage"]
                     if "CPUUtil" in system_usage:
                          cpu_load = system_usage.get("CPUUtil")
                          if cpu_load is not None:
                               if debug: print(f"DEBUG [{ip}] CPU via System OEM (SystemUsage.CPUUtil): {cpu_load}%")
                               return cpu_load

                # Fallback debug check for other potential keys
                elif debug:
                    potential_keys = [k for k in hpe_data if 'util' in k.lower() or 'load' in k.lower()]
                    if potential_keys:
                         print(f"DEBUG [{ip}] System OEM primary CPU keys not found, but found potential keys: {potential_keys}")
        except redfish.rest.connections.ConnectionError as ce:
            if debug: print(f"DEBUG [{ip}] Connection error in Method 1: {ce}")
        except Exception as e:
            if debug: print(f"DEBUG [{ip}] Error in Method 1: {e}")

    except Exception as e:
        if debug: print(f"DEBUG [{ip}] CPU Method 1 Error: {e}")

    # Method 2: System Metrics endpoint (Common in newer Redfish implementations)
    if cpu_load is None:
        try:
            methods_tried.append("System Metrics")
            try:
                metrics_resp = client.get(REDFISH_SYSTEM_METRICS_PATH, timeout=timeout)
                metrics_data = _safe_get_json(metrics_resp, ip, debug, context="Method 2 Metrics")
                if metrics_data:
                    if "ProcessorSummary" in metrics_data and isinstance(metrics_data["ProcessorSummary"], dict) and "CPUUtilization" in metrics_data["ProcessorSummary"]:
                        cpu_load = metrics_data["ProcessorSummary"].get("CPUUtilization") # Nested path
                    elif "CPUUtilization" in metrics_data: # Direct path
                        cpu_load = metrics_data.get("CPUUtilization")

                    if cpu_load is not None:
                        if debug: print(f"DEBUG [{ip}] CPU via System Metrics: {cpu_load}%")
                        return cpu_load
            except redfish.rest.connections.ConnectionError as ce:
                if debug: print(f"DEBUG [{ip}] Connection error in Method 2: {ce}")
            except Exception as e:
                if debug: print(f"DEBUG [{ip}] Error in Method 2: {e}")
        except Exception as e:
            if debug: print(f"DEBUG [{ip}] CPU Method 2 Error: {e}")

    # Method 3: Processor Summary Metrics (Also common in newer implementations)
    if cpu_load is None:
        try:
            methods_tried.append("Processor Summary Metrics")
            try:
                proc_resp = client.get(REDFISH_PROCESSOR_SUMMARY_METRICS_PATH, timeout=timeout)
                proc_data = _safe_get_json(proc_resp, ip, debug, context="Method 3 Proc Summary")
                if proc_data:
                    # Common keys: TotalCorePercent, AverageFrequencyMHz, CPUUtilizationPercent
                    if "TotalCorePercent" in proc_data:
                        cpu_load = proc_data.get("TotalCorePercent")
                    elif "CPUUtilizationPercent" in proc_data:
                        cpu_load = proc_data.get("CPUUtilizationPercent")

                    if cpu_load is not None:
                        if debug: print(f"DEBUG [{ip}] CPU via Processor Summary: {cpu_load}%")
                        return cpu_load
            except redfish.rest.connections.ConnectionError as ce:
                if debug: print(f"DEBUG [{ip}] Connection error in Method 3: {ce}")
            except Exception as e:
                if debug: print(f"DEBUG [{ip}] Error in Method 3: {e}")
        except Exception as e:
            if debug: print(f"DEBUG [{ip}] CPU Method 3 Error: {e}")

    # Method 4: Iterate through Individual Processors (More complex, checks OEM data per CPU)
    if cpu_load is None:
        try:
            methods_tried.append("Individual Processors OEM")
            try:
                proc_coll_resp = client.get(REDFISH_PROCESSOR_COLLECTION_PATH, timeout=timeout)
                proc_collection = _safe_get_json(proc_coll_resp, ip, debug, context="Method 4 Proc Collection")
                
                if proc_collection and "Members" in proc_collection and isinstance(proc_collection["Members"], list):
                     cpu_loads = []
                     for member in proc_collection["Members"]:
                         if "@odata.id" in member:
                             proc_url = member["@odata.id"]
                             try:
                                 proc_detail_resp = client.get(proc_url, timeout=timeout)
                                 # Pass proc_url in context for better debug messages
                                 proc_detail = _safe_get_json(proc_detail_resp, ip, debug, context=f"Method 4 Detail {proc_url}") 
                                 if proc_detail and "Oem" in proc_detail:
                                     oem_proc = proc_detail.get("Oem", {})
                                     hpe_proc = oem_proc.get("Hpe", oem_proc.get("Hp", {}))
                                     # Add specific debug print for the processor OEM dict
                                     if debug: print(f"DEBUG [{ip}] Processor {proc_url} OEM Data: {hpe_proc}")
                                     util = None
                                     if "CurrentUtilization" in hpe_proc:
                                         util = hpe_proc["CurrentUtilization"]
                                     elif "ProcessorUtilization" in hpe_proc:
                                         util = hpe_proc["ProcessorUtilization"]
                                     elif "UtilizationPercent" in hpe_proc:
                                         util = hpe_proc["UtilizationPercent"]

                                     if util is not None and isinstance(util, (int, float)):
                                          cpu_loads.append(util)
                                          if debug: print(f"DEBUG [{ip}] Found CPU load {util}% for {proc_url} via OEM")
                                     # Add check for other potential keys if debug
                                     elif debug:
                                         potential_keys = [k for k in hpe_proc if 'util' in k.lower() or 'load' in k.lower()]
                                         if potential_keys:
                                             print(f"DEBUG [{ip}] Processor {proc_url} OEM util not found, but found potential keys: {potential_keys}")
                             except redfish.rest.connections.ConnectionError as ce:
                                 if debug: print(f"DEBUG [{ip}] Connection error getting processor {proc_url}: {ce}")
                             except Exception as e_detail:
                                  if debug: print(f"DEBUG [{ip}] Error fetching/processing detail for {proc_url}: {e_detail}")

                     # Calculate average CPU load if we collected any
                     if cpu_loads:
                         # Use numpy for better numerical precision
                         cpu_load = np.mean(cpu_loads)
                         if debug: print(f"DEBUG [{ip}] CPU via Individual Processors OEM avg: {cpu_load}%")
                         return cpu_load
            except redfish.rest.connections.ConnectionError as ce:
                if debug: print(f"DEBUG [{ip}] Connection error in Method 4: {ce}")
            except Exception as e:
                if debug: print(f"DEBUG [{ip}] Error in Method 4: {e}")
        except Exception as e:
             if debug: print(f"DEBUG [{ip}] CPU Method 4 Error: {e}")

    # Method 5: HPE Specific Processor Collection (Alternative OEM path)
    if cpu_load is None:
         try:
            methods_tried.append("HPE Processor Collection")
            try:
                proc_resp = client.get(REDFISH_HPE_PROCESSOR_COLLECTION_PATH, timeout=timeout) # Note specific Hpe path
                proc_data = _safe_get_json(proc_resp, ip, debug, context="Method 5 HPE Proc Collection")

                if proc_data and "Members" in proc_data and isinstance(proc_data["Members"], list) and len(proc_data["Members"]) > 0:
                     # Often contains 'AverageProcessorUtilization' directly
                     if "AverageProcessorUtilization" in proc_data:
                         cpu_load = proc_data.get("AverageProcessorUtilization")
                         if cpu_load is not None:
                             if debug: print(f"DEBUG [{ip}] CPU via HPE Processor Collection (Avg): {cpu_load}%")
                             return cpu_load
                     # Fallback: average individual members if avg not present
                     else:
                         cpu_loads = []
                         for member in proc_data["Members"]:
                              # Keys might be ProcessorUtilization, Utilization, etc.
                              util = None
                              if "ProcessorUtilization" in member: util = member["ProcessorUtilization"]
                              elif "Utilization" in member: util = member["Utilization"]

                              if util is not None and isinstance(util, (int, float)):
                                   cpu_loads.append(util)
                         if cpu_loads:
                              # Use numpy for better numerical precision
                              cpu_load = np.mean(cpu_loads)
                              if debug: print(f"DEBUG [{ip}] CPU via HPE Processor Collection (Member Avg): {cpu_load}%")
                              return cpu_load
                # _safe_get_json handles non-200 responses including 404
                elif debug and proc_resp and proc_resp.status == 404:
                     print(f"DEBUG [{ip}] HPE Processor Collection path not found (404).")
            except redfish.rest.connections.ConnectionError as ce:
                if debug: print(f"DEBUG [{ip}] Connection error in Method 5: {ce}")
            except Exception as e:
                if debug: print(f"DEBUG [{ip}] Error in Method 5: {e}")
         except Exception as e:
            if debug: print(f"DEBUG [{ip}] CPU Method 5 Error: {e}")

    # If CPU load still not found
    if cpu_load is None:
        if debug:
            print(f"[{ip}] CPU utilization not found after trying: {', '.join(methods_tried)}")
        return None
    else:
        # Should have returned earlier, but handle just in case
        if debug: print(f"DEBUG [{ip}] Final CPU load found: {cpu_load}%")
        return cpu_load

def get_power_status(client, ip, debug=False):
    """Get server power status"""
    power_state = "Unknown"
    try:
        resp = client.get(REDFISH_SYSTEM_PATH)
        data = _safe_get_json(resp, ip, debug)
        if data:
            power_state = data.get("PowerState", "Unknown")
        # Error reporting handled by _safe_get_json
        
    except Exception as e:
        if debug: print(f"DEBUG [{ip}] Error in get_power_status GET request: {e}")
        # power_state remains "Unknown"
        
    # Map potential JSON parsing issues reported by _safe_get_json to status strings
    if data is None and resp and resp.status != 200:
        return f"Unknown (Status {resp.status})"
    elif data is None and resp and not resp.text:
         return "Unknown (Empty Response)"
    elif data is None: # Catch JSONDecodeError or other _safe_get_json failures
         return "Unknown (JSON Error)"
         
    return power_state


def get_system_status(system, detailed=False, debug=False, print_output=True):
    """Get system status using Redfish API, enhanced logic"""
    ip = system["ip"]
    result = {
        "ip": ip, "power_state": "Unknown", "identifier": "Unknown", "model": "Unknown",
        "bios_version": "Unknown", "ilo_version": "Unknown", "health": "Unknown",
        "watts": None, "cpu_load": None, "memory_gib": None,
        "processor_summary": "Unknown", "error": None
    }
    
    # Add any additional information from the system dictionary
    if "cluster" in system:
        result["cluster"] = system["cluster"]
    
    session_manager = RedfishSession(system)
    session_manager.debug = debug

    with session_manager as client:
        if not client:
            result["error"] = f"Failed to connect to {ip}"
            print(result["error"])
            return False if print_output else result # Indicate failure

        try:
            # Get basic system info
            resp = client.get(REDFISH_SYSTEM_PATH)
            if debug: print(f"DEBUG [{ip}] Initial System GET status: {getattr(resp, 'status', 'N/A')}")
            data = _safe_get_json(resp, ip, debug, context="Initial System Info")

            # --- Handle failure to get initial system info ---
            if data is None:
                 error_context = f"(Status: {resp.status})" if resp else "(No Response)"
                 result["error"] = f"Initial system info request failed {error_context}"
                 print(f"{ip}: {result['error']}")
                 if debug and resp: print(f"DEBUG [{ip}] System Response Text: {getattr(resp, 'text', 'N/A')}")
                 # Attempt to get iLO version even if system info is bad
                 try:
                     manager_resp = client.get(REDFISH_MANAGER_PATH)
                     manager_data = _safe_get_json(manager_resp, ip, debug, context="Fallback Manager Info")
                     result["ilo_version"] = manager_data.get("FirmwareVersion", "Unknown") if manager_data else "Unknown (Error/Empty)"
                 except Exception: pass
                 if print_output:
                     print(f"{ip} | Model: Unknown | S/N: Unknown | Pwr: Unknown | Use: Unknown | Health: Unknown | iLO: {result['ilo_version']} | Error: {result['error']}")
                 return False if print_output else result


            # --- Process Initial System Info (if successful) ---
            if debug: print(f"DEBUG [{ip}] System Data (JSON Parsed Successfully)") # Simplified debug message

            result["power_state"] = data.get("PowerState", "Unknown")
            result["model"] = data.get("Model", "Unknown")
            result["bios_version"] = data.get("BiosVersion", "Unknown")

            # Get identifier (AssetTag or SerialNumber)
            asset_tag = data.get("AssetTag", "")
            serial_number = data.get("SerialNumber", "")
            result["identifier"] = asset_tag if asset_tag and asset_tag.strip() else serial_number

            # Get HostName if available - used for sorting
            result["hostname"] = data.get("HostName", "")

            # Get health status
            status_info = data.get("Status", {})
            if isinstance(status_info, dict):
                 result["health"] = status_info.get("HealthRollup", status_info.get("Health", "Unknown")) # Prefer HealthRollup

            # Get Memory Summary
            if "MemorySummary" in data and isinstance(data["MemorySummary"], dict):
                mem = data["MemorySummary"]
                result["memory_gib"] = mem.get('TotalSystemMemoryGiB')
                mem_status = mem.get("Status", {})
                if isinstance(mem_status, dict) and "Health" in mem_status and result["health"] != "Unknown":
                    result["health"] += f" (Mem: {mem_status['Health']})"

            # Get Processor Summary
            if "ProcessorSummary" in data and isinstance(data["ProcessorSummary"], dict):
                proc = data["ProcessorSummary"]
                proc_count = proc.get('Count')
                proc_model = proc.get('Model', '')
                if proc_count is not None:
                     result["processor_summary"] = f"{proc_count}x {proc_model}" if proc_model else str(proc_count)
                proc_status = proc.get("Status", {})
                if isinstance(proc_status, dict) and "Health" in proc_status and result["health"] != "Unknown":
                     result["health"] += f" (CPU: {proc_status['Health']})"


            # --- Get Additional Info ---
            # Get iLO version (from Manager endpoint)
            try:
                manager_resp = client.get(REDFISH_MANAGER_PATH)
                manager_data = _safe_get_json(manager_resp, ip, debug, context="Manager Info")
                if manager_data:
                    result["ilo_version"] = manager_data.get("FirmwareVersion", "Unknown")
                else:
                     status = getattr(manager_resp, 'status', 'N/A')
                     result["ilo_version"] = f"Unknown (Status {status})" if status != 200 else "Unknown (Empty/Error)"
            except Exception as e_mgr:
                 if debug: print(f"DEBUG [{ip}] Error getting manager info: {e_mgr}")
                 result["ilo_version"] = "Error"

            # Get Power Consumption
            result["watts"] = get_power_watts(client, ip, result["identifier"], debug=debug)

            # Get CPU Utilization (only if detailed or if basic failed to get power state)
            needs_cpu = detailed or result["power_state"] == "Unknown"
            if needs_cpu:
                result["cpu_load"] = get_cpu_utilization(client, ip, result["identifier"], debug=debug)

            # --- Output Formatting ---
            id_str = f"{result['model']} (S/N: {result['identifier']})" if result['identifier'] != 'Unknown' and result['identifier'] else result['model']
            
            if print_output:
                if detailed:
                    print(f"Detailed status for {ip} [{id_str}]:")
                    # Print cluster if available
                    if "cluster" in result:
                        print(f"  Cluster: {result['cluster']}")
                    print(f"  Health: {result['health']}")
                    print(f"  Power State: {result['power_state']}")
                    print(f"  iLO Version: {result['ilo_version']}")
                    print(f"  BIOS Version: {result['bios_version']}")
                    watts_str = f"{result['watts']}W" if result['watts'] is not None else "Unknown"
                    print(f"  Power Consumption: {watts_str}")
                    cpu_str = f"{result['cpu_load']:.1f}%" if result['cpu_load'] is not None else "Unknown"
                    print(f"  CPU Utilization: {cpu_str}")
                    mem_str = f"{result['memory_gib']} GiB" if result['memory_gib'] is not None else "Unknown"
                    print(f"  Memory: {mem_str}")
                    print(f"  Processors: {result['processor_summary']}")
                else:
                    # Basic status line
                    watts_str = f"{result['watts']}W" if result['watts'] is not None else "Unknown"
                    # Basic format: IP | [Cluster] | Model | S/N | Power | Watts | Health | iLO FW
                    cluster_str = f"{result['cluster']} | " if "cluster" in result else ""
                    print(f"{ip} | {cluster_str}{result['model']} | {result['identifier']} | Pwr: {result['power_state']} | Use: {watts_str} | Health: {result['health']} | iLO: {result['ilo_version']}")

            return True if print_output else result # Indicate success or return data

        except ConnectionError as ce: # Catch connection errors from session enter
            result["error"] = f"Connection Error for {ip}: {ce}"
            if print_output:
                print(result["error"])
            return False if print_output else result
        except AuthenticationError as ae: # Catch auth errors from session enter
             result["error"] = f"Authentication Error for {ip}: {ae}"
             if print_output:
                 print(result["error"])
             return False if print_output else result
        except Exception as e: # Catch other errors during status processing
            result["error"] = f"Unexpected error processing status for {ip}: {e}"
            if print_output:
                print(result["error"])
            if debug: traceback.print_exc() # Print stack trace in debug mode
            # Print basic info available before the error
            if print_output:
                print(f"{ip} | Model: {result['model']} | S/N: {result['identifier']} | Pwr: {result['power_state']} | Use: {result['watts']}W | Health: {result['health']} | iLO: {result['ilo_version']} | Error: Short Error Info")
            return False if print_output else result # Indicate failure


def power_on_system(system, debug=False):
    """Power on a server"""
    ip = system["ip"]
    session_manager = RedfishSession(system)
    session_manager.debug = debug

    with session_manager as client:
        if not client:
            print(f"Cannot connect to {ip}")
            return False

        # Check current state
        current_state = get_power_status(client, ip, debug) # Pass params
        if current_state == "On":
            print(f"System at {ip} is already powered on.")
            return True
        elif current_state.startswith("Unknown"): # Check if unknown due to error
             print(f"Warning: Could not determine current power state for {ip} ({current_state}). Attempting power on.")


        # Power on the server
        print(f"Powering on system at {ip}...")
        try:
            resp = client.post(REDFISH_RESET_ACTION_PATH, body={"ResetType": "On"})

            if resp.status in [200, 202, 204]:
                print(f"Successfully initiated power on for {ip}")
                return True
            else:
                # Try to get response text for debugging
                response_text = getattr(resp, 'text', 'No response text available.')
                print(f"Failed to power on {ip}. Status: {resp.status}, Response: {response_text}")
                return False
        except Exception as e:
            print(f"Error during power on for {ip}: {e}")
            if debug: traceback.print_exc()
            return False

def power_off_system(system, force=False, debug=False):
    """Power off a server"""
    ip = system["ip"]
    session_manager = RedfishSession(system)
    session_manager.debug = debug

    with session_manager as client:
        if not client:
            print(f"Cannot connect to {ip}")
            return False

        # Check current state
        current_state = get_power_status(client, ip, debug) # Pass params
        if current_state == "Off":
            print(f"System at {ip} is already powered off.")
            return True
        elif current_state.startswith("Unknown"): # Check if unknown due to error
             print(f"Warning: Could not determine current power state for {ip} ({current_state}). Attempting power off.")


        # Power off the server
        reset_type = "ForceOff" if force else "GracefulShutdown"
        shutdown_desc = "forced" if force else "graceful"

        print(f"Performing {shutdown_desc} shutdown for {ip}...")
        try:
            resp = client.post(REDFISH_RESET_ACTION_PATH, body={"ResetType": reset_type})

            if resp.status in [200, 202, 204]:
                print(f"Successfully initiated {shutdown_desc} shutdown for {ip}")
                return True
            else:
                 # Try to get response text for debugging
                 response_text = getattr(resp, 'text', 'No response text available.')
                 print(f"Failed to shutdown {ip}. Status: {resp.status}, Response: {response_text}")
                 return False
        except Exception as e:
            print(f"Error during shutdown for {ip}: {e}")
            if debug: traceback.print_exc()
            return False

def get_power_policy(system, debug=False):
    """Get the current power policy settings"""
    ip = system["ip"]
    result = {
        'ip': ip, 'identifier': "Unknown", 'current_policy': None,
        'available_policies': [], 'source': None, 'raw_bios': None, 'raw_power': None
    }
    session_manager = RedfishSession(system)
    session_manager.debug = debug

    with session_manager as client:
        if not client:
            print(f"Cannot connect to {ip}")
            return result # Return default result

        # NOTE: Using global _safe_get_json now, internal helper removed

        # Get system identifier
        result['identifier'], _ = get_system_identifier(client, debug)

        # Try BIOS settings first
        try:
            resp = client.get(REDFISH_BIOS_PATH)
            bios_data = _safe_get_json(resp, ip, debug, context="BIOS Check")
            result['raw_bios'] = bios_data # Store raw data if needed
            if bios_data and "Attributes" in bios_data:
                attrs = bios_data["Attributes"]
                # Common BIOS power profile keys
                power_keys = ["PowerProfile", "WorkloadProfile", "SysProfile", 
                              "HPStaticPowerRegulator", "PowerRegulator"] 
                for key in attrs:
                    # Check known keys or keys containing 'power' and 'profile'/'regulator'/'mode'
                    if key in power_keys or ("power" in key.lower() and any(term in key.lower() for term in ["profile", "regulator", "mode"])):
                        result['current_policy'] = attrs[key]
                        result['source'] = f"BIOS:{key}"
                        # Try to find allowed values (often in the BIOS resource itself or linked registry)
                        allowable_key = f"{key}@Redfish.AllowableValues"
                        if allowable_key in bios_data:
                            result['available_policies'] = bios_data[allowable_key]
                        # Try alternative location within Attributes definition if available
                        elif "@Redfish.Settings" in bios_data and "SupportedValues" in bios_data["@Redfish.Settings"]:
                             # This path is speculative, depends on vendor implementation
                             settings_defs = bios_data["@Redfish.Settings"].get("SupportedValues", {})
                             if key in settings_defs and isinstance(settings_defs[key], list):
                                 result['available_policies'] = settings_defs[key]
                                 
                        if result['current_policy'] is not None: break # Found policy in BIOS attributes
        except Exception as e:
            if debug: print(f"DEBUG [{ip}] Error checking BIOS for power policy: {e}")

        # Try HPE OEM power settings if BIOS check failed or policy not found
        if result['current_policy'] is None:
            try:
                resp = client.get(REDFISH_POWER_PATH)
                power_data = _safe_get_json(resp, ip, debug, context="OEM Power Check")
                result['raw_power'] = power_data # Store raw data if needed
                if power_data and "Oem" in power_data:
                    oem = power_data["Oem"]
                    # Check for Hpe or Hp key
                    hpe_data = oem.get("Hpe", oem.get("Hp", {}))

                    if "PowerRegulator" in hpe_data:
                        result['current_policy'] = hpe_data["PowerRegulator"]
                        result['source'] = "Oem.PowerRegulator"
                        if "PowerRegulatorModes" in hpe_data:
                            result['available_policies'] = hpe_data["PowerRegulatorModes"]
                    elif "PowerMode" in hpe_data: # Fallback check
                         result['current_policy'] = hpe_data["PowerMode"]
                         result['source'] = "Oem.PowerMode"
                         # Attempt to find available modes might be harder here

            except Exception as e:
                if debug: print(f"DEBUG [{ip}] Error checking OEM power data: {e}")

        # If no available policies found, use common ones as fallback
        if not result['available_policies']:
             result['available_policies'] = [
                 "Static Low Power Mode", "Dynamic Power Savings Mode",
                 "Static High Performance Mode", "OS Control Mode", "Maximum Performance" # Added another common one
             ]

    # Print results
    if result['current_policy'] is not None:
        print(f"System at {ip}: Current power policy: {result['current_policy']} (Source: {result.get('source', 'Unknown')})")
        # Only print available if they were explicitly found
        if result.get('source'):
             print(f"  Available policies: {', '.join(map(str, result['available_policies']))}")
    else:
        print(f"System at {ip}: Unable to determine power policy.")

    return result

def set_power_policy(system, policy, debug=False):
    """Set the power policy"""
    ip = system["ip"]
    session_manager = RedfishSession(system)
    session_manager.debug = debug

    with session_manager as client:
        if not client:
            print(f"Cannot connect to {ip}")
            return False

        # Get current policy source to know where to PATCH
        current = get_power_policy(system, debug) # Reuse get function

        # Determine target URL and payload based on source
        target_url = None
        payload = None
        requires_reboot = False

        if current.get('source', '').startswith("BIOS:"):
            setting_name = current['source'].split(':')[-1]
            target_url = REDFISH_BIOS_SETTINGS_PATH
            payload = {"Attributes": {setting_name: policy}}
            requires_reboot = True # BIOS changes usually need a reboot
            
        elif current.get('source', '').startswith("Oem.PowerRegulator"):
             # PATCHing the Chassis/Power endpoint with OEM data is common for HPE
             target_url = REDFISH_POWER_PATH
             # Payload structure might need adjustment based on specific iLO vendor (assume Hpe/Hp)
             payload = { "Oem": { "Hpe": { "PowerRegulator": policy } } }
             # Check if 'Hp' is needed instead of 'Hpe' based on raw data? (Future enhancement)
             print(f"Attempting to set OEM PowerRegulator via PATCH to {target_url}")
             # Reboot requirement for OEM changes can vary, assume not required unless specified by error msg
             requires_reboot = False 
             
        # Add more conditions here if other sources are identified

        if not target_url or not payload:
            # Fallback: Try common BIOS attribute name if source wasn't clear or unsupported
            print(f"Source unknown or unsupported ('{current.get('source')}'), attempting common BIOS setting 'PowerProfile'...")
            target_url = REDFISH_BIOS_SETTINGS_PATH
            payload = {"Attributes": {"PowerProfile": policy}} # Common fallback name
            requires_reboot = True


        print(f"Attempting to set policy '{policy}' via PATCH to {target_url}")
        try:
            resp = client.patch(target_url, body=payload)

            if resp.status in [200, 202, 204]:
                print(f"Successfully applied power policy '{policy}' for {ip}")
                # Check response headers for task info or messages indicating reboot needed
                if requires_reboot or 'reboot required' in getattr(resp, 'text', '').lower():
                    print("System reboot likely required for the change to take effect.")
                return True
            else:
                # Use _safe_get_json to parse potential error response, or grab text
                error_details = "No details in response text."
                error_data = _safe_get_json(resp, ip, debug, context="Set Policy Error") # Use helper
                
                if error_data and 'error' in error_data and isinstance(error_data['error'], dict):
                     err = error_data['error']
                     message_info = err.get('@Message.ExtendedInfo', [])
                     if message_info and isinstance(message_info, list) and len(message_info) > 0 and message_info[0].get('Message'):
                         error_details = message_info[0]['Message']
                     elif err.get('message'):
                         error_details = err['message']
                     else: # Fallback to string representation of error dict
                         error_details = str(err)[:200]
                elif resp and resp.text: # Use raw text if JSON parsing failed or structure unknown
                     error_details = getattr(resp, 'text', 'N/A')[:200]

                print(f"Failed to set power policy for {ip}. Status: {resp.status}, Details: {error_details}")
                return False
        except Exception as e:
            print(f"Error during PATCH for setting power policy for {ip}: {e}")
            if debug: traceback.print_exc()
            return False

def get_system_metrics_detailed(system, debug=False):
    """Get comprehensive system metrics including power, CPU, and system usage data."""
    ip = system["ip"]
    result = {'ip': ip, 'watts': None, 'cpu_load': None, 'system_usage': None}
    session_manager = RedfishSession(system)
    session_manager.debug = debug

    with session_manager as client:
        if not client:
            print(f"Cannot connect to {ip}")
            return result

        try:
            # Get identifier
            identifier, _ = get_system_identifier(client, debug)

            # Get power
            result['watts'] = get_power_watts(client, ip, identifier, debug)

            # Get CPU and system usage data
            # First check main system endpoint for OEM data that contains SystemUsage
            sys_resp = client.get(REDFISH_SYSTEM_PATH)
            sys_data = _safe_get_json(sys_resp, ip, debug, context="SystemUsage Check")

            if sys_data and "Oem" in sys_data:
                 oem_data = sys_data.get("Oem", {})
                 hpe_data = oem_data.get("Hpe", oem_data.get("Hp", {}))

                 # Get SystemUsage data
                 if hpe_data and "SystemUsage" in hpe_data:
                     result['system_usage'] = hpe_data["SystemUsage"]

                     # Extract CPU load from SystemUsage if available
                     if "CPUUtil" in result['system_usage']:
                         result['cpu_load'] = result['system_usage'].get("CPUUtil") # Use .get for safety

                 # If CPU not found in SystemUsage, check for ProcessorUtilization directly in OEM
                 if result['cpu_load'] is None and hpe_data and "ProcessorUtilization" in hpe_data:
                     result['cpu_load'] = hpe_data.get("ProcessorUtilization")


            # If CPU load not found yet, try the standard get_cpu_utilization function
            if result['cpu_load'] is None:
                result['cpu_load'] = get_cpu_utilization(client, ip, identifier, debug)

            # Format metrics for output
            watts_str = f"{result['watts']}W" if result['watts'] is not None else "Power unknown"
            cpu_str = f"{result['cpu_load']:.1f}%" if result['cpu_load'] is not None else "CPU unknown"
            usage_str = ""

            # Add system usage summary if available
            if result['system_usage'] and isinstance(result['system_usage'], dict): # Ensure it's a dict
                # Select key metrics to display in console output
                usage_metrics = []
                # Use .get() for safer access
                if "AvgCPU0Freq" in result['system_usage']:
                    usage_metrics.append(f"CPU0: {result['system_usage'].get('AvgCPU0Freq')}MHz")
                if "AvgCPU1Freq" in result['system_usage']:
                    usage_metrics.append(f"CPU1: {result['system_usage'].get('AvgCPU1Freq')}MHz")
                if "MemoryBusUtil" in result['system_usage']:
                    usage_metrics.append(f"MemBus: {result['system_usage'].get('MemoryBusUtil')}%")
                if "IOBusUtil" in result['system_usage']: # Add IO Bus if present
                    usage_metrics.append(f"IOBus: {result['system_usage'].get('IOBusUtil')}%")
                
                if usage_metrics:
                    usage_str = ", " + ", ".join(m for m in usage_metrics if 'None' not in m) # Filter out None values

            print(f"{ip}: {watts_str}, {cpu_str}{usage_str}")

        except Exception as e:
            print(f"Error getting detailed metrics for {ip}: {e}")
            if debug:
                traceback.print_exc()

    return result

def monitor_power(ilo_systems, interval_minutes, output_csv, workers=10, iterations=None, debug=False):
    """Monitor power and CPU periodically (basic monitoring) with NumPy for calculations"""
    print(f"Starting power and CPU monitoring every {interval_minutes} minutes")
    print(f"Saving data to: {output_csv}")
    
    iteration = 0
    try:
        while iterations is None or iteration < iterations:
            iteration += 1
            current_time = datetime.datetime.now()
            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"\n[{timestamp}] Checking metrics for {len(ilo_systems)} systems...")
            
            # Function to get metrics for a single system
            def get_system_metrics(system):
                ip = system["ip"]
                result = {'ip': ip, 'watts': None, 'cpu_load': None}
                
                with RedfishSession(system) as client:
                    if client:
                        # Get identifier
                        identifier, _ = get_system_identifier(client)
                        
                        # Get power
                        result['watts'] = get_power_watts(client, ip, identifier, debug)
                        
                        # Get CPU
                        result['cpu_load'] = get_cpu_utilization(client, ip, identifier, debug)
                        
                        # Print system results
                        watts_str = f"{result['watts']}W" if result['watts'] is not None else "Power unknown"
                        cpu_str = f"{result['cpu_load']:.1f}%" if result['cpu_load'] is not None else "CPU unknown"
                        print(f"{ip}: {watts_str}, {cpu_str}")
                
                return result
            
            # Process systems in parallel
            results = []
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(get_system_metrics, ilo_systems))
                
            # Extract watts and CPU load using NumPy for more efficient calculations
            watts_values = np.array([r['watts'] for r in results if r['watts'] is not None], dtype=np.float64)
            cpu_values = np.array([r['cpu_load'] for r in results if r['cpu_load'] is not None], dtype=np.float64)
            
            # Calculate totals and averages using NumPy
            valid_power_readings = len(watts_values)
            valid_cpu_readings = len(cpu_values)
            
            total_watts = np.sum(watts_values) if valid_power_readings > 0 else 0
            avg_watts = np.mean(watts_values) if valid_power_readings > 0 else 0
            avg_cpu = np.mean(cpu_values) if valid_cpu_readings > 0 else None
            
            # Print summary
            print(f"\nMonitoring Summary:")
            print(f"  Total power consumption: {total_watts:.2f}W")
            print(f"  Average per server: {avg_watts:.2f}W")
            print(f"  Power readings from {valid_power_readings} out of {len(ilo_systems)} servers")
            
            if avg_cpu is not None:
                print(f"  Average CPU utilization: {avg_cpu:.2f}%")
                print(f"  CPU readings from {valid_cpu_readings} out of {len(ilo_systems)} servers")
            
            # Save to CSV
            saved_path = save_power_data_to_csv(
                output_csv, timestamp, total_watts, avg_watts,
                avg_cpu, valid_power_readings, len(ilo_systems)
            )
            
            if saved_path:
                print(f"  Data saved to {saved_path}")
            
            # Check for iteration limit
            if iterations is not None and iteration >= iterations:
                break
                
            # Wait for next interval
            next_time = current_time + datetime.timedelta(minutes=interval_minutes)
            print(f"Next check scheduled for: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(interval_minutes * 60)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nError during monitoring: {str(e)}")
        if debug:
            traceback.print_exc()

def monitor_full(ilo_systems, interval_minutes, output_csv, workers=10, iterations=None, debug=False):
    """Monitor comprehensive system metrics including power, CPU and detailed SystemUsage with NumPy for calculations"""
    print(f"Starting FULL monitoring (power, CPU, and system metrics) every {interval_minutes} minutes")
    print(f"Saving detailed data to: {output_csv}")
    
    iteration = 0
    try:
        while iterations is None or iteration < iterations:
            iteration += 1
            current_time = datetime.datetime.now()
            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"\n[{timestamp}] Collecting detailed metrics for {len(ilo_systems)} systems...")
            
            # Process systems in parallel
            results = []
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(
                    lambda system: get_system_metrics_detailed(system, debug), 
                    ilo_systems
                ))
                
            # Extract watts and CPU load with NumPy for more efficient calculations
            watts_values = np.array([r['watts'] for r in results if r['watts'] is not None], dtype=np.float64)
            cpu_values = np.array([r['cpu_load'] for r in results if r['cpu_load'] is not None], dtype=np.float64)
            
            # Calculate totals and averages using NumPy
            valid_power_readings = len(watts_values)
            valid_cpu_readings = len(cpu_values)
            
            total_watts = np.sum(watts_values) if valid_power_readings > 0 else 0
            avg_watts = np.mean(watts_values) if valid_power_readings > 0 else 0
            avg_cpu = np.mean(cpu_values) if valid_cpu_readings > 0 else None
            
            # Print summary
            print(f"\nFull Monitoring Summary:")
            print(f"  Total power consumption: {total_watts:.2f}W")
            print(f"  Average per server: {avg_watts:.2f}W")
            print(f"  Power readings from {valid_power_readings} out of {len(ilo_systems)} servers")
            
            if avg_cpu is not None:
                print(f"  Average CPU utilization: {avg_cpu:.2f}%")
                print(f"  CPU readings from {valid_cpu_readings} out of {len(ilo_systems)} servers")
            
            # Count systems with SystemUsage data
            systems_with_usage = sum(1 for r in results if r.get('system_usage'))
            if systems_with_usage > 0:
                print(f"  Detailed system metrics from {systems_with_usage} out of {len(ilo_systems)} servers")
            else:
                print("  No detailed system metrics available from any server")
            
            # Save detailed data to CSV
            saved_path = save_full_monitor_data_to_csv(output_csv, timestamp, results)
            
            if saved_path:
                print(f"  Detailed data saved to {saved_path}")
            
            # Check for iteration limit
            if iterations is not None and iteration >= iterations:
                break
                
            # Wait for next interval
            next_time = current_time + datetime.timedelta(minutes=interval_minutes)
            print(f"Next check scheduled for: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(interval_minutes * 60)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nError during full monitoring: {str(e)}")
        if debug:
            traceback.print_exc()

def sort_ip_address_key(ip_str):
    """Convert IP address string to a tuple of integers for proper sorting"""
    try:
        # Split the IP address by dots and convert each octet to an integer
        return tuple(int(octet) for octet in ip_str.split('.'))
    except (ValueError, AttributeError):
        # If not a valid IP format, return a tuple of zeros
        return (0, 0, 0, 0)

def main():
    """Main function"""
    try:
        # Direct test if requested
        if len(sys.argv) > 1 and sys.argv[1] == "--test-redfish":
            if len(sys.argv) >= 5:
                test_redfish_direct(sys.argv[2], sys.argv[3], sys.argv[4])
            else:
                print(f"Usage: python {os.path.basename(__file__)} --test-redfish <ip> <username> <password>")
            return

        # Check package versions and availability
        version_info = []
        version_info.append(f"HPE iLO Power Management Script v1.0.4 (redfish based with NumPy)")
        
        # Check NumPy
        try:
            version_info.append(f"NumPy: {np.__version__}")
        except (AttributeError, ImportError):
            version_info.append(f"NumPy: not available")
            print("Warning: NumPy module not found or version detection failed.")
            print("For best performance, please install it using: pip install numpy")
        
        # Check Pandas
        try:
            version_info.append(f"Pandas: {pd.__version__}")
        except (AttributeError, ImportError):
            version_info.append(f"Pandas: not available")
            print("Warning: Pandas module not found or version detection failed.")
            print("Please install it using: pip install pandas")
        
        # Check Redfish
        if REDFISH_AVAILABLE:
            version_info.append(f"Redfish: {getattr(redfish, '__version__', 'unknown')}")
        else:
            version_info.append(f"Redfish: not available")
            print("Error: redfish module not found.")
            print("Please install it using: pip install redfish")
            return
        
        # Print version information
        print("Starting " + version_info[0])
        print("Libraries: " + ", ".join(version_info[1:]))

        # Set up argument parser
        parser = argparse.ArgumentParser(
            description='Manage power states of HPE iLO systems using the Redfish API',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        
        # Input source arguments
        input_group = parser.add_mutually_exclusive_group(required=True)
        input_group.add_argument('-f', '--file', help='CSV file with iLO systems (ip,username,password)')
        input_group.add_argument('-i', '--ip', help='IP address of a single iLO system')
        
        # Authentication for single system
        parser.add_argument('-u', '--username', default='Administrator', help='Username for iLO')
        parser.add_argument('-p', '--password', help='Password for iLO')
        
        # General options
        parser.add_argument('-w', '--workers', type=int, default=10, help='Number of parallel workers')
        parser.add_argument('-y', '--yes', action='store_true', help='Skip confirmation prompts')
        parser.add_argument('-d', '--debug', action='store_true', help='Show debug information')
        parser.add_argument('--details', action='store_true', help='Show detailed status info')
        parser.add_argument('--sort', action='store_true', help='Sort status output by hostname/identifier')
        
        # Monitoring options
        parser.add_argument('--interval', type=int, default=15, help='Monitoring interval (minutes)')
        parser.add_argument('--output-csv', help='CSV file for output data')
        
        # Actions
        action_group = parser.add_mutually_exclusive_group(required=True)
        action_group.add_argument('-s', '--status', action='store_true', help='Check system status')
        action_group.add_argument('-watts', '--power-watts', action='store_true', help='Get power consumption')
        # New monitor options
        action_group.add_argument('-m', '--monitor', action='store_true', help='Monitor power and CPU over time')
        action_group.add_argument('-m-full', '--monitor-full', action='store_true', 
                                  help='Monitor power, CPU and all system metrics over time (comprehensive)')
        # Keep old monitor option for backward compatibility
        action_group.add_argument('-monitor', '--monitor-power', action='store_true', 
                                  help='Monitor power over time (same as --monitor for backward compatibility)')
        action_group.add_argument('-cpu', '--get-cpu', action='store_true', help='Get CPU utilization')
        action_group.add_argument('-pp', '--get-power-policy', action='store_true', help='Get power policy')
        action_group.add_argument('-spp', '--set-power-policy', metavar='POLICY', help='Set power policy')
        action_group.add_argument('-on', '--power-on', action='store_true', help='Power on systems')
        action_group.add_argument('-off', '--power-off', action='store_true', help='Graceful shutdown')
        action_group.add_argument('-force-off', '--force-power-off', action='store_true', help='Force power off')
        
        args = parser.parse_args()
        
        # Load systems
        if args.file:
            file_path = Path(args.file)
            if not file_path.exists():
                print(f"Error: File '{args.file}' not found")
                return
                
            print(f"Reading systems from CSV file: {file_path}")
            ilo_systems = read_ilo_systems_from_csv(file_path, args.debug)
            
            if not ilo_systems:
                print("No valid iLO systems found. Exiting.")
                return
                
            print(f"Loaded {len(ilo_systems)} systems.")
            
            # Set default output CSV if needed
            if not args.output_csv and (args.monitor or args.monitor_power or args.monitor_full or args.get_cpu or args.power_watts):
                # Get just the filename without directory and extension
                base_name = file_path.stem
                # Ensure output directory exists
                Path('output').mkdir(exist_ok=True)
                
                if args.monitor or args.monitor_power:
                    args.output_csv = f"output/{base_name}_power_cpu_history.csv"
                elif args.monitor_full:
                    args.output_csv = f"output/{base_name}_full_metrics.csv"
                elif args.get_cpu:
                    args.output_csv = f"output/{base_name}_cpu_data.csv"
                elif args.power_watts:
                    args.output_csv = f"output/{base_name}_power_data.csv"
        else:
            # Single system mode
            if not args.password:
                password = getpass.getpass(f"Enter password for {args.username}@{args.ip}: ")
            else:
                password = args.password
                
            ilo_systems = [{'ip': args.ip, 'username': args.username, 'password': password}]
            print(f"Using single system: {args.ip}")
            
            # Set default output CSV for single system
            if not args.output_csv and (args.monitor or args.monitor_power or args.monitor_full or args.get_cpu or args.power_watts):
                ip_safe = args.ip.replace('.', '_')
                # Ensure output directory exists
                Path('output').mkdir(exist_ok=True)
                
                if args.monitor or args.monitor_power:
                    args.output_csv = f"output/{ip_safe}_power_cpu_history.csv"
                elif args.monitor_full:
                    args.output_csv = f"output/{ip_safe}_full_metrics.csv"
                elif args.get_cpu:
                    args.output_csv = f"output/{ip_safe}_cpu_data.csv"
                elif args.power_watts:
                    args.output_csv = f"output/{ip_safe}_power_data.csv"

        # Execute requested action
        if args.status:
            print("Checking system status...")
            
            if args.sort:
                # When sorting, collect results first
                success_count = 0
                total_count = len(ilo_systems)
                all_results = []
                
                # Use ThreadPoolExecutor but don't print results yet
                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    # Map get_system_status with print_output=False
                    results = list(executor.map(
                        lambda s: get_system_status(s, detailed=args.details, debug=args.debug, print_output=False),
                        ilo_systems
                    ))
                    
                    # Filter out failures and count successes
                    for r in results:
                        if r is not True and r is not False:  # It's a result dict
                            all_results.append(r)
                            if "error" not in r or not r["error"]:
                                success_count += 1
                
                # Sort results by IP address using the dedicated sort_ip_address_key function
                sorted_results = sorted(all_results, key=lambda x: sort_ip_address_key(x.get("ip", "")))
                
                # Save results to CSV if using input file
                if args.file:
                    # Create output filename based on input filename
                    input_file = Path(args.file)
                    base_name = input_file.stem
                    # Ensure output directory exists
                    Path('output').mkdir(exist_ok=True)
                    output_csv = f"output/{base_name}_status.csv"
                    
                    # Save status to CSV
                    saved_path = save_status_to_csv(output_csv, sorted_results)
                    if saved_path:
                        print(f"Status information saved to {saved_path}")
                
                # Print sorted results
                for result in sorted_results:
                    ip = result["ip"]
                    id_str = f"{result['model']} (S/N: {result['identifier']})" if result['identifier'] != 'Unknown' and result['identifier'] else result['model']
                    
                    if args.details:
                        print(f"Detailed status for {ip} [{id_str}]:")
                        # Print cluster if available
                        if "cluster" in result:
                            print(f"  Cluster: {result['cluster']}")
                        print(f"  Health: {result['health']}")
                        print(f"  Power State: {result['power_state']}")
                        print(f"  iLO Version: {result['ilo_version']}")
                        print(f"  BIOS Version: {result['bios_version']}")
                        watts_str = f"{result['watts']}W" if result['watts'] is not None else "Unknown"
                        print(f"  Power Consumption: {watts_str}")
                        cpu_str = f"{result['cpu_load']:.1f}%" if result['cpu_load'] is not None else "Unknown"
                        print(f"  CPU Utilization: {cpu_str}")
                        mem_str = f"{result['memory_gib']} GiB" if result['memory_gib'] is not None else "Unknown"
                        print(f"  Memory: {mem_str}")
                        print(f"  Processors: {result['processor_summary']}")
                    else:
                        # Basic status line
                        watts_str = f"{result['watts']}W" if result['watts'] is not None else "Unknown"
                        # Basic format: IP | [Cluster] | Model | S/N | Power | Watts | Health | iLO FW
                        cluster_str = f"{result['cluster']} | " if "cluster" in result else ""
                        print(f"{ip} | {cluster_str}{result['model']} | {result['identifier']} | Pwr: {result['power_state']} | Use: {watts_str} | Health: {result['health']} | iLO: {result['ilo_version']}")
                
            else:
                # Original behavior - print as we go
                success_count = 0
                total_count = len(ilo_systems)
                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    # Map get_system_status to each system, passing necessary args
                    results = list(executor.map(
                        lambda s: get_system_status(s, detailed=args.details, debug=args.debug),
                        ilo_systems
                    ))
                    # Count successful results (get_system_status returns True on success)
                    success_count = sum(1 for r in results if r is True)
            
            print(f"Status check complete. Successfully retrieved status for {success_count}/{total_count} systems.")

        elif args.power_watts:
            print("Getting power consumption...")
            results = []
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                def get_power(system):
                    ip = system['ip']
                    # Use a try-except block within the thread function for robustness
                    try:
                        with RedfishSession(system) as client:
                            if client:
                                identifier, _ = get_system_identifier(client, debug=args.debug)
                                # Pass debug flag down
                                watts = get_power_watts(client, ip, identifier, debug=args.debug)
                                if watts is not None:
                                    print(f"{ip}: {watts}W")
                                    return watts
                        return None # Return None if client failed or watts not found
                    except (ConnectionError, AuthenticationError) as sess_err:
                         # Errors already printed by RedfishSession context manager
                         if args.debug: print(f"DEBUG [{ip}] Session error in get_power thread: {sess_err}")
                         return None
                    except Exception as e:
                         print(f"Error getting power in thread for {ip}: {e}")
                         if args.debug: traceback.print_exc()
                         return None

                results = list(executor.map(get_power, ilo_systems))

            # Calculate total power using NumPy
            valid_results = np.array([w for w in results if w is not None], dtype=np.float64)
            if len(valid_results) > 0:
                total_watts = np.sum(valid_results)
                avg_watts = np.mean(valid_results)
                print(f"Total power consumption: {total_watts:.2f}W")
                print(f"Average per server: {avg_watts:.2f}W")
                print(f"Readings from {len(valid_results)} out of {len(ilo_systems)} servers")

                # Save to CSV if requested
                if args.output_csv:
                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # Note: save_power_data_to_csv expects avg_cpu_load, pass None here
                    saved_path = save_power_data_to_csv(
                        args.output_csv, timestamp, total_watts, avg_watts,
                        None, len(valid_results), len(ilo_systems)
                    )
                    if saved_path: print(f"Data saved to {saved_path}")
            else:
                print("No valid power readings obtained.")

        elif args.monitor or args.monitor_power:
            # Check if output CSV is provided
            if not args.output_csv:
                 print("Error: --output-csv is required for monitoring modes.")
                 return
            # Use standard monitoring function (includes CPU)
            monitor_power(
                ilo_systems=ilo_systems,
                interval_minutes=args.interval,
                output_csv=args.output_csv,
                workers=args.workers,
                debug=args.debug
            )

        elif args.monitor_full:
             # Check if output CSV is provided
            if not args.output_csv:
                 print("Error: --output-csv is required for monitoring modes.")
                 return
            # Use comprehensive monitoring function
            monitor_full(
                ilo_systems=ilo_systems,
                interval_minutes=args.interval,
                output_csv=args.output_csv,
                workers=args.workers,
                debug=args.debug
            )

        elif args.get_cpu:
            print("Getting CPU utilization...")
            results = []
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                def get_cpu(system):
                    ip = system['ip']
                    try:
                        with RedfishSession(system) as client:
                            if client:
                                identifier, _ = get_system_identifier(client, debug=args.debug)
                                # Pass debug flag down
                                cpu = get_cpu_utilization(client, ip, identifier, debug=args.debug)
                                if cpu is not None:
                                    print(f"{ip}: {cpu:.1f}%") # Format output
                                    return cpu
                        return None
                    except (ConnectionError, AuthenticationError) as sess_err:
                         if args.debug: print(f"DEBUG [{ip}] Session error in get_cpu thread: {sess_err}")
                         return None
                    except Exception as e:
                         print(f"Error getting CPU in thread for {ip}: {e}")
                         if args.debug: traceback.print_exc()
                         return None

                results = list(executor.map(get_cpu, ilo_systems))

            # Calculate average CPU using NumPy
            valid_results = np.array([c for c in results if c is not None], dtype=np.float64)
            if len(valid_results) > 0:
                avg_cpu = np.mean(valid_results)
                std_cpu = np.std(valid_results)  # Also calculate standard deviation with NumPy
                min_cpu = np.min(valid_results)  # Min CPU value
                max_cpu = np.max(valid_results)  # Max CPU value
                
                print(f"Average CPU utilization: {avg_cpu:.2f}%")
                print(f"CPU utilization std dev: {std_cpu:.2f}%")
                print(f"Min/Max CPU utilization: {min_cpu:.2f}% / {max_cpu:.2f}%")
                print(f"Readings from {len(valid_results)} out of {len(ilo_systems)} servers")
            else:
                print("No valid CPU readings obtained.")

        elif args.get_power_policy:
            print("Getting power policy settings...")
            # Use ThreadPoolExecutor for potentially faster checks
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                 # Map get_power_policy, passing the debug flag
                 list(executor.map(lambda s: get_power_policy(s, debug=args.debug), ilo_systems))
            print("Power policy check complete.")


        elif args.set_power_policy:
            # Confirm operation
            if not args.yes:
                confirm = input(f"Set power policy to '{args.set_power_policy}' for {len(ilo_systems)} system(s)? (y/n): ")
                if confirm.lower() != 'y':
                    print("Operation cancelled.")
                    return

            print(f"Setting power policy to '{args.set_power_policy}'...")
            success_count = 0
            # Use ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                 results = list(executor.map(
                     lambda s: set_power_policy(s, args.set_power_policy, debug=args.debug), # Pass debug
                     ilo_systems
                 ))
                 success_count = sum(1 for r in results if r is True)

            print(f"Power policy update completed. Success: {success_count}/{len(ilo_systems)}")

        elif args.power_on:
            # Confirm operation
            if not args.yes:
                confirm = input(f"Power ON {len(ilo_systems)} system(s)? (y/n): ")
                if confirm.lower() != 'y':
                    print("Operation cancelled.")
                    return

            print("Powering on systems...")
            success_count = 0
            # Use ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                 results = list(executor.map(
                     lambda s: power_on_system(s, debug=args.debug), # Pass debug
                     ilo_systems
                 ))
                 success_count = sum(1 for r in results if r is True)

            print(f"Power on completed. Success: {success_count}/{len(ilo_systems)}")

        elif args.power_off or args.force_power_off:
            force_mode = args.force_power_off
            action = "Force power OFF" if force_mode else "Graceful shutdown"

            # Confirm operation
            if not args.yes:
                confirm = input(f"{action} {len(ilo_systems)} system(s)? (y/n): ")
                if confirm.lower() != 'y':
                    print("Operation cancelled.")
                    return

            print(f"Performing {action.lower()}...")
            success_count = 0
            # Use ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                 results = list(executor.map(
                     lambda s: power_off_system(s, force=force_mode, debug=args.debug), # Pass debug
                     ilo_systems
                 ))
                 success_count = sum(1 for r in results if r is True)

            print(f"{action} completed. Success: {success_count}/{len(ilo_systems)}")

    except KeyboardInterrupt:
        print("Operation cancelled by user.")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

# Simple test function to directly test the redfish client
def test_redfish_direct(ip, username, password):
    """Simple test of redfish connectivity"""
    print(f"Testing direct redfish connection to {ip}")
    debug = True # Enable debug for this test function

    try:
        print("Creating redfish client...")
        client = redfish.redfish_client(
            base_url=f"https://{ip}",
            username=username,
            password=password,
            default_prefix='/redfish/v1'
        )

        # Disable SSL warnings
        if hasattr(client, 'session'):
            client.session.verify = False

        print("Logging in...")
        # Using the RedfishSession context manager is preferred even for tests
        system_info = {'ip': ip, 'username': username, 'password': password}
        with RedfishSession(system_info) as test_client: # Use context manager
            if not test_client:
                 print("Login failed via RedfishSession.")
                 return # Exit if session failed

            print("Login successful!")

            print("Getting system information...")
            response = test_client.get(REDFISH_SYSTEM_PATH)
            print(f"Response status: {response.status}")
            data = _safe_get_json(response, ip, debug, context="Test System Info") # Use safe helper

            if data:
                print(f"System Model: {data.get('Model', 'Unknown')}")
                print(f"Serial Number: {data.get('SerialNumber', 'Unknown')}")
                print(f"Power State: {data.get('PowerState', 'Unknown')}")
            else:
                print("Could not retrieve or parse system data.")

            # No explicit logout needed with context manager
            print("Session automatically logged out.")
        
        print("Test completed successfully")

    except (ConnectionError, AuthenticationError) as sess_err:
         print(f"Test failed during session setup: {sess_err}")
    except Exception as e:
        print(f"Test failed with unexpected error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 