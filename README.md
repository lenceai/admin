# HPE iLO Power Management Tool

A powerful Python utility for monitoring and managing HPE servers through the iLO interface using the Redfish API.

## Version 1.0.4

## Overview

This script provides comprehensive functionality to:
- Monitor power consumption and CPU utilization of HPE servers
- Check detailed system status and health information
- Control power states (on, off, graceful shutdown)
- Manage power policies
- Record monitoring data to CSV files
- Perform operations on single servers or large groups via CSV input

## Requirements

- Python 3.6 or later
- Required Python packages:
  - `redfish` - HPE Redfish API client
  - `pandas` - For data handling and CSV operations
  - `numpy` - For numerical calculations

Install required packages:
```
pip install redfish pandas numpy
```

## Features

### Status Checking
- Basic status: `--status` - Shows basic information for each server
- Detailed status: `--status --details` - Shows comprehensive server information
- Sorted output: `--status --sort` - Sort status results by IP address and save to CSV file
- Display cluster information: Automatically displays cluster information if present in CSV input

### Power Monitoring
- Current power consumption: `--power-watts` - Get current power consumption
- Power monitoring: `--monitor` - Track power and CPU over time
- Full monitoring: `--monitor-full` - Comprehensive metrics collection
- CPU utilization: `--get-cpu` - Get current CPU usage

### Power Management
- Power on: `--power-on` - Turn on servers
- Graceful shutdown: `--power-off` - Gracefully shut down servers
- Force power off: `--force-power-off` - Force immediate shutdown
- Get power policy: `--get-power-policy` - View current power policy
- Set power policy: `--set-power-policy POLICY` - Set power management policy

### Input/Output Options
- Single server: `-i IP -u USERNAME` - Operate on a single server
- Multiple servers: `-f servers.csv` - Operate on servers listed in CSV
- Custom output: `--output-csv PATH` - Specify output location for monitoring data
- Monitoring interval: `--interval MINUTES` - Set time between monitoring checks
- Automatic CSV export: When using `--status --sort` with a CSV input file, results are automatically saved to `output/inputfilename_status.csv`

### Performance and Debugging
- Parallel operations: `--workers N` - Control number of parallel operations
- Yes to all: `--yes` - Skip confirmation prompts
- Debug mode: `--debug` - Show detailed diagnostic information

## Usage Examples

### Basic Status Check
```
python ilo_power_1.0.4.py -i 10.0.0.100 -u Administrator -s
```

### Check Status with Sorting and CSV Export
```
python ilo_power_1.0.4.py -f servers.csv --status --sort
```
This will display sorted servers in the console and also save results to `output/servers_status.csv`

### Monitor Power Over Time
```
python ilo_power_1.0.4.py -f servers.csv --monitor --interval 15
```

### Power On All Servers in CSV
```
python ilo_power_1.0.4.py -f servers.csv --power-on
```

### Get Current Power Consumption
```
python ilo_power_1.0.4.py -f servers.csv --power-watts
```

### Set Power Policy
```
python ilo_power_1.0.4.py -f servers.csv --set-power-policy "Dynamic Power Savings Mode"
```

### Monitor Full Metrics with Custom Output
```
python ilo_power_1.0.4.py -f servers.csv --monitor-full --output-csv ./output/server_metrics.csv
```

## CSV Format

The script accepts CSV files with the following format:

```
ip,username,password,cluster
10.0.0.1,Administrator,password1,Cluster1
10.0.0.2,Administrator,password2,Cluster2
```

Required columns:
- `ip` - IP address of the iLO interface
- `username` - iLO username
- `password` - iLO password

Optional columns:
- `cluster` - Cluster name/ID displayed in status output

## CSV Outputs

The script generates various CSV outputs depending on the operation:

### Status CSV (--status --sort with -f option)
When using `--status --sort` with an input CSV file, a status CSV is saved to the `output` directory with the following columns:
- IP address, cluster, hostname, identifier, model
- Power state, health status, power consumption
- CPU utilization, iLO version, BIOS version
- And other system details

### Monitoring CSVs (--monitor, --monitor-full)
When using the monitoring features, regular snapshots of server metrics are saved to CSV files.

## Notes

- The script uses SSL connections with verification disabled
- Passwords in CSV files should be properly secured
- The script is optimized for handling large numbers of servers efficiently
- Error handling is robust with retry mechanisms for common issues 