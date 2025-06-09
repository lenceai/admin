# Cohesity Manager 2.2.1

A simplified Python interface to the Cohesity REST API. This module provides common operations for managing Cohesity clusters.

## Features

- Authentication and session management
- Cluster information retrieval using `/public/cluster` API endpoint
- CSV-based credential management for storing cluster access information
- Cluster stop operations (API and iris_cli methods)
- Direct cluster information via iris_cli utility

## Requirements

- Python 3.6+
- Required packages: requests, pandas, numpy
- Optional package: paramiko (for SSH-based cluster stop operations)
- Optional: iris_cli utility available locally or in PATH

## Installation

1. Clone or download this repository
2. Install required dependencies:

```
pip install -r requirements.txt
```

3. For SSH-based operations (fallback for cluster stop):

```
pip install paramiko
```

## Usage

### As a Command-Line Tool

```bash
# Display version information
python cohesity_manager.py --version

# Connect to a cluster and display detailed information
python cohesity_manager.py --info -s cluster.example.com -u admin

# Connect to a cluster and display brief status
python cohesity_manager.py --status -s cluster.example.com -u admin

# Save cluster credentials
python cohesity_manager.py --save-cluster cluster.example.com -u admin -d local

# List saved clusters
python cohesity_manager.py --list-clusters

# Connect using saved credentials
python cohesity_manager.py --info --saved-cluster cluster1

# Use a custom CSV file for credentials
python cohesity_manager.py --file /path/to/clusters.csv --list-clusters

# Get information for all clusters in a CSV file
python cohesity_manager.py --file /path/to/clusters.csv --info

# Get status for all clusters in a CSV file
python cohesity_manager.py --file /path/to/clusters.csv --status

# Get status with detailed debugging information
python cohesity_manager.py --file /path/to/clusters.csv --status --debug

# Force all clusters to be reported as healthy (skip health checks)
python cohesity_manager.py --file /path/to/clusters.csv --status --force-healthy

# Get quick status with minimal health checks (faster but less comprehensive)
python cohesity_manager.py --file /path/to/clusters.csv --status --quick

# Set a custom timeout for API requests (useful for slow/unresponsive clusters)
python cohesity_manager.py --file /path/to/clusters.csv --status --timeout 5

# Stop all clusters in a CSV file using API methods
python cohesity_manager.py --file /path/to/clusters.csv --cluster-stop

# Stop all clusters in a CSV file using iris_cli command directly
python cohesity_manager.py --file /path/to/clusters.csv --iris-stop

# Run iris_cli cluster info on all clusters in a CSV file
python cohesity_manager.py --file /path/to/clusters.csv --iris-info
```

### As a Python Module

```python
from cohesity_manager import cohesity_manager

# Optionally set a custom CSV file path
cohesity_manager.set_credential_file('/path/to/clusters.csv')

# Connect to a cluster
cohesity_manager.connect(
    cluster="cluster.example.com",
    username="admin",
    domain="local"
)

# Get cluster information
cluster_info = cohesity_manager.get_cluster_info()
print(f"Connected to cluster: {cluster_info['name']}")

# Save credentials for later use
cohesity_manager.save_cluster_credentials(
    cluster="cluster.example.com",
    username="admin",
    domain="local",
    password="your-password",
    description="Production Cluster"
)

# Connect using saved credentials
cohesity_manager.connect_from_csv("cluster.example.com")

# Disconnect
cohesity_manager.disconnect()
```

## CSV Credential Storage

By default, credentials are stored in `~/.cohesity/credentials.csv` with the following format:

```
hostname,username,domain,password,description
cluster1.example.com,admin,local,password1,Production
cluster2.example.com,admin,local,password2,Development
```

Alternatively, you can use this format (with cluster_ip instead of hostname):

```
cluster_ip,username,password,domain
10.220.132.11,admin,Cohe$1ty,LOCAL
10.220.132.203,admin,Cohe$1ty,LOCAL
```

You can specify a custom CSV file path using the `--file` option in the command line or the `set_credential_file()` method when using the module programmatically.

**Note:** Passwords are stored in plain text. Ensure appropriate file permissions to protect sensitive information.

## Cluster Stop Operations

The tool provides two methods to stop clusters:

### API-based Method (--cluster-stop)

The `--cluster-stop` command attempts to stop clusters using the Cohesity REST API:

1. Reads cluster credentials from a CSV file
2. Asks for confirmation once with the "STOP" keyword
3. Processes all cluster stops in parallel for efficiency
4. Tries multiple API endpoints for different Cohesity versions
5. Provides a summary of success/failure for each cluster

### iris_cli Method (--iris-stop)

The `--iris-stop` command stops clusters using the local iris_cli utility:

1. Reads cluster credentials from a CSV file
2. Asks for confirmation once with the "STOP" keyword
3. For each cluster, executes the command:
   ```
   iris_cli -server <cluster_ip> -username=<username> -password=<password> -skip_password_prompt=true cluster stop
   ```
4. Displays command output for each cluster
5. Provides a summary of success/failure for each cluster

Use `--iris-stop` when the API method fails or if you prefer to use the native Cohesity command line tool directly.

**WARNING**: Both methods are destructive operations. Stopping clusters will make them and all their services unavailable until manually restarted.

## Iris CLI Operations

The `--iris-info` command allows you to retrieve detailed cluster information directly using the iris_cli utility. This operation:

1. Reads cluster credentials from a CSV file
2. For each cluster, executes the iris_cli command:
   ```
   iris_cli -server <cluster_ip> -username=<username> -password=<password> -skip_password_prompt=true cluster info
   ```
3. Displays formatted output for each cluster
4. Provides a summary of success/failure for each cluster

**Requirements:**
- The iris_cli tool must be installed locally (in PATH or current directory)
- The tool automatically retrieves credentials from the CSV file
- Network access to each cluster is required

This command provides more detailed and native cluster information than the API-based methods, using the same information that would be available through the Cohesity command line.

## License

This module is free to use for any purpose. 