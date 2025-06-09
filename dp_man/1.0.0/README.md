# Cohesity Cluster Management Tool

A Python-based management tool for Cohesity clusters that enables:
- Upgrading clusters to the latest Cohesity software version
- Creating protection groups for Linux physical servers across multiple clusters
- Checking status and health of Cohesity clusters
- Reading configurations from CSV files

## Prerequisites

- Python 3.6 or newer
- Required packages: `cohesity_sdk`, `pandas`, `numpy`

## Installation

1. Clone this repository or download the script files

2. Install the required packages:
   ```
   pip install cohesity_sdk pandas numpy
   ```

3. Create the necessary directory structure:
   ```
   mkdir -p data output
   ```

4. Prepare your configuration CSV files in the `data` directory:
   - `data/clusters.csv`: Contains cluster information
   - `data/filesets.csv`: Contains fileset information for Linux physical servers

## Directory Structure

```
cohesity-manager/
├── data/               # Input data files (CSV)
│   ├── clusters.csv    # Cluster connection information
│   └── filesets.csv    # File paths and patterns to protect
├── output/             # Output files and logs
├── cohesity_manager.py # Main script 
├── example_usage.py    # Example usage script
└── README.md           # This file
```

## Configuration Files

### Clusters CSV Format

The `data/clusters.csv` file should contain the following columns:
- `cluster_ip`: IP address of the Cohesity cluster
- `username`: Admin username for accessing the cluster
- `password`: Password for the admin account
- `domain` (optional): Authentication domain, defaults to "LOCAL"

Example:
```
cluster_ip,username,password,domain
10.0.1.100,admin,Password123,LOCAL
10.0.1.101,admin,Password456,LOCAL
10.0.2.200,administrator,Password789,AD
```

### Filesets CSV Format

The `data/filesets.csv` file should contain the following columns:
- `server_name`: Name of the physical Linux server (must match the registered name in Cohesity)
- `path`: Directory path to protect
- `include_patterns` (optional): File patterns to include, separated by semicolons
- `exclude_patterns` (optional): File patterns to exclude, separated by semicolons

Example:
```
server_name,path,include_patterns,exclude_patterns
linux-server-01,/opt/data,*.log;*.xml,*.tmp;*.bak
linux-server-01,/var/log,*.log,*.gz;*.old
linux-server-02,/opt/application,*.dat;*.xml,*.tmp
```

## Usage

### Show Script Version

```
python cohesity_manager.py -v
```

### Check Cluster Status

To check the status of all clusters using the default clusters file:

```
python cohesity_manager.py -s
```

To specify a different clusters file:

```
python cohesity_manager.py -s --clusters path/to/clusters.csv
```

For detailed status information:

```
python cohesity_manager.py -s --details
```

The status command uses NumPy and pandas for data analysis, providing:
- Storage usage analysis (total, percentage, variance between clusters)
- Health status distribution
- Node count statistics
- Protection group statistics

The results are saved in both JSON and CSV formats in the output directory for further analysis.

### Upgrade Clusters

To upgrade all clusters to the latest available version:

```
python cohesity_manager.py -u
```

To upgrade to a specific version:

```
python cohesity_manager.py -u --target-version 7.1.2_u3
```

### Create Protection Groups

To create protection groups for Linux physical servers:

```
python cohesity_manager.py -c data/filesets.csv
```

With a custom policy and group name:

```
python cohesity_manager.py -c data/filesets.csv --policy "Gold" --group-name "Linux_Daily_Backup"
```

### Additional Options

- `--clusters`: Path to CSV file with cluster information (default: data/clusters.csv)
- `--target-version`: Specific version to upgrade to (if not provided, latest will be used)
- `--policy`: Specify the protection policy name (default: "Bronze")
- `--group-name`: Template for protection group names (default: "Linux_Filesystems")
- `--output-dir`: Directory for output files (default: "output")
- `--details`: Show detailed status information (when used with `-s`)
- `--debug`: Enable detailed debug logging

## Logging

The script creates detailed log files in the `output` directory with the format:
```
cohesity_manager_YYYYMMDD_HHMMSS.log
```

## Output Files

The following output files are generated in the `output` directory:

- Upgrade results: `upgrade_results_YYYYMMDD_HHMMSS.json`
- Protection group results: `protection_group_results_YYYYMMDD_HHMMSS.json`
- Cluster status results: `cluster_status_YYYYMMDD_HHMMSS.json` and `.csv`
- Example execution logs: `example_usage_YYYYMMDD_HHMMSS.log`

## Notes

1. The script requires the Cohesity Agent to be installed on all Linux servers before they can be protected
2. Physical servers must be registered in Cohesity before creating protection groups
3. Protection policies must already exist on the clusters with the specified name

## Troubleshooting

- **Connection Issues**: Ensure the cluster IP, username, and password are correct
- **Missing Servers**: Verify that the server names in the filesets CSV match exactly with the registered server names in Cohesity
- **Policy Not Found**: Confirm that the protection policy exists on all clusters 