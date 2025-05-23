# Data Protection Manager v0.1

This version focuses on **Physical Linux Servers** with file-based protection sources in Cohesity clusters.

## Features

- **Physical Server Inventory**: List and monitor physical servers registered with Cohesity
- **Protection Job Management**: View and analyze protection jobs for physical file-based backups
- **Backup Status Monitoring**: Track backup success rates and job performance
- **Comprehensive Reporting**: Generate detailed JSON reports for servers and protection jobs
- **API Integration**: Full integration with Cohesity REST API v2

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Make the script executable (optional):
```bash
chmod +x dp_man.py
```

## Usage

### Basic Command Structure

```bash
python dp_man.py -s <cluster_ip> -u <user> [-p <pass>] [options]
```

**Note:** If `-p` is not provided, you will be prompted to enter the password securely.

### Command Line Arguments

| Argument | Required | Description | Default |
|----------|----------|-------------|---------|
| `-s`, `--server` | Yes | Cohesity cluster hostname or IP address | - |
| `-u`, `--username` | Yes | Username for authentication | - |
| `-p`, `--password` | No | Password for authentication (will prompt if not provided) | prompt |
| `-d`, `--domain` | No | Authentication domain | LOCAL |
| `-a`, `--action` | No | Report type: `servers`, `jobs`, or `both` | both |
| `-o`, `--output` | No | Custom output filename | timestamp-based |
| `-g`, `--gflags` | No | Get and display cluster feature flags | false |

### Examples

#### Generate Server Report Only
```bash
python dp_man.py -s 10.1.1.100 -u admin -p mypass -a servers
```

#### Generate Protection Jobs Report Only
```bash
python dp_man.py -s cohesity.company.com -u admin -p mypass -a jobs
```

#### Generate Combined Report
```bash
python dp_man.py -s 10.1.1.100 -u admin -p mypass -a both
```

#### Generate Report with Custom Filename
```bash
python dp_man.py -s 10.1.1.100 -u admin -p mypass -o monthly_backup_report.json
```

#### Using Active Directory Authentication
```bash
python dp_man.py -s 10.1.1.100 -u john.doe -p mypass -d COMPANY.COM
```

#### Get Cluster Feature Flags
```bash
python dp_man.py -s 10.1.1.100 -u admin -p mypass -g
```

#### Get Feature Flags with Custom Output
```bash
python dp_man.py -s 10.1.1.100 -u admin -p mypass -g -o cluster_flags.json
```

#### Secure Password Prompt (Recommended)
```bash
python dp_man.py -s 10.1.1.100 -u admin -a both
# You will be prompted: Enter password for admin@10.1.1.100: 
```

#### Using Password with Special Characters
```bash
# Method 1: Let the tool prompt for password (most secure)
python dp_man.py -s 10.1.1.100 -u admin -g

# Method 2: Quote the password on command line
python dp_man.py -s 10.1.1.100 -u admin -p 'R00tm8n!23' -g
```

## Output

The tool generates JSON reports containing:

### Server Report Structure
```json
{
  "timestamp": "2024-01-15 14:30:00",
  "total_servers": 25,
  "servers": [
    {
      "id": 123,
      "name": "web-server-01",
      "hostname": "web01.company.com",
      "ip_address": "10.1.2.100",
      "os_type": "Linux",
      "agent_version": "6.8.1",
      "connection_status": "kConnected",
      "last_backup": "2024-01-15 02:00:00",
      "protection_jobs": ["WebServer-Backup"],
      "backup_size": 1073741824,
      "status": "Success"
    }
  ]
}
```

### Protection Job Report Structure
```json
{
  "timestamp": "2024-01-15 14:30:00",
  "total_jobs": 10,
  "jobs": [
    {
      "id": 456,
      "name": "WebServer-Backup",
      "description": "Daily backup of web servers",
      "policy_name": "Daily-Retention-30",
      "environment": "kPhysical",
      "status": "kSuccess",
      "last_run": "2024-01-15 02:00:00",
      "next_run": "2024-01-16 02:00:00",
      "success_rate": 95.5,
      "avg_backup_size": 2147483648,
      "recent_runs": [...]
    }
  ]
}
```

### Feature Flags Report Structure
```json
{
  "timestamp": "2024-01-15 14:30:00",
  "cluster": "cohesity.company.com",
  "feature_flags": {
    "iris_ui_datadog_integration": false,
    "magneto_aws_native_protection": true,
    "bridge_enable_linux_support": true,
    "magneto_gcp_native_protection": false,
    "iris_ui_support_multiclusters": true,
    "...": "additional flags..."
  }
}
```

## Data Models

### PhysicalServer
Represents a physical server protection source:
- Server identification and networking information
- Agent status and version
- Backup history and protection job associations

### ProtectionJob
Represents a file-based protection job:
- Job configuration and policy details
- Backup paths and exclusions
- Performance metrics and run history

### CohesityCluster
Configuration for cluster connectivity:
- Connection parameters
- Authentication details
- API version settings

## Logging

The tool creates a `dp_man.log` file with detailed execution logs. Log levels include:
- **INFO**: Normal operation messages
- **ERROR**: API errors and connection issues
- **DEBUG**: Detailed API request/response information (when enabled)

## Error Handling

Common error scenarios and solutions:

| Error | Cause | Solution |
|-------|-------|----------|
| Authentication failed | Invalid credentials | Check username/password/domain |
| Connection timeout | Network/firewall issues | Verify cluster accessibility |
| SSL certificate errors | Self-signed certificates | Already handled (SSL verification disabled) |
| API version mismatch | Older Cohesity version | Update `api_version` in CohesityCluster |

## Security Considerations

- **Password Handling**: 
  - **Recommended**: Omit `-p` option to use secure password prompt
  - **If using command line**: Quote passwords with special characters: `-p 'R00tm8n!23'`
  - **Never**: Store passwords in scripts or configuration files
- **SSL**: Tool disables SSL verification for self-signed certificates
- **Permissions**: Requires Cohesity user with backup administrator privileges
- **Network**: Ensure secure network connection to Cohesity cluster
- **Process Visibility**: Command line passwords may be visible in process lists - use prompt method for better security

## Extending the Tool

The tool is designed for extensibility:

1. **Add new protection source types**: Create new manager classes following the `PhysicalServerManager` pattern
2. **Enhanced reporting**: Extend the report generation methods
3. **Additional API endpoints**: Add methods to `CohesityAPIClient`
4. **Custom filters**: Implement filtering options for specific server groups or job types

## Troubleshooting

### Common Issues

1. **"Failed to connect to Cohesity cluster"**
   - Verify server hostname/IP is correct with `-s` option
   - Check network connectivity: `ping <hostname>`
   - Ensure port 443 is accessible

2. **"Authentication failed"**
   - Verify username and password with `-u` and `-p` options
   - Check if account is locked or expired
   - Confirm domain name for AD authentication with `-d` option

3. **"No protection sources found"**
   - Verify physical servers are registered with cluster
   - Check user permissions for viewing protection sources
   - Ensure Cohesity agents are installed on target servers

### Debug Mode

To enable verbose logging, modify the logging level in the script:
```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Version History

- **v0.1**: Initial release with Physical Linux Server support
  - Basic server inventory and protection job reporting
  - JSON export functionality
  - Cohesity REST API v2 integration 