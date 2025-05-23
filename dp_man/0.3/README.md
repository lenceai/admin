# Data Protection Manager v0.3

A clean, simple tool for managing Cohesity cluster protection using the modern `cohesity_sdk` with V2 APIs.

## ✨ Features

- **Modern SDK**: Uses the latest `cohesity_sdk` with V2 APIs
- **Clean & Simple**: Streamlined code with clear structure
- **Secure**: Password prompting with no command-line exposure
- **Comprehensive Reporting**: Get protection sources and groups data
- **JSON Export**: Export reports to organized output directory

## 🚀 Quick Start

### 1. Install Prerequisites

The script requires the modern `cohesity_sdk`. Install it from source:

```bash
# Clone the modern SDK
git clone https://github.com/cohesity/cohesity_sdk.git
cd cohesity_sdk

# Install dependencies and SDK
pip install -r requirements.txt
python setup.py install
```

### 2. Run the Script

```bash
# Basic usage - will prompt for password
python dp_man_v3.py -s your-cluster.example.com -u admin

# With specific report type
python dp_man_v3.py -s 10.1.1.100 -u backup_admin -a sources

# With domain and custom output
python dp_man_v3.py -s cluster.local -u admin -d DOMAIN -o my_report.json
```

## 📋 Usage Examples

```bash
# Get all protection data (sources + groups)
python dp_man_v3.py -s cluster.cohesity.com -u admin

# Get only protection sources
python dp_man_v3.py -s cluster.cohesity.com -u admin -a sources

# Get only protection groups (jobs)
python dp_man_v3.py -s cluster.cohesity.com -u admin -a groups

# Verbose output for debugging
python dp_man_v3.py -s cluster.cohesity.com -u admin -v

# Save to specific file
python dp_man_v3.py -s cluster.cohesity.com -u admin -o backup_report.json
```

## 🛠️ Command Line Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `-s, --server` | Cohesity cluster hostname or IP | ✅ | - |
| `-u, --username` | Username for authentication | ✅ | - |
| `-p, --password` | Password (not recommended) | ❌ | Secure prompt |
| `-d, --domain` | Authentication domain | ❌ | `LOCAL` |
| `-a, --action` | Report type: `sources`, `groups`, `both` | ❌ | `both` |
| `-o, --output` | Output filename | ❌ | Auto-generated |
| `-v, --verbose` | Enable verbose logging | ❌ | `False` |

## 📊 Output Format

The script generates a JSON report with the following structure:

```json
{
  "timestamp": "2025-01-27T10:30:00",
  "cluster": {
    "name": "Cluster-01",
    "id": "123456",
    "software_version": "7.1.2_u3",
    "nodes_count": 3
  },
  "protection_sources": [...],
  "protection_groups": [...]
}
```

## 📁 Output Organization

All output files are automatically organized in the `output/` directory:

- **Reports**: JSON reports with cluster data (e.g., `output/cohesity_report_20250127_103000.json`)
- **Logs**: Application logs (`output/dp_man_v3.log`)
- **Examples**: Community example outputs

The output directory is created automatically if it doesn't exist.

## 🔧 Troubleshooting

### SDK Not Found
```
❌ cohesity_sdk not found!
```
**Solution**: Install the modern SDK from source as shown in the Quick Start section.

### Connection Issues
```
❌ Failed to connect to cluster
```
**Solutions**:
- Verify cluster hostname/IP is reachable
- Check username and password
- Ensure the cluster is running and accessible
- Try with verbose mode (`-v`) for more details

### Authentication Issues
```
❌ Authentication failed
```
**Solutions**:
- Verify credentials are correct
- Check if the domain is correct (use `-d DOMAIN`)
- Ensure the user has appropriate permissions

## 🆚 SDK Comparison

| Feature | cohesity_sdk (v0.3) | management-sdk-python (v0.2) |
|---------|---------------------|-------------------------------|
| **API Version** | V2 (Modern) | V1 (Legacy) |
| **Last Updated** | March 2025 | August 2023 |
| **Architecture** | Clean, modular | Monolithic |
| **Performance** | Optimized | Older patterns |
| **Future Support** | Active development | Limited updates |

## 📝 Development Notes

This version focuses on:
- **Simplicity**: Clean, readable code
- **Modern APIs**: Using V2 REST APIs for better performance
- **Best Practices**: Secure password handling, proper error handling
- **Extensibility**: Easy to add new features

## 🤝 Contributing

1. Follow the existing code style
2. Add logging for important operations
3. Handle errors gracefully
4. Update documentation for new features

## 📞 Support

For issues related to:
- **Script functionality**: Create an issue in this repository
- **Cohesity SDK**: Visit https://github.com/cohesity/cohesity_sdk
- **Cohesity platform**: Contact Cohesity support 