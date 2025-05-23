# Data Protection Manager (dp_man)

A tool for managing Cohesity cluster protection jobs and backup resources.

## Overview

This tool is designed to pull and manage protection jobs from Cohesity clusters. Since each protection group source can have different concepts and requirements, we implement separate models per protection source type to handle their unique characteristics.

## Protection Source Types

Different backup sources require different handling:
- **VMware**: Virtual machine backups
- **SQL Server**: Database backups  
- **Physical Servers**: File-based backups from physical Linux/Windows servers
- **NAS**: Network-attached storage backups
- And more...

## Version Structure

Each version is organized in numbered folders (e.g., `0.1`, `0.2`, etc.) to track evolution and improvements of the tool.

### Version 0.1

The initial version focuses on **Physical Linux Servers** with file-based protection sources. This provides a foundation for managing:
- File and folder protection policies
- Linux agent-based backups
- Physical server inventory and status
- Protection job monitoring and reporting

## Usage

Each version folder contains the specific implementation for that iteration of the tool. See the individual version README files for detailed usage instructions.

## Future Versions

Planned expansions include:
- Additional protection source types (VMware, SQL Server, etc.)
- Enhanced reporting and analytics
- Automated policy management
- Integration with other admin tools 