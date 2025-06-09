# Cohesity Manager Module

## Overview

The Cohesity Manager module provides a simplified interface to interact with Cohesity clusters via their REST API. This module leverages the pyhesity module to handle authentication, HTTP requests, and data conversions, while providing a higher-level, object-oriented interface for common operations.

Version: 2.0.0

## Prerequisites

- Python 3.6 or later
- pyhesity module

## Installation

1. Download the pyhesity module (required dependency):

```bash
curl -O https://raw.githubusercontent.com/cohesity/community-automation-samples/main/python/pyhesity/pyhesity.py
```

2. Place the `cohesity_manager.py` file in your project directory or in your Python path.

## Basic Usage

```python
from cohesity_manager import cohesity_manager as cm

# Connect to a Cohesity cluster
cm.connect(
    cluster='cluster.example.com',
    username='admin',
    domain='local'
)

# Or connect to Helios
# cm.connect(username='helios-user@example.com')

# Get cluster information
cluster_info = cm.get_cluster_info()
print(f"Connected to cluster: {cluster_info['name']}")

# List protection jobs
jobs = cm.list_protection_jobs()
for job in jobs:
    print(f"Job: {job['name']}, Environment: {job['environment']}")

# Run a protection job
cm.run_protection_job(job_name='VM Backup')

# List views
views = cm.list_views()
for view in views:
    print(f"View: {view['name']}")

# Disconnect when done
cm.disconnect()
```

## Helios Support

Connect to Helios and manage multiple clusters:

```python
from cohesity_manager import cohesity_manager as cm

# Connect to Helios (uses 'helios.cohesity.com' by default)
cm.connect(username='user@example.com')

# List connected clusters
clusters = cm.list_helios_clusters()
for cluster in clusters:
    print(f"Cluster: {cluster['name']}")

# Connect to a specific cluster via Helios
cm.connect_to_helios_cluster('prod-cluster')

# Now perform operations on the selected cluster
jobs = cm.list_protection_jobs()
```

## Key Features

- **Authentication Management**: Handles authentication to clusters or Helios
- **Protection Job Operations**: List, run, and manage protection jobs
- **Viewing Backup Runs**: Access detailed information about protection job runs
- **Source Management**: List and search registered protection sources
- **View Operations**: Create, list, and manage views
- **Restore Operations**: Restore VMs and other objects
- **Alert Management**: Access and manage cluster alerts
- **Task Management**: Track and manage running tasks
- **Multi-tenancy Support**: Impersonate tenants for multi-tenant operations
- **Date Handling**: Simplified functions for date/time conversions

## Advanced Examples

### Create a new view with quota

```python
cm.create_view(
    view_name='test-view',
    storage_quota_gb=100,
    description='Test view with 100GB quota'
)
```

### Search for protected objects

```python
vms = cm.search_objects(
    search_term='web',
    object_type='kVMware'
)

for vm in vms:
    print(f"Found VM: {vm['name']}")
```

### Get storage statistics

```python
stats = cm.get_storage_stats()
print(f"Total capacity: {stats['totalCapacityBytes'] / (1024**3):.2f} GB")
print(f"Used capacity: {stats['usedCapacityBytes'] / (1024**3):.2f} GB")
```

### Working with protection runs

```python
# Get recent runs for a job
job = cm.get_protection_job_by_name('VM Backup')
runs = cm.get_protection_job_runs(job_id=job['id'], num_runs=5)

for run in runs:
    start_time = cm.format_timestamp(run['backupRun']['stats']['startTimeUsecs'])
    status = run['backupRun']['status']
    print(f"Run at {start_time}, Status: {status}")
```

### Multi-tenant operations

```python
# List available tenants
tenants = cm.get_tenant_info()
for tenant in tenants:
    print(f"Tenant: {tenant['name']}")

# Impersonate a tenant
cm.impersonate_tenant('tenant-1')

# Perform operations as the tenant
tenant_jobs = cm.list_protection_jobs()

# Stop impersonation
cm.stop_tenant_impersonation()
```

## Error Handling

The module provides error handling for common operations. Most methods will return `None` or `False` if an operation fails, and will print an error message if appropriate.

Always check for `None` or `False` return values when using methods that might fail:

```python
result = cm.run_protection_job(job_name='NonexistentJob')
if result is None:
    print("Failed to run job")
else:
    print("Job started successfully")
``` 