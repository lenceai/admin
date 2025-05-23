#!/usr/bin/env python3
"""
Data Protection Manager CLI (dp_man_cli) v0.1
A comprehensive management tool for Cohesity clusters using iris_cli interface.

This tool provides administrative capabilities for managing:
- Cluster configuration and settings
- Protection policies and jobs
- Storage domains and views
- Alerts and monitoring
- User and role management
- Backup and restore operations
"""

import os
import sys
import json
import subprocess
import argparse
import logging
import getpass
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import tempfile
import shlex
import shutil

try:
    import pexpect
    PEXPECT_AVAILABLE = True
except ImportError:
    PEXPECT_AVAILABLE = False

# Ensure output directory exists
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'dp_man_cli.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ClusterConfig:
    """Cohesity cluster configuration."""
    cluster_ip: str
    username: str
    password: str
    domain: str = "LOCAL"
    iris_cli_path: str = "iris_cli"


class IrisCLIWrapper:
    """Wrapper for iris_cli commands."""
    
    def __init__(self, config: ClusterConfig):
        self.config = config
        self.connected = False
        self._resolve_iris_cli_path()
        self._validate_iris_cli()
        
    def _resolve_iris_cli_path(self):
        """Resolve iris_cli path, checking local directory first."""
        # If it's just 'iris_cli', check local directory first
        if self.config.iris_cli_path == "iris_cli":
            script_dir = os.path.dirname(os.path.abspath(__file__))
            local_iris_cli = os.path.join(script_dir, "iris_cli")
            
            # Check if iris_cli exists in the local directory
            if os.path.isfile(local_iris_cli):
                self.config.iris_cli_path = local_iris_cli
                logger.info(f"Using local iris_cli: {local_iris_cli}")
            # If not found locally, try to find in PATH
            elif shutil.which("iris_cli"):
                self.config.iris_cli_path = shutil.which("iris_cli")
                logger.info(f"Using system iris_cli: {self.config.iris_cli_path}")
        
    def _validate_iris_cli(self):
        """Validate that iris_cli is accessible."""
        # Check if the resolved path exists
        if not os.path.isfile(self.config.iris_cli_path) and not shutil.which(self.config.iris_cli_path):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            error_msg = f"""
ERROR: iris_cli not found at '{self.config.iris_cli_path}'

To resolve this issue:
1. Place iris_cli in the local project directory: {script_dir}/iris_cli
2. Download iris_cli from your Cohesity cluster's web interface (Help > Downloads)
3. Make sure iris_cli is executable: chmod +x iris_cli
4. OR specify a different path using --iris-cli-path parameter

Example:
  {sys.argv[0]} --iris-cli-path /path/to/iris_cli [other options]

For more information, consult your Cohesity documentation.
"""
            logger.error(error_msg)
            print(error_msg)
            sys.exit(1)
        
        # Ensure iris_cli is executable
        if os.path.isfile(self.config.iris_cli_path) and not os.access(self.config.iris_cli_path, os.X_OK):
            logger.warning(f"iris_cli found but not executable. Making it executable...")
            os.chmod(self.config.iris_cli_path, 0o755)
        
    def _build_base_cmd(self) -> List[str]:
        """Build base iris_cli command with authentication."""
        return [
            self.config.iris_cli_path,
            "-server", self.config.cluster_ip,
            "-username", self.config.username
        ]
    
    def _execute_command(self, cmd_args: List[str], stdin_input: Optional[str] = None) -> Tuple[bool, str, str]:
        """Execute iris_cli command and return success, stdout, stderr."""
        try:
            # Build full command
            full_cmd = self._build_base_cmd() + cmd_args
            cmd_str = ' '.join(full_cmd)
            
            # Log command without password
            logger.debug(f"Executing: {cmd_str}")
            print(f"DEBUG: Executing command: {cmd_str}")
            
            if PEXPECT_AVAILABLE:
                # Use pexpect for interactive terminal handling
                print(f"DEBUG: Using pexpect for terminal interaction")
                child = pexpect.spawn(cmd_str, timeout=30)
                
                # Wait for password prompt and send password
                try:
                    child.expect("Password:", timeout=10)
                    child.sendline(self.config.password)
                    child.expect(pexpect.EOF, timeout=20)
                    
                    output = child.before.decode('utf-8') if child.before else ""
                    success = child.exitstatus == 0
                    
                    print(f"DEBUG: pexpect exit status: {child.exitstatus}")
                    print(f"DEBUG: pexpect output length: {len(output)} chars")
                    
                    if not success:
                        print(f"DEBUG: Command failed with output: {output[:500]}...")
                    
                    return success, output, ""
                    
                except pexpect.TIMEOUT:
                    print("DEBUG: pexpect timeout waiting for password prompt")
                    child.close(force=True)
                    return False, "", "Timeout waiting for password prompt"
                except Exception as e:
                    print(f"DEBUG: pexpect error: {e}")
                    child.close(force=True)
                    return False, "", str(e)
            else:
                # Fallback to subprocess method
                print(f"DEBUG: pexpect not available, using subprocess")
                print(f"DEBUG: Providing password via stdin")
                process = subprocess.Popen(
                    full_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Send password when prompted
                stdout, stderr = process.communicate(input=f"{self.config.password}\n")
                
                success = process.returncode == 0
                print(f"DEBUG: Return code: {process.returncode}")
                print(f"DEBUG: stdout length: {len(stdout)} chars")
                print(f"DEBUG: stderr length: {len(stderr)} chars")
                
                if not success:
                    print(f"DEBUG: Command failed with stderr: {stderr[:500]}...")
                    print(f"DEBUG: Command failed with stdout: {stdout[:500]}...")
                
                return success, stdout, stderr
            
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            return False, "", str(e)
    
    def test_connection(self) -> bool:
        """Test connection to cluster."""
        success, stdout, stderr = self._execute_command(["cluster"])
        
        if success:
            logger.info(f"Successfully connected to cluster: {self.config.cluster_ip}")
            self.connected = True
            return True
        else:
            # Check if we got cluster operations help (which means auth worked)
            if "Available operations for the entity" in stdout:
                logger.info(f"Successfully connected to cluster: {self.config.cluster_ip}")
                self.connected = True
                return True
            logger.error(f"Failed to connect: {stderr}")
            return False
    
    def execute_iris_command(self, command: str) -> Tuple[bool, str]:
        """Execute an iris_cli command string."""
        cmd_parts = shlex.split(command)
        success, stdout, stderr = self._execute_command(cmd_parts)
        
        if not success:
            logger.error(f"Command failed: {stderr}")
            return False, stderr
            
        return True, stdout


class ClusterManager:
    """Manage cluster-level operations."""
    
    def __init__(self, iris_cli: IrisCLIWrapper):
        self.iris = iris_cli
        
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information."""
        success, output = self.iris.execute_iris_command("cluster ls")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get cluster statistics."""
        success, output = self.iris.execute_iris_command("cluster get-stats")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def get_nodes(self) -> Dict[str, Any]:
        """Get cluster nodes information."""
        success, output = self.iris.execute_iris_command("node ls")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}


class ProtectionManager:
    """Manage protection policies and jobs."""
    
    def __init__(self, iris_cli: IrisCLIWrapper):
        self.iris = iris_cli
        
    def list_protection_jobs(self) -> Dict[str, Any]:
        """List all protection jobs."""
        success, output = self.iris.execute_iris_command("job ls")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def get_job_details(self, job_name: str) -> Dict[str, Any]:
        """Get details of a specific job."""
        success, output = self.iris.execute_iris_command(f"job ls --name={job_name}")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def list_policies(self) -> Dict[str, Any]:
        """List all protection policies."""
        success, output = self.iris.execute_iris_command("policy ls")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def run_job(self, job_name: str, run_type: str = "regular") -> Dict[str, Any]:
        """Run a protection job."""
        cmd = f"job run --name={job_name}"
        if run_type == "full":
            cmd += " --full"
        
        success, output = self.iris.execute_iris_command(cmd)
        if success:
            return {"raw_output": output, "status": "success", "message": f"Job {job_name} started"}
        return {"error": output, "status": "failed"}
    
    def pause_job(self, job_name: str) -> Dict[str, Any]:
        """Pause a protection job."""
        success, output = self.iris.execute_iris_command(f"job pause --name={job_name}")
        if success:
            return {"raw_output": output, "status": "success", "message": f"Job {job_name} paused"}
        return {"error": output, "status": "failed"}
    
    def resume_job(self, job_name: str) -> Dict[str, Any]:
        """Resume a protection job."""
        success, output = self.iris.execute_iris_command(f"job resume --name={job_name}")
        if success:
            return {"raw_output": output, "status": "success", "message": f"Job {job_name} resumed"}
        return {"error": output, "status": "failed"}


class StorageManager:
    """Manage storage domains and views."""
    
    def __init__(self, iris_cli: IrisCLIWrapper):
        self.iris = iris_cli
        
    def list_storage_domains(self) -> Dict[str, Any]:
        """List all storage domains."""
        success, output = self.iris.execute_iris_command("sd ls")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def list_views(self) -> Dict[str, Any]:
        """List all views."""
        success, output = self.iris.execute_iris_command("view ls")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def create_view(self, view_name: str, protocol: str = "nfs") -> Dict[str, Any]:
        """Create a new view."""
        cmd = f"view create --name={view_name} --protocol={protocol}"
        success, output = self.iris.execute_iris_command(cmd)
        if success:
            return {"raw_output": output, "status": "success", "message": f"View {view_name} created"}
        return {"error": output, "status": "failed"}


class AlertManager:
    """Manage alerts and monitoring."""
    
    def __init__(self, iris_cli: IrisCLIWrapper):
        self.iris = iris_cli
        
    def list_alerts(self, severity: Optional[str] = None) -> Dict[str, Any]:
        """List alerts, optionally filtered by severity."""
        cmd = "alert ls"
        if severity:
            cmd += f" --severity={severity}"
            
        success, output = self.iris.execute_iris_command(cmd)
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def acknowledge_alert(self, alert_id: str) -> Dict[str, Any]:
        """Acknowledge an alert."""
        success, output = self.iris.execute_iris_command(f"alert ack --id={alert_id}")
        if success:
            return {"raw_output": output, "status": "success", "message": f"Alert {alert_id} acknowledged"}
        return {"error": output, "status": "failed"}


class UserManager:
    """Manage users and roles."""
    
    def __init__(self, iris_cli: IrisCLIWrapper):
        self.iris = iris_cli
        
    def list_users(self) -> Dict[str, Any]:
        """List all users."""
        success, output = self.iris.execute_iris_command("user ls")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def list_roles(self) -> Dict[str, Any]:
        """List all roles."""
        success, output = self.iris.execute_iris_command("role ls")
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def create_user(self, username: str, password: str, roles: List[str]) -> Dict[str, Any]:
        """Create a new user."""
        roles_str = ",".join(roles)
        cmd = f"user create --name={username} --password={password} --roles={roles_str}"
        success, output = self.iris.execute_iris_command(cmd)
        if success:
            return {"raw_output": output, "status": "success", "message": f"User {username} created"}
        return {"error": output, "status": "failed"}


class DataProtectionManagerCLI:
    """Main CLI manager class."""
    
    def __init__(self, config: ClusterConfig):
        self.config = config
        self.iris = IrisCLIWrapper(config)
        
        # Initialize managers
        self.cluster = ClusterManager(self.iris)
        self.protection = ProtectionManager(self.iris)
        self.storage = StorageManager(self.iris)
        self.alerts = AlertManager(self.iris)
        self.users = UserManager(self.iris)
        
    def connect(self) -> bool:
        """Connect to the cluster."""
        return self.iris.test_connection()
    
    def execute_custom_command(self, command: str) -> Dict[str, Any]:
        """Execute a custom iris_cli command."""
        success, output = self.iris.execute_iris_command(command)
        if success:
            return {"raw_output": output, "status": "success"}
        return {"error": output, "status": "failed"}
    
    def generate_cluster_report(self) -> Dict[str, Any]:
        """Generate comprehensive cluster report."""
        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cluster": self.config.cluster_ip,
            "sections": {}
        }
        
        # Cluster info
        logger.info("Gathering cluster information...")
        report["sections"]["cluster_info"] = self.cluster.get_cluster_info()
        report["sections"]["cluster_stats"] = self.cluster.get_cluster_stats()
        report["sections"]["nodes"] = self.cluster.get_nodes()
        
        # Protection info
        logger.info("Gathering protection information...")
        report["sections"]["protection_jobs"] = self.protection.list_protection_jobs()
        report["sections"]["policies"] = self.protection.list_policies()
        
        # Storage info
        logger.info("Gathering storage information...")
        report["sections"]["storage_domains"] = self.storage.list_storage_domains()
        report["sections"]["views"] = self.storage.list_views()
        
        # Alerts
        logger.info("Gathering alerts...")
        report["sections"]["alerts"] = self.alerts.list_alerts()
        
        # Users
        logger.info("Gathering user information...")
        report["sections"]["users"] = self.users.list_users()
        report["sections"]["roles"] = self.users.list_roles()
        
        return report
    
    def export_report(self, report: Dict, filename: str):
        """Export report to file."""
        try:
            # Ensure output directory for report
            if not os.path.isabs(filename):
                filename = os.path.join(OUTPUT_DIR, filename)
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Report exported to {filename}")
        except Exception as e:
            logger.error(f"Failed to export report: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Cohesity Data Protection Manager CLI v0.1",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate full cluster report
  dp_man_cli.py -s cluster.domain.com -u admin report

  # List protection jobs
  dp_man_cli.py -s cluster.domain.com -u admin list-jobs

  # Run a specific job
  dp_man_cli.py -s cluster.domain.com -u admin run-job --name MyBackupJob

  # Execute custom iris_cli command
  dp_man_cli.py -s cluster.domain.com -u admin custom "job ls --active"

  # List alerts with severity filter
  dp_man_cli.py -s cluster.domain.com -u admin list-alerts --severity critical
        """
    )
    
    # Connection arguments
    parser.add_argument("-s", "--server", required=True, help="Cohesity cluster hostname/IP")
    parser.add_argument("-u", "--username", required=True, help="Username for authentication")
    parser.add_argument("-p", "--password", help="Password (omit for secure prompt)")
    parser.add_argument("-d", "--domain", default="LOCAL", help="Authentication domain")
    parser.add_argument("--iris-cli-path", default="iris_cli", help="Path to iris_cli executable")
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Report command
    report_parser = subparsers.add_parser("report", help="Generate comprehensive cluster report")
    report_parser.add_argument("-o", "--output", help="Output filename")
    
    # Cluster commands
    cluster_parser = subparsers.add_parser("cluster-info", help="Get cluster information")
    stats_parser = subparsers.add_parser("cluster-stats", help="Get cluster statistics")
    nodes_parser = subparsers.add_parser("list-nodes", help="List cluster nodes")
    
    # Protection commands
    list_jobs_parser = subparsers.add_parser("list-jobs", help="List protection jobs")
    
    job_details_parser = subparsers.add_parser("job-details", help="Get job details")
    job_details_parser.add_argument("--name", required=True, help="Job name")
    
    run_job_parser = subparsers.add_parser("run-job", help="Run a protection job")
    run_job_parser.add_argument("--name", required=True, help="Job name")
    run_job_parser.add_argument("--full", action="store_true", help="Run full backup")
    
    pause_job_parser = subparsers.add_parser("pause-job", help="Pause a protection job")
    pause_job_parser.add_argument("--name", required=True, help="Job name")
    
    resume_job_parser = subparsers.add_parser("resume-job", help="Resume a protection job")
    resume_job_parser.add_argument("--name", required=True, help="Job name")
    
    list_policies_parser = subparsers.add_parser("list-policies", help="List protection policies")
    
    # Storage commands
    list_domains_parser = subparsers.add_parser("list-domains", help="List storage domains")
    list_views_parser = subparsers.add_parser("list-views", help="List views")
    
    create_view_parser = subparsers.add_parser("create-view", help="Create a new view")
    create_view_parser.add_argument("--name", required=True, help="View name")
    create_view_parser.add_argument("--protocol", default="nfs", choices=["nfs", "smb"], help="Protocol")
    
    # Alert commands
    list_alerts_parser = subparsers.add_parser("list-alerts", help="List alerts")
    list_alerts_parser.add_argument("--severity", choices=["critical", "warning", "info"], help="Filter by severity")
    
    ack_alert_parser = subparsers.add_parser("ack-alert", help="Acknowledge an alert")
    ack_alert_parser.add_argument("--id", required=True, help="Alert ID")
    
    # User commands
    list_users_parser = subparsers.add_parser("list-users", help="List users")
    list_roles_parser = subparsers.add_parser("list-roles", help="List roles")
    
    create_user_parser = subparsers.add_parser("create-user", help="Create a new user")
    create_user_parser.add_argument("--name", required=True, help="Username")
    create_user_parser.add_argument("--password", required=True, help="Password")
    create_user_parser.add_argument("--roles", required=True, help="Comma-separated list of roles")
    
    # Custom command
    custom_parser = subparsers.add_parser("custom", help="Execute custom iris_cli command")
    custom_parser.add_argument("command", help="iris_cli command to execute")
    
    args = parser.parse_args()
    
    # Validate command
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Handle password
    password = args.password
    if not password:
        try:
            password = getpass.getpass(f"Enter password for {args.username}@{args.server}: ")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            sys.exit(1)
    
    # Create configuration
    config = ClusterConfig(
        cluster_ip=args.server,
        username=args.username,
        password=password,
        domain=args.domain,
        iris_cli_path=args.iris_cli_path
    )
    
    # Initialize CLI manager
    cli = DataProtectionManagerCLI(config)
    
    # Test connection
    if not cli.connect():
        logger.error("Failed to connect to cluster")
        sys.exit(1)
    
    # Execute command
    result = None
    
    if args.command == "report":
        logger.info("Generating comprehensive cluster report...")
        result = cli.generate_cluster_report()
        
        # Export report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = args.output or f"cluster_report_{timestamp}.json"
        cli.export_report(result, output_file)
        
        # Print summary
        print(f"\n📊 Cluster Report Summary")
        print(f"{'=' * 50}")
        print(f"Cluster: {config.cluster_ip}")
        print(f"Generated: {result['timestamp']}")
        print(f"Report saved to: {output_file}")
        
        for section, data in result["sections"].items():
            status = "✅" if data.get("status") == "success" else "❌"
            print(f"{status} {section.replace('_', ' ').title()}")
    
    elif args.command == "cluster-info":
        result = cli.cluster.get_cluster_info()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "cluster-stats":
        result = cli.cluster.get_cluster_stats()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "list-nodes":
        result = cli.cluster.get_nodes()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "list-jobs":
        result = cli.protection.list_protection_jobs()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "job-details":
        result = cli.protection.get_job_details(args.name)
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "run-job":
        run_type = "full" if args.full else "regular"
        result = cli.protection.run_job(args.name, run_type)
        print(result.get("message", result.get("error")))
    
    elif args.command == "pause-job":
        result = cli.protection.pause_job(args.name)
        print(result.get("message", result.get("error")))
    
    elif args.command == "resume-job":
        result = cli.protection.resume_job(args.name)
        print(result.get("message", result.get("error")))
    
    elif args.command == "list-policies":
        result = cli.protection.list_policies()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "list-domains":
        result = cli.storage.list_storage_domains()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "list-views":
        result = cli.storage.list_views()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "create-view":
        result = cli.storage.create_view(args.name, args.protocol)
        print(result.get("message", result.get("error")))
    
    elif args.command == "list-alerts":
        result = cli.alerts.list_alerts(args.severity)
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "ack-alert":
        result = cli.alerts.acknowledge_alert(args.id)
        print(result.get("message", result.get("error")))
    
    elif args.command == "list-users":
        result = cli.users.list_users()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "list-roles":
        result = cli.users.list_roles()
        print(result.get("raw_output", result.get("error")))
    
    elif args.command == "create-user":
        roles = args.roles.split(",")
        result = cli.users.create_user(args.name, args.password, roles)
        print(result.get("message", result.get("error")))
    
    elif args.command == "custom":
        result = cli.execute_custom_command(args.command)
        print(result.get("raw_output", result.get("error")))
    
    # Exit with appropriate code
    if result and result.get("status") == "failed":
        sys.exit(1)
    
    logger.info("Operation completed successfully")


if __name__ == "__main__":
    main() 