#!/usr/bin/env python3
"""
Debug script to explore protection sources API endpoints
"""

import getpass
import urllib3
from cohesity_sdk.cluster.cluster_client import ClusterClient

# Disable SSL warnings for self-signed certificates (common in Cohesity clusters)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def debug_sources_api():
    """Test different source API endpoints to find the correct one."""
    
    print("🔍 Debugging Protection Sources API...")
    print("=" * 50)
    
    # Connection details
    cluster_vip = "192.168.77.14"
    username = "admin"
    password = getpass.getpass(f"🔐 Enter password for {username}@{cluster_vip}: ")
    
    try:
        client = ClusterClient(
            cluster_vip=cluster_vip,
            username=username,
            password=password,
            domain="LOCAL"
        )
        
        print("📡 Testing various source API methods...")
        
        # Test different source API methods
        source_methods = [
            ('get_protection_sources', 'Get protection sources'),
            ('get_source_registrations', 'Get source registrations'),
            ('protection_source_by_id', 'Get source by ID (needs ID)'),
        ]
        
        for method_name, description in source_methods:
            if hasattr(client.source_api, method_name):
                print(f"\n🧪 Testing: {method_name} - {description}")
                try:
                    method = getattr(client.source_api, method_name)
                    if method_name == 'protection_source_by_id':
                        print(f"  ⚠️  Skipped: {method_name} (requires ID parameter)")
                        continue
                    
                    result = method()
                    print(f"  ✅ Success: {type(result)}")
                    
                    # Try to get some info about the result
                    if hasattr(result, '__dict__'):
                        attrs = [attr for attr in dir(result) if not attr.startswith('_')]
                        print(f"  📋 Attributes: {attrs[:5]}{'...' if len(attrs) > 5 else ''}")
                    
                    # Check for common list attributes
                    for attr in ['protection_sources', 'sources', 'registrations']:
                        if hasattr(result, attr):
                            attr_value = getattr(result, attr)
                            if attr_value:
                                print(f"  📊 {attr}: {len(attr_value)} items")
                            else:
                                print(f"  📊 {attr}: empty")
                    
                except Exception as e:
                    print(f"  ❌ Error: {e}")
            else:
                print(f"❌ Method not found: {method_name}")
        
        # Also test registration API which might be more relevant
        print(f"\n🧪 Testing registration methods...")
        registration_methods = [
            'get_source_registrations',
            'get_protection_source_registration'
        ]
        
        for method_name in registration_methods:
            if hasattr(client.source_api, method_name):
                print(f"\n🔧 Testing: {method_name}")
                try:
                    method = getattr(client.source_api, method_name)
                    result = method()
                    print(f"  ✅ Success: {type(result)}")
                    
                    # Check for data
                    if hasattr(result, 'registrations') and result.registrations:
                        print(f"  📊 Found {len(result.registrations)} registrations")
                    elif hasattr(result, '__dict__'):
                        print(f"  📋 Result attributes: {[attr for attr in dir(result) if not attr.startswith('_')][:5]}")
                        
                except Exception as e:
                    print(f"  ❌ Error: {e}")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    debug_sources_api() 