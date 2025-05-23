#!/usr/bin/env python3
"""
Test script to discover the actual API structure of cohesity_sdk
"""

import sys

def test_sdk_structure():
    try:
        from cohesity_sdk.cluster.cluster_client import ClusterClient
        
        print("🔍 Exploring cohesity_sdk ClusterClient structure...")
        print("=" * 60)
        
        # Create a client instance (without connecting)
        client = ClusterClient(
            cluster_vip="test",
            username="test", 
            password="test"
        )
        
        print(f"📦 ClusterClient type: {type(client)}")
        print(f"📦 ClusterClient dir: {dir(client)}")
        
        print("\n🔍 Available attributes and methods:")
        print("-" * 40)
        for attr in sorted(dir(client)):
            if not attr.startswith('_'):
                try:
                    attr_obj = getattr(client, attr)
                    print(f"  {attr}: {type(attr_obj)}")
                    
                    # If it's an object, explore its attributes too
                    if hasattr(attr_obj, '__dict__') and not callable(attr_obj):
                        print(f"    └─ {attr} attributes: {dir(attr_obj)}")
                        
                except Exception as e:
                    print(f"  {attr}: Error accessing - {e}")
        
        print("\n🔍 Searching for API controllers...")
        print("-" * 40)
        
        # Look for common controller patterns
        possible_controllers = [
            'platform', 'platform_controller', 'cluster',
            'data_protect', 'protection_sources', 'protection_groups',
            'protection_sources_controller', 'protection_groups_controller'
        ]
        
        for controller in possible_controllers:
            if hasattr(client, controller):
                controller_obj = getattr(client, controller)
                print(f"  ✅ Found: {controller} -> {type(controller_obj)}")
                
                # Explore methods of the controller
                if hasattr(controller_obj, '__dict__'):
                    methods = [m for m in dir(controller_obj) if not m.startswith('_')]
                    print(f"    └─ Methods: {methods[:5]}{'...' if len(methods) > 5 else ''}")
            else:
                print(f"  ❌ Not found: {controller}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing cohesity_sdk structure...")
    test_sdk_structure() 