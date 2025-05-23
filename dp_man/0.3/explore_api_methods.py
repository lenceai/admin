#!/usr/bin/env python3
"""
Explore specific API methods in cohesity_sdk
"""

def explore_api_methods():
    try:
        from cohesity_sdk.cluster.cluster_client import ClusterClient
        
        print("🔍 Exploring API Methods...")
        print("=" * 60)
        
        # Create a client instance (without connecting)
        client = ClusterClient(
            cluster_vip="test",
            username="test", 
            password="test"
        )
        
        # Explore source_api methods
        if hasattr(client, 'source_api'):
            source_api = client.source_api
            print(f"\n📋 SOURCE_API methods:")
            print("-" * 30)
            methods = [m for m in dir(source_api) if not m.startswith('_') and callable(getattr(source_api, m))]
            for method in sorted(methods):
                if 'source' in method.lower() or 'get' in method.lower():
                    print(f"  ✅ {method}")
        
        # Explore protection_group_api methods
        if hasattr(client, 'protection_group_api'):
            protection_group_api = client.protection_group_api
            print(f"\n🛡️  PROTECTION_GROUP_API methods:")
            print("-" * 30)
            methods = [m for m in dir(protection_group_api) if not m.startswith('_') and callable(getattr(protection_group_api, m))]
            for method in sorted(methods):
                if 'protection' in method.lower() or 'get' in method.lower():
                    print(f"  ✅ {method}")
        
        # Explore platform_api methods for reference
        if hasattr(client, 'platform_api'):
            platform_api = client.platform_api
            print(f"\n🏗️  PLATFORM_API methods:")
            print("-" * 30)
            methods = [m for m in dir(platform_api) if not m.startswith('_') and callable(getattr(platform_api, m))]
            for method in sorted(methods):
                if 'cluster' in method.lower() or 'get' in method.lower():
                    print(f"  ✅ {method}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Exploring cohesity_sdk API methods...")
    explore_api_methods() 