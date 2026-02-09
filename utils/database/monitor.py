"""
Utility script to monitor shared database connection status
"""
import asyncio
import json
from utils.database.shared_manager import get_shared_database_manager

async def monitor_db_status():
    """Monitor shared database connection status"""
    shared_db = get_shared_database_manager()
    
    try:
        await shared_db.initialize()
        print(f"✅ Shared Database Connection Status:")
        print(f"   - Connection ID: {shared_db.instance_id}")
        print(f"   - Is Active: {shared_db.is_active}")
        print(f"   - Reference Count: {shared_db.reference_count}")
        print(f"   - Database: {shared_db.db.name}")
        
        # Test a simple operation
        result = await shared_db.find_one('users', {}, {'_id': 1})
        print(f"   - Test Query: {'✅ Success' if result else '⚠️  No users found'}")
        
    except Exception as e:
        print(f"❌ Database Connection Error: {e}")
    finally:
        await shared_db.close()

if __name__ == "__main__":
    asyncio.run(monitor_db_status())
