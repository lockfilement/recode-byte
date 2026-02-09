
import json
import asyncio
from utils.database.manager import DatabaseManager
from utils.schemas import user_config_schema

async def migrate_configs():
    db = DatabaseManager()
    await db.initialize()
    
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            
        for user in config['users']:
            user_id = int(user.get('user_id', 0))  # You'll need to add user_id to your config.json
            if not user_id:
                continue
                
            existing_config = await db.find_one('user_configs', {"_id": user_id})
            if not existing_config:
                # Create new user config using schema
                new_config = user_config_schema({
                    "user_id": user_id,
                    "command_prefix": config.get('command_prefix', '-'),
                    "auto_delete": config.get('auto_delete', {"enabled": True, "delay": 5}),
                    "presence": config.get('presence', {})
                })
                
                await db.ensure_write(
                    'user_configs',
                    'insert_one',
                    new_config
                )
                print(f"Migrated config for user {user_id}")
                
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(migrate_configs())