from datetime import datetime
from typing import Optional, List, Dict, Any

def user_schema(user_data: dict) -> dict:
    """Schema for user documents"""
    current_time = datetime.utcnow().replace(microsecond=0)
    
    return {
        "_id": user_data["_id"],  # Discord user ID
        "name": user_data.get("name"),
        "current_username": user_data.get("current_username"),
        "current_displayname": user_data.get("current_displayname"),
        "current_avatar_url": user_data.get("current_avatar_url"),
        "current_banner_url": user_data.get("current_banner_url"),
        "first_seen": user_data.get("first_seen", current_time),
        "last_seen": user_data.get("last_seen", current_time),
        "username_history": user_data.get("username_history", []),
        "displayname_history": user_data.get("displayname_history", []),
        "avatar_history": user_data.get("avatar_history", []),
        "banner_history": user_data.get("banner_history", [])
    }

def deleted_message_schema(message_data: dict) -> dict:
    """Schema for deleted messages"""
    current_time = datetime.utcnow().replace(microsecond=0)
    
    return {
        "_id": message_data["message_id"],  # Message ID
        "user_id": message_data["user_id"],  # Author's user ID
        "channel_id": message_data["channel_id"],
        "guild_id": message_data.get("guild_id"),  # Optional for DMs
        "content": message_data.get("content", ""),
        "attachments": message_data.get("attachments", []),
        "deleted_at": message_data.get("deleted_at", current_time),
        "channel_name": message_data.get("channel_name", "Unknown"),
        "guild_name": message_data.get("guild_name"),
        "author_name": message_data.get("author_name", "Unknown")
    }

def history_entry_schema(entry_data: dict) -> dict:
    """Schema for history entries"""
    return {
        "value": entry_data["value"],
        "changed_at": entry_data.get("changed_at", datetime.utcnow().replace(microsecond=0))
    }

def validate_user_data(user_data: dict) -> bool:
    """Validate user data against schema"""
    required_fields = ["_id", "name", "current_username"]
    return all(field in user_data for field in required_fields)

def validate_deleted_message(message_data: dict) -> bool:
    """Validate deleted message data against schema"""
    required_fields = ["message_id", "user_id", "channel_id"]
    return all(field in message_data for field in required_fields) 