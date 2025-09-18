"""
MongoDB Database Manager for Discord Bot Management System
"""

import os
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
from typing import Optional, Dict, Any, List
import logging
from db.mongodb_schema import COLLECTIONS, INDEXES

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[Any] = None
        self.connected = False
    
    async def connect(self, uri: Optional[str] = None) -> bool:
        """Connect to MongoDB"""
        try:
            if uri is None:
                uri = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
            
            self.client = AsyncIOMotorClient(uri)
            # Test the connection
            await self.client.admin.command('ismaster')
            
            # Get database name from URI or use default
            db_name = os.getenv('MONGODB_DB', 'discord_bot_manager')
            self.db = self.client[db_name]
            
            self.connected = True
            logger.info(f"Connected to MongoDB: {db_name}")
            
            # Create indexes
            await self.create_indexes()
            
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.connected = False
            return False
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            self.connected = False
            logger.info("Disconnected from MongoDB")
    
    async def create_indexes(self):
        """Create all necessary indexes"""
        if not self.connected or self.db is None:
            logger.error("Not connected to database")
            return
        
        try:
            for collection_name, indexes in INDEXES.items():
                collection = self.db[collection_name]
                
                for index in indexes:
                    if isinstance(index, list):
                        # Compound index
                        index_spec = [(field, direction) for field, direction in index]
                        await collection.create_index(index_spec, unique=True)
                        logger.debug(f"Created compound index on {collection_name}: {index_spec}")
                    else:
                        # Single field index
                        field, direction = index if isinstance(index, tuple) else (index, 1)
                        
                        # Check if this should be unique
                        unique = False
                        if collection_name == 'bots' and field == 'token_fingerprint':
                            unique = True
                        elif collection_name == 'bot_health' and field == 'bot_id':
                            unique = True
                        
                        await collection.create_index([(field, direction)], unique=unique)
                        logger.debug(f"Created index on {collection_name}.{field}")
            
            logger.info("All indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    def get_collection(self, collection_name: str):
        """Get a collection by name"""
        if not self.connected or self.db is None:
            raise RuntimeError("Not connected to database")
        
        if collection_name not in COLLECTIONS.values():
            raise ValueError(f"Unknown collection: {collection_name}")
        
        return self.db[collection_name]
    
    async def health_check(self) -> bool:
        """Check if database connection is healthy"""
        try:
            if not self.connected or self.client is None:
                return False
            
            # Ping the database
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

# Global database manager instance
db_manager = DatabaseManager()

# Convenience functions
async def get_collection(collection_name: str):
    """Get a collection - convenience function"""
    return db_manager.get_collection(collection_name)

async def init_database(uri: Optional[str] = None) -> bool:
    """Initialize database connection"""
    return await db_manager.connect(uri)

async def close_database():
    """Close database connection"""
    await db_manager.disconnect()

# Collection getters for type safety
async def get_bots_collection():
    return await get_collection(COLLECTIONS['bots'])

async def get_guilds_collection():
    return await get_collection(COLLECTIONS['guilds'])

async def get_members_collection():
    return await get_collection(COLLECTIONS['members'])


async def get_blacklist_collection():
    return await get_collection(COLLECTIONS['blacklist'])

async def get_whitelist_collection():
    return await get_collection(COLLECTIONS['whitelist'])

async def get_campaigns_collection():
    return await get_collection(COLLECTIONS['campaigns'])

async def get_campaign_targets_collection():
    return await get_collection(COLLECTIONS['campaign_targets'])

async def get_sends_collection():
    return await get_collection(COLLECTIONS['sends'])

async def get_ratelimit_state_collection():
    return await get_collection(COLLECTIONS['ratelimit_state'])

async def get_audits_collection():
    return await get_collection(COLLECTIONS['audits'])

async def get_bot_health_collection():
    return await get_collection(COLLECTIONS['bot_health'])

async def get_member_bot_assignments_collection():
    return await get_collection(COLLECTIONS['member_bot_assignments'])

async def get_target_servers_collection():
    return await get_collection(COLLECTIONS['target_servers'])