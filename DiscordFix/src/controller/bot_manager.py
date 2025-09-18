"""
Bot Manager - Handles bot token management and worker coordination
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import hashlib

from src.shared.database import get_bots_collection, get_bot_health_collection
from src.shared.encryption import encrypt_bot_token, decrypt_bot_token, verify_token_fingerprint
from src.shared.utils import is_valid_discord_token, test_bot_token, generate_fingerprint
from src.worker.bot_worker import start_bot_worker, stop_bot_worker, get_bot_worker, active_workers
from db.mongodb_schema import Bot, BotStatus, BotHealth, HealthStatus

logger = logging.getLogger(__name__)

class BotManager:
    """Manages Discord bot tokens and workers"""
    
    def __init__(self):
        self.bots: Dict[str, Bot] = {}
        self.loading = False
    
    async def add_bot(self, name: str, token: str, created_by: str) -> Dict[str, Any]:
        """
        Add a new Discord bot token
        
        Args:
            name: Human-readable name for the bot
            token: Discord bot token
            created_by: Discord user ID who added the bot
            
        Returns:
            Dict with status and bot info
        """
        try:
            # Validate token format
            if not is_valid_discord_token(token):
                return {
                    'success': False,
                    'error': 'Invalid Discord token format'
                }
            
            # Test the token
            test_result = await test_bot_token(token)
            if not test_result.get('valid'):
                return {
                    'success': False,
                    'error': f"Invalid token: {test_result.get('error', 'Token authentication failed')}"
                }
            
            # Encrypt the token
            encrypted_token, fingerprint = encrypt_bot_token(token)
            
            # Check if token already exists
            bots_collection = await get_bots_collection()
            existing_bot = await bots_collection.find_one({'token_fingerprint': fingerprint})
            if existing_bot:
                return {
                    'success': False,
                    'error': 'This bot token is already registered'
                }
            
            # Create bot record
            bot_data = Bot(
                name=name,
                token_ciphertext=encrypted_token,
                token_fingerprint=fingerprint,
                status=BotStatus.INACTIVE,
                created_at=datetime.utcnow()
            )
            
            # Insert into database
            result = await bots_collection.insert_one(bot_data.dict(exclude={'_id'}))
            bot_id = str(result.inserted_id)
            bot_data.id = bot_id
            bot_data._id = bot_id
            
            # Store in memory
            self.bots[bot_id] = bot_data
            
            # Start the bot worker
            try:
                worker = await start_bot_worker(bot_data)
                
                # Update status to active
                await bots_collection.update_one(
                    {'_id': result.inserted_id},
                    {'$set': {'status': BotStatus.ACTIVE.value, 'last_seen': datetime.utcnow()}}
                )
                
                logger.info(f"Successfully added bot {name} ({bot_id}) by user {created_by}")
                
                return {
                    'success': True,
                    'bot_id': bot_id,
                    'name': name,
                    'status': BotStatus.ACTIVE.value,
                    'bot_info': test_result
                }
                
            except Exception as worker_error:
                # If worker fails to start, mark bot as error status
                await bots_collection.update_one(
                    {'_id': result.inserted_id},
                    {'$set': {'status': BotStatus.ERROR.value}}
                )
                
                logger.error(f"Failed to start worker for bot {bot_id}: {worker_error}")
                
                return {
                    'success': True,  # Bot was added but worker failed
                    'bot_id': bot_id,
                    'name': name,
                    'status': BotStatus.ERROR.value,
                    'warning': f"Bot added but worker failed to start: {worker_error}"
                }
                
        except Exception as e:
            logger.error(f"Failed to add bot {name}: {e}")
            return {
                'success': False,
                'error': f"Failed to add bot: {str(e)}"
            }
    
    async def remove_bot(self, bot_id: str, removed_by: str) -> Dict[str, Any]:
        """
        Remove a Discord bot
        
        Args:
            bot_id: Bot ID to remove
            removed_by: Discord user ID who removed the bot
            
        Returns:
            Dict with status
        """
        try:
            bots_collection = await get_bots_collection()
            
            # Find the bot
            bot_doc = await bots_collection.find_one({'_id': bot_id})
            if not bot_doc:
                return {
                    'success': False,
                    'error': 'Bot not found'
                }
            
            # Stop the worker if running
            await stop_bot_worker(bot_id)
            
            # Remove from database
            await bots_collection.delete_one({'_id': bot_id})
            
            # Remove health record
            health_collection = await get_bot_health_collection()
            await health_collection.delete_one({'bot_id': bot_id})
            
            # Remove from memory
            if bot_id in self.bots:
                del self.bots[bot_id]
            
            logger.info(f"Successfully removed bot {bot_doc.get('name')} ({bot_id}) by user {removed_by}")
            
            return {
                'success': True,
                'message': f"Bot {bot_doc.get('name')} removed successfully"
            }
            
        except Exception as e:
            logger.error(f"Failed to remove bot {bot_id}: {e}")
            return {
                'success': False,
                'error': f"Failed to remove bot: {str(e)}"
            }
    
    async def list_bots(self, include_health: bool = True) -> List[Dict[str, Any]]:
        """
        List all registered bots
        
        Args:
            include_health: Whether to include health status
            
        Returns:
            List of bot information
        """
        try:
            bots_collection = await get_bots_collection()
            bots_cursor = bots_collection.find({})
            
            bot_list = []
            async for bot_doc in bots_cursor:
                bot_info = {
                    'id': str(bot_doc['_id']),
                    'name': bot_doc.get('name'),
                    'status': bot_doc.get('status'),
                    'last_seen': bot_doc.get('last_seen'),
                    'created_at': bot_doc.get('created_at'),
                    'worker_running': str(bot_doc['_id']) in active_workers
                }
                
                # Add health information if requested
                if include_health:
                    health_collection = await get_bot_health_collection()
                    health_doc = await health_collection.find_one({'bot_id': str(bot_doc['_id'])})
                    if health_doc:
                        bot_info.update({
                            'health_status': health_doc.get('status'),
                            'latency': health_doc.get('latency'),
                            'errors_last_hour': health_doc.get('errors_last_hour', 0),
                            'last_heartbeat': health_doc.get('last_heartbeat')
                        })
                    else:
                        bot_info.update({
                            'health_status': 'unknown',
                            'latency': None,
                            'errors_last_hour': 0
                        })
                
                bot_list.append(bot_info)
            
            return bot_list
            
        except Exception as e:
            logger.error(f"Failed to list bots: {e}")
            return []
    
    async def restart_bot(self, bot_id: str) -> Dict[str, Any]:
        """Restart a bot worker"""
        try:
            # Get bot data
            bots_collection = await get_bots_collection()
            bot_doc = await bots_collection.find_one({'_id': bot_id})
            if not bot_doc:
                return {
                    'success': False,
                    'error': 'Bot not found'
                }
            
            # Stop existing worker
            await stop_bot_worker(bot_id)
            
            # Create bot object
            bot_data = Bot(**bot_doc)
            bot_data.id = str(bot_doc['_id'])
            bot_data._id = str(bot_doc['_id'])
            
            # Start new worker
            await start_bot_worker(bot_data)
            
            # Update status
            await bots_collection.update_one(
                {'_id': bot_id},
                {'$set': {'status': BotStatus.ACTIVE.value, 'last_seen': datetime.utcnow()}}
            )
            
            return {
                'success': True,
                'message': f"Bot {bot_doc.get('name')} restarted successfully"
            }
            
        except Exception as e:
            logger.error(f"Failed to restart bot {bot_id}: {e}")
            return {
                'success': False,
                'error': f"Failed to restart bot: {str(e)}"
            }
    
    async def get_bot_status(self, bot_id: str) -> Dict[str, Any]:
        """Get detailed status of a specific bot"""
        try:
            worker = await get_bot_worker(bot_id)
            if worker:
                return await worker.get_status()
            else:
                # Get from database
                bots_collection = await get_bots_collection()
                bot_doc = await bots_collection.find_one({'_id': bot_id})
                if not bot_doc:
                    return {'error': 'Bot not found'}
                
                return {
                    'bot_id': bot_id,
                    'bot_name': bot_doc.get('name'),
                    'is_running': False,
                    'is_ready': False,
                    'status': bot_doc.get('status'),
                    'last_seen': bot_doc.get('last_seen')
                }
                
        except Exception as e:
            logger.error(f"Failed to get bot status {bot_id}: {e}")
            return {'error': str(e)}
    
    async def load_bots_from_database(self):
        """Load all bots from database and start workers"""
        if self.loading:
            return
        
        self.loading = True
        try:
            logger.info("Loading bots from database...")
            
            bots_collection = await get_bots_collection()
            bots_cursor = bots_collection.find({'status': {'$ne': BotStatus.ERROR.value}})
            
            loaded_count = 0
            failed_count = 0
            
            async for bot_doc in bots_cursor:
                try:
                    bot_data = Bot(**bot_doc)
                    bot_data.id = str(bot_doc['_id'])
                    bot_data._id = str(bot_doc['_id'])
                    
                    self.bots[bot_data.id] = bot_data
                    
                    # Start worker
                    await start_bot_worker(bot_data)
                    loaded_count += 1
                    
                    logger.info(f"Loaded bot {bot_data.name} ({bot_data.id})")
                    
                except Exception as e:
                    logger.error(f"Failed to load bot {bot_doc.get('name')}: {e}")
                    failed_count += 1
                    
                    # Mark as error in database
                    await bots_collection.update_one(
                        {'_id': bot_doc['_id']},
                        {'$set': {'status': BotStatus.ERROR.value}}
                    )
            
            logger.info(f"Bot loading complete: {loaded_count} loaded, {failed_count} failed")
            
        except Exception as e:
            logger.error(f"Failed to load bots from database: {e}")
        finally:
            self.loading = False
    
    async def get_available_bots_for_guild(self, guild_id: str) -> List[Dict[str, Any]]:
        """Get list of healthy bots available for a guild"""
        try:
            available_bots = []
            
            for bot_id, worker in active_workers.items():
                status = await worker.get_status()
                
                # Only include healthy, ready bots
                if (status.get('is_ready') and 
                    status.get('health_status') in ['healthy', 'degraded'] and
                    status.get('queue_size', 0) < 100):  # Not overloaded
                    
                    available_bots.append({
                        'bot_id': bot_id,
                        'name': status.get('bot_name'),
                        'queue_size': status.get('queue_size', 0),
                        'health_status': status.get('health_status'),
                        'latency': status.get('latency')
                    })
            
            # Sort by queue size (prefer less loaded bots)
            available_bots.sort(key=lambda x: x['queue_size'])
            
            return available_bots
            
        except Exception as e:
            logger.error(f"Failed to get available bots for guild {guild_id}: {e}")
            return []

# Global bot manager instance
bot_manager = BotManager()

async def init_bot_manager():
    """Initialize bot manager and load bots"""
    await bot_manager.load_bots_from_database()
    return bot_manager

async def get_bot_manager() -> BotManager:
    """Get the global bot manager"""
    return bot_manager