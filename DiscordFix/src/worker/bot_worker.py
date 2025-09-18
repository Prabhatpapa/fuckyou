"""
Discord Bot Worker - Individual bot process for message sending
"""

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable
import discord
from discord.ext import tasks
import json

from src.shared.database import (
    get_bots_collection, get_campaign_targets_collection, 
    get_sends_collection, get_bot_health_collection,
    get_ratelimit_state_collection, get_blacklist_collection
)
from src.shared.rate_limiter import get_rate_limiter
from src.shared.utils import (
    create_premium_embed, create_error_embed, safe_get_user_info,
    format_duration
)
from src.shared.encryption import decrypt_bot_token
from db.mongodb_schema import (
    Bot, CampaignTarget, Send, BotHealth, TargetStatus, 
    SendStatus, HealthStatus, EmbedConfig
)

logger = logging.getLogger(__name__)

class DiscordBotWorker:
    """Individual Discord bot worker for handling DM campaigns"""
    
    def __init__(self, bot_data: Bot):
        self.bot_data = bot_data
        self.bot_id = bot_data.id or str(bot_data._id)
        self.rate_limiter = get_rate_limiter(self.bot_id)
        self.is_running = False
        self.health_status = HealthStatus.HEALTHY
        self.last_heartbeat = datetime.utcnow()
        self.errors_last_hour = 0
        self.message_queue = asyncio.Queue()
        self.processed_messages = 0
        self.failed_messages = 0
        
        # Create Discord client with required intents
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.dm_messages = True
        
        self.client: discord.Client = discord.Client(intents=intents)
        self._setup_events()
    
    def _setup_events(self):
        """Setup Discord client events"""
        
        @self.client.event
        async def on_ready():
            logger.info(f"Bot worker {self.bot_id} ({self.client.user}) is ready")
            await self._update_health_status(HealthStatus.HEALTHY)
            
            # Start background tasks
            if not self.health_monitor.is_running():
                self.health_monitor.start()
            if not self.message_processor.is_running():
                self.message_processor.start()
        
        @self.client.event
        async def on_disconnect():
            logger.warning(f"Bot worker {self.bot_id} disconnected")
            await self._update_health_status(HealthStatus.UNHEALTHY)
        
        @self.client.event
        async def on_resumed():
            logger.info(f"Bot worker {self.bot_id} resumed connection")
            await self._update_health_status(HealthStatus.HEALTHY)
        
        @self.client.event
        async def on_error(event, *args, **kwargs):
            logger.error(f"Bot worker {self.bot_id} error in {event}: {traceback.format_exc()}")
            self.errors_last_hour += 1
            if self.errors_last_hour >= 10:
                await self._update_health_status(HealthStatus.DEGRADED)
    
    async def start(self):
        """Start the bot worker"""
        try:
            # Decrypt the bot token
            token = decrypt_bot_token(self.bot_data.token_ciphertext)
            
            # Start the bot
            self.is_running = True
            await self.client.start(token)
            
        except Exception as e:
            logger.error(f"Failed to start bot worker {self.bot_id}: {e}")
            await self._update_health_status(HealthStatus.UNHEALTHY)
            raise
    
    async def stop(self):
        """Stop the bot worker"""
        self.is_running = False
        
        # Stop background tasks
        if self.health_monitor.is_running():
            self.health_monitor.cancel()
        if self.message_processor.is_running():
            self.message_processor.cancel()
        
        # Close the Discord client
        if self.client and not self.client.is_closed():
            await self.client.close()
        
        logger.info(f"Bot worker {self.bot_id} stopped")
    
    async def send_dm(self, user_id: str, content: str, embed_config: Optional[EmbedConfig] = None, campaign_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Send a DM to a user
        
        Args:
            user_id: Discord user ID
            content: Message content
            embed_config: Optional embed configuration
            campaign_id: Optional campaign ID for tracking
        
        Returns:
            Dict with status and details
        """
        try:
            # Check rate limits
            await self.rate_limiter.acquire('POST', '/users/@me/channels')
            
            # Get user
            user = await self.client.fetch_user(int(user_id))
            if not user:
                return {
                    'success': False,
                    'error': 'User not found',
                    'status': SendStatus.FAILED
                }
            
            # Check if user has DMs disabled or blocked the bot
            try:
                # Create embed if config provided
                embed = None
                if embed_config:
                    bot_name = self.client.user.name if self.client.user else "Discord Bot"
                    embed = create_premium_embed(embed_config, bot_name)
                
                # Send the message
                if embed and content:
                    message = await user.send(content=content, embed=embed)
                elif embed:
                    message = await user.send(embed=embed)
                else:
                    message = await user.send(content=content)
                
                # Record successful send
                await self._record_send(campaign_id, user_id, SendStatus.SUCCESS)
                self.processed_messages += 1
                
                logger.debug(f"Bot {self.bot_id} sent DM to {user_id}")
                
                return {
                    'success': True,
                    'message_id': str(message.id),
                    'status': SendStatus.SUCCESS
                }
                
            except discord.Forbidden:
                # User has DMs disabled or blocked the bot
                await self._record_send(campaign_id, user_id, SendStatus.FAILED, 'forbidden', 'User has DMs disabled or blocked the bot')
                return {
                    'success': False,
                    'error': 'User has DMs disabled or blocked the bot',
                    'status': SendStatus.FAILED
                }
            
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited
                    # Update rate limiter with response headers
                    headers_dict = dict(e.response.headers) if hasattr(e, 'response') and hasattr(e.response, 'headers') else {}
                    self.rate_limiter.update_rate_limits('POST', '/users/@me/channels', headers_dict, 429)
                    await self._record_send(campaign_id, user_id, SendStatus.RATE_LIMITED, '429', str(e))
                    return {
                        'success': False,
                        'error': 'Rate limited',
                        'status': SendStatus.RATE_LIMITED,
                        'retry_after': e.response.headers.get('retry-after', 1)
                    }
                else:
                    await self._record_send(campaign_id, user_id, SendStatus.FAILED, str(e.status), str(e))
                    return {
                        'success': False,
                        'error': str(e),
                        'status': SendStatus.FAILED
                    }
        
        except Exception as e:
            logger.error(f"Bot {self.bot_id} error sending DM to {user_id}: {e}")
            await self._record_send(campaign_id, user_id, SendStatus.FAILED, 'exception', str(e))
            self.failed_messages += 1
            self.errors_last_hour += 1
            
            return {
                'success': False,
                'error': str(e),
                'status': SendStatus.FAILED
            }
    
    async def queue_message(self, user_id: str, content: str, embed_config: Optional[EmbedConfig] = None, campaign_id: Optional[str] = None):
        """Queue a message for sending"""
        message_data = {
            'user_id': user_id,
            'content': content,
            'embed_config': embed_config.dict() if embed_config else None,
            'campaign_id': campaign_id,
            'queued_at': datetime.utcnow().isoformat()
        }
        
        await self.message_queue.put(message_data)
        logger.debug(f"Bot {self.bot_id} queued message for {user_id}")
    
    async def _record_send(self, campaign_id: Optional[str], user_id: str, status: SendStatus, error_code: Optional[str] = None, error_message: Optional[str] = None):
        """Record a message send attempt in the database"""
        try:
            sends_collection = await get_sends_collection()
            
            send_record = Send(
                campaign_id=campaign_id or "",
                user_id=user_id,
                bot_id=self.bot_id,
                status=status,
                error_code=error_code,
                error_message=error_message
            )
            
            await sends_collection.insert_one(send_record.dict(exclude={'_id'}))
            
        except Exception as e:
            logger.error(f"Failed to record send: {e}")
    
    async def _update_health_status(self, status: HealthStatus):
        """Update bot health status in database"""
        try:
            self.health_status = status
            self.last_heartbeat = datetime.utcnow()
            
            health_collection = await get_bot_health_collection()
            
            health_data = BotHealth(
                bot_id=self.bot_id,
                status=status,
                latency=int(self.client.latency * 1000) if self.client else None,
                errors_last_hour=self.errors_last_hour,
                last_heartbeat=self.last_heartbeat
            )
            
            # Upsert health record
            await health_collection.update_one(
                {'bot_id': self.bot_id},
                {'$set': health_data.dict(exclude={'_id'})},
                upsert=True
            )
            
        except Exception as e:
            logger.error(f"Failed to update health status: {e}")
    
    @tasks.loop(minutes=5)
    async def health_monitor(self):
        """Monitor bot health and update status"""
        try:
            if self.client and self.client.is_ready():
                # Reset error count every hour
                current_time = datetime.utcnow()
                if not hasattr(self, '_last_error_reset') or (current_time - self._last_error_reset).seconds >= 3600:
                    self.errors_last_hour = 0
                    self._last_error_reset = current_time
                
                # Determine health status
                if self.errors_last_hour >= 20:
                    status = HealthStatus.UNHEALTHY
                elif self.errors_last_hour >= 10:
                    status = HealthStatus.DEGRADED
                else:
                    status = HealthStatus.HEALTHY
                
                await self._update_health_status(status)
            else:
                await self._update_health_status(HealthStatus.UNHEALTHY)
        
        except Exception as e:
            logger.error(f"Health monitor error for bot {self.bot_id}: {e}")
    
    @tasks.loop(seconds=1)
    async def message_processor(self):
        """Process queued messages"""
        try:
            if not self.message_queue.empty():
                message_data = await asyncio.wait_for(self.message_queue.get(), timeout=0.1)
                
                # Reconstruct embed config if present
                embed_config = None
                if message_data.get('embed_config'):
                    embed_config = EmbedConfig(**message_data['embed_config'])
                
                # Send the message
                result = await self.send_dm(
                    user_id=message_data['user_id'],
                    content=message_data['content'],
                    embed_config=embed_config,
                    campaign_id=message_data.get('campaign_id')
                )
                
                # Handle rate limiting
                if result.get('status') == SendStatus.RATE_LIMITED:
                    retry_after = float(result.get('retry_after', 1))
                    logger.info(f"Bot {self.bot_id} rate limited, waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    # Re-queue the message
                    await self.message_queue.put(message_data)
        
        except asyncio.TimeoutError:
            # No messages in queue, continue
            pass
        except Exception as e:
            logger.error(f"Message processor error for bot {self.bot_id}: {e}")
    
    async def get_status(self) -> Dict[str, Any]:
        """Get current bot worker status"""
        return {
            'bot_id': self.bot_id,
            'bot_name': self.bot_data.name,
            'is_running': self.is_running,
            'is_ready': self.client.is_ready() if self.client else False,
            'health_status': self.health_status.value,
            'latency': int(self.client.latency * 1000) if self.client and self.client.is_ready() else None,
            'processed_messages': self.processed_messages,
            'failed_messages': self.failed_messages,
            'queue_size': self.message_queue.qsize(),
            'errors_last_hour': self.errors_last_hour,
            'last_heartbeat': self.last_heartbeat.isoformat()
        }
    
    async def check_user_blacklist(self, guild_id: str, user_id: str) -> Dict[str, Any]:
        """Check if user is blacklisted (DMs ALL members by default now)"""
        try:
            # Check blacklist only - no consent needed anymore
            blacklist_collection = await get_blacklist_collection()
            blacklist_entry = await blacklist_collection.find_one({
                'guild_id': guild_id,
                'user_id': user_id
            })
            
            if blacklist_entry:
                return {
                    'can_send': False,
                    'reason': 'blacklisted',
                    'details': blacklist_entry.get('reason', 'User is blacklisted')
                }
            
            # No consent check - DM ALL members now!
            return {
                'can_send': True,
                'reason': 'approved',
                'details': 'User can receive DMs (not blacklisted)'
            }
            
        except Exception as e:
            logger.error(f"Error checking blacklist: {e}")
            return {
                'can_send': False,
                'reason': 'error',
                'details': str(e)
            }

# Worker registry
active_workers: Dict[str, DiscordBotWorker] = {}

async def start_bot_worker(bot_data: Bot) -> DiscordBotWorker:
    """Start a bot worker"""
    worker = DiscordBotWorker(bot_data)
    active_workers[bot_data.id or str(bot_data._id)] = worker
    
    # Start the worker in a background task
    asyncio.create_task(worker.start())
    
    logger.info(f"Started bot worker for {bot_data.name} ({bot_data.id or bot_data._id})")
    return worker

async def stop_bot_worker(bot_id: str):
    """Stop a bot worker"""
    if bot_id in active_workers:
        worker = active_workers[bot_id]
        await worker.stop()
        del active_workers[bot_id]
        logger.info(f"Stopped bot worker {bot_id}")

async def get_bot_worker(bot_id: str) -> Optional[DiscordBotWorker]:
    """Get a bot worker by ID"""
    return active_workers.get(bot_id)

async def get_all_worker_status() -> List[Dict[str, Any]]:
    """Get status of all active workers"""
    status_list = []
    for worker in active_workers.values():
        status = await worker.get_status()
        status_list.append(status)
    return status_list