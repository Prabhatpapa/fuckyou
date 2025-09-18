"""
Campaign Controller - Manages DM campaigns and distribution
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import random

from src.shared.database import (
    get_campaigns_collection, get_campaign_targets_collection, 
    get_members_collection, get_blacklist_collection,
    get_whitelist_collection, get_guilds_collection
)
from src.shared.utils import chunk_list, get_optimal_bot_count, calculate_eta
from src.controller.bot_manager import bot_manager
from src.worker.bot_worker import get_bot_worker
from db.mongodb_schema import (
    Campaign, CampaignTarget, CampaignMode, CampaignStatus, 
    TargetStatus, EmbedConfig
)

logger = logging.getLogger(__name__)

class CampaignController:
    """Controls DM campaigns and manages distribution across bots"""
    
    def __init__(self):
        self.active_campaigns: Dict[str, Campaign] = {}
        self.campaign_tasks: Dict[str, asyncio.Task] = {}
    
    async def create_campaign(
        self, 
        guild_id: str, 
        name: str, 
        message_content: str,
        created_by: str,
        embed_config: Optional[EmbedConfig] = None,
        mode: CampaignMode = CampaignMode.INSTANT,
        pace: int = 10,
        start_at: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Create a new DM campaign
        
        Args:
            guild_id: Discord guild ID
            name: Campaign name
            message_content: Message to send
            created_by: Discord user ID who created campaign
            embed_config: Optional premium embed configuration
            mode: Campaign mode (instant, paced, scheduled)
            pace: Messages per minute for paced mode
            start_at: Start time for scheduled campaigns
            
        Returns:
            Dict with campaign info and status
        """
        try:
            # Validate guild exists and get member count
            guilds_collection = await get_guilds_collection()
            guild_doc = await guilds_collection.find_one({'_id': guild_id})
            if not guild_doc:
                return {
                    'success': False,
                    'error': 'Guild not found. Please ensure the bot is in the server.'
                }
            
            # Get ALL members for this guild  
            all_members = await self._get_all_members(guild_id)
            if not all_members:
                return {
                    'success': False,
                    'error': 'No members found in this guild. Please make sure the bot is in the server and has proper permissions.'
                }
            
            # Get available bots
            available_bots = await bot_manager.get_available_bots_for_guild(guild_id)
            if not available_bots:
                return {
                    'success': False,
                    'error': 'No healthy bots available. Please add bot tokens using /bot add.'
                }
            
            # Calculate optimal distribution
            total_members = len(all_members)
            recommended_bots = get_optimal_bot_count(total_members, pace)
            
            if len(available_bots) < recommended_bots:
                logger.warning(f"Only {len(available_bots)} bots available, but {recommended_bots} recommended for {total_members} members")
            
            # Create campaign
            campaign = Campaign(
                guild_id=guild_id,
                name=name,
                message_content=message_content,
                embed_data=embed_config,
                mode=mode,
                pace=pace,
                start_at=start_at or datetime.utcnow(),
                created_by=created_by,
                total_targets=total_members,
                status=CampaignStatus.PENDING
            )
            
            # Insert campaign into database
            campaigns_collection = await get_campaigns_collection()
            result = await campaigns_collection.insert_one(campaign.dict(exclude={'_id'}))
            campaign_id = str(result.inserted_id)
            campaign.id = campaign_id
            campaign._id = campaign_id
            
            # Create campaign targets with bot assignments
            targets = await self._create_campaign_targets(campaign_id, all_members, available_bots)
            
            # Store in memory
            self.active_campaigns[campaign_id] = campaign
            
            # Calculate ETA
            eta = calculate_eta(total_members, 0, campaign.created_at, pace)
            
            logger.info(f"Created campaign {name} ({campaign_id}) with {total_members} targets using {len(available_bots)} bots")
            
            return {
                'success': True,
                'campaign_id': campaign_id,
                'name': name,
                'total_targets': total_members,
                'available_bots': len(available_bots),
                'recommended_bots': recommended_bots,
                'eta': eta,
                'status': campaign.status.value
            }
            
        except Exception as e:
            logger.error(f"Failed to create campaign: {e}")
            return {
                'success': False,
                'error': f"Failed to create campaign: {str(e)}"
            }
    
    async def start_campaign(self, campaign_id: str, started_by: str) -> Dict[str, Any]:
        """Start a campaign"""
        try:
            campaigns_collection = await get_campaigns_collection()
            
            # Get campaign
            campaign_doc = await campaigns_collection.find_one({'_id': campaign_id})
            if not campaign_doc:
                return {
                    'success': False,
                    'error': 'Campaign not found'
                }
            
            campaign = Campaign(**campaign_doc)
            campaign.id = campaign_id
            campaign._id = campaign_id
            
            if campaign.status != CampaignStatus.PENDING:
                return {
                    'success': False,
                    'error': f'Campaign is {campaign.status.value}, can only start pending campaigns'
                }
            
            # Update status to running
            campaign.status = CampaignStatus.RUNNING
            await campaigns_collection.update_one(
                {'_id': campaign_id},
                {'$set': {'status': CampaignStatus.RUNNING.value, 'updated_at': datetime.utcnow()}}
            )
            
            # Start campaign task
            if campaign.mode == CampaignMode.SCHEDULED and campaign.start_at:
                # Schedule for later
                delay = (campaign.start_at - datetime.utcnow()).total_seconds()
                if delay > 0:
                    task = asyncio.create_task(self._schedule_campaign(campaign_id, delay))
                else:
                    task = asyncio.create_task(self._execute_campaign(campaign_id))
            else:
                # Start immediately
                task = asyncio.create_task(self._execute_campaign(campaign_id))
            
            self.campaign_tasks[campaign_id] = task
            self.active_campaigns[campaign_id] = campaign
            
            logger.info(f"Started campaign {campaign.name} ({campaign_id}) by user {started_by}")
            
            return {
                'success': True,
                'message': f"Campaign {campaign.name} started successfully",
                'status': CampaignStatus.RUNNING.value
            }
            
        except Exception as e:
            logger.error(f"Failed to start campaign {campaign_id}: {e}")
            return {
                'success': False,
                'error': f"Failed to start campaign: {str(e)}"
            }
    
    async def pause_campaign(self, campaign_id: str, paused_by: str) -> Dict[str, Any]:
        """Pause a running campaign"""
        try:
            campaigns_collection = await get_campaigns_collection()
            
            # Update status
            result = await campaigns_collection.update_one(
                {'_id': campaign_id, 'status': CampaignStatus.RUNNING.value},
                {'$set': {'status': CampaignStatus.PAUSED.value, 'updated_at': datetime.utcnow()}}
            )
            
            if result.matched_count == 0:
                return {
                    'success': False,
                    'error': 'Campaign not found or not running'
                }
            
            # Cancel campaign task
            if campaign_id in self.campaign_tasks:
                self.campaign_tasks[campaign_id].cancel()
                del self.campaign_tasks[campaign_id]
            
            # Update in memory
            if campaign_id in self.active_campaigns:
                self.active_campaigns[campaign_id].status = CampaignStatus.PAUSED
            
            logger.info(f"Paused campaign {campaign_id} by user {paused_by}")
            
            return {
                'success': True,
                'message': 'Campaign paused successfully',
                'status': CampaignStatus.PAUSED.value
            }
            
        except Exception as e:
            logger.error(f"Failed to pause campaign {campaign_id}: {e}")
            return {
                'success': False,
                'error': f"Failed to pause campaign: {str(e)}"
            }
    
    async def cancel_campaign(self, campaign_id: str, cancelled_by: str) -> Dict[str, Any]:
        """Cancel a campaign"""
        try:
            campaigns_collection = await get_campaigns_collection()
            
            # Update status
            result = await campaigns_collection.update_one(
                {'_id': campaign_id},
                {'$set': {'status': CampaignStatus.CANCELLED.value, 'updated_at': datetime.utcnow()}}
            )
            
            if result.matched_count == 0:
                return {
                    'success': False,
                    'error': 'Campaign not found'
                }
            
            # Cancel campaign task
            if campaign_id in self.campaign_tasks:
                self.campaign_tasks[campaign_id].cancel()
                del self.campaign_tasks[campaign_id]
            
            # Remove from memory
            if campaign_id in self.active_campaigns:
                del self.active_campaigns[campaign_id]
            
            logger.info(f"Cancelled campaign {campaign_id} by user {cancelled_by}")
            
            return {
                'success': True,
                'message': 'Campaign cancelled successfully',
                'status': CampaignStatus.CANCELLED.value
            }
            
        except Exception as e:
            logger.error(f"Failed to cancel campaign {campaign_id}: {e}")
            return {
                'success': False,
                'error': f"Failed to cancel campaign: {str(e)}"
            }
    
    async def get_campaign_status(self, campaign_id: str) -> Dict[str, Any]:
        """Get detailed campaign status"""
        try:
            campaigns_collection = await get_campaigns_collection()
            campaign_doc = await campaigns_collection.find_one({'_id': campaign_id})
            
            if not campaign_doc:
                return {'error': 'Campaign not found'}
            
            # Get target statistics
            targets_collection = await get_campaign_targets_collection()
            target_stats = await targets_collection.aggregate([
                {'$match': {'campaign_id': campaign_id}},
                {'$group': {
                    '_id': '$status',
                    'count': {'$sum': 1}
                }}
            ]).to_list(length=None)
            
            stats = {status: 0 for status in ['pending', 'sent', 'failed', 'skipped']}
            for stat in target_stats:
                stats[stat['_id']] = stat['count']
            
            # Calculate progress
            total = sum(stats.values())
            completed = stats['sent'] + stats['failed'] + stats['skipped']
            progress = (completed / total * 100) if total > 0 else 0
            
            # Calculate ETA
            eta = calculate_eta(
                total, 
                completed, 
                campaign_doc.get('created_at'), 
                campaign_doc.get('pace', 10)
            )
            
            return {
                'campaign_id': campaign_id,
                'name': campaign_doc.get('name'),
                'status': campaign_doc.get('status'),
                'mode': campaign_doc.get('mode'),
                'pace': campaign_doc.get('pace'),
                'total_targets': total,
                'completed_targets': completed,
                'progress_percent': round(progress, 1),
                'target_stats': stats,
                'eta': eta,
                'created_at': campaign_doc.get('created_at'),
                'started_at': campaign_doc.get('start_at'),
                'updated_at': campaign_doc.get('updated_at')
            }
            
        except Exception as e:
            logger.error(f"Failed to get campaign status {campaign_id}: {e}")
            return {'error': str(e)}
    
    async def list_campaigns(self, guild_id: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """List campaigns with optional guild filter"""
        try:
            campaigns_collection = await get_campaigns_collection()
            
            query = {}
            if guild_id:
                query['guild_id'] = guild_id
            
            campaigns_cursor = campaigns_collection.find(query).sort('created_at', -1).limit(limit)
            
            campaign_list = []
            async for campaign_doc in campaigns_cursor:
                # Get basic stats
                targets_collection = await get_campaign_targets_collection()
                total_targets = await targets_collection.count_documents({'campaign_id': str(campaign_doc['_id'])})
                completed_targets = await targets_collection.count_documents({
                    'campaign_id': str(campaign_doc['_id']),
                    'status': {'$in': ['sent', 'failed', 'skipped']}
                })
                
                campaign_list.append({
                    'id': str(campaign_doc['_id']),
                    'name': campaign_doc.get('name'),
                    'status': campaign_doc.get('status'),
                    'guild_id': campaign_doc.get('guild_id'),
                    'total_targets': total_targets,
                    'completed_targets': completed_targets,
                    'progress_percent': round((completed_targets / total_targets * 100) if total_targets > 0 else 0, 1),
                    'created_at': campaign_doc.get('created_at'),
                    'created_by': campaign_doc.get('created_by')
                })
            
            return campaign_list
            
        except Exception as e:
            logger.error(f"Failed to list campaigns: {e}")
            return []
    
    async def _get_all_members(self, guild_id: str) -> List[str]:
        """Get list of ALL members who can receive DMs (excluding blacklisted)"""
        try:
            blacklist_collection = await get_blacklist_collection()
            whitelist_collection = await get_whitelist_collection()
            
            # Get ALL members from the guild (this would need to be populated by the main bot)
            members_collection = await get_members_collection()
            members_cursor = members_collection.find({'guild_id': guild_id})
            
            all_users = set()
            async for member in members_cursor:
                all_users.add(member['user_id'])
            
            # Add whitelisted users (priority users)
            whitelist_cursor = whitelist_collection.find({'guild_id': guild_id})
            async for whitelist_entry in whitelist_cursor:
                all_users.add(whitelist_entry['user_id'])
            
            # Remove blacklisted users (blacklist overrides everything)
            blacklisted_cursor = blacklist_collection.find({'guild_id': guild_id})
            async for blacklist in blacklisted_cursor:
                all_users.discard(blacklist['user_id'])
            
            logger.info(f"Found {len(all_users)} eligible members for guild {guild_id} (ALL members except blacklisted)")
            return list(all_users)
            
        except Exception as e:
            logger.error(f"Failed to get all members for guild {guild_id}: {e}")
            return []
    
    async def _create_campaign_targets(self, campaign_id: str, members: List[str], available_bots: List[Dict[str, Any]]) -> List[CampaignTarget]:
        """Create campaign targets with bot assignments"""
        try:
            targets_collection = await get_campaign_targets_collection()
            targets = []
            
            # Distribute members across available bots
            bot_count = len(available_bots)
            for i, user_id in enumerate(members):
                assigned_bot = available_bots[i % bot_count]
                
                target = CampaignTarget(
                    campaign_id=campaign_id,
                    user_id=user_id,
                    assigned_bot_id=assigned_bot['bot_id'],
                    status=TargetStatus.PENDING
                )
                
                targets.append(target)
            
            # Insert all targets
            if targets:
                target_docs = [target.dict(exclude={'_id'}) for target in targets]
                await targets_collection.insert_many(target_docs)
            
            logger.info(f"Created {len(targets)} targets for campaign {campaign_id}")
            return targets
            
        except Exception as e:
            logger.error(f"Failed to create campaign targets: {e}")
            return []
    
    async def _schedule_campaign(self, campaign_id: str, delay: float):
        """Schedule a campaign to start after delay"""
        try:
            await asyncio.sleep(delay)
            await self._execute_campaign(campaign_id)
        except asyncio.CancelledError:
            logger.info(f"Scheduled campaign {campaign_id} was cancelled")
        except Exception as e:
            logger.error(f"Error in scheduled campaign {campaign_id}: {e}")
    
    async def _execute_campaign(self, campaign_id: str):
        """Execute a campaign by sending messages"""
        try:
            logger.info(f"Starting execution of campaign {campaign_id}")
            
            # Get campaign and targets
            campaigns_collection = await get_campaigns_collection()
            targets_collection = await get_campaign_targets_collection()
            
            campaign_doc = await campaigns_collection.find_one({'_id': campaign_id})
            if not campaign_doc:
                logger.error(f"Campaign {campaign_id} not found")
                return
            
            campaign = Campaign(**campaign_doc)
            
            # Get pending targets
            targets_cursor = targets_collection.find({
                'campaign_id': campaign_id,
                'status': TargetStatus.PENDING.value
            })
            
            sent_count = 0
            failed_count = 0
            
            # Process targets based on campaign mode
            if campaign.mode == CampaignMode.INSTANT:
                # Send all messages as fast as possible (respecting bot rate limits)
                async for target_doc in targets_cursor:
                    try:
                        await self._send_campaign_message(campaign, target_doc)
                        sent_count += 1
                    except Exception as e:
                        logger.error(f"Failed to send message for target {target_doc.get('user_id')}: {e}")
                        failed_count += 1
            
            elif campaign.mode == CampaignMode.PACED:
                # Send messages at controlled pace
                message_interval = 60.0 / campaign.pace  # seconds between messages
                
                async for target_doc in targets_cursor:
                    try:
                        await self._send_campaign_message(campaign, target_doc)
                        sent_count += 1
                        
                        # Wait before next message
                        await asyncio.sleep(message_interval)
                        
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.error(f"Failed to send message for target {target_doc.get('user_id')}: {e}")
                        failed_count += 1
            
            # Mark campaign as completed
            await campaigns_collection.update_one(
                {'_id': campaign_id},
                {'$set': {
                    'status': CampaignStatus.COMPLETED.value,
                    'completed_targets': sent_count,
                    'failed_targets': failed_count,
                    'updated_at': datetime.utcnow()
                }}
            )
            
            # Clean up
            if campaign_id in self.campaign_tasks:
                del self.campaign_tasks[campaign_id]
            if campaign_id in self.active_campaigns:
                del self.active_campaigns[campaign_id]
            
            logger.info(f"Campaign {campaign_id} completed: {sent_count} sent, {failed_count} failed")
            
        except asyncio.CancelledError:
            logger.info(f"Campaign {campaign_id} execution was cancelled")
        except Exception as e:
            logger.error(f"Error executing campaign {campaign_id}: {e}")
    
    async def _send_campaign_message(self, campaign: Campaign, target_doc: Dict[str, Any]):
        """Send a single campaign message"""
        try:
            bot_worker = await get_bot_worker(target_doc['assigned_bot_id'])
            if not bot_worker:
                raise Exception(f"Bot worker {target_doc['assigned_bot_id']} not available")
            
            # Queue the message
            await bot_worker.queue_message(
                user_id=target_doc['user_id'],
                content=campaign.message_content,
                embed_config=campaign.embed_data,
                campaign_id=campaign.id
            )
            
            # Update target status
            targets_collection = await get_campaign_targets_collection()
            await targets_collection.update_one(
                {'campaign_id': campaign.id, 'user_id': target_doc['user_id']},
                {'$set': {'status': TargetStatus.SENT.value, 'sent_at': datetime.utcnow()}}
            )
            
        except Exception as e:
            # Update target status to failed
            targets_collection = await get_campaign_targets_collection()
            await targets_collection.update_one(
                {'campaign_id': campaign.id, 'user_id': target_doc['user_id']},
                {'$set': {'status': TargetStatus.FAILED.value, 'last_error': str(e)}}
            )
            raise

# Global campaign controller instance
campaign_controller = CampaignController()

async def get_campaign_controller() -> CampaignController:
    """Get the global campaign controller"""
    return campaign_controller