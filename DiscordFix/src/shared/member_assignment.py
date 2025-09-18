"""
Advanced Member-to-Bot Assignment System
Handles persistent bot assignments for DM campaigns with fallback logic
"""

import logging
import random
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from src.shared.database import get_member_bot_assignments_collection, get_members_collection
from src.controller.bot_manager import bot_manager
from db.mongodb_schema import MemberBotAssignment

logger = logging.getLogger(__name__)

class MemberAssignmentManager:
    """Manages persistent member-to-bot assignments for DM campaigns"""
    
    def __init__(self):
        self.assignment_cache = {}  # Cache for recent assignments
    
    async def get_or_create_assignment(self, guild_id: str, user_id: str, available_bots: List[Dict[str, Any]]) -> Optional[str]:
        """
        Get existing bot assignment for a member or create a new one
        
        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID  
            available_bots: List of available healthy bots
            
        Returns:
            Bot ID that should DM this member, or None if no bots available
        """
        try:
            assignments_collection = await get_member_bot_assignments_collection()
            
            # Check for existing active assignment
            existing_assignment = await assignments_collection.find_one({
                'guild_id': guild_id,
                'user_id': user_id,
                'is_active': True
            })
            
            if existing_assignment:
                assigned_bot_id = existing_assignment['assigned_bot_id']
                
                # Verify the assigned bot is still available and healthy
                is_bot_available = any(bot['bot_id'] == assigned_bot_id for bot in available_bots)
                
                if is_bot_available:
                    # Update last assignment time (increment counter happens after successful send)
                    await assignments_collection.update_one(
                        {'_id': existing_assignment['_id']},
                        {
                            '$set': {'last_dm_at': datetime.utcnow(), 'updated_at': datetime.utcnow()}
                        }
                    )
                    logger.debug(f"Using existing assignment: {user_id} -> {assigned_bot_id}")
                    return assigned_bot_id
                else:
                    # Main bot not available, try fallback bots
                    fallback_bots = existing_assignment.get('fallback_bot_ids', [])
                    for fallback_bot_id in fallback_bots:
                        if any(bot['bot_id'] == fallback_bot_id for bot in available_bots):
                            # Update assignment to use fallback bot
                            await assignments_collection.update_one(
                                {'_id': existing_assignment['_id']},
                                {
                                    '$set': {
                                        'assigned_bot_id': fallback_bot_id,
                                        'last_dm_at': datetime.utcnow(),
                                        'updated_at': datetime.utcnow(),
                                        'assignment_reason': 'fallback_used'
                                    },
                                    '$inc': {'total_dms_sent': 1}
                                }
                            )
                            logger.info(f"Using fallback bot for {user_id}: {fallback_bot_id}")
                            return fallback_bot_id
                    
                    # No fallbacks available, need to reassign to a new bot
                    logger.warning(f"Main bot and fallbacks unavailable for {user_id}, reassigning...")
            
            # Create new assignment or reassign
            return await self._create_new_assignment(guild_id, user_id, available_bots)
            
        except Exception as e:
            logger.error(f"Error in get_or_create_assignment: {e}")
            # Fallback to simple round-robin if assignment system fails
            if available_bots:
                return available_bots[0]['bot_id']
            return None
    
    async def _create_new_assignment(self, guild_id: str, user_id: str, available_bots: List[Dict[str, Any]]) -> Optional[str]:
        """Create a new assignment using intelligent bot selection"""
        if not available_bots:
            return None
        
        try:
            # Get current assignment distribution to balance load
            assignments_collection = await get_member_bot_assignments_collection()
            
            # Count current assignments per bot
            bot_assignment_counts = {}
            for bot in available_bots:
                bot_id = bot['bot_id']
                count = await assignments_collection.count_documents({
                    'guild_id': guild_id,
                    'assigned_bot_id': bot_id,
                    'is_active': True
                })
                bot_assignment_counts[bot_id] = count
            
            # Select bot with least assignments (load balancing)
            selected_bot = min(available_bots, key=lambda bot: bot_assignment_counts.get(bot['bot_id'], 0))
            selected_bot_id = selected_bot['bot_id']
            
            # Generate fallback bot list (exclude selected bot)
            fallback_bots = [bot['bot_id'] for bot in available_bots if bot['bot_id'] != selected_bot_id]
            random.shuffle(fallback_bots)  # Randomize fallback order
            
            # Create assignment document
            assignment = MemberBotAssignment(
                guild_id=guild_id,
                user_id=user_id,
                assigned_bot_id=selected_bot_id,
                assigned_at=datetime.utcnow(),
                last_dm_at=datetime.utcnow(),
                total_dms_sent=1,
                assignment_reason="new_assignment",
                fallback_bot_ids=fallback_bots[:3],  # Keep max 3 fallbacks
                is_active=True
            )
            
            # Handle existing inactive assignments
            await assignments_collection.update_many(
                {'guild_id': guild_id, 'user_id': user_id},
                {'$set': {'is_active': False}}
            )
            
            # Insert new assignment
            await assignments_collection.insert_one(assignment.dict(exclude={'_id'}))
            
            logger.info(f"Created new assignment: {user_id} -> {selected_bot_id} (load: {bot_assignment_counts.get(selected_bot_id, 0)})")
            return selected_bot_id
            
        except Exception as e:
            logger.error(f"Error creating new assignment: {e}")
            # Fallback to random selection
            return random.choice(available_bots)['bot_id']
    
    async def verify_server_membership(self, guild_id: str, user_ids: List[str], discord_guild) -> List[str]:
        """
        Verify which users are still members of the server
        
        Args:
            guild_id: Discord guild ID
            user_ids: List of user IDs to check
            discord_guild: Discord guild object
            
        Returns:
            List of user IDs who are still in the server
        """
        verified_members = []
        
        try:
            # Batch check server membership
            for user_id in user_ids:
                try:
                    # First try cache (fast)
                    member = discord_guild.get_member(int(user_id))
                    
                    # If not in cache, try fetching (slower but accurate)
                    if not member:
                        try:
                            member = await discord_guild.fetch_member(int(user_id))
                        except discord.NotFound:
                            # Member definitely left server, deactivate their assignment
                            logger.info(f"Member {user_id} left server {guild_id}, deactivating assignment")
                            await self._deactivate_member_assignment(guild_id, user_id)
                            continue
                        except discord.HTTPException:
                            # Rate limited or other API error, keep them (benefit of doubt)
                            logger.warning(f"API error verifying membership for {user_id}, keeping in list")
                            verified_members.append(user_id)
                            continue
                    
                    if member and not member.bot:
                        verified_members.append(user_id)
                        # Update member info in database
                        await self._update_member_info(guild_id, user_id, member)
                    elif member and member.bot:
                        # Skip bots
                        continue
                        
                except Exception as e:
                    logger.warning(f"Could not verify membership for {user_id}: {e}")
                    # Keep them in the list if we can't verify (benefit of doubt)
                    verified_members.append(user_id)
            
            logger.info(f"Verified {len(verified_members)}/{len(user_ids)} members still in server {guild_id}")
            return verified_members
            
        except Exception as e:
            logger.error(f"Error verifying server membership: {e}")
            # Return all users if verification fails
            return user_ids
    
    async def _update_member_info(self, guild_id: str, user_id: str, member):
        """Update member information in database"""
        try:
            members_collection = await get_members_collection()
            await members_collection.update_one(
                {'guild_id': guild_id, 'user_id': user_id},
                {
                    '$set': {
                        'username': str(member),
                        'display_name': member.display_name,
                        'last_seen': datetime.utcnow()
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.warning(f"Could not update member info for {user_id}: {e}")
    
    async def _deactivate_member_assignment(self, guild_id: str, user_id: str):
        """Deactivate assignment for member who left the server"""
        try:
            assignments_collection = await get_member_bot_assignments_collection()
            await assignments_collection.update_many(
                {'guild_id': guild_id, 'user_id': user_id},
                {'$set': {'is_active': False, 'updated_at': datetime.utcnow()}}
            )
            logger.info(f"Deactivated assignment for user who left: {user_id}")
        except Exception as e:
            logger.error(f"Could not deactivate assignment for {user_id}: {e}")
    
    async def get_assignment_stats(self, guild_id: str) -> Dict[str, Any]:
        """Get assignment statistics for a guild"""
        try:
            assignments_collection = await get_member_bot_assignments_collection()
            
            # Total active assignments
            total_assignments = await assignments_collection.count_documents({
                'guild_id': guild_id,
                'is_active': True
            })
            
            # Assignments per bot
            pipeline = [
                {'$match': {'guild_id': guild_id, 'is_active': True}},
                {'$group': {
                    '_id': '$assigned_bot_id',
                    'count': {'$sum': 1},
                    'total_dms': {'$sum': '$total_dms_sent'}
                }},
                {'$sort': {'count': -1}}
            ]
            
            bot_stats = await assignments_collection.aggregate(pipeline).to_list(length=None)
            
            return {
                'total_assignments': total_assignments,
                'bot_distribution': bot_stats,
                'guild_id': guild_id
            }
            
        except Exception as e:
            logger.error(f"Error getting assignment stats: {e}")
            return {'total_assignments': 0, 'bot_distribution': [], 'guild_id': guild_id}
    
    async def reassign_bot_members(self, old_bot_id: str, new_bot_id: str, guild_id: str) -> int:
        """Reassign all members from one bot to another (for bot failures)"""
        try:
            assignments_collection = await get_member_bot_assignments_collection()
            
            result = await assignments_collection.update_many(
                {
                    'guild_id': guild_id,
                    'assigned_bot_id': old_bot_id,
                    'is_active': True
                },
                {
                    '$set': {
                        'assigned_bot_id': new_bot_id,
                        'assignment_reason': 'bot_failure_reassignment',
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            
            logger.info(f"Reassigned {result.modified_count} members from {old_bot_id} to {new_bot_id}")
            return result.modified_count
            
        except Exception as e:
            logger.error(f"Error reassigning bot members: {e}")
            return 0

# Global instance
member_assignment_manager = MemberAssignmentManager()