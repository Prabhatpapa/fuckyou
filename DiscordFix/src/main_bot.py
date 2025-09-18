"""
Main Discord Bot - User Interface for Bot Management System
Handles slash commands and user interactions
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
import discord
from discord.ext import commands
from discord import app_commands

from src.shared.database import init_database, close_database
from src.shared.utils import (
    create_premium_embed, create_error_embed, create_success_embed, 
    create_info_embed, is_valid_discord_token, sanitize_input
)
from src.controller.bot_manager import bot_manager, init_bot_manager
from src.controller.campaign_controller import campaign_controller
from db.mongodb_schema import EmbedConfig, CampaignMode

# Setup minimal logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

class PremiumBotManager(commands.Bot):
    """Premium Discord Bot Manager"""
    
    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.message_content = True
        
        super().__init__(
            command_prefix='!',
            intents=intents,
            description="Advanced Discord Bot Management System"
        )
        
        # Initialize components
        self.bot_manager = None
        self.campaign_controller = None
    
    async def setup_hook(self):
        """Setup hook called when bot is starting"""
        logger.info("Setting up Premium Bot Manager...")
        
        # Initialize database
        mongodb_uri = os.getenv('MONGODB_URI')
        if not await init_database(mongodb_uri):
            logger.error("Failed to connect to database")
            return
        
        # Initialize managers
        self.bot_manager = await init_bot_manager()
        self.campaign_controller = campaign_controller
        
        # Sync commands
        try:
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} commands")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")
    
    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f'{self.user} has connected to Discord!')
        logger.info(f'Bot is in {len(self.guilds)} guilds')
        
        # Set mobile-style status for rich presence
        activity = discord.Activity(
            type=discord.ActivityType.playing,
            name="📱 Mobile Online | Made by Potato <3 🍟"
        )
        await self.change_presence(activity=activity, status=discord.Status.online)
    
    async def on_guild_join(self, guild):
        """Called when bot joins a guild"""
        logger.info(f"Joined guild {guild.name} ({guild.id})")
        
        # Store guild info
        from src.shared.database import get_guilds_collection
        from src.shared.utils import safe_get_guild_info
        
        try:
            guilds_collection = await get_guilds_collection()
            guild_info = safe_get_guild_info(guild)
            
            await guilds_collection.update_one(
                {'_id': str(guild.id)},
                {'$set': guild_info},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Failed to store guild info: {e}")
    
    async def on_error(self, event, *args, **kwargs):
        """Global error handler"""
        logger.error(f"Error in {event}: {args}, {kwargs}")

# Initialize bot instance
bot = PremiumBotManager()

# Bot Management Commands
@bot.tree.command(name="bot", description="🤖 Manage bot tokens")
@app_commands.describe(
    action="Action to perform",
    name="Bot name (for add action)",
    token="Bot token (for add action)"
)
@app_commands.choices(action=[
    app_commands.Choice(name="add", value="add"),
    app_commands.Choice(name="remove", value="remove"),
    app_commands.Choice(name="list", value="list"),
    app_commands.Choice(name="status", value="status")
])
async def bot_command(interaction: discord.Interaction, action: str, name: Optional[str] = None, token: Optional[str] = None):
    """Bot management command"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        if action == "add":
            if not name or not token:
                embed = create_error_embed("Missing parameters", "Both name and token are required for adding a bot")
                await interaction.followup.send(embed=embed)
                return
            
            # Sanitize input
            name = sanitize_input(name, 50)
            
            # Add the bot
            result = await bot_manager.add_bot(name, token, str(interaction.user.id))
            
            if result['success']:
                embed = create_success_embed(
                    "Bot Added Successfully!", 
                    f"**{name}** has been added and is now active.\n"
                    f"Bot ID: `{result['bot_id']}`\n"
                    f"Status: {result['status']}"
                )
                if 'warning' in result:
                    embed.add_field(name="⚠️ Warning", value=result['warning'], inline=False)
            else:
                embed = create_error_embed("Failed to Add Bot", result['error'])
            
            await interaction.followup.send(embed=embed)
        
        elif action == "remove":
            # Show bot selection view
            bots = await bot_manager.list_bots(include_health=False)
            if not bots:
                embed = create_error_embed("No bots found", "There are no bots to remove")
                await interaction.followup.send(embed=embed)
                return
            
            # Create selection dropdown
            options = []
            for bot_info in bots[:25]:  # Discord limit
                options.append(discord.SelectOption(
                    label=bot_info['name'],
                    value=bot_info['id'],
                    description=f"Status: {bot_info['status']} | ID: {bot_info['id'][:8]}..."
                ))
            
            select = discord.ui.Select(
                placeholder="Choose a bot to remove...",
                options=options
            )
            
            async def remove_callback(interaction: discord.Interaction):
                bot_id = select.values[0]
                result = await bot_manager.remove_bot(bot_id, str(interaction.user.id))
                
                if result['success']:
                    embed = create_success_embed("Bot Removed", result['message'])
                else:
                    embed = create_error_embed("Failed to Remove Bot", result['error'])
                
                await interaction.response.send_message(embed=embed, ephemeral=True)
            
            select.callback = remove_callback
            view = discord.ui.View()
            view.add_item(select)
            
            embed = create_info_embed("Remove Bot", "Select a bot to remove:")
            await interaction.followup.send(embed=embed, view=view)
        
        elif action == "list":
            bots = await bot_manager.list_bots()
            
            if not bots:
                embed = create_info_embed("No Bots", "No bots are currently registered")
                await interaction.followup.send(embed=embed)
                return
            
            embed = discord.Embed(
                title="🤖 Registered Bots",
                color=0x800080
            )
            
            for bot_info in bots:
                status_emoji = {
                    'active': '🟢',
                    'inactive': '🔴',
                    'error': '❌',
                    'healthy': '💚',
                    'degraded': '🟡',
                    'unhealthy': '💔'
                }
                
                health_status = bot_info.get('health_status', 'unknown')
                status = bot_info.get('status', 'unknown')
                
                field_value = (
                    f"**Status:** {status_emoji.get(status, '❓')} {status.title()}\n"
                    f"**Health:** {status_emoji.get(health_status, '❓')} {health_status.title()}\n"
                    f"**Worker:** {'✅' if bot_info.get('worker_running') else '❌'}\n"
                    f"**ID:** `{bot_info['id'][:8]}...`"
                )
                
                if bot_info.get('latency'):
                    field_value += f"\n**Latency:** {bot_info['latency']}ms"
                
                embed.add_field(
                    name=bot_info['name'],
                    value=field_value,
                    inline=True
                )
            
            embed.set_footer(text=f"Total: {len(bots)} bots")
            await interaction.followup.send(embed=embed)
        
        elif action == "status":
            # Show detailed status of all bots
            bots = await bot_manager.list_bots()
            
            if not bots:
                embed = create_info_embed("No Bots", "No bots are currently registered")
                await interaction.followup.send(embed=embed)
                return
            
            embed = discord.Embed(
                title="🔍 Detailed Bot Status",
                color=0x800080
            )
            
            healthy_count = sum(1 for b in bots if b.get('health_status') == 'healthy')
            total_errors = sum(b.get('errors_last_hour', 0) for b in bots)
            
            embed.add_field(
                name="📊 Overview",
                value=(
                    f"**Total Bots:** {len(bots)}\n"
                    f"**Healthy:** {healthy_count}\n"
                    f"**Total Errors (1h):** {total_errors}"
                ),
                inline=False
            )
            
            for bot_info in bots[:10]:  # Limit to prevent embed size issues
                last_seen = bot_info.get('last_seen')
                last_seen_str = last_seen.strftime("%H:%M:%S") if last_seen else "Never"
                
                field_value = (
                    f"**Status:** {bot_info.get('status', 'unknown').title()}\n"
                    f"**Health:** {bot_info.get('health_status', 'unknown').title()}\n"
                    f"**Errors (1h):** {bot_info.get('errors_last_hour', 0)}\n"
                    f"**Last Seen:** {last_seen_str}"
                )
                
                embed.add_field(
                    name=f"🤖 {bot_info['name']}",
                    value=field_value,
                    inline=True
                )
            
            await interaction.followup.send(embed=embed)
    
    except Exception as e:
        logger.error(f"Error in bot command: {e}")
        embed = create_error_embed("Command Error", f"An error occurred: {str(e)}")
        await interaction.followup.send(embed=embed)

# DM Command - Advanced DM System for All Members
@bot.tree.command(name="dm", description="📩 Send DMs to ALL server members")
@app_commands.describe(
    message="Message to send to members",
    target_role="Target specific role (optional)",
    server_id="Target specific server ID (optional)"
)
async def dm_command(
    interaction: discord.Interaction, 
    message: str,
    target_role: Optional[discord.Role] = None,
    server_id: Optional[str] = None
):
    """Advanced DM sending command - targets ALL members"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Check if user is owner or whitelisted for DM command access
        user_id = str(interaction.user.id)
        
        # Allow owners
        if not is_owner(user_id):
            # Check if user is whitelisted
            from src.shared.database import get_whitelist_collection
            whitelist_collection = await get_whitelist_collection()
            whitelist_entry = await whitelist_collection.find_one({
                'guild_id': str(interaction.guild_id),
                'user_id': user_id
            })
            
            if not whitelist_entry:
                embed = create_error_embed(
                    "Permission Denied", 
                    "Only the bot owner or whitelisted members can use the DM command."
                )
                await interaction.followup.send(embed=embed)
                return
        
        # Determine target guild
        if server_id:
            # Target specific server
            try:
                target_guild_id = int(server_id)
                target_guild = bot.get_guild(target_guild_id)
                if not target_guild:
                    embed = create_error_embed(
                        "Server Not Found", 
                        f"Bot is not in server with ID {server_id} or server doesn't exist."
                    )
                    await interaction.followup.send(embed=embed)
                    return
                guild_id = str(target_guild_id)
                guild = target_guild
            except ValueError:
                embed = create_error_embed("Invalid Server ID", "Please provide a valid Discord server ID (numbers only)")
                await interaction.followup.send(embed=embed)
                return
        else:
            # Target current server
            guild = interaction.guild
            if not guild:
                embed = create_error_embed("Error", "Could not access guild information")
                await interaction.followup.send(embed=embed)
                return
            guild_id = str(interaction.guild_id)
        
        # Sanitize message
        message = sanitize_input(message, 2000)
        
        # Get available bots
        available_bots = await bot_manager.get_available_bots_for_guild(guild_id)
        if not available_bots:
            embed = create_error_embed(
                "No Bots Available", 
                "No healthy bots available. Please add bot tokens using `/bot add`."
            )
            await interaction.followup.send(embed=embed)
            return
        
        # Load ALL members from the target guild
        await guild.chunk()  # Ensure all members are cached
        
        # Batch fetch blacklist for performance (single DB query)
        from src.shared.database import get_blacklist_collection
        blacklist_collection = await get_blacklist_collection()
        blacklist_cursor = blacklist_collection.find({'guild_id': guild_id})
        blacklist_entries = await blacklist_cursor.to_list(length=None)
        blacklisted_user_ids = {entry['user_id'] for entry in blacklist_entries}
        
        target_members = []
        
        if target_role:
            # Target specific role members
            for member in target_role.members:
                if not member.bot and str(member.id) not in blacklisted_user_ids:
                    target_members.append(str(member.id))
        else:
            # Target ALL members in the server
            for member in guild.members:
                if not member.bot and str(member.id) not in blacklisted_user_ids:
                    target_members.append(str(member.id))
        
        if not target_members:
            embed = create_error_embed(
                "No Target Members", 
                "No valid members found to DM" + (f" in role {target_role.name}" if target_role else "") + 
                (f" in server {guild.name}" if server_id else "") + "."
            )
            await interaction.followup.send(embed=embed)
            return
        
        # Simple message format - no embed, just clean text
        embed_config = None  # No embed for cleaner messages
        
        # Use advanced persistent assignment system
        from src.shared.member_assignment import member_assignment_manager
        
        # Get persistent bot assignments for each member
        member_assignments = {}
        for member_id in target_members:
            assigned_bot_id = await member_assignment_manager.get_or_create_assignment(
                guild_id, member_id, available_bots
            )
            if assigned_bot_id:
                member_assignments[member_id] = assigned_bot_id
        
        # Send confirmation
        estimated_time = len(member_assignments) * 0.5  # Average 0.5 seconds per message
        embed = create_success_embed(
            "⚡ Instant DM Campaign Started!",
            f"**Sending message to {len(member_assignments)} members**\n"
            f"**Message:** {message[:100]}{'...' if len(message) > 100 else ''}\n"
            f"**Bots Used:** {len(set(member_assignments.values()))} unique bots\n"
            f"**Assignment Type:** Persistent (same bot per member)\n"
            f"**Target:** {'All members' if not target_role else f'Role: {target_role.name}'}" + (f" in {guild.name}" if server_id else "") + "\n"
            f"**Speed:** Instant queueing with worker-controlled rate limiting\n"
            f"**ETA:** ~{estimated_time/60:.1f} minutes"
        )
        await interaction.followup.send(embed=embed)
        
        # Queue messages instantly (delays handled by worker send-time rate limiting)
        from src.worker.bot_worker import get_bot_worker
        sent_count = 0
        campaign_id = f"dm_{user_id}_{datetime.utcnow().timestamp()}"  # Single campaign ID
        
        for member_id, bot_id in member_assignments.items():
            try:
                worker = await get_bot_worker(bot_id)
                if worker:
                    await worker.queue_message(
                        user_id=member_id,
                        content=message,
                        embed_config=embed_config,  # None for clean messages
                        campaign_id=campaign_id
                    )
                    sent_count += 1
                        
            except Exception as e:
                logger.error(f"Failed to queue message for {member_id}: {e}")
        
        logger.info(f"Advanced DM campaign started by {user_id} in {guild_id}: {sent_count} messages queued to ALL members with persistent assignments")
    
    except Exception as e:
        logger.error(f"Error in dm command: {e}")
        embed = create_error_embed("Command Error", f"An error occurred: {str(e)}")
        await interaction.followup.send(embed=embed)


# Whitelist Management Command
@bot.tree.command(name="whitelist", description="📝 Manage user whitelist for DM campaigns")
@app_commands.describe(
    action="Action to perform",
    user="User to add/remove from whitelist",
    reason="Reason for whitelist action",
    priority="Priority level (1=normal, 2=high, 3=critical)"
)
@app_commands.choices(action=[
    app_commands.Choice(name="add", value="add"),
    app_commands.Choice(name="remove", value="remove"),
    app_commands.Choice(name="list", value="list"),
    app_commands.Choice(name="clear", value="clear")
])
async def whitelist_command(interaction: discord.Interaction, action: str, user: Optional[discord.Member] = None, reason: Optional[str] = None, priority: Optional[int] = 1):
    """Manage user whitelist for DM campaigns"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Check if user is owner for security
        if not is_owner(str(interaction.user.id)):
            embed = create_error_embed(
                "Permission Denied", 
                "Only the bot owner can manage the whitelist."
            )
            await interaction.followup.send(embed=embed)
            return
        
        from src.shared.database import get_whitelist_collection
        from db.mongodb_schema import WhitelistEntry
        
        whitelist_collection = await get_whitelist_collection()
        guild_id = str(interaction.guild_id)
        owner_id = str(interaction.user.id)
        
        if action == "add":
            if not user:
                embed = create_error_embed("Missing User", "Please specify a user to add to the whitelist")
                await interaction.followup.send(embed=embed)
                return
            
            user_id = str(user.id)
            
            # Check if already whitelisted
            existing_entry = await whitelist_collection.find_one({
                'guild_id': guild_id,
                'user_id': user_id
            })
            
            if existing_entry:
                embed = create_error_embed("Already Whitelisted", f"{user.mention} is already on the whitelist")
                await interaction.followup.send(embed=embed)
                return
            
            # Validate priority
            if priority and (priority < 1 or priority > 3):
                priority = 1
            
            # Add to whitelist
            whitelist_entry = WhitelistEntry(
                guild_id=guild_id,
                user_id=user_id,
                reason=reason or f"Added by {interaction.user.display_name}",
                added_by=owner_id,
                priority_level=priority or 1
            )
            
            await whitelist_collection.insert_one(whitelist_entry.dict(exclude={'_id'}))
            
            priority_names = {1: "Normal", 2: "High", 3: "Critical"}
            embed = create_success_embed(
                "User Added to Whitelist!",
                f"**User:** {user.mention}\n"
                f"**Priority:** {priority_names.get(priority or 1)}\n"
                f"**Reason:** {reason or 'No reason provided'}\n\n"
                f"This user will now receive DMs even without explicit consent."
            )
            await interaction.followup.send(embed=embed)
            
        elif action == "remove":
            if not user:
                # Show user selection dropdown
                whitelist_cursor = whitelist_collection.find({'guild_id': guild_id})
                whitelist_entries = await whitelist_cursor.to_list(length=25)
                
                if not whitelist_entries:
                    embed = create_error_embed("No Whitelisted Users", "No users are currently whitelisted")
                    await interaction.followup.send(embed=embed)
                    return
                
                # Create selection dropdown
                options = []
                for entry in whitelist_entries:
                    try:
                        member = interaction.guild.get_member(int(entry['user_id'])) if interaction.guild else None
                        display_name = member.display_name if member else f"User {entry['user_id']}"
                        priority_names = {1: "Normal", 2: "High", 3: "Critical"}
                        priority_name = priority_names.get(entry.get('priority_level', 1), "Normal")
                        
                        options.append(discord.SelectOption(
                            label=display_name[:100],
                            value=entry['user_id'],
                            description=f"Priority: {priority_name} | Added: {entry['created_at'].strftime('%Y-%m-%d')}"
                        ))
                    except Exception as e:
                        logger.warning(f"Could not process whitelist entry {entry['user_id']}: {e}")
                
                if not options:
                    embed = create_error_embed("No Valid Users", "No valid users found in whitelist")
                    await interaction.followup.send(embed=embed)
                    return
                
                select = discord.ui.Select(
                    placeholder="Choose a user to remove from whitelist...",
                    options=options
                )
                
                async def remove_callback(interaction: discord.Interaction):
                    selected_user_id = select.values[0]
                    result = await whitelist_collection.delete_one({
                        'guild_id': guild_id,
                        'user_id': selected_user_id
                    })
                    
                    if result.deleted_count > 0:
                        embed = create_success_embed("User Removed", f"User removed from whitelist")
                    else:
                        embed = create_error_embed("Remove Failed", "Failed to remove user from whitelist")
                    
                    await interaction.response.send_message(embed=embed, ephemeral=True)
                
                select.callback = remove_callback
                view = discord.ui.View()
                view.add_item(select)
                
                embed = create_info_embed("Remove from Whitelist", "Select a user to remove from the whitelist:")
                await interaction.followup.send(embed=embed, view=view)
            else:
                # Direct removal by user
                user_id = str(user.id)
                result = await whitelist_collection.delete_one({
                    'guild_id': guild_id,
                    'user_id': user_id
                })
                
                if result.deleted_count > 0:
                    embed = create_success_embed("User Removed", f"{user.mention} removed from whitelist")
                else:
                    embed = create_error_embed("Remove Failed", "User was not found in the whitelist")
                
                await interaction.followup.send(embed=embed)
                
        elif action == "list":
            whitelist_cursor = whitelist_collection.find({'guild_id': guild_id}).sort('priority_level', -1).sort('created_at', -1)
            whitelist_entries = await whitelist_cursor.to_list(length=50)
            
            if not whitelist_entries:
                embed = create_info_embed("No Whitelisted Users", "No users are currently on the whitelist")
                await interaction.followup.send(embed=embed)
                return
            
            embed = discord.Embed(
                title="📝 Whitelisted Users",
                description=f"Showing {len(whitelist_entries)} whitelisted users",
                color=0x00FF00
            )
            
            priority_names = {1: "🟢 Normal", 2: "🟡 High", 3: "🔴 Critical"}
            priority_groups = {1: [], 2: [], 3: []}
            
            for entry in whitelist_entries:
                try:
                    member = interaction.guild.get_member(int(entry['user_id'])) if interaction.guild else None
                    display_name = member.mention if member else f"<@{entry['user_id']}>"
                    priority_level = entry.get('priority_level', 1)
                    
                    priority_groups[priority_level].append({
                        'name': display_name,
                        'reason': entry.get('reason', 'No reason'),
                        'added': entry['created_at'].strftime('%Y-%m-%d')
                    })
                except Exception as e:
                    logger.warning(f"Could not process whitelist entry {entry['user_id']}: {e}")
            
            # Add fields by priority
            for priority_level in [3, 2, 1]:  # Critical, High, Normal
                entries = priority_groups[priority_level]
                if entries:
                    field_value = ""
                    for entry in entries[:10]:  # Limit to prevent embed size issues
                        field_value += f"{entry['name']} - *{entry['reason']}* (Added: {entry['added']})\n"
                    
                    embed.add_field(
                        name=f"{priority_names[priority_level]} Priority ({len(entries)} users)",
                        value=field_value or "No users",
                        inline=False
                    )
            
            embed.set_footer(text=f"Total: {len(whitelist_entries)} whitelisted users")
            await interaction.followup.send(embed=embed)
            
        elif action == "clear":
            # Count entries
            count = await whitelist_collection.count_documents({'guild_id': guild_id})
            if count == 0:
                embed = create_info_embed("Already Empty", "The whitelist is already empty")
                await interaction.followup.send(embed=embed)
                return
            
            # Clear all whitelist entries for this guild
            result = await whitelist_collection.delete_many({'guild_id': guild_id})
            
            embed = create_success_embed(
                "Whitelist Cleared",
                f"Removed {result.deleted_count} users from the whitelist"
            )
            await interaction.followup.send(embed=embed)
    
    except Exception as e:
        logger.error(f"Error in whitelist command: {e}")
        embed = create_error_embed("Command Error", f"An error occurred: {str(e)}")
        await interaction.followup.send(embed=embed)

# Guide Command - Comprehensive instructions
@bot.tree.command(name="guide", description="📚 Complete guide on how to use the bot system")
async def guide_command(interaction: discord.Interaction):
    """Complete guide for using the Discord bot system"""
    embed = discord.Embed(
        title="📚 Complete Bot Usage Guide",
        description="Learn how to use the Discord Bot Manager effectively",
        color=0x800080
    )
    
    embed.add_field(
        name="🚀 Getting Started",
        value=(
            "**1.** Add bot tokens using `/bot add <name> <token>`\n"
            "**2.** Bot loads all server members automatically\n"
            "**3.** Messages are sent to ALL members in the server\n"
            "**4.** Send DMs using `/dm <message>`"
        ),
        inline=False
    )
    
    
    embed.add_field(
        name="📩 Advanced DM System",
        value=(
            "• **Smart Distribution:** Only 1 bot DMs each member\n"
            "• **No Spam:** Prevents duplicate messages\n"
            "• **All Members:** DMs ALL server members\n"
            "• **Role Targeting:** Target specific roles\n"
            "• **Rate Limiting:** Configurable pace (messages/minute)"
        ),
        inline=False
    )
    
    embed.add_field(
        name="⚡ DM Command Examples",
        value=(
            "`/dm Hello everyone!` - Send to ALL server members\n"
            "`/dm Hello team! target_role:@Staff` - Send to Staff role only\n"
            "`/dm Announcement pace:5` - Send at 5 messages per minute\n"
            "`/dm Important news target_role:@VIP pace:20` - VIP role, fast pace"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🛡️ Safety & Compliance",
        value=(
            "• **Advanced System:** Persistent bot assignments\n"
            "• **Anti-Spam:** 1 bot = 1 member assignment\n"
            "• **Blacklist Support:** Users can opt-out anytime\n"
            "• **Rate Limits:** Respects Discord API limits\n"
            "• **Health Monitoring:** Automatic bot health checks"
        ),
        inline=False
    )
    
    embed.add_field(
        name="👥 Member Management",
        value=(
            "• Bot automatically loads ALL server members\n"
            "• Only blacklisted users are excluded\n"
            "• System automatically excludes bots\n"
            "• Respects blacklist and privacy settings\n"
            "• Targets ALL members in the server"
        ),
        inline=False
    )
    
    embed.set_footer(text="For more help, use /help for command list")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# Test Command - Test specific bots by having them DM you
@bot.tree.command(name="test", description="🧪 Test specific bots by having them DM you")
@app_commands.describe(
    message="Test message to send (optional)",
    bot_choice="Choose which bots to test"
)
@app_commands.choices(bot_choice=[
    app_commands.Choice(name="All Bots", value="all"),
    app_commands.Choice(name="Single Bot", value="single"),
    app_commands.Choice(name="Select Multiple", value="select")
])
async def test_command(
    interaction: discord.Interaction, 
    bot_choice: str = "single",
    message: Optional[str] = None
):
    """Test command to have specific bots DM the user"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Check if user is owner for security
        if not is_owner(str(interaction.user.id)):
            embed = create_error_embed(
                "Permission Denied", 
                "Only the bot owner can use the test command."
            )
            await interaction.followup.send(embed=embed)
            return
        
        user_id = str(interaction.user.id)
        guild_id = str(interaction.guild_id)
        test_message = message or f"🧪 Test message from bot - Sent at {datetime.utcnow().strftime('%H:%M:%S')}"
        
        # Get available bots
        available_bots = await bot_manager.get_available_bots_for_guild(guild_id)
        if not available_bots:
            embed = create_error_embed(
                "No Bots Available", 
                "No healthy bots available for testing. Please add bot tokens using `/bot add`."
            )
            await interaction.followup.send(embed=embed)
            return
        
        if bot_choice == "all":
            # Test all available bots
            embed = create_success_embed(
                "🧪 Testing All Bots!",
                f"**Testing {len(available_bots)} bots**\n"
                f"**Message:** {test_message[:100]}{'...' if len(test_message) > 100 else ''}\n"
                f"**Target:** {interaction.user.mention}\n\n"
                f"Each bot will send you a DM!"
            )
            await interaction.followup.send(embed=embed)
            
            from src.worker.bot_worker import get_bot_worker
            import random
            success_count = 0
            
            for bot_info in available_bots:
                try:
                    worker = await get_bot_worker(bot_info['bot_id'])
                    if worker:
                        await worker.queue_message(
                            user_id=user_id,
                            content=f"🧪 All Bots Test: {test_message}\nFrom: {bot_info['name']}",
                            embed_config=None,
                            campaign_id=f"test_{user_id}_{datetime.utcnow().timestamp()}"
                        )
                        success_count += 1
                        # Small delay between test messages
                        await asyncio.sleep(random.uniform(0.5, 1.0))
                except Exception as e:
                    logger.error(f"Failed to send test DM from {bot_info['name']}: {e}")
            
            print(f"Test: {success_count}/{len(available_bots)} bots sent test DMs to {interaction.user.name}")
            
        elif bot_choice == "single":
            # Let user select one bot
            options = []
            for bot_info in available_bots[:25]:  # Discord limit
                options.append(discord.SelectOption(
                    label=bot_info['name'],
                    value=bot_info['bot_id'],
                    description=f"Status: {bot_info['status']} | Health: {bot_info.get('health_status', 'unknown')}"
                ))
            
            select = discord.ui.Select(
                placeholder="Choose a bot to test...",
                options=options
            )
            
            async def single_test_callback(select_interaction: discord.Interaction):
                selected_bot_id = select.values[0]
                selected_bot = next(bot for bot in available_bots if bot['bot_id'] == selected_bot_id)
                
                # Send test DM
                from src.worker.bot_worker import get_bot_worker
                worker = await get_bot_worker(selected_bot_id)
                if worker:
                    await worker.queue_message(
                        user_id=user_id,
                        content=f"🧪 Single Bot Test: {test_message}\nFrom: {selected_bot['name']}",
                        embed_config=None,
                        campaign_id=f"test_{user_id}_{datetime.utcnow().timestamp()}"
                    )
                    
                    embed = create_success_embed(
                        "✅ Single Bot Test Started!",
                        f"**Bot:** {selected_bot['name']}\n"
                        f"**Message:** {test_message}\n"
                        f"**Target:** {interaction.user.mention}\n\n"
                        f"Check your DMs!"
                    )
                else:
                    embed = create_error_embed("Test Failed", f"Could not access bot worker for {selected_bot['name']}")
                
                await select_interaction.response.send_message(embed=embed, ephemeral=True)
            
            select.callback = single_test_callback
            view = discord.ui.View()
            view.add_item(select)
            
            embed = create_info_embed("🧪 Select Bot for Test", "Choose which bot should send you a test DM:")
            await interaction.followup.send(embed=embed, view=view)
            
        else:  # select multiple
            # Let user select multiple bots
            options = []
            for bot_info in available_bots[:25]:  # Discord limit
                options.append(discord.SelectOption(
                    label=bot_info['name'],
                    value=bot_info['bot_id'],
                    description=f"Status: {bot_info['status']} | Health: {bot_info.get('health_status', 'unknown')}"
                ))
            
            select = discord.ui.Select(
                placeholder="Choose bots to test (multiple)...",
                options=options,
                max_values=min(len(options), 25)
            )
            
            async def multi_test_callback(select_interaction: discord.Interaction):
                selected_bot_ids = select.values
                selected_bots = [bot for bot in available_bots if bot['bot_id'] in selected_bot_ids]
                
                # Send test DMs from all selected bots
                from src.worker.bot_worker import get_bot_worker
                import random
                success_count = 0
                
                for bot_info in selected_bots:
                    try:
                        worker = await get_bot_worker(bot_info['bot_id'])
                        if worker:
                            await worker.queue_message(
                                user_id=user_id,
                                content=f"🧪 Multi Bot Test: {test_message}\nFrom: {bot_info['name']}",
                                embed_config=None,
                                campaign_id=f"test_{user_id}_{datetime.utcnow().timestamp()}"
                            )
                            success_count += 1
                            # Small delay between test messages
                            await asyncio.sleep(random.uniform(0.3, 0.7))
                    except Exception as e:
                        logger.error(f"Failed to send test DM from {bot_info['name']}: {e}")
                
                embed = create_success_embed(
                    "✅ Multi-Bot Test Started!",
                    f"**Bots Tested:** {success_count}/{len(selected_bots)}\n"
                    f"**Message:** {test_message}\n"
                    f"**Target:** {interaction.user.mention}\n\n"
                    f"Check your DMs for messages from each bot!"
                )
                await select_interaction.response.send_message(embed=embed, ephemeral=True)
            
            select.callback = multi_test_callback
            view = discord.ui.View()
            view.add_item(select)
            
            embed = create_info_embed("🧪 Select Bots for Test", "Choose which bots should send you test DMs:")
            await interaction.followup.send(embed=embed, view=view)
        
    except Exception as e:
        logger.error(f"Error in test command: {e}")
        embed = create_error_embed("Test Error", f"An error occurred: {str(e)}")
        await interaction.followup.send(embed=embed)

# Setup Command - Quick Member Grabber
@bot.tree.command(name="setup", description="⚡ Quickly grab all server members for DM campaigns")
async def setup_command(interaction: discord.Interaction):
    """Quick setup command to grab all server members efficiently"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Check if user is owner for security
        if not is_owner(str(interaction.user.id)):
            embed = create_error_embed(
                "Permission Denied", 
                "Only the bot owner can use the setup command."
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild = interaction.guild
        if not guild:
            embed = create_error_embed("Error", "Could not access guild information")
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = str(interaction.guild_id)
        import time
        start_time = time.monotonic()
        
        # Efficiently chunk all members to ensure complete member list
        await guild.chunk()
        
        # Count total members and valid DM targets
        total_members = len(guild.members)
        bot_count = sum(1 for member in guild.members if member.bot)
        human_members = total_members - bot_count
        
        # Quick check for blacklisted members (single DB query for efficiency)
        from src.shared.database import get_blacklist_collection
        blacklist_collection = await get_blacklist_collection()
        blacklisted_count = await blacklist_collection.count_documents({'guild_id': guild_id})
        
        valid_targets = human_members - blacklisted_count
        
        # Calculate processing time in milliseconds
        processing_time = int((time.monotonic() - start_time) * 1000)
        
        # Create success embed with detailed stats
        embed = create_success_embed(
            "⚡ Server Setup Complete!",
            f"**Server:** {guild.name}\n"
            f"**Total Members:** {total_members:,}\n"
            f"**Human Members:** {human_members:,}\n"
            f"**Bots:** {bot_count:,}\n"
            f"**Blacklisted:** {blacklisted_count:,}\n"
            f"**Valid DM Targets:** {valid_targets:,}\n"
            f"**Processing Time:** {processing_time:.0f}ms\n\n"
            f"✅ All members cached and ready for DM campaigns!\n"
            f"Use `/dm <message>` to send DMs to all {valid_targets:,} valid members."
        )
        
        embed.add_field(
            name="📊 Member Breakdown",
            value=(
                f"🟢 **Ready for DMs:** {valid_targets:,}\n"
                f"🤖 **Bots (skipped):** {bot_count:,}\n"
                f"🚫 **Blacklisted:** {blacklisted_count:,}"
            ),
            inline=False
        )
        
        embed.set_footer(text=f"Setup completed in {processing_time:.0f}ms")
        await interaction.followup.send(embed=embed)
        
        # Log only essential information (minimal logging as requested)
        print(f"Setup: Grabbed {valid_targets:,} members from {guild.name} in {processing_time:.0f}ms")
        
    except Exception as e:
        embed = create_error_embed("Setup Error", f"An error occurred during setup: {str(e)}")
        await interaction.followup.send(embed=embed)

# Help Command
@bot.tree.command(name="help", description="📖 Show help information")
async def help_command(interaction: discord.Interaction):
    """Show help information"""
    embed = discord.Embed(
        title="🏆 Premium Bot Manager - Help",
        description="Advanced Discord Bot Management System with premium features",
        color=0x800080
    )
    
    embed.add_field(
        name="🤖 Bot Management",
        value=(
            "`/bot add <name> <token>` - Add a new bot token\n"
            "`/bot remove` - Remove a bot token\n"
            "`/bot list` - List all registered bots\n"
            "`/bot status` - Show detailed bot status"
        ),
        inline=False
    )
    
    embed.add_field(
        name="📩 Advanced DM System",
        value=(
            "`/dm <message>` - Send DM to ALL server members\n"
            "`/dm <message> server_id:<id>` - Send DM to specific server\n"
            "`/dm <message> target_role:<role>` - Send DM to specific role\n"
            "`/dm <message> pace:<number>` - Set messages per minute (default: 10)\n"
            "\n**🎯 Advanced Features:**\n"
            "• **Persistent Bot Assignment** - Each member always gets DMed by the same bot\n"
            "• **Server Membership Verification** - Only DMs active server members\n"
            "• **Fallback System** - Automatic bot switching if main bot fails\n"
            "• **Load Balancing** - Distributes members evenly across bots"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🛡️ User Management",
        value=(
            "`/whitelist <action>` - Manage priority users (owner only)\n"
            "• Bot targets ALL members automatically\n"
            "• Only blacklisted users are excluded"
        ),
        inline=False
    )
    
    
    # Add admin commands only for owner
    if is_owner(str(interaction.user.id)):
        embed.add_field(
            name="🔒 Owner Commands",
            value=(
                "`/status <status> [message]` - Change bot's Discord status\n"
                "`/mass_dm_server add <server_id>` - Add server for mass DM\n"
                "`/mass_dm_server list` - View all target servers\n"
                "`/mass_dm_server stats` - View assignment statistics"
            ),
            inline=False
        )
    
    embed.add_field(
        name="🔧 Features",
        value=(
            "✨ Premium embeds with custom themes\n"
            "⚡ Smart rate limiting and bot distribution\n"
            "🛡️ Advanced persistent bot assignments\n"
            "📊 Real-time analytics and monitoring\n"
            "🔄 Automatic failover and health checks\n"
            "⏱️ Multiple campaign modes (instant, paced, scheduled)"
        ),
        inline=False
    )
    
    embed.set_footer(
        text="Premium Bot Manager • Built for Discord Communities",
        icon_url=interaction.client.user.avatar.url if interaction.client.user and interaction.client.user.avatar else None
    )
    embed.timestamp = datetime.utcnow()
    
    await interaction.response.send_message(embed=embed, ephemeral=True)


# License Management Commands  

# Owner-only functions
def is_owner(user_id: str) -> bool:
    """Check if user is the bot owner"""
    owner_id = os.getenv('OWNER_ID')
    return bool(owner_id and str(user_id) == str(owner_id))

# Owner-only License Management Commands

# Redeem License Command (separate from license command for clarity)

# Mobile-Style Status Command with Rich Presence
@bot.tree.command(name="status", description="📱 Set mobile-style status with rich presence")
@app_commands.describe(
    status="Discord status to set",
    message="Custom status message (like 'Humans are tasty...😋')",
    details="Rich presence details (optional)",
    state="Rich presence state (optional)"
)
@app_commands.choices(status=[
    app_commands.Choice(name="Online 🟢", value="online"),
    app_commands.Choice(name="Idle 🟡", value="idle"),
    app_commands.Choice(name="Do Not Disturb 🔴", value="dnd"),
    app_commands.Choice(name="Invisible ⚫", value="invisible")
])
async def status_command(
    interaction: discord.Interaction, 
    status: str,
    message: Optional[str] = "Made by Potato <3",
    details: Optional[str] = None,
    state: Optional[str] = None
):
    """Change the bot's Discord status"""
    await interaction.response.defer(ephemeral=True)

    try:
        # Check if user is owner
        if not is_owner(str(interaction.user.id)):
            embed = create_error_embed(
                "Permission Denied", 
                "Only the bot owner can change the status."
            )
            await interaction.followup.send(embed=embed)
            return

        # Map status strings to Discord status objects
        status_map = {
            "online": discord.Status.online,
            "idle": discord.Status.idle,
            "dnd": discord.Status.dnd,
            "invisible": discord.Status.invisible
        }

        discord_status = status_map.get(status, discord.Status.online)

        # Set mobile-style rich presence status
        activity = discord.Activity(
            type=discord.ActivityType.custom,
            name=message or "Made by Potato <3",
            state=state or "📱 Mobile Online",
            details=details or "🚀 Advanced DM System Ready"
        )

        # Update bot presence
        await bot.change_presence(activity=activity, status=discord_status)

        # Also update all managed bot workers
        from src.worker.bot_worker import active_workers
        for worker in active_workers.values():
            try:
                if worker.client and not worker.client.is_closed():
                    await worker.client.change_presence(activity=activity, status=discord_status)
            except Exception as e:
                logger.error(f"Failed to update status for worker {worker.bot_id}: {e}")

        # Send confirmation
        status_emojis = {
            "online": "🟢",
            "idle": "🟡", 
            "dnd": "🔴",
            "invisible": "⚫"
        }

        embed = create_success_embed(
            "📱 Mobile Status Updated!",
            f"**Status:** {status_emojis.get(status, '🟢')} {status.title()} (Mobile-Style)\n"
            f"**Message:** {message or 'Made by Potato <3'}\n"
            f"**Details:** {details or '🚀 Advanced DM System Ready'}\n"
            f"**State:** {state or '📱 Mobile Online'}\n"
            f"**Applied to:** Main bot + {len(active_workers)} worker bots\n\n"
            f"✨ **Rich Presence Active** - Bot now appears with mobile-style status!"
        )

        await interaction.followup.send(embed=embed)

        logger.info(f"Status changed to {status} with message '{message}' by {interaction.user.id}")

    except Exception as e:
        logger.error(f"Error in status command: {e}")
        embed = create_error_embed("Status Error", f"An error occurred: {str(e)}")
        await interaction.followup.send(embed=embed)

# Run the bot
async def main():
    """Main function to run the bot"""
    async with bot:
        # Get bot token from environment
        bot_token = os.getenv('DISCORD_TOKEN')
        if not bot_token:
            logger.error("DISCORD_TOKEN environment variable not set")
            return
        
        try:
            await bot.start(bot_token)
        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
        finally:
            await close_database()

if __name__ == "__main__":
    asyncio.run(main())