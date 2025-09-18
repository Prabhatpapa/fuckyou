"""
Utility functions for Discord Bot Management System
"""

import hashlib
import re
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import discord
from db.mongodb_schema import EmbedConfig

logger = logging.getLogger(__name__)

def generate_fingerprint(data: str) -> str:
    """Generate SHA-256 fingerprint for data"""
    return hashlib.sha256(data.encode('utf-8')).hexdigest()

def is_valid_discord_token(token: str) -> bool:
    """Validate Discord bot token format"""
    # Discord bot tokens follow the format: MTxxxxx.xxxxxx.xxxxxxxxxxxxxxxxxxx
    # But we'll use a more flexible validation
    if not token or len(token) < 50:
        return False
    
    # Check for basic structure
    parts = token.split('.')
    if len(parts) != 3:
        return False
    
    # Check if first part looks like a Discord bot ID (base64 encoded)
    try:
        import base64
        base64.b64decode(parts[0] + '==')  # Add padding if needed
        return True
    except:
        return False

def validate_discord_id(discord_id: str) -> bool:
    """Validate Discord ID format (snowflake)"""
    try:
        id_int = int(discord_id)
        # Discord IDs are 64-bit integers, typically 17-19 digits
        return 100000000000000000 <= id_int <= 999999999999999999  # 18-19 digit range
    except (ValueError, TypeError):
        return False

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split a list into chunks of specified size"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def safe_get_user_info(user: discord.User) -> Dict[str, Any]:
    """Safely extract user information"""
    try:
        return {
            'id': str(user.id),
            'username': user.name,
            'display_name': user.display_name,
            'discriminator': user.discriminator if hasattr(user, 'discriminator') else None,
            'avatar_url': str(user.avatar.url) if user.avatar else None,
            'bot': user.bot
        }
    except Exception as e:
        logger.error(f"Error extracting user info: {e}")
        return {
            'id': str(user.id) if hasattr(user, 'id') else 'unknown',
            'username': 'unknown',
            'display_name': 'unknown',
            'bot': False
        }

def safe_get_guild_info(guild: discord.Guild) -> Dict[str, Any]:
    """Safely extract guild information"""
    try:
        return {
            'id': str(guild.id),
            'name': guild.name,
            'member_count': guild.member_count,
            'owner_id': str(guild.owner_id) if guild.owner_id else None,
            'icon_url': str(guild.icon.url) if guild.icon else None,
            'description': guild.description,
            'verification_level': str(guild.verification_level),
            'created_at': guild.created_at.isoformat() if guild.created_at else None
        }
    except Exception as e:
        logger.error(f"Error extracting guild info: {e}")
        return {
            'id': str(guild.id) if hasattr(guild, 'id') else 'unknown',
            'name': 'unknown',
            'member_count': 0
        }

def create_premium_embed(config: EmbedConfig, bot_name: Optional[str] = None) -> discord.Embed:
    """Create a premium-styled Discord embed"""
    try:
        # Default colors for premium themes
        color_themes = {
            'purple': 0x800080,  # Premium purple
            'white': 0xFFFFFF,   # Clean white
            'dark_purple': 0x4A0E4E,  # Dark premium purple
            'gradient_purple': 0x6A0DAD,  # Gradient purple
            'royal_purple': 0x663399,    # Royal purple
            'black': 0x000000,   # Sleek black
            'gold': 0xFFD700,    # Premium gold
            'silver': 0xC0C0C0   # Premium silver
        }
        
        # Use configured color or default purple
        embed_color = config.color if config.color else color_themes['purple']
        
        # Create the embed
        embed = discord.Embed(
            title=config.title,
            description=config.description,
            color=embed_color
        )
        
        # Add thumbnail
        if config.thumbnail_url:
            embed.set_thumbnail(url=config.thumbnail_url)
        
        # Add image
        if config.image_url:
            embed.set_image(url=config.image_url)
        
        # Add footer
        if config.footer_text:
            embed.set_footer(
                text=config.footer_text,
                icon_url=config.footer_icon_url
            )
        elif bot_name:
            embed.set_footer(
                text=f"Powered by {bot_name} • Premium Bot Manager",
                icon_url="https://cdn.discordapp.com/emojis/123456789.png"  # Premium icon
            )
        
        # Add author
        if config.author_name:
            embed.set_author(
                name=config.author_name,
                icon_url=config.author_icon_url
            )
        
        # Add fields
        for field in config.fields:
            embed.add_field(
                name=field.get('name', 'Field'),
                value=field.get('value', 'No value'),
                inline=field.get('inline', True)
            )
        
        # Add timestamp for premium feel
        embed.timestamp = datetime.now(timezone.utc)
        
        return embed
        
    except Exception as e:
        logger.error(f"Error creating premium embed: {e}")
        # Return a basic embed as fallback
        return discord.Embed(
            title="Premium Bot Manager",
            description="An error occurred while creating the embed.",
            color=0x800080
        )

def create_error_embed(message: str, details: Optional[str] = None) -> discord.Embed:
    """Create a premium error embed"""
    embed = discord.Embed(
        title="❌ Error",
        description=message,
        color=0xFF0000  # Red color for errors
    )
    
    if details:
        embed.add_field(name="Details", value=details, inline=False)
    
    embed.set_footer(text="Premium Bot Manager • Error Handler")
    embed.timestamp = datetime.now(timezone.utc)
    
    return embed

def create_success_embed(message: str, details: Optional[str] = None) -> discord.Embed:
    """Create a premium success embed"""
    embed = discord.Embed(
        title="✅ Success",
        description=message,
        color=0x00FF00  # Green color for success
    )
    
    if details:
        embed.add_field(name="Details", value=details, inline=False)
    
    embed.set_footer(text="Premium Bot Manager • Success")
    embed.timestamp = datetime.now(timezone.utc)
    
    return embed

def create_info_embed(title: str, message: str, color: int = 0x800080) -> discord.Embed:
    """Create a premium info embed"""
    embed = discord.Embed(
        title=f"ℹ️ {title}",
        description=message,
        color=color
    )
    
    embed.set_footer(text="Premium Bot Manager • Information")
    embed.timestamp = datetime.now(timezone.utc)
    
    return embed

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

def calculate_eta(total_targets: int, completed_targets: int, start_time: datetime, pace_per_minute: int) -> str:
    """Calculate estimated time to completion for campaigns"""
    remaining_targets = total_targets - completed_targets
    
    if remaining_targets <= 0:
        return "Completed"
    
    if pace_per_minute <= 0:
        return "Unknown"
    
    remaining_minutes = remaining_targets / pace_per_minute
    remaining_seconds = remaining_minutes * 60
    
    return format_duration(remaining_seconds)

def get_optimal_bot_count(member_count: int, target_rate_per_minute: int = 50) -> int:
    """
    Calculate optimal number of bots needed for a guild based on member count
    and desired messaging rate while respecting Discord limits
    """
    # Discord DM rate limits (approximate):
    # - 10 DMs per 10 seconds per bot
    # - 200 DMs per 10 minutes per bot  
    # - Conservative estimate: ~60 DMs per minute per bot safely
    
    safe_rate_per_bot = 50  # Conservative rate per bot per minute
    
    if member_count <= safe_rate_per_bot:
        return 1
    
    # Calculate needed bots, with minimum of 1 and reasonable maximum
    needed_bots = max(1, min(20, (member_count // safe_rate_per_bot) + 1))
    
    return needed_bots

async def test_bot_token(token: str) -> Dict[str, Any]:
    """Test if a bot token is valid and get bot info"""
    try:
        # Create a temporary client to test the token
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        
        client = discord.Client(intents=intents)
        
        @client.event
        async def on_ready():
            # Bot is ready, we can get info and disconnect
            pass
        
        # Try to login with the token
        await client.login(token)
        
        # If we get here, token is valid
        user = client.user
        if user:
            result = {
                'valid': True,
                'id': str(user.id),
                'name': user.name,
                'discriminator': user.discriminator if hasattr(user, 'discriminator') else None,
                'avatar_url': str(user.avatar.url) if user.avatar else None
            }
        else:
            result = {'valid': False, 'error': 'Could not get bot user info'}
        
        await client.close()
        return result
        
    except discord.LoginFailure:
        return {'valid': False, 'error': 'Invalid token'}
    except discord.HTTPException as e:
        return {'valid': False, 'error': f'HTTP error: {e}'}
    except Exception as e:
        return {'valid': False, 'error': f'Unexpected error: {e}'}

def sanitize_input(text: str, max_length: int = 1000) -> str:
    """Sanitize user input for safe processing"""
    if not text:
        return ""
    
    # Remove potential harmful characters
    sanitized = re.sub(r'[^\w\s\-_.,!?@#$%&*()+={}[\]:;"\'<>|\\\/~`]', '', text)
    
    # Limit length
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + "..."
    
    return sanitized.strip()

class AsyncRateLimiter:
    """Simple async rate limiter"""
    
    def __init__(self, calls_per_second: float):
        self.calls_per_second = calls_per_second
        self.last_call = 0.0
    
    async def acquire(self):
        """Acquire rate limit permission"""
        now = asyncio.get_event_loop().time()
        time_since_last = now - self.last_call
        min_interval = 1.0 / self.calls_per_second
        
        if time_since_last < min_interval:
            await asyncio.sleep(min_interval - time_since_last)
        
        self.last_call = asyncio.get_event_loop().time()