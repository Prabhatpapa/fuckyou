"""
Premium Embed System with Advanced Discord Formatting
Enhanced decorative styles using Discord markdown
"""

import discord
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import random

class PremiumEmbedStyles:
    """Advanced Discord formatting styles for premium embeds"""
    
    # Color Themes
    COLORS = {
        'royal_purple': 0x6A0DAD,
        'deep_purple': 0x4A148C,
        'neon_purple': 0x9C27B0,
        'cosmic_purple': 0x673AB7,
        'diamond_white': 0xFFFFFF,
        'pearl_white': 0xF8F8FF,
        'platinum': 0xE5E4E2,
        'gold': 0xFFD700,
        'rose_gold': 0xE8B4CB,
        'dark_luxury': 0x1A1A1A,
        'midnight_black': 0x000000,
        'electric_blue': 0x007FFF,
        'emerald_green': 0x50C878,
        'ruby_red': 0xE0115F,
        'cyber_cyan': 0x00FFFF
    }
    
    # Premium Emojis and Symbols
    SYMBOLS = {
        'crown': '👑',
        'diamond': '💎',
        'star': '⭐',
        'sparkle': '✨',
        'fire': '🔥',
        'rocket': '🚀',
        'shield': '🛡️',
        'gem': '💜',
        'lightning': '⚡',
        'magic': '🪄',
        'crystal': '🔮',
        'ring': '💍',
        'trophy': '🏆',
        'medal': '🏅',
        'check': '✅',
        'cross': '❌',
        'warning': '⚠️',
        'info': 'ℹ️',
        'arrow_right': '▶️',
        'arrow_left': '◀️',
        'double_arrow': '⏭️',
        'play': '▶️',
        'pause': '⏸️',
        'stop': '⏹️',
        'settings': '⚙️',
        'lock': '🔒',
        'unlock': '🔓',
        'key': '🔑',
        'money': '💰',
        'bank': '🏦',
        'card': '💳'
    }
    
    # Dividers and Decorative Elements
    DIVIDERS = {
        'premium': '═══════════════════════',
        'diamond': '◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆',
        'star': '★☆★☆★☆★☆★☆★☆★☆★☆★☆★',
        'wave': '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',
        'dots': '• • • • • • • • • • • • • • • • • • • •',
        'line': '▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬',
        'double_line': '══════════════════════════',
        'fancy': '╔══════════════════════════╗',
        'border': '┌─────────────────────────┐'
    }

def create_ultra_premium_embed(
    title: str,
    description: str = "",
    color: str = "royal_purple",
    style: str = "luxury",
    add_timestamp: bool = True,
    author_name: Optional[str] = None,
    author_icon: Optional[str] = None,
    thumbnail: Optional[str] = None,
    image: Optional[str] = None,
    footer_text: Optional[str] = None,
    footer_icon: Optional[str] = None
) -> discord.Embed:
    """
    Create an ultra premium embed with advanced Discord formatting
    
    Styles: luxury, gaming, business, royal, cosmic, minimal
    """
    styles = PremiumEmbedStyles()
    
    # Get color
    embed_color = styles.COLORS.get(color, styles.COLORS['royal_purple'])
    
    # Style-specific formatting
    if style == "luxury":
        formatted_title = f"{styles.SYMBOLS['crown']} **{title}** {styles.SYMBOLS['crown']}"
        formatted_desc = f"> {styles.SYMBOLS['diamond']} {description}\n\n{styles.DIVIDERS['premium']}"
        
    elif style == "gaming":
        formatted_title = f"{styles.SYMBOLS['fire']} **__{title}__** {styles.SYMBOLS['rocket']}"
        formatted_desc = f">>> {styles.SYMBOLS['lightning']} **{description}**\n{styles.DIVIDERS['star']}"
        
    elif style == "business":
        formatted_title = f"{styles.SYMBOLS['shield']} **{title.upper()}**"
        formatted_desc = f"```yaml\n{description}\n```\n{styles.DIVIDERS['line']}"
        
    elif style == "royal":
        formatted_title = f"{styles.SYMBOLS['gem']} ***__{title}__*** {styles.SYMBOLS['gem']}"
        formatted_desc = f">>> {styles.SYMBOLS['crystal']} *{description}*\n\n{styles.DIVIDERS['diamond']}"
        
    elif style == "cosmic":
        formatted_title = f"{styles.SYMBOLS['sparkle']} **~{title}~** {styles.SYMBOLS['magic']}"
        formatted_desc = f"> {styles.SYMBOLS['star']} ***{description}***\n{styles.DIVIDERS['wave']}"
        
    else:  # minimal
        formatted_title = f"**{title}**"
        formatted_desc = f"> {description}\n{styles.DIVIDERS['dots']}"
    
    # Create embed
    embed = discord.Embed(
        title=formatted_title,
        description=formatted_desc,
        color=embed_color
    )
    
    # Add optional elements
    if author_name:
        embed.set_author(name=author_name, icon_url=author_icon)
    
    if thumbnail:
        embed.set_thumbnail(url=thumbnail)
    
    if image:
        embed.set_image(url=image)
    
    if footer_text:
        embed.set_footer(text=footer_text, icon_url=footer_icon)
    else:
        embed.set_footer(
            text=f"{styles.SYMBOLS['diamond']} Premium Bot Manager {styles.SYMBOLS['crown']} • Powered by Excellence",
            icon_url=footer_icon
        )
    
    if add_timestamp:
        embed.timestamp = datetime.now(timezone.utc)
    
    return embed

def create_status_embed(
    title: str,
    status: str,  # success, error, warning, info, loading
    description: str = "",
    details: Optional[str] = None,
    add_fields: Optional[List[Dict[str, Any]]] = None
) -> discord.Embed:
    """Create a premium status embed"""
    styles = PremiumEmbedStyles()
    
    # Status configurations
    status_config = {
        'success': {
            'color': 'emerald_green',
            'emoji': styles.SYMBOLS['check'],
            'prefix': '**SUCCESS**',
            'style': 'luxury'
        },
        'error': {
            'color': 'ruby_red',
            'emoji': styles.SYMBOLS['cross'],
            'prefix': '**ERROR**',
            'style': 'business'
        },
        'warning': {
            'color': 'gold',
            'emoji': styles.SYMBOLS['warning'],
            'prefix': '**WARNING**',
            'style': 'royal'
        },
        'info': {
            'color': 'electric_blue',
            'emoji': styles.SYMBOLS['info'],
            'prefix': '**INFO**',
            'style': 'cosmic'
        },
        'loading': {
            'color': 'cyber_cyan',
            'emoji': styles.SYMBOLS['settings'],
            'prefix': '**PROCESSING**',
            'style': 'gaming'
        }
    }
    
    config = status_config.get(status, status_config['info'])
    
    # Format title and description
    formatted_title = f"{config['emoji']} {config['prefix']}: {title}"
    formatted_desc = f">>> {config['emoji']} **{description}**"
    
    if details:
        formatted_desc += f"\n\n> {styles.SYMBOLS['arrow_right']} *{details}*"
    
    formatted_desc += f"\n{styles.DIVIDERS['line']}"
    
    embed = discord.Embed(
        title=formatted_title,
        description=formatted_desc,
        color=styles.COLORS[config['color']]
    )
    
    # Add fields if provided
    if add_fields:
        for field in add_fields:
            embed.add_field(
                name=f"{styles.SYMBOLS['diamond']} **{field.get('name', 'Field')}**",
                value=f"> {field.get('value', 'No value')}",
                inline=field.get('inline', True)
            )
    
    embed.set_footer(
        text=f"{styles.SYMBOLS['crown']} Premium Bot Manager • {status.title()} Alert",
        icon_url=None
    )
    embed.timestamp = datetime.now(timezone.utc)
    
    return embed

def create_stats_embed(
    title: str,
    stats: Dict[str, Any],
    chart_style: str = "bars"  # bars, progress, list
) -> discord.Embed:
    """Create a premium statistics embed"""
    styles = PremiumEmbedStyles()
    
    embed = discord.Embed(
        title=f"{styles.SYMBOLS['trophy']} **{title}** {styles.SYMBOLS['trophy']}",
        color=styles.COLORS['royal_purple']
    )
    
    description = f"{styles.DIVIDERS['premium']}\n"
    
    for stat_name, stat_value in stats.items():
        if isinstance(stat_value, (int, float)):
            if chart_style == "bars":
                # Create ASCII bar chart
                bar_length = min(int(stat_value / 10), 20) if stat_value > 0 else 0
                bar = "█" * bar_length + "░" * (20 - bar_length)
                description += f"> **{stat_name}**: `{stat_value:,}`\n> `{bar}` **{stat_value}%**\n\n"
            
            elif chart_style == "progress":
                # Progress bar style
                progress = min(stat_value, 100) if isinstance(stat_value, (int, float)) else 0
                filled = int(progress / 5)
                empty = 20 - filled
                progress_bar = "▰" * filled + "▱" * empty
                description += f"> **{stat_name}**: {progress:.1f}%\n> {progress_bar}\n\n"
            
            else:  # list
                description += f"• **{stat_name}**: `{stat_value:,}`\n"
        else:
            description += f"• **{stat_name}**: {stat_value}\n"
    
    description += f"\n{styles.DIVIDERS['premium']}"
    
    embed.description = description
    embed.set_footer(
        text=f"{styles.SYMBOLS['diamond']} Live Statistics • Updated: {datetime.now().strftime('%H:%M:%S')}"
    )
    embed.timestamp = datetime.now(timezone.utc)
    
    return embed

def create_campaign_embed(
    campaign_name: str,
    status: str,
    progress: float,
    total_targets: int,
    completed: int,
    failed: int,
    eta: str,
    mode: str = "instant"
) -> discord.Embed:
    """Create a premium campaign status embed"""
    styles = PremiumEmbedStyles()
    
    # Status colors and emojis
    status_mapping = {
        'pending': {'color': 'gold', 'emoji': styles.SYMBOLS['pause']},
        'running': {'color': 'emerald_green', 'emoji': styles.SYMBOLS['play']},
        'completed': {'color': 'royal_purple', 'emoji': styles.SYMBOLS['check']},
        'paused': {'color': 'cyber_cyan', 'emoji': styles.SYMBOLS['pause']},
        'cancelled': {'color': 'ruby_red', 'emoji': styles.SYMBOLS['stop']}
    }
    
    config = status_mapping.get(status, status_mapping['pending'])
    
    embed = discord.Embed(
        title=f"{config['emoji']} **CAMPAIGN: {campaign_name.upper()}** {config['emoji']}",
        color=styles.COLORS[config['color']]
    )
    
    # Progress bar
    progress_filled = int(progress / 5)
    progress_empty = 20 - progress_filled
    progress_bar = "▰" * progress_filled + "▱" * progress_empty
    
    # Main description
    description = f"""
>>> {styles.SYMBOLS['rocket']} **Campaign Status**: {status.title()}
{styles.SYMBOLS['lightning']} **Progress**: {progress:.1f}%
`{progress_bar}` **{completed}/{total_targets}**

{styles.DIVIDERS['diamond']}

{styles.SYMBOLS['check']} **Delivered**: `{completed:,}` messages
{styles.SYMBOLS['cross']} **Failed**: `{failed:,}` messages
{styles.SYMBOLS['settings']} **Mode**: `{mode.title()}`
{styles.SYMBOLS['crystal']} **ETA**: `{eta}`

{styles.DIVIDERS['star']}
"""
    
    embed.description = description
    
    # Add mode-specific field
    if mode == "paced":
        embed.add_field(
            name=f"{styles.SYMBOLS['arrow_right']} **Pacing**",
            value="> Controlled delivery\n> Rate-limit compliant\n> Premium distribution",
            inline=True
        )
    elif mode == "instant":
        embed.add_field(
            name=f"{styles.SYMBOLS['fire']} **Instant Mode**",
            value="> Maximum speed\n> All bots active\n> Real-time delivery",
            inline=True
        )
    elif mode == "scheduled":
        embed.add_field(
            name=f"{styles.SYMBOLS['crystal']} **Scheduled**",
            value="> Timed execution\n> Automated start\n> Perfect timing",
            inline=True
        )
    
    # Performance metrics
    success_rate = (completed / total_targets * 100) if total_targets > 0 else 0
    embed.add_field(
        name=f"{styles.SYMBOLS['trophy']} **Performance**",
        value=f"> Success Rate: `{success_rate:.1f}%`\n> Quality: `Premium`\n> Priority: `High`",
        inline=True
    )
    
    embed.set_footer(
        text=f"{styles.SYMBOLS['crown']} Premium Campaign Manager • Live Updates",
        icon_url=None
    )
    embed.timestamp = datetime.now(timezone.utc)
    
    return embed

def create_bot_list_embed(bots: List[Dict[str, Any]]) -> discord.Embed:
    """Create a premium bot listing embed"""
    styles = PremiumEmbedStyles()
    
    embed = discord.Embed(
        title=f"{styles.SYMBOLS['shield']} **BOT MANAGEMENT DASHBOARD** {styles.SYMBOLS['shield']}",
        color=styles.COLORS['royal_purple']
    )
    
    if not bots:
        embed.description = f"""
>>> {styles.SYMBOLS['warning']} **No bots registered**
{styles.SYMBOLS['arrow_right']} Use `/bot add` to register your first bot
{styles.SYMBOLS['diamond']} Premium features await!

{styles.DIVIDERS['line']}
"""
        return embed
    
    # Group bots by status
    healthy_bots = [b for b in bots if b.get('health_status') == 'healthy']
    degraded_bots = [b for b in bots if b.get('health_status') == 'degraded']
    unhealthy_bots = [b for b in bots if b.get('health_status') == 'unhealthy']
    
    # Main statistics
    description = f"""
{styles.DIVIDERS['premium']}

{styles.SYMBOLS['trophy']} **FLEET OVERVIEW**
{styles.SYMBOLS['check']} **Healthy**: `{len(healthy_bots)}` bots
{styles.SYMBOLS['warning']} **Degraded**: `{len(degraded_bots)}` bots  
{styles.SYMBOLS['cross']} **Unhealthy**: `{len(unhealthy_bots)}` bots
{styles.SYMBOLS['diamond']} **Total**: `{len(bots)}` registered

{styles.DIVIDERS['star']}
"""
    
    embed.description = description
    
    # Add individual bot fields (limit to 10 for embed limits)
    for bot in bots[:10]:
        status_emoji = {
            'active': styles.SYMBOLS['check'],
            'inactive': styles.SYMBOLS['pause'],
            'error': styles.SYMBOLS['cross']
        }.get(bot.get('status', 'inactive'), styles.SYMBOLS['warning'])
        
        health_emoji = {
            'healthy': '💚',
            'degraded': '💛',
            'unhealthy': '❤️'
        }.get(bot.get('health_status', 'unknown'), '🤍')
        
        latency = bot.get('latency', 0)
        latency_bar = "█" * min(int((100 - latency) / 10), 10) if latency else "██████████"
        
        field_value = f"""
> {status_emoji} **Status**: {bot.get('status', 'unknown').title()}
> {health_emoji} **Health**: {bot.get('health_status', 'unknown').title()}
> {styles.SYMBOLS['lightning']} **Ping**: `{latency}ms`
> `{latency_bar}` Response
"""
        
        embed.add_field(
            name=f"{styles.SYMBOLS['shield']} **{bot['name']}**",
            value=field_value,
            inline=True
        )
    
    embed.set_footer(
        text=f"{styles.SYMBOLS['crown']} Premium Bot Fleet • Managed with Excellence",
        icon_url=None
    )
    embed.timestamp = datetime.now(timezone.utc)
    
    return embed