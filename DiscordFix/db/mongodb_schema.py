"""
MongoDB Collections Schema for Discord Bot Management System
"""

from datetime import datetime
from typing import Optional, Dict, List, Any
from pydantic import BaseModel, Field
from enum import Enum

class BotStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive" 
    ERROR = "error"

class CampaignMode(str, Enum):
    INSTANT = "instant"
    PACED = "paced"
    SCHEDULED = "scheduled"

class CampaignStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

class TargetStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    SKIPPED = "skipped"

class SendStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"

class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

# Collection: bots
class Bot(BaseModel):
    _id: Optional[str] = None
    id: Optional[str] = None  # MongoDB ObjectId as string
    name: str
    token_ciphertext: str  # Encrypted bot token
    token_fingerprint: str  # SHA-256 hash for reference
    status: BotStatus = BotStatus.INACTIVE
    last_seen: Optional[datetime] = None
    rate_limit_remaining: int = 0
    rate_limit_reset: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: guilds
class Guild(BaseModel):
    _id: str  # Discord guild ID as string
    name: str
    settings: Dict[str, Any] = Field(default_factory=dict)
    member_count: int = 0
    last_sync: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: members
class Member(BaseModel):
    _id: Optional[str] = None
    guild_id: str  # Discord guild ID
    user_id: str   # Discord user ID
    username: Optional[str] = None
    display_name: Optional[str] = None
    joined_at: Optional[datetime] = None
    last_seen: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow)


# Collection: blacklist
class BlacklistEntry(BaseModel):
    _id: Optional[str] = None
    guild_id: str
    user_id: str
    reason: Optional[str] = None
    added_by: str  # User ID who added to blacklist
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: whitelist - Priority users who can receive DMs
class WhitelistEntry(BaseModel):
    _id: Optional[str] = None
    guild_id: str
    user_id: str
    reason: Optional[str] = None
    added_by: str  # User ID who added to whitelist
    priority_level: int = 1  # 1 = normal, 2 = high, 3 = critical
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: campaigns
class EmbedConfig(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    color: int = 0x800080  # Default purple
    thumbnail_url: Optional[str] = None
    image_url: Optional[str] = None
    footer_text: Optional[str] = None
    footer_icon_url: Optional[str] = None
    author_name: Optional[str] = None
    author_icon_url: Optional[str] = None
    fields: List[Dict[str, Any]] = Field(default_factory=list)

class Campaign(BaseModel):
    _id: Optional[str] = None
    id: Optional[str] = None  # MongoDB ObjectId as string
    guild_id: str
    name: str
    message_content: str
    embed_data: Optional[EmbedConfig] = None
    mode: CampaignMode = CampaignMode.INSTANT
    pace: int = 10  # Messages per minute for paced mode
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    created_by: str  # Discord user ID
    status: CampaignStatus = CampaignStatus.PENDING
    total_targets: int = 0
    completed_targets: int = 0
    failed_targets: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: campaign_targets
class CampaignTarget(BaseModel):
    _id: Optional[str] = None
    campaign_id: str
    user_id: str
    assigned_bot_id: Optional[str] = None
    status: TargetStatus = TargetStatus.PENDING
    attempts: int = 0
    last_error: Optional[str] = None
    sent_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: sends
class Send(BaseModel):
    _id: Optional[str] = None
    campaign_id: str
    user_id: str
    bot_id: str
    status: SendStatus
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: ratelimit_state
class RateLimitState(BaseModel):
    _id: Optional[str] = None
    bot_id: str
    bucket_key: str  # Discord rate limit bucket
    remaining: int = 0
    reset_at: Optional[datetime] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)

# Collection: audits
class Audit(BaseModel):
    _id: Optional[str] = None
    actor: str  # Discord user ID performing action
    action: str
    entity_type: Optional[str] = None  # bot, campaign, etc.
    entity_id: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: bot_health
class BotHealth(BaseModel):
    _id: Optional[str] = None
    bot_id: str
    status: HealthStatus
    latency: Optional[int] = None  # Response time in ms
    errors_last_hour: int = 0
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: member_bot_assignments - Persistent bot assignments for DM campaigns
class MemberBotAssignment(BaseModel):
    _id: Optional[str] = None
    guild_id: str  # Discord guild ID
    user_id: str   # Discord user ID  
    assigned_bot_id: str  # Bot ID that should always DM this member
    assigned_at: datetime = Field(default_factory=datetime.utcnow)
    last_dm_at: Optional[datetime] = None  # Last time this member was DMed
    total_dms_sent: int = 0  # Total DMs sent to this member
    assignment_reason: str = "persistent_assignment"  # Reason for assignment
    fallback_bot_ids: List[str] = Field(default_factory=list)  # Backup bots if main bot fails
    is_active: bool = True  # Whether this assignment is active
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

# Collection: target_servers - Servers configured for mass DM targeting  
class TargetServer(BaseModel):
    _id: Optional[str] = None
    guild_id: str  # Discord guild ID to target
    guild_name: str  # Guild name for reference
    added_by: str  # User ID who added this server
    is_active: bool = True  # Whether this server is active for targeting
    member_count: int = 0  # Cached member count
    last_member_sync: Optional[datetime] = None  # Last time members were synced
    settings: Dict[str, Any] = Field(default_factory=dict)  # Server-specific settings
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

# MongoDB Collection Names
COLLECTIONS = {
    'bots': 'bots',
    'guilds': 'guilds', 
    'members': 'members',
    'blacklist': 'blacklist',
    'whitelist': 'whitelist',
    'campaigns': 'campaigns',
    'campaign_targets': 'campaign_targets',
    'sends': 'sends',
    'ratelimit_state': 'ratelimit_state',
    'audits': 'audits',
    'bot_health': 'bot_health',
    'member_bot_assignments': 'member_bot_assignments',
    'target_servers': 'target_servers'
}

# MongoDB Indexes to create
INDEXES = {
    'members': [
        ('guild_id', 1),
        ('user_id', 1),
        [('guild_id', 1), ('user_id', 1)]  # Compound index
    ],
    'campaign_targets': [
        ('campaign_id', 1),
        ('status', 1),
        [('campaign_id', 1), ('user_id', 1)]  # Unique compound
    ],
    'sends': [
        ('campaign_id', 1),
        ('created_at', -1)
    ],
    'bots': [
        ('status', 1),
        ('token_fingerprint', 1)  # Unique
    ],
    'campaigns': [
        ('guild_id', 1),
        ('status', 1),
        ('created_at', -1)
    ],
    'audits': [
        ('created_at', -1),
        ('actor', 1)
    ],
    'blacklist': [
        [('guild_id', 1), ('user_id', 1)]  # Unique compound
    ],
    'whitelist': [
        [('guild_id', 1), ('user_id', 1)],  # Unique compound
        ('priority_level', -1),
        ('created_at', -1)
    ],
    'ratelimit_state': [
        [('bot_id', 1), ('bucket_key', 1)]  # Unique compound
    ],
    'bot_health': [
        ('bot_id', 1)  # Unique
    ],
    'member_bot_assignments': [
        [('guild_id', 1), ('user_id', 1), ('is_active', 1)],  # Unique compound - one active assignment per user per guild
        ('assigned_bot_id', 1),
        ('last_dm_at', -1)
    ],
    'target_servers': [
        ('guild_id', 1),  # Unique
        ('added_by', 1),
        ('is_active', 1)
    ]
}