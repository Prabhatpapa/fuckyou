"""
Rate Limiting System for Discord Bot Management
Implements per-bot rate limiting with Discord API compliance
"""

import asyncio
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
from asyncio import Lock
import json

logger = logging.getLogger(__name__)

@dataclass
class RateLimitBucket:
    """Represents a Discord rate limit bucket"""
    remaining: int = 0
    limit: int = 1
    reset_after: float = 0.0
    reset_timestamp: float = 0.0
    retry_after: Optional[float] = None
    
    def is_rate_limited(self) -> bool:
        """Check if we're currently rate limited"""
        current_time = time.time()
        
        # Check if we have a retry_after from a 429
        if self.retry_after and current_time < self.retry_after:
            return True
        
        # Check if we've exceeded the rate limit
        if self.remaining <= 0 and current_time < self.reset_timestamp:
            return True
        
        return False
    
    def time_until_reset(self) -> float:
        """Get time until rate limit resets"""
        current_time = time.time()
        
        if self.retry_after and current_time < self.retry_after:
            return self.retry_after - current_time
        
        if current_time < self.reset_timestamp:
            return self.reset_timestamp - current_time
        
        return 0.0
    
    def update_from_headers(self, headers: Dict[str, Any]):
        """Update bucket from Discord API response headers"""
        try:
            if 'x-ratelimit-remaining' in headers:
                self.remaining = int(headers['x-ratelimit-remaining'])
            
            if 'x-ratelimit-limit' in headers:
                self.limit = int(headers['x-ratelimit-limit'])
            
            if 'x-ratelimit-reset-after' in headers:
                self.reset_after = float(headers['x-ratelimit-reset-after'])
                self.reset_timestamp = time.time() + self.reset_after
            
            if 'retry-after' in headers:
                self.retry_after = time.time() + float(headers['retry-after'])
            
            logger.debug(f"Updated rate limit bucket: remaining={self.remaining}, reset_after={self.reset_after}")
            
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse rate limit headers: {e}")

class DiscordRateLimiter:
    """Discord API Rate Limiter"""
    
    def __init__(self, bot_id: str):
        self.bot_id = bot_id
        self.buckets: Dict[str, RateLimitBucket] = {}
        self.locks: Dict[str, Lock] = {}
        self.global_rate_limit: Optional[float] = None
        self.global_lock = Lock()
    
    def _get_bucket_key(self, method: str, endpoint: str, guild_id: Optional[str] = None) -> str:
        """Generate bucket key for rate limiting"""
        # Discord uses different buckets for different endpoints
        if endpoint.startswith('/channels/'):
            # Channel-specific endpoints
            if '/messages' in endpoint:
                return f"channel_messages:{endpoint.split('/')[2]}"
            return f"channel:{endpoint.split('/')[2]}"
        elif endpoint.startswith('/guilds/'):
            # Guild-specific endpoints
            guild_id = endpoint.split('/')[2]
            if '/members' in endpoint:
                return f"guild_members:{guild_id}"
            return f"guild:{guild_id}"
        elif '/users/' in endpoint and '/channels' in endpoint:
            # DM endpoints
            return "dm_channels"
        else:
            # Generic bucket
            return f"{method}:{endpoint}"
    
    async def acquire(self, method: str, endpoint: str, guild_id: Optional[str] = None) -> bool:
        """
        Acquire rate limit permission for an API call
        
        Returns:
            True if request can proceed, False if rate limited
        """
        bucket_key = self._get_bucket_key(method, endpoint, guild_id)
        
        # Ensure we have a lock for this bucket
        if bucket_key not in self.locks:
            self.locks[bucket_key] = Lock()
        
        async with self.locks[bucket_key]:
            # Check global rate limit first
            if self.global_rate_limit and time.time() < self.global_rate_limit:
                wait_time = self.global_rate_limit - time.time()
                logger.info(f"Bot {self.bot_id}: Global rate limit active, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
                self.global_rate_limit = None
            
            # Get or create bucket
            if bucket_key not in self.buckets:
                self.buckets[bucket_key] = RateLimitBucket()
            
            bucket = self.buckets[bucket_key]
            
            # Check if we're rate limited
            if bucket.is_rate_limited():
                wait_time = bucket.time_until_reset()
                logger.info(f"Bot {self.bot_id}: Rate limited on {bucket_key}, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
                
                # Reset the bucket after waiting
                bucket.remaining = bucket.limit
                bucket.retry_after = None
            
            # Decrease remaining requests
            if bucket.remaining > 0:
                bucket.remaining -= 1
            
            return True
    
    def update_rate_limits(self, method: str, endpoint: str, headers: Dict[str, Any], status_code: int):
        """Update rate limits based on API response"""
        bucket_key = self._get_bucket_key(method, endpoint)
        
        # Handle 429 Too Many Requests
        if status_code == 429:
            is_global = headers.get('x-ratelimit-global', 'false').lower() == 'true'
            
            if is_global:
                retry_after = float(headers.get('retry-after', 1))
                self.global_rate_limit = time.time() + retry_after
                logger.warning(f"Bot {self.bot_id}: Global rate limit hit, retry after {retry_after}s")
            else:
                if bucket_key not in self.buckets:
                    self.buckets[bucket_key] = RateLimitBucket()
                self.buckets[bucket_key].update_from_headers(headers)
                logger.warning(f"Bot {self.bot_id}: Rate limit hit on {bucket_key}")
        
        # Update bucket from headers
        elif bucket_key in self.buckets:
            self.buckets[bucket_key].update_from_headers(headers)
    
    def get_bucket_status(self, method: str, endpoint: str) -> Dict[str, Any]:
        """Get current status of a rate limit bucket"""
        bucket_key = self._get_bucket_key(method, endpoint)
        
        if bucket_key not in self.buckets:
            return {"bucket": bucket_key, "remaining": "unknown", "reset_in": 0}
        
        bucket = self.buckets[bucket_key]
        return {
            "bucket": bucket_key,
            "remaining": bucket.remaining,
            "limit": bucket.limit,
            "reset_in": bucket.time_until_reset(),
            "is_limited": bucket.is_rate_limited()
        }

class CampaignRateLimiter:
    """Campaign-level rate limiter for controlling message pace"""
    
    def __init__(self, messages_per_minute: int = 10):
        self.messages_per_minute = messages_per_minute
        self.message_times: List[float] = []
        self.lock = Lock()
    
    async def acquire(self) -> bool:
        """Acquire permission to send a message"""
        async with self.lock:
            current_time = time.time()
            
            # Remove messages older than 1 minute
            cutoff_time = current_time - 60.0
            self.message_times = [t for t in self.message_times if t > cutoff_time]
            
            # Check if we can send another message
            if len(self.message_times) >= self.messages_per_minute:
                # Calculate wait time until we can send
                oldest_message_time = min(self.message_times)
                wait_time = 60.0 - (current_time - oldest_message_time)
                
                if wait_time > 0:
                    logger.debug(f"Campaign rate limit: waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    return await self.acquire()  # Recursive call after waiting
            
            # Record this message time
            self.message_times.append(current_time)
            return True
    
    def update_pace(self, new_messages_per_minute: int):
        """Update the campaign pace"""
        self.messages_per_minute = new_messages_per_minute
        logger.info(f"Updated campaign pace to {new_messages_per_minute} messages/minute")

# Global rate limiter registry
rate_limiters: Dict[str, DiscordRateLimiter] = {}

def get_rate_limiter(bot_id: str) -> DiscordRateLimiter:
    """Get or create a rate limiter for a bot"""
    if bot_id not in rate_limiters:
        rate_limiters[bot_id] = DiscordRateLimiter(bot_id)
    return rate_limiters[bot_id]

def remove_rate_limiter(bot_id: str):
    """Remove a rate limiter (when bot is removed)"""
    if bot_id in rate_limiters:
        del rate_limiters[bot_id]
        logger.info(f"Removed rate limiter for bot {bot_id}")