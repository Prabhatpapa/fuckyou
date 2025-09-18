#!/usr/bin/env python3
"""
Premium Discord Bot Manager - Main Startup Script

A powerful Discord bot management system that allows you to:
- Manage multiple bot tokens
- Create and distribute DM campaigns 
- Handle rate limiting and health monitoring
- Premium licensing system for selling access
- Consent-based messaging (Discord ToS compliant)

Author: Your Name
Version: 1.0.0
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def setup_logging():
    """Setup minimal logging configuration"""
    logging.basicConfig(
        level=logging.WARNING,  # Only show warnings and errors
        format='%(levelname)s: %(message)s',  # Simplified format
        handlers=[
            logging.FileHandler('bot.log', encoding='utf-8')  # Only log to file
        ]
    )
    
    # Set specific log levels to minimize noise
    logging.getLogger('discord').setLevel(logging.ERROR)
    logging.getLogger('discord.http').setLevel(logging.ERROR)
    logging.getLogger('motor').setLevel(logging.ERROR)
    logging.getLogger('asyncio').setLevel(logging.ERROR)

def check_environment():
    """Check if all required environment variables are set"""
    required_vars = [
        'DISCORD_TOKEN',
        'OWNER_ID',
        'MONGODB_URI'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these environment variables before running the bot.")
        print("You can use the Replit Secrets tab to add them securely.")
        return False
    
    print("✅ Environment check passed!")
    return True

def print_startup_banner():
    """Print startup banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║            🏆 PREMIUM DISCORD BOT MANAGER 🏆                ║
    ║                                                              ║
    ║  Advanced Discord bot management with premium features       ║
    ║  • Advanced DM system - targets ALL members                 ║
    ║  • Smart campaign distribution and rate limiting             ║
    ║  • Premium embeds and whitelist management                   ║
    ║  • Rate limiting and health monitoring                       ║
    ║  • Mobile-style status with rich presence                   ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    
    🚀 Starting Premium Bot Manager...
    """
    print(banner)

async def main():
    """Main function"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Print banner
    print_startup_banner()
    
    # Check environment variables
    if not check_environment():
        sys.exit(1)
    
    # Import and run the bot
    try:
        from main_bot import main as bot_main
        logger.info("Environment check passed. Starting bot...")
        await bot_main()
    except ImportError as e:
        logger.error(f"Failed to import bot: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot shutdown complete. Goodbye!")