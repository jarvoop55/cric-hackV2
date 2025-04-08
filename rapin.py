import os
import logging
import asyncio
import random
import time
from typing import Dict, Any, Optional, List
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, MessageNotModified, RPCError
from pyrogram.types import Message
from Mukund import Mukund
from flask import Flask

# Configure Logging with rotation to prevent large log files
import logging.handlers
log_handler = logging.handlers.RotatingFileHandler(
    "telegram_collector.log", 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=3
)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler()]
)

# Initialize Databases with error handling
try:
    storage_vegeta = Mukund("Vegeta")
    storage_goku = Mukund("Goku")
    
    db_vegeta = storage_vegeta.database("players")
    db_goku = storage_goku.database("players")
    
    # Track active database
    current_db = db_vegeta  # Default database
    current_db_name = "Vegeta"  # Track the name for response messages
except Exception as e:
    logging.critical(f"Failed to initialize databases: {e}")
    raise

# In-memory cache with TTL for performance optimization
class TTLCache:
    def __init__(self, ttl_seconds: int = 3600):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.timestamps: Dict[str, float] = {}
        self.ttl_seconds = ttl_seconds
        
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        self._clean_expired()
        return self.cache.get(key)
        
    def set(self, key: str, value: Dict[str, Any]) -> None:
        self.cache[key] = value
        self.timestamps[key] = time.time()
        
    def _clean_expired(self) -> None:
        """Remove expired entries."""
        now = time.time()
        expired_keys = [k for k, t in self.timestamps.items() if now - t > self.ttl_seconds]
        for k in expired_keys:
            self.cache.pop(k, None)
            self.timestamps.pop(k, None)
    
    def clear(self) -> None:
        """Clear all cache entries."""
        self.cache.clear()
        self.timestamps.clear()
        
    def __len__(self) -> int:
        self._clean_expired()
        return len(self.cache)

# Initialize cache with 1-hour TTL
player_cache = TTLCache(ttl_seconds=3600)

def preload_players() -> None:
    """Load players into cache from the active database with retry logic."""
    global player_cache
    max_retries = 3
    retry_delay = 2
    
    logging.info(f"Preloading players from {current_db_name}...")
    
    for attempt in range(max_retries):
        try:
            all_players = current_db.all()
            if isinstance(all_players, dict):
                # Clear and update the cache
                player_cache.clear()
                for key, value in all_players.items():
                    player_cache.set(key, value)
                logging.info(f"Loaded {len(player_cache)} players from {current_db_name}.")
                return
            else:
                logging.error(f"Database returned unexpected data format: {type(all_players)}")
                
        except Exception as e:
            logging.error(f"Failed to preload database (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.critical("All attempts to load database failed!")

# Flask health check
web_app = Flask(__name__)

@web_app.route('/health')
def health_check():
    """Health check endpoint for monitoring."""
    cache_size = len(player_cache)
    return {
        "status": "OK", 
        "database": current_db_name,
        "cache_size": cache_size,
        "collect_running": collect_running,
        "collect_main_running": collect_main_running
    }, 200

@web_app.route('/stats')
def stats():
    """Return statistics about the running service."""
    return {
        "database": current_db_name,
        "cache_size": len(player_cache),
        "collect_running": collect_running,
        "collect_main_running": collect_main_running,
        "uptime_seconds": time.time() - start_time
    }, 200

async def run_flask():
    """Runs Flask server for health checks"""
    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    config = Config()
    config.bind = ["0.0.0.0:8000"]
    await serve(web_app, config)

# Environment variables with more secure handling
API_ID = int(os.environ.get("API_ID", "20061115"))
API_HASH = os.environ.get("API_HASH", "c30d56d90d59b3efc7954013c580e076")
SESSION_STRING = os.environ.get("SESSION_STRING", "BQA4ntkAdv7Yn7dZ2dN67mPWA6EFx5_eCStgr205gdPHAe3pMkWrgZt11XIpdOpTvVUMEdbRA2em8es5klLLkZ0PgeU1k9F4wcxRLzCUnmvW-0F2fq3NODO7GP-Pgxyfxw1YZGTtHzHQ4za6GHeIsAnGQpGRdmpl6PgIZGVBomKoZpYr2U446urNs-pqwPtXW5GbfUXTOAnAg0blpP3f5MC6eyI4uTN05Vhdhc0IRdoYU8i8oO6eBzAWxHF4au4B-OuCgH1FxIFOmmoDhQcKPlvMHL_SoddADZgKGkqbVTjjQt91rNZUdvbgOpYAye1mEA3ZIRUFdHArO4QLVp9Ta89zjppWRQAAAAGEzYnbAA")

# Initialize the client with optimized settings
bot = Client(
    "pro",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=25,  # Increased workers for better concurrency
    max_concurrent_transmissions=15,  # Increased for better throughput
)

# Configuration settings
RARITIES_TO_FORWARD = ["Cosmic", "Limited Edition", "Exclusive", "Ultimate"]
TARGET_GROUP_ID = -1002348881334  # Original target group
MAIN_GROUP_ID = -1002436920609  # Main group for /startmain command
FORWARD_CHANNEL_ID = -1002260368357  # Forwarding channel

# Control flags for collect functions
collect_running = False
collect_main_running = False
start_time = time.time()

# Admin User IDs (replace with actual admin IDs)
ADMIN_USER_IDS = [1745451559, 1710597756, 7522153272, 7946198415, 7742832624, 7859049019, 7828242164, 7957490622, 6523029979]

# User IDs permitted to trigger the collect function
COLLECTOR_USER_IDS = [
    7522153272, 7946198415, 7742832624, 7859049019, 1710597756, 7828242164, 
    7957490622, 7957490622, 7509527964, 8079928714
]

# Rate limiting for collect commands
last_collect_time = {}
COLLECT_COOLDOWN = 2  # seconds between collects

# Target captions to look for
TARGET_CAPTIONS = [
    "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴘʟᴀʏᴇʀ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ/Her ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ",
    "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴀᴛʜʟᴇᴛᴇ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ/Her ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ",
    "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴄᴇʟᴇʙʀɪᴛʏ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ/Her ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ",
    "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴘʟᴀʏᴇʀ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ"
]

def should_forward_message(text: str) -> bool:
    """Check if a message should be forwarded based on the rarity."""
    if not text:
        return False
        
    for rarity in RARITIES_TO_FORWARD:
        if f"Rarity : {rarity}" in text:
            return True
    return False

@bot.on_message(filters.command("switchdb") & filters.chat(TARGET_GROUP_ID) & filters.user([7508462500, 1710597756, 6895497681, 7435756663, 6523029979]))
async def switch_database(_, message: Message):
    """Switch between Vegeta and Goku databases with error handling."""
    global current_db, current_db_name, player_cache

    try:
        new_db_name = message.text.split(maxsplit=1)[1].strip().lower() if len(message.text.split()) > 1 else ""
        
        if new_db_name == "vegeta":
            current_db = db_vegeta
            current_db_name = "Vegeta"
        elif new_db_name == "goku":
            current_db = db_goku
            current_db_name = "Goku"
        else:
            await message.reply("⚠ Invalid database! Use: `/switchdb vegeta` or `/switchdb goku`")
            return

        # Clear cache and reload with new database
        player_cache.clear()
        preload_players()
        
        await message.reply(f"✅ Switched to **{current_db_name}** database with {len(player_cache)} players loaded.")
    except Exception as e:
        logging.error(f"Error switching database: {e}")
        await message.reply(f"❌ Error switching database: {str(e)}")

@bot.on_message(filters.command("startcollect") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def start_collect(_, message: Message):
    global collect_running
    try:
        if not collect_running:
            collect_running = True
            reply_msg = await message.reply(f"✅ Collect function started in {current_db_name} mode!")
            logging.info(f"Collect function started in {message.chat.title}")
        else:
            reply_msg = await message.reply("⚠ Collect function is already running!")

        # Auto-delete after delay for cleaner chat
        await asyncio.sleep(5)
        await message.delete()
        await reply_msg.delete()
    except Exception as e:
        logging.error(f"Error in start_collect: {e}")

@bot.on_message(filters.command("stopcollect") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def stop_collect(_, message: Message):
    global collect_running
    try:
        collect_running = False
        reply_msg = await message.reply("🛑 Collect function stopped!")
        logging.info(f"Collect function stopped in {message.chat.title}")

        # Auto-delete after delay
        await asyncio.sleep(5)
        await message.delete()
        await reply_msg.delete()
    except Exception as e:
        logging.error(f"Error in stop_collect: {e}")

@bot.on_message(filters.command("startmain") & filters.user(ADMIN_USER_IDS))
async def start_main_collect(_, message: Message):
    """Starts the main collect function for MAIN_GROUP_ID."""
    global collect_main_running
    try:
        if not collect_main_running:
            collect_main_running = True
            reply_msg = await message.reply("✅ Main collect function started!")
            logging.info("Main collect function started for MAIN_GROUP_ID.")
        else:
            reply_msg = await message.reply("⚠ Main collect function is already running!")
            
        # Auto-delete after delay
        await asyncio.sleep(5)
        await message.delete()
        await reply_msg.delete()
    except Exception as e:
        logging.error(f"Error in start_main_collect: {e}")

@bot.on_message(filters.command("stopmain") & filters.user(ADMIN_USER_IDS))
async def stop_main_collect(_, message: Message):
    """Stops the main collect function for MAIN_GROUP_ID."""
    global collect_main_running
    try:
        if collect_main_running:
            collect_main_running = False
            reply_msg = await message.reply("🛑 Main collect function stopped!")
            logging.info("Main collect function stopped for MAIN_GROUP_ID.")
        else:
            reply_msg = await message.reply("⚠ Main collect function is not running!")
            
        # Auto-delete after delay
        await asyncio.sleep(5)
        await message.delete()
        await reply_msg.delete()
    except Exception as e:
        logging.error(f"Error in stop_main_collect: {e}")

@bot.on_message(filters.command("colstats") & filters.user(ADMIN_USER_IDS))
async def status_command(_, message: Message):
    """Shows the current status of the collector bot."""
    uptime_seconds = int(time.time() - start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    status_text = (
        f"📊 **Collector Status**\n\n"
        f"📂 **Active Database:** `{current_db_name}`\n"
        f"🔢 **Cached Players:** `{len(player_cache)}`\n"
        f"⏱ **Uptime:** `{hours}h {minutes}m {seconds}s`\n"
        f"🎯 **Target Group:** `{TARGET_GROUP_ID}` - {'✅ Active' if collect_running else '❌ Inactive'}\n"
        f"🏠 **Main Group:** `{MAIN_GROUP_ID}` - {'✅ Active' if collect_main_running else '❌ Inactive'}\n"
    )
    
    await message.reply(status_text)

@bot.on_message(filters.command("colrefresh") & filters.user(ADMIN_USER_IDS))
async def refresh_cache(_, message: Message):
    """Refresh the player cache from the database."""
    try:
        status_msg = await message.reply("🔄 Refreshing player cache...")
        player_cache.clear()
        preload_players()
        await status_msg.edit(f"✅ Cache refreshed! Loaded {len(player_cache)} players from {current_db_name}.")
    except Exception as e:
        logging.error(f"Error refreshing cache: {e}")
        await message.reply(f"❌ Error refreshing cache: {str(e)}")

async def handle_flood_wait(e: FloodWait) -> None:
    """Handle flood wait with exponential backoff."""
    wait_time = e.value + random.uniform(1, 5)
    logging.warning(f"Rate limit hit! Waiting for {wait_time:.2f} seconds...")
    await asyncio.sleep(wait_time)

@bot.on_message((filters.photo | filters.video) & filters.chat([TARGET_GROUP_ID, MAIN_GROUP_ID]) & filters.user(COLLECTOR_USER_IDS))
async def handle_media(c: Client, m: Message):
    """Enhanced handler for image and video messages to collect players."""
    # Check if collection is enabled for this chat
    if (m.chat.id == TARGET_GROUP_ID and not collect_running) or \
       (m.chat.id == MAIN_GROUP_ID and not collect_main_running):
        return

    try:
        # Add slight randomization to appear more human-like
        await asyncio.sleep(random.uniform(0.8, 2.2))

        if not m.caption:
            return

        logging.debug(f"Received caption: {m.caption}")

        # Check if caption matches any target captions
        if m.caption.strip() not in TARGET_CAPTIONS:
            return

        # Get file ID for photo or video
        if m.photo:
            file_id = m.photo.file_unique_id
            file_type = "photo"
        elif m.video:
            file_id = m.video.file_unique_id
            file_type = "video"
        else:
            return

        # Rate limiting check
        chat_id = str(m.chat.id)
        current_time = time.time()
        if chat_id in last_collect_time and current_time - last_collect_time[chat_id] < COLLECT_COOLDOWN:
            logging.debug(f"Skipping collect due to cooldown in {m.chat.title}")
            return
        
        last_collect_time[chat_id] = current_time

        # Check cache first, then database if needed
        cached_data = player_cache.get(file_id)
        if cached_data:
            player_name = cached_data['name']
            logging.info(f"Cache hit for {file_id} - {player_name}")
        else:
            try:
                file_data = current_db.get(file_id)
                if file_data and isinstance(file_data, dict) and 'name' in file_data:
                    player_name = file_data['name']
                    player_cache.set(file_id, file_data)  # Update cache
                    logging.info(f"Database lookup success for {file_id} - {player_name}")
                else:
                    logging.warning(f"Media ID {file_id} ({file_type}) not found in {current_db_name}!")
                    return
            except Exception as e:
                logging.error(f"Database lookup failed for {file_id}: {e}")
                return

        logging.info(f"Collecting player: {player_name} from {current_db_name} in {m.chat.title}")
        
        # Add slight delay for human-like behavior
        await asyncio.sleep(random.uniform(0.5, 1.5))
        
        # Send collection command with error handling
        try:
            sent_message = await bot.send_message(m.chat.id, f"/collect {player_name}")
            
            # Wait for bot's reply with timeout
            start_check_time = time.time()
            max_wait_time = 10  # Maximum seconds to wait for a reply
            reply_found = False
            
            # Smart wait strategy
            while time.time() - start_check_time < max_wait_time and not reply_found:
                await asyncio.sleep(1)  # Check every second
                
                async for reply in bot.get_chat_history(m.chat.id, limit=10):
                    if (reply.reply_to_message and 
                        reply.reply_to_message.message_id == sent_message.message_id):
                        reply_found = True
                        
                        if should_forward_message(reply.text):
                            try:
                                await reply.forward(FORWARD_CHANNEL_ID)
                                logging.info(f"Forwarded rare find: {player_name}")
                            except Exception as e:
                                logging.error(f"Failed to forward message: {e}")
                        break
                
                if reply_found:
                    break
            
            if not reply_found:
                logging.warning(f"No reply received for {player_name} collection after {max_wait_time}s")
                
        except FloodWait as e:
            await handle_flood_wait(e)
        except Exception as e:
            logging.error(f"Error sending collect command: {e}")

    except FloodWait as e:
        await handle_flood_wait(e)
    except Exception as e:
        logging.error(f"Error processing message: {e}")

@bot.on_message(filters.chat([TARGET_GROUP_ID, FORWARD_CHANNEL_ID]))
async def check_rarity_and_forward(_, message: Message):
    """Check messages for rare finds and forward them."""
    if not message.text:
        return  

    if "✅ Look You Collected A " in message.text:
        logging.info(f"Checking message for rarity")

        for rarity in RARITIES_TO_FORWARD:
            if f"Rarity : {rarity}" in message.text:
                logging.info(f"Detected {rarity} item! Forwarding...")
                try:
                    await bot.send_message(FORWARD_CHANNEL_ID, message.text)
                    # Add slight delay after forwarding
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logging.error(f"Failed to forward rare item: {e}")
                break

@bot.on_message(filters.command("fileid") & filters.group)
async def extract_file_id(_, message: Message):
    """Extracts and sends the unique file ID of a replied photo or video."""
    try:
        if not message.reply_to_message:
            await message.reply("⚠ Please reply to a **photo or video** to extract the file ID.")
            return

        if message.reply_to_message.photo:
            file_unique_id = message.reply_to_message.photo.file_unique_id
            file_id = message.reply_to_message.photo.file_id
            file_type = "Photo"
        elif message.reply_to_message.video:
            file_unique_id = message.reply_to_message.video.file_unique_id
            file_id = message.reply_to_message.video.file_id
            file_type = "Video"
        else:
            await message.reply("⚠ Please reply to a **photo or video** to extract the file ID.")
            return

        # Check if this file is in our database
        in_database = "No"
        in_cache = "No"
        
        if player_cache.get(file_unique_id):
            in_cache = "Yes"
            player_name = player_cache.get(file_unique_id)['name']
        else:
            file_data = current_db.get(file_unique_id)
            if file_data:
                in_database = "Yes"
                player_name = file_data.get('name', 'Unknown')
                # Add to cache for future reference
                player_cache.set(file_unique_id, file_data)
            else:
                player_name = "Not found"

        response = (
            f"📂 **{file_type} Details:**\n\n"
            f"**Unique ID:** `{file_unique_id}`\n"
            f"**In Database:** `{in_database}`\n"
            f"**In Cache:** `{in_cache}`\n"
            f"**Player Name:** `{player_name}`"
        )
        
        await message.reply(response)
    except Exception as e:
        logging.error(f"Error in /fileid command: {e}")
        await message.reply("❌ An error occurred while processing this request.")

@bot.on_message(filters.command("addplayer") & filters.user(ADMIN_USER_IDS))
async def add_player(_, message: Message):
    """Add a player to the current database."""
    try:
        # Check if replying to a media message
        if not message.reply_to_message or (not message.reply_to_message.photo and not message.reply_to_message.video):
            await message.reply("⚠ Please reply to a photo or video with `/addplayer Player Name`")
            return
            
        # Get the command parts
        parts = message.text.split(" ", 1)
        if len(parts) < 2:
            await message.reply("⚠ Please provide a player name: `/addplayer Player Name`")
            return
            
        player_name = parts[1].strip()
        
        # Get file ID
        if message.reply_to_message.photo:
            file_id = message.reply_to_message.photo.file_unique_id
            file_type = "photo"
        else:
            file_id = message.reply_to_message.video.file_unique_id
            file_type = "video"
            
        # Add to database
        current_db.set(file_id, {"name": player_name, "type": file_type})
        
        # Add to cache
        player_cache.set(file_id, {"name": player_name, "type": file_type})
        
        await message.reply(f"✅ Added `{player_name}` to {current_db_name} database!")
        logging.info(f"Added player {player_name} ({file_id}) to {current_db_name} database")
        
    except Exception as e:
        logging.error(f"Error adding player: {e}")
        await message.reply(f"❌ Error adding player: {str(e)}")

@bot.on_message(filters.command("colhelp") & filters.user(ADMIN_USER_IDS))
async def help_command(_, message: Message):
    """Display available commands and their usage."""
    help_text = """
📚 **Available Commands**

**Collection Controls:**
• `/startcollect` - Start auto-collection in target group
• `/stopcollect` - Stop auto-collection in target group
• `/startmain` - Start auto-collection in main group
• `/stopmain` - Stop auto-collection in main group

**Database Management:**
• `/switchdb vegeta|goku` - Switch active database
• `/refresh` - Refresh player cache from database
• `/addplayer Player Name` - Add a player to database (reply to media)

**Information:**
• `/status` - Show collector status and statistics
• `/fileid` - Show file ID and database info (reply to media)
• `/help` - Show this help message

ℹ️ Collection is currently running in **{TARGET_GROUP_ID}**: {'✅' if collect_running else '❌'}
ℹ️ Collection is currently running in **{MAIN_GROUP_ID}**: {'✅' if collect_main_running else '❌'}
📊 Current database: **{current_db_name}** with **{len(player_cache)}** players in cache
"""
    await message.reply(help_text)

async def main():
    """Runs Pyrogram client and Flask server concurrently with error handling."""
    global start_time
    
    # Set start time for uptime tracking
    start_time = time.time()
    
    try:
        # Preload players from database
        preload_players()
        
        # Start the client
        await bot.start()
        logging.info(f"Client started successfully!")
        logging.info(f"Loaded {len(player_cache)} players from {current_db_name}")
        
        # Run Flask server and idle together
        await asyncio.gather(run_flask(), idle())
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
    finally:
        # Ensure clean shutdown
        await bot.stop()
        logging.info("Client stopped. Exiting...")

if __name__ == "__main__":
    # Use uvloop for improved performance if available
    try:
        import uvloop
        uvloop.install()
        logging.info("Using uvloop for improved performance")
    except ImportError:
        logging.info("uvloop not available, using standard event loop")
    
    # Run the main function
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
