import os
import logging
import asyncio
import random
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait
from pyrogram.types import Message
from Mukund import Mukund
from flask import Flask

# Configure Logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO  # Change to logging.DEBUG for more detailed logs
)

# Initialize Database
storage = Mukund("Vegeta")
db = storage.database("celebs")

# In-memory cache for quick lookups
player_cache = {}

# Preload players from the database at startup
def preload_players():
    global player_cache
    logging.info("Preloading player database into cache...")
    try:
        all_players = db.all()
        if isinstance(all_players, dict):
            player_cache = all_players
            logging.info(f"Loaded {len(player_cache)} players into cache.")
        else:
            logging.error("Database returned unexpected data format!")
    except Exception as e:
        logging.error(f"Failed to preload database: {e}")

# Create Flask app for health check
web_app = Flask(__name__)

@web_app.route('/health')
def health_check():
    return "OK", 200

async def run_flask():
    """Runs Flask server for health checks."""
    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    config = Config()
    config.bind = ["0.0.0.0:8000"]
    await serve(web_app, config)

# Bot Credentials
API_ID = 25798238  # Replace with your actual API ID
API_HASH = "c87b9a11c6307857687cbfe0ee818755"  # Replace with your actual API Hash
SESSION_STRING = "BQGJpl4AUxPSt8MNPzq9Qh98aAF8NF4VAfCdvrvRBtbb4BVmn2ApB6rqMacc2k5Vdo3545SZu5vGUG3CG9lcw041x1Z8aDEzRXmr8W-IoeB_Hxb5oqYXATh_GoTm0od9-17KowlJ2135jy9pH0pL1zNCwSQmDh1XpVDneqimUD1U-N3MpAwsOs5ggBxg-6nFSOq-S4dqAeZRpRmV7aNpU3iAGt2m0oUCg3g9tWuNTRqDD34FhVdUlzYfHaeG1wJNf2RO7LmlYpG1OnbhdCDgj504UxI-8cmMobdFi74-gRFAj7WQ4okYKvCeh2pgCrIVYce6UkRWAeWZw6UokfOJruSGYY2tcAAAAAHbxqSHAA"  # Replace with your actual Session String

# Initialize Pyrogram bot
bot = Client(
    "pro",
    api_id=int(API_ID),
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=20,
    max_concurrent_transmissions=10
)

# Define Group IDs
TARGET_GROUP_ID = -1002395952299  # Original target group
MAIN_GROUP_ID = -1002499388382 # Main group for /startmain command
FORWARD_CHANNEL_ID = -1002254491223  # Forwarding channel (disabled)

# Control flags for collect functions
collect_running = False  # For /startcollect command in TARGET_GROUP_ID
collect_main_running = False  # For /startmain command in MAIN_GROUP_ID

# Admin User IDs (replace with actual admin IDs)
ADMIN_USER_IDS = [7859049019, 7508462500, 1710597756, 6895497681, 7435756663]

# User IDs permitted to trigger the collect function
COLLECTOR_USER_IDS = [
    7522153272, 7946198415, 7742832624, 7859049019,
    1710597756, 7828242164, 7957490622
]

# Preload players at startup
preload_players()

# Start/Stop Collect Commands for TARGET_GROUP_ID
@bot.on_message(filters.command("startcollect") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def start_collect(_, message: Message):
    global collect_running
    if not collect_running:
        collect_running = True
        await message.reply("✅ Collect function started!")
        logging.info("Collect function started in TARGET_GROUP_ID.")
    else:
        await message.reply("⚠ Collect function is already running!")
        logging.info("Collect function already running in TARGET_GROUP_ID.")

@bot.on_message(filters.command("stopcollect") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def stop_collect(_, message: Message):
    global collect_running
    if collect_running:
        collect_running = False
        await message.reply("🛑 Collect function stopped!")
        logging.info("Collect function stopped in TARGET_GROUP_ID.")
    else:
        await message.reply("⚠ Collect function is not running!")
        logging.info("Collect function was not running in TARGET_GROUP_ID.")

# Start/Stop Collect Commands for MAIN_GROUP_ID
@bot.on_message(filters.command("startmain") & filters.user(ADMIN_USER_IDS))
async def start_main_collect(_, message: Message):
    """Starts the main collect function but only affects MAIN_GROUP_ID."""
    global collect_main_running
    if not collect_main_running:
        collect_main_running = True
        await message.reply("✅ Main collect function started!")
        logging.info("Main collect function started in MAIN_GROUP_ID.")
    else:
        await message.reply("⚠ Main collect function is already running!")


@bot.on_message(filters.command("stopmain") & filters.user(ADMIN_USER_IDS))
async def stop_main_collect(_, message: Message):
    """Stops the main collect function but only affects MAIN_GROUP_ID."""
    global collect_main_running
    if collect_main_running:
        collect_main_running = False
        await message.reply("🛑 Main collect function stopped!")
        logging.info("Main collect function stopped in MAIN_GROUP_ID.")
    else:
        await message.reply("⚠ Main collect function is not running!")
# Collect Celebrity Function
@bot.on_message(
    filters.photo &
    filters.chat([TARGET_GROUP_ID, MAIN_GROUP_ID]) &
    filters.user(COLLECTOR_USER_IDS)
)
async def collect_celebrity(c: Client, m: Message):
    global collect_running, collect_main_running

    # Determine which group the message is from and check if collecting is running
    if m.chat.id == TARGET_GROUP_ID:
        if not collect_running:
            return
    elif m.chat.id == MAIN_GROUP_ID:
        if not collect_main_running:
            return
    else:
        return  # Ignore messages from other groups

    try:
        await asyncio.sleep(random.uniform(1.0, 2.0))

        if not m.caption:
            return

        logging.debug(f"Received caption: {m.caption}")

        # Check for the exact caption
        target_caption = "❄️ ʟᴏᴏᴋ ᴀɴ ᴀᴡsᴏᴍᴇ ᴄᴇʟᴇʙʀɪᴛʏ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜᴇʀ/ʜɪᴍ ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ"

        if m.caption.strip() != target_caption:
            return

        file_id = m.photo.file_unique_id

        # Use cache for quick lookup
        if file_id in player_cache:
            player_name = player_cache[file_id]['name']
        else:
            file_data = db.get(file_id)
            if file_data:
                player_name = file_data['name']
                player_cache[file_id] = file_data
            else:
                logging.warning(f"Image ID {file_id} not found in DB!")
                return

        logging.info(f"Collecting celebrity: {player_name}")
        await bot.send_message(m.chat.id, f"/collect {player_name}")

    except FloodWait as e:
        wait_time = e.value + random.randint(1, 5)
        logging.warning(f"Rate limit hit! Waiting for {wait_time} seconds...")
        await asyncio.sleep(wait_time)
    except Exception as e:
        logging.error(f"Error processing message: {e}")

# Extract File ID Command
@bot.on_message(filters.command("fileid") & filters.reply & filters.user(ADMIN_USER_IDS))
async def extract_file_id(_, message: Message):
    """Extracts and sends the unique file ID of a replied photo."""
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.reply("⚠ Please reply to a photo to extract the file ID.")
        return

    file_unique_id = message.reply_to_message.photo.file_unique_id
    await message.reply(f"📂 **File Unique ID:** `{file_unique_id}`")

# Main function to run the bot and Flask server concurrently
async def main():
    """Runs Pyrogram bot and Flask server concurrently."""
    await bot.start()
    logging.info("Bot started successfully!")
    await asyncio.gather(run_flask(), idle())
    await bot.stop()

if __name__ == "__main__":
    # Use get_event_loop instead of asyncio.run to avoid event loop conflicts
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
