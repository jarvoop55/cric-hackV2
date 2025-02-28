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
    level=logging.INFO
)

# Initialize Databases
storage_vegeta = Mukund("Vegeta")
storage_goku = Mukund("Goku")

db_vegeta = storage_vegeta.database("players")
db_goku = storage_goku.database("players")

# Track active database
current_db = db_vegeta  # Default database
current_db_name = "Vegeta"  # Track the name for response messages

# In-memory cache for quick lookups
player_cache = {}

def preload_players():
    """Load players into cache from the active database."""
    global player_cache
    logging.info(f"🔄 Preloading players from {current_db_name}...")
    try:
        all_players = current_db.all()
        if isinstance(all_players, dict):
            player_cache = all_players
            logging.info(f"✅ Loaded {len(player_cache)} players from {current_db_name}.")
        else:
            logging.error("⚠ Database returned unexpected data format!")
    except Exception as e:
        logging.error(f"❌ Failed to preload database: {e}")

# Flask health check
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

# Environment variables
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION")

assert API_ID, "Missing API_ID!"
assert API_HASH, "Missing API_HASH!"
assert SESSION_STRING, "Missing SESSION!"

bot = Client(
    "pro",
    api_id=int(API_ID),
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=20,
    max_concurrent_transmissions=10
)

TARGET_GROUP_ID = -1002395952299
collect_running = False

@bot.on_message(filters.command("startcollect") & filters.chat(TARGET_GROUP_ID) & filters.user([7508462500, 1710597756, 6895497681, 7435756663]))
async def start_collect(_, message: Message):
    global collect_running
    if not collect_running:
        collect_running = True
        await message.reply(f"✅ Collection started using `{current_db_name}` database!")
    else:
        await message.reply("⚠ Collection is already running!")

@bot.on_message(filters.command("stopcollect") & filters.chat(TARGET_GROUP_ID) & filters.user([7508462500, 1710597756, 6895497681, 7435756663]))
async def stop_collect(_, message: Message):
    global collect_running
    collect_running = False
    await message.reply("🛑 Collection stopped!")

async def human_like_typing(message: str, chat_id):
    """Simulates human-like typing by sending characters with random delay."""
    typed_text = ""
    for char in message:
        typed_text += char
        await asyncio.sleep(random.uniform(0.05, 0.1))  # Typing delay per character

    await bot.send_message(chat_id, typed_text)

@bot.on_message(filters.photo & filters.chat(TARGET_GROUP_ID) & filters.user([7522153272, 7946198415, 7742832624, 1710597756, 7828242164, 7957490622]))
async def hacke(c: Client, m: Message):
    """Handles image messages and collects OG players."""
    global collect_running
    if not collect_running:
        return

    try:
        # Simulate thinking time before reacting
        await asyncio.sleep(random.uniform(0.3, 0.5))

        if not m.caption:
            return  # Ignore messages without captions

        logging.debug(f"📩 Received caption: {m.caption}")

        if "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴘʟᴀʏᴇʀ" not in m.caption:
            return  # Ignore non-player messages

        file_id = m.photo.file_unique_id

        # Use cache for quick lookup
        if file_id in player_cache:
            player_name = player_cache[file_id]['name']
        else:
            file_data = current_db.get(file_id)  # Query database only if not in cache
            if file_data:
                player_name = file_data['name']
                player_cache[file_id] = file_data  # Cache result
            else:
                logging.warning(f"⚠ Image ID {file_id} not found in {current_db_name}!")
                return

        logging.info(f"🛠 Collecting player: {player_name} from {current_db_name}")

        # Simulating typing instead of instant message sending
        await human_like_typing(f"/collect {player_name}", m.chat.id)

        # Random cooldowns to avoid looking like a bot
        if random.random() < 0.1:  # 10% chance to take a longer break
            cooldown_time = random.uniform(5, 8)
            logging.info(f"🛑 Taking a cooldown break for {cooldown_time:.1f} seconds...")
            await asyncio.sleep(cooldown_time)

    except FloodWait as e:
        wait_time = e.value + random.randint(1, 5)
        logging.warning(f"🚨 Rate limit hit! Waiting for {wait_time} seconds...")
        await asyncio.sleep(wait_time)
    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")

@bot.on_message(filters.command("fileid") & filters.chat(TARGET_GROUP_ID) & filters.reply & filters.user([7508462500, 1710597756, 6895497681, 7435756663]))
async def extract_file_id(_, message: Message):
    """Extracts and sends the unique file ID of a replied photo."""
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.reply("⚠ Please reply to a photo to extract the file ID.")
        return
    
    file_unique_id = message.reply_to_message.photo.file_unique_id
    await human_like_typing(f"📂 **File Unique ID:** `{file_unique_id}`", message.chat.id)

async def main():
    """Runs Pyrogram bot and Flask server concurrently."""
    preload_players()  # Load players into memory before starting
    await bot.start()
    logging.info("🤖 Bot started successfully!")
    await asyncio.gather(run_flask(), idle())
    await bot.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
