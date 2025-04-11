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
    logging.info(f"Preloading players from {current_db_name}...")
    try:
        all_players = current_db.all()
        if isinstance(all_players, dict):
            player_cache = all_players
            logging.info(f"Loaded {len(player_cache)} players from {current_db_name}.")
        else:
            logging.error("Database returned unexpected data format!")
    except Exception as e:
        logging.error(f"Failed to preload database: {e}")

# Flask health check
web_app = Flask(__name__)

@web_app.route('/health')
def health_check():
    return "OK", 200

async def run_flask():
    """ Runs Flask server for health checks """
    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    config = Config()
    config.bind = ["0.0.0.0:8000"]
    await serve(web_app, config)

# Environment variables
API_ID = 20061115
API_HASH = "c30d56d90d59b3efc7954013c580e076"
SESSION_STRING = "BQA4ntkAdv7Yn7dZ2dN67mPWA6EFx5_eCStgr205gdPHAe3pMkWrgZt11XIpdOpTvVUMEdbRA2em8es5klLLkZ0PgeU1k9F4wcxRLzCUnmvW-0F2fq3NODO7GP-Pgxyfxw1YZGTtHzHQ4za6GHeIsAnGQpGRdmpl6PgIZGVBomKoZpYr2U446urNs-pqwPtXW5GbfUXTOAnAg0blpP3f5MC6eyI4uTN05Vhdhc0IRdoYU8i8oO6eBzAWxHF4au4B-OuCgH1FxIFOmmoDhQcKPlvMHL_SoddADZgKGkqbVTjjQt91rNZUdvbgOpYAye1mEA3ZIRUFdHArO4QLVp9Ta89zjppWRQAAAAGEzYnbAA"



bot = Client(
    "pro",
    api_id=int(API_ID),
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=20,
    max_concurrent_transmissions=10
)

RARITIES_TO_FORWARD = ["Cosmic", "Limited Edition", "Exclusive", "Ultimate"]
TARGET_GROUP_ID = -1002348881334  # Original target group
MAIN_GROUP_ID = [-1002404629452,
    -1002510720624,
    -1002482984360,
    -1002607746947,
    -1002348881334] # Main group for /startmain command
FORWARD_CHANNEL_ID =  -1002260368357  # Forwarding channel (disabled)
# Control flags for collect functions
collect_running = False
# For /startcollect command in TARGET_GROUP_ID
collect_main_running = False  # For /startmain command in MAIN_GROUP_ID
# Admin User IDs (replace with actual admin IDs)
ADMIN_USER_IDS = [1745451559, 1710597756, 7522153272, 7946198415, 7742832624, 7859049019, 7828242164, 7957490622, 6523029979]
# User IDs permitted to trigger the collect function
COLLECTOR_USER_IDS = [
    7522153272, 7946198415, 7742832624, 7859049019, 1710597756, 7828242164, 7957490622, 7957490622, 7509527964]




@bot.on_message(filters.command("switchdb") & filters.chat(TARGET_GROUP_ID) & filters.user([7508462500, 1710597756, 6895497681, 7435756663, 6523029979]))
async def switch_database(_, message: Message):
    """Switch between Vegeta and Goku databases."""
    global current_db, current_db_name, player_cache

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

    preload_players()  # Reload cache with new database
    await message.reply(f"✅ Switched to **{current_db_name}** database.")


@bot.on_message(filters.command("startcollect") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def start_collect(_, message: Message):
    global collect_running, spam_task
    if not collect_running:
        collect_running = True
          # Start forward spam
        reply_msg = await message.reply(f"✅ Collect function started!")
    else:
        reply_msg = await message.reply("⚠ Collect function is already running!")

    await asyncio.sleep(2)
    await reply_msg.delete()

@bot.on_message(filters.command("stopcollect") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def stop_collect(_, message: Message):
    global collect_running, spam_task
    collect_running = False
    reply_msg = await message.reply("🛑 Collect function stopped!")

    await asyncio.sleep(2)
    await reply_msg.delete()

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


@bot.on_message(filters.photo | filters.video & filters.chat(TARGET_GROUP_ID) & filters.user([7522153272, 7946198415, 7742832624, 1710597756, 7828242164, 7957490622, 8152092974, 7509527964, 8079928714]))
async def hacke(c: Client, m: Message):
    """Handles image and video messages and collects OG players, athletes, and celebrities."""
    global collect_running, collect_main_running
    if not collect_running:
        return
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

        # Define target captions
        target_captions = [
            "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴘʟᴀʏᴇʀ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ/Her ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ",
            "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴀᴛʜʟᴇᴛᴇ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ/Her ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ",
            "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴄᴇʟᴇʙʀɪᴛʏ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ/Her ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ",
            "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴘʟᴀʏᴇʀ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ"
        ]

        if m.caption.strip() not in target_captions:
            return

        # Get file ID for photo or video
        if m.photo:
            file_id = m.photo.file_unique_id
        elif m.video:
            file_id = m.video.file_unique_id
        else:
            return

        # Use cache for quick lookup
        if file_id in player_cache:
            player_name = player_cache[file_id]['name']
        else:
            file_data = current_db.get(file_id)  # Query database only if not in cache
            if file_data:
                player_name = file_data['name']
                player_cache[file_id] = file_data  # Cache result
            else:
                logging.warning(f"Media ID {file_id} not found in {current_db_name}!")
                return

        logging.info(f"Collecting player: {player_name} from {current_db_name}")
        sent_message = await bot.send_message(m.chat.id, f"/collect {player_name}")

        # Wait for bot's reply
        await asyncio.sleep(1)

        async for reply in bot.get_chat_history(m.chat.id, limit=5):  # FIXED
            if reply.reply_to_message and reply.reply_to_message.message_id == sent_message.message_id:
                if should_forward_message(reply.text):
                    await reply.forward(FORWARD_CHANNEL_ID)
                    logging.info(f"Forwarded message: {reply.text}")

    except FloodWait as e:
        wait_time = e.value + random.randint(1, 5)
        logging.warning(f"Rate limit hit! Waiting for {wait_time} seconds...")
        await asyncio.sleep(wait_time)
    except Exception as e:
        logging.error(f"Error processing message: {e}")

@bot.on_message(filters.chat(TARGET_GROUP_ID))
async def check_rarity_and_forward(_, message: Message):
    if not message.text:
        return  

    if "✅ Look You Collected A " in message.text:
        logging.info(f"Checking message for rarity:\n{message.text}")

        for rarity in RARITIES_TO_FORWARD:
            if f"Rarity : {rarity}" in message.text:
                logging.info(f"Detected {rarity} celebrity! Forwarding...")
                await bot.send_message(FORWARD_CHANNEL_ID, message.text)
                break  

import logging

@bot.on_message(filters.command("fileid") & filters.group)
async def extract_file_id(_, message: Message):
    """Extracts and sends the unique file ID of a replied photo or video."""
    try:
        if not message.reply_to_message:
            await message.reply("⚠ Please reply to a **photo or video** to extract the file ID.")
            return

        if message.reply_to_message.photo:
            file_unique_id = message.reply_to_message.photo.file_unique_id
            file_type = "Photo"
        elif message.reply_to_message.video:
            file_unique_id = message.reply_to_message.video.file_unique_id
            file_type = "Video"
        else:
            await message.reply("⚠ Please reply to a **photo or video** to extract the file ID.")
            return

        await message.reply(f"📂 **{file_type} Unique ID:** `{file_unique_id}`")
    except Exception as e:
        logging.error(f"Error in /fileid command: {e}")
        await message.reply("❌ An error occurred. Check bot logs.")

async def main():
    """ Runs Pyrogram bot and Flask server concurrently """
    preload_players()  # Load players into memory before starting
    await bot.start()
    logging.info("Bot started successfully!")
    await asyncio.gather(run_flask(), idle())
    await bot.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
