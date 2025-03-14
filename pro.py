import os
import logging
import asyncio
import random
import datetime
from collections import defaultdict

from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from flask import Flask
from hypercorn.asyncio import serve
from hypercorn.config import Config

from Mukund import Mukund

# ---------- CONFIGURATION & CONSTANTS ----------
API_ID = 20061115
API_HASH = "c30d56d90d59b3efc7954013c580e076"
SESSION_STRING = "BQEyG7sAiPlaKtmI-9rpx9Y2gFctOWa7B5Opxw73qv-XTHpWVkPyMeSe_foRrkpeGv9St8fdqKFNVss7gzNa3q-R5Q8RRGp64OY2DO9D79ZPPt6X5YbzQSRsnIofb_lmkS0Z6N9Uk-9N93vszna0Q5mENBfamCKWhr6CtvS7eeSVw2Kt0AyhAqBphjwZi84UyqpuezZIOEQt7YjoAPKBfPsuMwIrdpo1rahirjTRJZ6ExK2BJTh-u7oTm5n1uyGCKbEeno9fIYRSCt6Q8NKV3O0FRgqw30W_dVCK_fqo3LsJrxJ0F8vVwIO7aKfVNsrXb-NZs-R_njMrEkckGSbm04B_4qNG2gAAAAGEzYnbAA"

# Group and Channel IDs
TARGET_GROUP_ID = -1002348881334
MAIN_GROUP_ID = -1002499388382
FORWARD_CHANNEL_ID = -1002264265999

# Control Flags
collect_running = False
collect_main_running = False
propose_running = False

# User Permissions
ADMIN_USER_IDS = [7859049019, 7508462500, 1710597756, 6895497681, 7435756663, 6523029979]
COLLECTOR_USER_IDS = [
    7522153272, 7946198415, 7742832624, 7859049019,
    1710597756, 7828242164, 7957490622
]

RARITIES_TO_FORWARD = ["Cosmic", "Limited Edition", "Exclusive", "Ultimate"]

# Logging Configuration
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

# Initialize Databases
storage_vegeta = Mukund("Vegeta")
storage_goku = Mukund("Goku")

db_vegeta = storage_vegeta.database("players")
db_goku = storage_goku.database("players")

current_db = db_vegeta
current_db_name = "Vegeta"

# Caching players
player_cache = {}

# Telemetry Stats
telemetry_stats = {
    "total_collected": 0,
    "total_skipped": 0,
    "duplicate_skipped": 0,
    "failed_collections": 0,
    "hourly_stats": defaultdict(int),
    "daily_stats": defaultdict(int),
}

# ---------- BOT INITIALIZATION ----------
bot = Client(
    "pro",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=20,
    max_concurrent_transmissions=10,
)

# ---------- HELPER FUNCTIONS ----------
def preload_players():
    """Load players into cache from the active database."""
    global player_cache
    logging.info(f"Preloading players from {current_db_name}...")
    try:
        all_players = current_db.all()
        if isinstance(all_players, dict):
            player_cache = all_players
            logging.info(f"Loaded {len(player_cache)} players.")
        else:
            logging.error("Database returned unexpected data format!")
    except Exception as e:
        logging.error(f"Failed to preload database: {e}")


# ---------- FLASK HEALTH CHECK ----------
web_app = Flask(__name__)

@web_app.route('/health')
def health_check():
    return "OK", 200

async def run_flask():
    config = Config()
    config.bind = ["0.0.0.0:8000"]
    await serve(web_app, config)

# ---------- BOT COMMANDS ----------
@bot.on_message(filters.command("switchdb") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def switch_database(_, message: Message):
    """Switch between Vegeta and Goku databases."""
    global current_db, current_db_name, player_cache

    args = message.text.split()
    if len(args) < 2:
        await message.reply("⚠ Use: `/switchdb vegeta` or `/switchdb goku`")
        return

    new_db_name = args[1].lower()
    if new_db_name == "goku":
        current_db = db_goku
        current_db_name = "Goku"
    elif new_db_name == "vegeta":
        current_db = db_vegeta
        current_db_name = "Vegeta"
    else:
        await message.reply("⚠ Invalid database selection!")
        return

    preload_players()
    await message.reply(f"✅ Switched to **{current_db_name}** database.")


@bot.on_message(filters.command("startcollect") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def start_collect(_, message: Message):
    global collect_running
    collect_running = True
    await message.reply("✅ Collect function started!")


@bot.on_message(filters.command("stopcollect") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def stop_collect(_, message: Message):
    global collect_running
    collect_running = False
    await message.reply("🛑 Collect function stopped!")


@bot.on_message(filters.photo & filters.chat(TARGET_GROUP_ID) & filters.user(COLLECTOR_USER_IDS))
async def handle_photo_message(c: Client, m: Message):
    """Handles image messages and collects OG players."""
    global collect_running
    if not collect_running:
        return

    if not m.caption or m.caption.strip() != "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴘʟᴀʏᴇʀ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ ᴜꜱɪɴɢ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ":
        telemetry_stats["total_skipped"] += 1
        return

    file_id = m.photo.file_unique_id
    player_name = player_cache.get(file_id, {}).get("name") or current_db.get(file_id, {}).get("name")

    if not player_name:
        logging.warning(f"Image ID {file_id} not found in {current_db_name}!")
        telemetry_stats["total_skipped"] += 1
        return

    await bot.send_message(m.chat.id, f"/collect {player_name}")
    telemetry_stats["total_collected"] += 1


@bot.on_message(filters.chat(TARGET_GROUP_ID))
async def check_rarity_and_forward(_, message: Message):
    """Checks messages for specific rarities and forwards them."""
    if not message.text:
        return

    for rarity in RARITIES_TO_FORWARD:
        if f"Rarity : {rarity}" in message.text:
            await bot.send_message(FORWARD_CHANNEL_ID, message.text)
            break


@bot.on_message(filters.command("stats") & filters.chat(TARGET_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def show_stats(c: Client, m: Message):
    """Shows bot performance telemetry stats."""
    hour = datetime.datetime.now().strftime("%Y-%m-%d %H:00")
    day = datetime.datetime.now().strftime("%Y-%m-%d")

    stats_report = (
        f"📊 **Bot Telemetry Report**\n"
        f"🔹 **Total Players Collected:** {telemetry_stats['total_collected']}\n"
        f"🔹 **Total Players Skipped:** {telemetry_stats['total_skipped']}\n"
        f"📅 **Today's Stats ({day}):** {telemetry_stats['daily_stats'][day]} collected\n"
        f"⏳ **Last Hour ({hour}):** {telemetry_stats['hourly_stats'][hour]} collected\n"
    )

    await m.reply_text(stats_report)


# ---------- MAIN FUNCTION ----------
async def main():
    preload_players()  # Load players before starting
    await bot.start()
    logging.info("Bot started successfully!")
    await asyncio.gather(run_flask(), idle())
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
