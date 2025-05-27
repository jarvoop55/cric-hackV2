import os
import logging
import asyncio
import random
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait
from pyrogram.types import Message
from flask import Flask
from motor.motor_asyncio import AsyncIOMotorClient
import json

# Configure Logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# MongoDB Configuration
MONGO_URI = "mongodb+srv://swami2006:8Cs0LYC1mPGFC8el@cluster0.iapxnlf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"  # Change this to your MongoDB URI
client = AsyncIOMotorClient(MONGO_URI)
db = client["players_db"]  # Database name
db_goku = db["goku_players"]  # Collection for Goku players
db_vegeta = db["vegeta_players"]  # Collection for Vegeta players

# Track active database
current_db = db_vegeta  # Default database
current_db_name = "Vegeta"  # Track the name for response messages

# In-memory cache for quick lookups
player_cache = {}

async def preload_players():
    """Load players into cache from the active database."""
    global player_cache
    logging.info(f"Preloading players from {current_db_name}...")
    try:
        cursor = current_db.find({})
        player_cache = {}
        async for doc in cursor:
            if "file_id" in doc and "name" in doc:
                player_cache[doc["file_id"]] = {
                    "name": doc["name"]
                }
        logging.info(f"Loaded {len(player_cache)} players from {current_db_name}.")
    except Exception as e:
        logging.error(f"Failed to preload database: {e}")
        player_cache = {}  # Reset cache on error

# Flask health check
web_app = Flask(__name__)

@web_app.route('/health')
def health_check():
    return "OK", 200

# Run Flask in a separate thread
def run_flask_app():
    """Runs Flask server in a separate thread for health checks"""
    web_app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)

# Import threading
import threading

# Environment variables
API_ID = 20061115
API_HASH = "c30d56d90d59b3efc7954013c580e076"
SESSION_STRING = "BQA4ntkAMRaMnCqRqsLaL1CD3Oi5BzKvwhxCwibfERFVIrF3DLO9NldBcmhWbt_OuYHsOayXs9AsogmxR6pYzAl8N5_t_xPCCmJYqM1j0O7wsyhGKKDoPC7ssun92OkWSihjlCALbW8KcHYS9X80spgCieMuilFbRuA1G2iW5Osf01t1NC3iihPtzNbvouTffKA53KTqr0fnrx3rTW3-suVmfknfed3h2QFUPqP_gBLtAA_d5CtLLUAeyF52LEO3bAfXfH-sJ54vretEE5paoezQ7q6rXWob2LtKK9eqgHOb2b_tZ8BsCVLuu_7X-d-OSE4B9NC8emdz4wg3rOvelLIn5RM19wAAAAGEzYnbAA"

bot = Client(
    "pro",
    api_id=int(API_ID),
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=20,
    max_concurrent_transmissions=10
)

RARITIES_TO_FORWARD = ["Cosmic", "Limited Edition", "Exclusive", "Ultimate", "Mythic"]
TARGET_GROUP_IDS = [
    -1002404629452,
    -1002510720624,
    -1002482984360,
    -1002607746947,
    -1002348881334,
    -1002694502602,
    -1002515574434,
    -1002258939999,
    -1002534259807,
] # Target groups
MAIN_GROUP_ID = -1002499388382 # Main group for /startmain command
FORWARD_CHANNEL_ID = -1002260368357 # Forwarding channel

# Track collection status for each group
collection_status = {group_id: False for group_id in TARGET_GROUP_IDS}
collection_status[MAIN_GROUP_ID] = False  # Also track main group

# Admin User IDs
ADMIN_USER_IDS = [1745451559, 1710597756, 7522153272, 7946198415, 7742832624, 7859049019, 7828242164, 7957490622, 7323291282, 6523029979]
# User IDs permitted to trigger the collect function
COLLECTOR_USER_IDS = [
    
    7522153272, 7946198415, 7742832624, 7859049019, 1710597756, 7828242164, 7957490622, 7323291282, 8131155741, 8079928714, 7509527964, 7509527964, 8167692585, 7783859799, 7927201578, 7795661257
]

def should_forward_message(text):
    """Check if a message contains rare celebrity criteria for forwarding"""
    if not text:
        return False
    
    for rarity in RARITIES_TO_FORWARD:
        if f"Rarity : {rarity}" in text:
            return True
    
    return False

@bot.on_message(filters.command("switchdb") & filters.chat(TARGET_GROUP_IDS + [MAIN_GROUP_ID]) & filters.user([7508462500, 1710597756, 6895497681, 7435756663, 7323291282, 6523029979]))
async def switch_database(_, message: Message):
    """Switch between databases."""
    global current_db, current_db_name, player_cache

    new_db_name = message.text.split(maxsplit=1)[1].strip().lower() if len(message.text.split()) > 1 else ""
    
    if new_db_name == "goku":
        current_db = db_goku
        current_db_name = "Goku"
    elif new_db_name == "vegeta":
        current_db = db_vegeta
        current_db_name = "Vegeta"
    else:
        await message.reply("⚠ Invalid database! Use: `/switchdb goku` or `/switchdb vegeta`")
        return

    await preload_players()  # Add await here
    await message.reply(f"✅ Switched to **{current_db_name}** database.")

@bot.on_message(filters.command("startgroup") & filters.user(ADMIN_USER_IDS))
async def start_group_collect(_, message: Message):
    """Start collection in a specific group."""
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.reply("⚠ Usage: `/startgroup [group_id]` or `/startgroup all`")
            return
            
        target = args[1].lower()
        
        if target == "all":
            # Start in all groups
            for group_id in TARGET_GROUP_IDS:
                collection_status[group_id] = True
            await message.reply("✅ Collection started in ALL target groups!")
            logging.info("Collection started in all target groups")
        else:
            # Try to parse as a group ID
            try:
                group_id = int(target)
                if group_id in TARGET_GROUP_IDS or group_id == MAIN_GROUP_ID:
                    collection_status[group_id] = True
                    group_name = "main group" if group_id == MAIN_GROUP_ID else f"group {group_id}"
                    await message.reply(f"✅ Collection started in {group_name}!")
                    logging.info(f"Collection started in {group_name}")
                else:
                    await message.reply("⚠ Group ID not in configured targets!")
            except ValueError:
                await message.reply("⚠ Invalid group ID format!")
    except Exception as e:
        logging.error(f"Error in start_group_collect: {e}")
        await message.reply(f"⚠ Error: {str(e)}")

@bot.on_message(filters.command("stopgroup") & filters.user(ADMIN_USER_IDS))
async def stop_group_collect(_, message: Message):
    """Stop collection in a specific group."""
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.reply("⚠ Usage: `/stopgroup [group_id]` or `/stopgroup all`")
            return
            
        target = args[1].lower()
        
        if target == "all":
            # Stop in all groups
            for group_id in TARGET_GROUP_IDS + [MAIN_GROUP_ID]:
                collection_status[group_id] = False
            await message.reply("🛑 Collection stopped in ALL groups!")
            logging.info("Collection stopped in all groups")
        else:
            # Try to parse as a group ID
            try:
                group_id = int(target)
                if group_id in collection_status:
                    collection_status[group_id] = False
                    group_name = "main group" if group_id == MAIN_GROUP_ID else f"group {group_id}"
                    await message.reply(f"🛑 Collection stopped in {group_name}!")
                    logging.info(f"Collection stopped in {group_name}")
                else:
                    await message.reply("⚠ Group ID not in configured targets!")
            except ValueError:
                await message.reply("⚠ Invalid group ID format!")
    except Exception as e:
        logging.error(f"Error in stop_group_collect: {e}")
        await message.reply(f"⚠ Error: {str(e)}")

@bot.on_message(filters.command("status") & filters.chat(TARGET_GROUP_IDS) & filters.user(ADMIN_USER_IDS))
async def check_status(_, message: Message):
    """Check collection status in all groups."""
    status_text = "📊 **Collection Status**:\n\n"
    
    # Main group status
    main_status = "✅ Active" if collection_status.get(MAIN_GROUP_ID, False) else "🛑 Inactive"
    status_text += f"**Main Group**: {main_status}\n\n"
    
    # Target groups status
    status_text += "**Target Groups**:\n"
    for i, group_id in enumerate(TARGET_GROUP_IDS, 1):
        group_status = "✅ Active" if collection_status.get(group_id, False) else "🛑 Inactive"
        status_text += f"{i}. Group `{group_id}`: {group_status}\n"
    
    status_text += f"\n**Database**: {current_db_name}"
    
    await message.reply(status_text)

# Modified to start collection in all groups
@bot.on_message(filters.command("startcollect") & filters.chat(TARGET_GROUP_IDS) & filters.user(ADMIN_USER_IDS))
async def start_collect(_, message: Message):
    # Start collection in all target groups
    for group_id in TARGET_GROUP_IDS:
        collection_status[group_id] = True
    
    reply_msg = await message.reply(f"✅ Collection started in ALL target groups!")
    logging.info("Collection started in all target groups via /startcollect command")
    
    await asyncio.sleep(2)
    await reply_msg.delete()

@bot.on_message(filters.command("stopcollect") & filters.chat(TARGET_GROUP_IDS) & filters.user(ADMIN_USER_IDS))
async def stop_collect(_, message: Message):
    # Stop collection in all target groups 
    for group_id in TARGET_GROUP_IDS:
        collection_status[group_id] = False
        
    reply_msg = await message.reply("🛑 Collection stopped in ALL target groups!")
    logging.info("Collection stopped in all target groups via /stopcollect command")

    await asyncio.sleep(2)
    await reply_msg.delete()

@bot.on_message(filters.command("startmain") & filters.chat(MAIN_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def start_main_collect(_, message: Message):
    """Starts the main collect function but only affects MAIN_GROUP_ID."""
    if not collection_status.get(MAIN_GROUP_ID, False):
        collection_status[MAIN_GROUP_ID] = True
        await message.reply("✅ Main collect function started!")
        logging.info("Main collect function started in MAIN_GROUP_ID.")
    else:
        await message.reply("⚠ Main collect function is already running!")

@bot.on_message(filters.command("stopmain") & filters.chat(MAIN_GROUP_ID) & filters.user(ADMIN_USER_IDS))
async def stop_main_collect(_, message: Message):
    """Stops the main collect function but only affects MAIN_GROUP_ID."""
    if collection_status.get(MAIN_GROUP_ID, False):
        collection_status[MAIN_GROUP_ID] = False
        await message.reply("🛑 Main collect function stopped!")
        logging.info("Main collect function stopped in MAIN_GROUP_ID.")
    else:
        await message.reply("⚠ Main collect function is not running!")

@bot.on_message(filters.photo & (filters.chat(TARGET_GROUP_IDS) | filters.chat(MAIN_GROUP_ID)) & filters.user(COLLECTOR_USER_IDS))
async def hacke(c: Client, m: Message):
    group_id = m.chat.id
    
    if not collection_status.get(group_id, False):
        return

    try:
        if not m.caption:
            return

        # Exact target captions to match
        target_captions = [
            "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴘʟᴀʏᴇʀ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ/Her ᴜꜱɪɴɡ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ",
            "🔥 ʟᴏᴏᴋ ᴀɴ ᴏɢ ᴀᴛʜʟᴇᴛᴇ ᴊᴜꜱᴛ ᴀʀʀɪᴠᴇᴅ ᴄᴏʟʟᴇᴄᴛ ʜɪᴍ/Her ᴜꜱɪɴɡ /ᴄᴏʟʟᴇᴄᴛ ɴᴀᴍᴇ"
        ]

        # Simple direct string matching
        received_caption = m.caption.strip()
        
        # Plain exact matching
        if received_caption in target_captions:
            logging.info("Caption matched exactly!")
        else:
            logging.info("Caption not matched")
            return
        
        # Process image ID
        file_id = m.photo.file_unique_id
        logging.info(f"Processing file_id: {file_id}")

        # Check player in cache
        if file_id in player_cache:
            player_name = player_cache[file_id]['name']
            logging.info(f"Found player {player_name} in cache")
        else:
            file_data = await current_db.find_one({"file_id": file_id})
            if file_data:
                player_name = file_data['name']
                player_cache[file_id] = {"name": player_name}
                logging.info(f"Found player {player_name} in database")
            else:
                logging.warning(f"Image ID {file_id} not found in database")
                return

        # Send collect command
        sent_message = await bot.send_message(m.chat.id, f"/collect {player_name}")
        
        # Wait for bot's reply
        await asyncio.sleep(1)
        
        # Check for rare player response
        async for reply in bot.get_chat_history(m.chat.id, limit=5):
            if (reply.reply_to_message and 
                reply.reply_to_message.message_id == sent_message.message_id):
                if should_forward_message(reply.text):
                    await reply.forward(FORWARD_CHANNEL_ID)
                    logging.info("Forwarded rare player message")
                break
                    
    except FloodWait as e:
        wait_time = e.value + random.randint(1, 5)
        logging.warning(f"Rate limit hit! Waiting for {wait_time} seconds...")
        await asyncio.sleep(wait_time)
    except Exception as e:
        logging.error(f"Error in hacke: {str(e)}")

@bot.on_message(filters.chat(TARGET_GROUP_IDS + [MAIN_GROUP_ID]))
async def check_rarity_and_forward(_, message: Message):
    if not message.text:
        return  

    if "✅ Look You Collected A" in message.text:
        group_id = message.chat.id
        logging.info(f"Checking message for rarity in group {group_id}:\n{message.text}")

        for rarity in RARITIES_TO_FORWARD:
            if f"Rarity : {rarity}" in message.text:
                logging.info(f"Detected {rarity} celebrity in group {group_id}! Forwarding...")
                await bot.send_message(FORWARD_CHANNEL_ID, f"From group {group_id}:\n\n{message.text}")
                break

@bot.on_message(filters.command("fileid") & filters.chat(TARGET_GROUP_IDS) & filters.user(ADMIN_USER_IDS))
async def extract_file_id(_, message: Message):
    """Extracts and sends the unique file ID of a replied photo."""
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.reply("⚠ Please reply to a photo to extract the file ID.")
        return
    
    file_unique_id = message.reply_to_message.photo.file_unique_id
    await message.reply(f"📂 **File Unique ID:** `{file_unique_id}`")

@bot.on_message(filters.command(["addgroup", "delgroup"]) & filters.user(ADMIN_USER_IDS))
async def manage_groups(_, message: Message):
    """Add or remove a group from target groups."""
    global TARGET_GROUP_IDS, collection_status
    
    command = message.command[0]
    is_add = command == "addgroup"
    action_word = "add" if is_add else "remove"
    
    if len(message.command) != 2:
        await message.reply(f"⚠ Usage: `/{command} [group_id]`")
        return
    
    try:
        group_id = int(message.command[1])
        
        if is_add:
            if group_id in TARGET_GROUP_IDS:
                await message.reply(f"⚠ Group `{group_id}` is already in target groups!")
                return
                
            TARGET_GROUP_IDS.append(group_id)
            collection_status[group_id] = False  # Add to status tracking with default off
            await message.reply(f"✅ Group `{group_id}` added to target groups!")
            logging.info(f"Added group {group_id} to target groups")
        else:
            if group_id not in TARGET_GROUP_IDS:
                await message.reply(f"⚠ Group `{group_id}` is not in target groups!")
                return
                
            TARGET_GROUP_IDS.remove(group_id)
            if group_id in collection_status:
                del collection_status[group_id]  # Remove from status tracking
            await message.reply(f"✅ Group `{group_id}` removed from target groups!")
            logging.info(f"Removed group {group_id} from target groups")
    
    except ValueError:
        await message.reply(f"⚠ Invalid group ID format! Please provide a valid integer.")
    except Exception as e:
        logging.error(f"Error in {action_word}_group: {e}")
        await message.reply(f"⚠ Error: {str(e)}")

@bot.on_message(filters.command("groups") & filters.user(ADMIN_USER_IDS))
async def list_groups(_, message: Message):
    """List all target groups."""
    groups_text = "📋 **Target Groups**:\n\n"
    
    for i, group_id in enumerate(TARGET_GROUP_IDS, 1):
        groups_text += f"{i}. `{group_id}`\n"
    
    groups_text += f"\n**Main Group**: `{MAIN_GROUP_ID}`\n"
    groups_text += f"**Forward Channel**: `{FORWARD_CHANNEL_ID}`"
    
    await message.reply(groups_text)

@bot.on_message(filters.command("importjson") & filters.user(ADMIN_USER_IDS))
async def import_json_to_mongodb(_, message: Message):
    """Import player data from JSON file to MongoDB."""
    try:
        if not message.reply_to_message or not message.reply_to_message.document:
            await message.reply("⚠ Please reply to a JSON file with this command.")
            return
            
        doc = message.reply_to_message.document
        if not doc.file_name.endswith('.json'):
            await message.reply("⚠ Please provide a JSON file.")
            return

        status_msg = await message.reply("⏳ Downloading and processing JSON file...")
        
        # Download the file
        path = await message.reply_to_message.download()
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert the data format
        formatted_data = []
        for file_id, player_info in data.items():
            formatted_data.append({
                "file_id": file_id,
                "name": player_info["name"]
            })
            
        # Import to current database
        if formatted_data:
            # First, clear existing data
            await current_db.delete_many({})
            # Then insert new data
            result = await current_db.insert_many(formatted_data)
            
            # Reload cache
            await preload_players()
            
            await status_msg.edit_text(
                f"✅ Successfully imported {len(result.inserted_ids)} players to {current_db_name} database!"
            )
        else:
            await status_msg.edit_text("⚠ No valid data found in JSON file!")
            
        # Clean up downloaded file
        os.remove(path)
        
    except json.JSONDecodeError:
        await message.reply("⚠ Invalid JSON format! Please check your file.")
    except Exception as e:
        logging.error(f"Error importing JSON: {e}")
        await message.reply(f"⚠ Error: {str(e)}")

@bot.on_message(filters.command("addplayer") & filters.chat(TARGET_GROUP_IDS) & filters.user(ADMIN_USER_IDS))
async def add_player(_, message: Message):
    """Add a player to a specific database."""
    try:
        # Check command format
        if len(message.command) < 3:
            await message.reply(
                "⚠ **Usage**: `/addplayer [database] [player_name]`\n"
                "**Example**: `/addplayer goku John Cena`\n"
                "**Note**: Reply to a photo when using this command"
            )
            return

        # Check if replying to a photo
        if not message.reply_to_message or not message.reply_to_message.photo:
            await message.reply("⚠ Please reply to a photo!")
            return

        # Get database name and player name
        db_name = message.command[1].lower()
        player_name = " ".join(message.command[2:])
        file_id = message.reply_to_message.photo.file_unique_id

        # Validate database name
        if db_name not in ["goku", "vegeta"]:
            await message.reply("⚠ Invalid database! Use either `goku` or `vegeta`")
            return

        # Select the correct database
        target_db = db_goku if db_name == "goku" else db_vegeta
        db_display_name = "Goku" if db_name == "goku" else "Vegeta"

        # Check if player already exists in the specified database
        existing_player = await target_db.find_one({"file_id": file_id})
        if existing_player:
            await message.reply(
                f"⚠ This photo is already registered in **{db_display_name}** database as: "
                f"`{existing_player['name']}`"
            )
            return

        # Add player to database
        new_player = {
            "file_id": file_id,
            "name": player_name
        }
        
        await target_db.insert_one(new_player)
        
        # Update cache if this is the current active database
        if target_db == current_db:
            player_cache[file_id] = {"name": player_name}
        
        await message.reply(
            f"✅ Successfully added player to **{db_display_name}** database!\n\n"
            f"**Player**: `{player_name}`\n"
            f"**File ID**: `{file_id}`"
        )
        logging.info(f"Added player {player_name} with file_id {file_id} to {db_display_name} database")

    except Exception as e:
        logging.error(f"Error adding player: {e}")
        await message.reply(f"⚠ Error: {str(e)}")

@bot.on_message(filters.command("dbinfo") & filters.chat(TARGET_GROUP_IDS) & filters.user(ADMIN_USER_IDS))
async def database_info(_, message: Message):
    """Show information about both databases."""
    try:
        # Get counts for both databases
        goku_count = await db_goku.count_documents({})
        vegeta_count = await db_vegeta.count_documents({})
        
        # Get sample players from each database
        goku_players = []
        vegeta_players = []
        
        async for doc in db_goku.find().limit(5):
            goku_players.append(doc["name"])
        
        async for doc in db_vegeta.find().limit(5):
            vegeta_players.append(doc["name"])
        
        # Create info message
        info_text = "📊 **Database Information**\n\n"
        
        # Goku DB info
        info_text += "**Goku Database**:\n"
        info_text += f"• Total Players: `{goku_count}`\n"
        if goku_players:
            info_text += "• Sample Players: " + ", ".join([f"`{p}`" for p in goku_players]) + "\n"
        
        info_text += "\n**Vegeta Database**:\n"
        info_text += f"• Total Players: `{vegeta_count}`\n"
        if vegeta_players:
            info_text += "• Sample Players: " + ", ".join([f"`{p}`" for p in vegeta_players]) + "\n"
        
        # Current active database
        info_text += f"\n**Current Active Database**: `{current_db_name}`\n"
        info_text += f"• Cached Players: `{len(player_cache)}`"
        
        await message.reply(info_text)
        
    except Exception as e:
        logging.error(f"Error getting database info: {e}")
        await message.reply(f"⚠ Error: {str(e)}")

async def main():
    """ Runs Pyrogram bot and Flask server concurrently """
    await preload_players()  # Load players into memory before starting
    
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask_app, daemon=True)
    flask_thread.start()
    logging.info("Health check server started on port 8000")
    
    await bot.start()
    logging.info("Bot started successfully!")
    logging.info(f"Monitoring {len(TARGET_GROUP_IDS)} groups: {TARGET_GROUP_IDS}")
    await idle()
    await bot.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
