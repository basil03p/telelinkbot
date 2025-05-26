import sys
import asyncio
import logging
import traceback
import logging.handlers as handlers
from FileStream.config import Telegram, Server
from aiohttp import web
from pyrogram import idle
import signal
import gc  # For garbage collection

from FileStream.bot import FileStream
from FileStream.server import web_server
from FileStream.bot.clients import initialize_clients

# Configure more efficient logging
logging.basicConfig(
    level=logging.INFO,
    datefmt="%d/%m/%Y %H:%M:%S",
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(stream=sys.stdout),
              handlers.RotatingFileHandler("streambot.log", mode="a", maxBytes=5242880, backupCount=1, encoding="utf-8")],)  # Reduced log size

# Lower log levels for noisy libraries
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("pyrogram").setLevel(logging.ERROR)  # Changed to ERROR to reduce logging
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

server = web.AppRunner(web_server())
loop = asyncio.get_event_loop()

# Add signal handlers for graceful shutdown in Koyeb environment
for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGABRT):
    try:
        loop.add_signal_handler(sig, lambda: asyncio.create_task(cleanup()))
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        pass

# Configure conservative garbage collection for better memory usage
gc.set_threshold(700, 10, 5)  # More aggressive GC

# Memory optimization function
async def optimize_memory():
    while True:
        await asyncio.sleep(60)  # Run every minute
        gc.collect()  # Force garbage collection
        current_mem = 0
        try:
            import resource
            current_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB to MB
            if current_mem > 450:  # If using more than 450MB, take action
                logging.warning(f"Memory usage high: {current_mem:.2f} MB, forcing cleanup")
                import os
                os._exit(1)  # Let Koyeb restart the container
        except ImportError:
            pass  # resource module not available

async def start_services():
    print()
    if Telegram.SECONDARY:
        print("------------------ Starting as Secondary Server ------------------")
    else:
        print("------------------- Starting as Primary Server -------------------")
    print()
    print("-------------------- Initializing Telegram Bot --------------------")

    try:
        # Configure for Koyeb free tier
        FileStream.connect_timeout = 20.0  # Longer timeout
        FileStream.read_timeout = 30.0
        FileStream.write_timeout = 30.0
        
        await FileStream.start()
        bot_info = await FileStream.get_me()
        FileStream.id = bot_info.id
        FileStream.username = bot_info.username
        FileStream.fname=bot_info.first_name
        print("------------------------------ DONE ------------------------------")
        print()
        print("---------------------- Initializing Clients ----------------------")
        await initialize_clients()
        print("------------------------------ DONE ------------------------------")
        print()
        print("--------------------- Initializing Web Server ---------------------")
        await server.setup()
        site = web.TCPSite(server, Server.BIND_ADDRESS, Server.PORT)
        await site.start()
        print("------------------------------ DONE ------------------------------")
        print()
        print("------------------------- Service Started -------------------------")
        print("                        bot =>> {}".format(bot_info.first_name))
        if hasattr(bot_info, 'dc_id') and bot_info.dc_id:
            print("                        DC ID =>> {}".format(str(bot_info.dc_id)))
        print(" URL =>> {}".format(Server.URL))
        print("------------------------------------------------------------------")
        
        # Simple reconnection for Koyeb free tier
        @FileStream.on_connection_error()
        async def connection_error_handler(_, error):
            logging.error(f"Connection error occurred: {error}")
            await asyncio.sleep(5)
            logging.info("Attempting to reconnect...")
            
        # Start memory optimization task
        asyncio.create_task(optimize_memory())
            
        await idle()
    except Exception as e:
        logging.error(f"Error during startup: {str(e)}")
        raise

async def cleanup():
    logging.info("Shutting down services...")
    try:
        await server.cleanup()
    except Exception as e:
        logging.error(f"Error during server cleanup: {str(e)}")
    
    try:
        await FileStream.stop()
    except Exception as e:
        logging.error(f"Error during bot cleanup: {str(e)}")
    
    loop.stop()

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        pass
    except Exception as err:
        logging.error(traceback.format_exc())
    finally:
        loop.run_until_complete(cleanup())
        loop.stop()
        print("------------------------ Stopped Services ------------------------")