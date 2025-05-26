import os
import sys
import asyncio
import logging
import traceback
import logging.handlers as handlers
from FileStream.config import Telegram, Server
from aiohttp import web
from pyrogram import idle
from FileStream.bot import FileStream
from FileStream.server import web_server
from FileStream.bot.clients import initialize_clients
from FileStream.connection_fix import maintain_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    datefmt="%d/%m/%Y %H:%M:%S",
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(stream=sys.stdout),
              handlers.RotatingFileHandler("streambot.log", mode="a", maxBytes=52428800, backupCount=3, encoding="utf-8")],
)

log = logging.getLogger(__name__)
log.info("Starting File2Link Bot...")

server = None
loop = asyncio.get_event_loop()

async def start_services():
    log.info('------------------- Starting as Primary Server -------------------')
    
    log.info('-------------------- Initializing Telegram Bot --------------------')
    await FileStream.start()
    bot_info = await FileStream.get_me()
    FileStream.id = bot_info.id
    FileStream.username = bot_info.username
    FileStream.fname=bot_info.first_name
    log.info('------------------------------ DONE ------------------------------')

    log.info('---------------------- Initializing Clients ----------------------')
    if os.path.exists('./FileStream/vars.py') and os.path.exists('./FileStream/accounts'):
        await initialize_clients()
    else:
        log.info('No additional clients found, using default client')
        # Instead of using on_connection_error decorator, start a background task
        asyncio.create_task(maintain_connection(FileStream))
    
    log.info('------------------------------ DONE ------------------------------')

    log.info('--------------------- Initializing Web Server ---------------------')
    # Fix: Don't use await with web_server() if it returns an Application object rather than a coroutine
    app = web.AppRunner(web_server())
    await app.setup()
    bind_address = "0.0.0.0"
    await web.TCPSite(app, bind_address, Server.PORT).start()
    log.info('------------------------------ DONE ------------------------------')

    log.info('------------------------- Service Started -------------------------')
    log.info("                        bot =>> {}".format(bot_info.first_name))
    if hasattr(bot_info, 'dc_id') and bot_info.dc_id:
        log.info("                        DC ID =>> {}".format(str(bot_info.dc_id)))
    log.info(" URL =>> {}".format(Server.URL))
    log.info("------------------------------------------------------------------")
    
    # REMOVE the problematic connection error handler
    # @FileStream.on_connection_error() - This line caused the error
    
    await idle()

async def cleanup():
    log.info("Shutting down services...")
    try:
        if server:
            await server.cleanup()
    except Exception as e:
        log.error(f"Error during server cleanup: {str(e)}")
    
    try:
        await FileStream.stop()
    except Exception as e:
        log.error(f"Error during bot cleanup: {str(e)}")

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.error(f"Error during startup: {str(e)}")
        log.error(traceback.format_exc())
    finally:
        log.info("Shutting down services...")
        loop.run_until_complete(cleanup())
        loop.stop()
        print("------------------------ Stopped Services ------------------------")