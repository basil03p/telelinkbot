import time
import math
import asyncio
import logging
import mimetypes
import traceback
import gc  # For garbage collection
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from FileStream.bot import multi_clients, work_loads, FileStream
from FileStream.config import Telegram, Server
from FileStream.server.exceptions import FIleNotFound, InvalidHash
from FileStream import utils, StartTime, __version__
from FileStream.utils.render_template import render_page

routes = web.RouteTableDef()

# Cache control
MAX_CACHE_SIZE = 10  # Maximum number of items to keep in memory
class_cache = {}
request_locks = {}
last_cache_cleanup = time.time()

@routes.get("/status", allow_head=True)
async def root_route_handler(_):
    return web.json_response(
        {
            "server_status": "running",
            "uptime": utils.get_readable_time(time.time() - StartTime),
            "telegram_bot": "@" + FileStream.username,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": __version__,
        }
    )

@routes.get("/watch/{path}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        return web.Response(text=await render_page(path), content_type='text/html')
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass


@routes.get("/dl/{path}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        return await media_streamer(request, path)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        traceback.print_exc()
        logging.critical(e.with_traceback(None))
        logging.debug(traceback.format_exc())
        raise web.HTTPInternalServerError(text=str(e))

async def cleanup_caches():
    """Periodically clean up caches to conserve memory"""
    global last_cache_cleanup, class_cache, request_locks
    
    current_time = time.time()
    if current_time - last_cache_cleanup < 300:  # Every 5 minutes
        return
        
    last_cache_cleanup = current_time
    
    # Clean up request locks
    if len(request_locks) > MAX_CACHE_SIZE:
        old_keys = list(request_locks.keys())[:(len(request_locks) - MAX_CACHE_SIZE)]
        for k in old_keys:
            request_locks.pop(k, None)
    
    # Only keep a limited number of ByteStreamers in memory
    if len(class_cache) > MAX_CACHE_SIZE:
        items_to_remove = len(class_cache) - MAX_CACHE_SIZE
        keys_to_remove = list(class_cache.keys())[:items_to_remove]
        for k in keys_to_remove:
            class_cache.pop(k, None)
    
    # Force garbage collection
    gc.collect()
    logging.debug("Cache cleaned up")

async def media_streamer(request: web.Request, db_id: str):
    # Clean up caches periodically
    await cleanup_caches()
    
    # Create a lock for this request if it doesn't exist
    if db_id not in request_locks:
        request_locks[db_id] = asyncio.Lock()
    
    # Use a lock to prevent multiple requests for the same file from causing issues
    async with request_locks[db_id]:
        try:
            range_header = request.headers.get("Range", 0)
            
            index = min(work_loads, key=work_loads.get)
            faster_client = multi_clients[index]
            
            if Telegram.MULTI_CLIENT:
                logging.info(f"Client {index} serving {request.remote}")
            
            if faster_client in class_cache:
                tg_connect = class_cache[faster_client]
            else:
                tg_connect = utils.ByteStreamer(faster_client)
                class_cache[faster_client] = tg_connect
            
            file_id = await tg_connect.get_file_properties(db_id, multi_clients)
            file_size = file_id.file_size
            
            # Handle range header
            try:
                if range_header:
                    from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
                    from_bytes = int(from_bytes)
                    until_bytes = int(until_bytes) if until_bytes else file_size - 1
                else:
                    from_bytes = request.http_range.start or 0
                    until_bytes = (request.http_range.stop or file_size) - 1
            except (ValueError, AttributeError):
                from_bytes = 0
                until_bytes = file_size - 1

            # Validate range
            if (until_bytes >= file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
                return web.Response(
                    status=416,
                    body="416: Range not satisfiable",
                    headers={"Content-Range": f"bytes */{file_size}"},
                )

            # Optimize chunk size for Koyeb free tier (default 1MB)
            chunk_size = 1024 * 1024  # 1MB chunks for better memory usage
            until_bytes = min(until_bytes, file_size - 1)
            
            offset = from_bytes - (from_bytes % chunk_size)
            first_part_cut = from_bytes - offset
            last_part_cut = until_bytes % chunk_size + 1
            
            req_length = until_bytes - from_bytes + 1
            part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
            
            body = tg_connect.yield_file(
                file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
            )
            
            mime_type = file_id.mime_type
            file_name = utils.get_name(file_id)
            disposition = "attachment"
            
            if not mime_type:
                mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
            
            return web.Response(
                status=206 if range_header else 200,
                body=body,
                headers={
                    "Content-Type": f"{mime_type}",
                    "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
                    "Content-Length": str(req_length),
                    "Content-Disposition": f'{disposition}; filename="{file_name}"',
                    "Accept-Ranges": "bytes",
                    "Cache-Control": "public, max-age=3600",
                },
            )
            
        except Exception as e:
            logging.error(f"Error in media_streamer: {str(e)}")
            raise
