import asyncio
import logging
import time
from typing import Dict, Union
from FileStream.bot import work_loads
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.types import Message

class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 60 * 10  # Reduced cache time to 10 minutes
        self.client: Client = client
        self.cached_file_ids: Dict[str, FileId] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._session_locks: Dict[int, asyncio.Lock] = {}
        self._session_retries: Dict[int, int] = {}
        self._max_retries = 3
        self._last_usage: Dict[str, float] = {}  # Track when each file was last accessed
        self._max_cache_size = 25  # Maximum number of items in cache
        asyncio.create_task(self.clean_cache())

    def get_lock(self, key: str) -> asyncio.Lock:
        """Get a lock for a specific file ID to prevent concurrent access"""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]
        
    def get_session_lock(self, dc_id: int) -> asyncio.Lock:
        """Get a lock for a specific DC to prevent concurrent session operations"""
        if dc_id not in self._session_locks:
            self._session_locks[dc_id] = asyncio.Lock()
        return self._session_locks[dc_id]

    async def get_file_properties(self, db_id: str, multi_clients) -> FileId:
        """
        Returns the properties of a media of a specific message in a FileId class.
        with optimized caching for Koyeb free tier.
        """
        async with self.get_lock(db_id):
            # Update last usage time whenever a file is accessed
            self._last_usage[db_id] = time.time()
            
            # Check if we need to trim the cache
            if len(self.cached_file_ids) > self._max_cache_size:
                await self._trim_cache()
                
            if db_id not in self.cached_file_ids:
                await self.generate_file_properties(db_id, multi_clients)
                
            return self.cached_file_ids[db_id]
    
    async def _trim_cache(self):
        """Remove oldest items from cache when it gets too large"""
        if not self._last_usage:
            return
            
        # Find the oldest items
        sorted_items = sorted(self._last_usage.items(), key=lambda x: x[1])
        items_to_remove = len(self.cached_file_ids) - self._max_cache_size
        
        if items_to_remove <= 0:
            return
            
        # Remove oldest items
        for i in range(min(items_to_remove, len(sorted_items))):
            key = sorted_items[i][0]
            if key in self.cached_file_ids:
                self.cached_file_ids.pop(key)
            if key in self._last_usage:
                self._last_usage.pop(key)
            if key in self._locks:
                self._locks.pop(key)
    
    async def generate_file_properties(self, db_id: str, multi_clients) -> FileId:
        """
        Generates the properties of a media file on a specific message.
        returns the properties in a FileId class.
        """
        logging.debug("Before calling get_file_ids")
        file_id = await get_file_ids(self.client, db_id, multi_clients, Message)
        logging.debug(f"Generated file ID and Unique ID for file with ID {db_id}")
        self.cached_file_ids[db_id] = file_id
        logging.debug(f"Cached media file with ID {db_id}")
        return self.cached_file_ids[db_id]

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        Optimized for Koyeb free tier with better error handling.
        """
        dc_id = file_id.dc_id
        async with self.get_session_lock(dc_id):
            media_session = client.media_sessions.get(dc_id, None)

            # Check if session exists and is healthy with a simple check
            if media_session is not None:
                try:
                    # Simple faster check
                    if hasattr(media_session, '_is_connected') and media_session._is_connected:
                        return media_session
                except:
                    # Session unreachable
                    client.media_sessions.pop(dc_id, None)
                    media_session = None

            if media_session is None:
                retry_count = self._session_retries.get(dc_id, 0)
                if retry_count >= self._max_retries:
                    self._session_retries[dc_id] = 0  # Reset for future attempts
                    raise RuntimeError(f"Failed to create media session after {self._max_retries} attempts")
                
                self._session_retries[dc_id] = retry_count + 1
                
                try:
                    # Simplified session creation with fewer retries
                    if dc_id != await client.storage.dc_id():
                        media_session = Session(
                            client,
                            dc_id,
                            await Auth(client, dc_id, await client.storage.test_mode()).create(),
                            await client.storage.test_mode(),
                            is_media=True,
                        )
                        await media_session.start()

                        # Try to import authorization - single attempt to save resources
                        exported_auth = await client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                        )

                        await media_session.invoke(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                    else:
                        media_session = Session(
                            client,
                            dc_id,
                            await client.storage.auth_key(),
                            await client.storage.test_mode(),
                            is_media=True,
                        )
                        await media_session.start()
                    
                    client.media_sessions[dc_id] = media_session
                    
                except Exception as e:
                    logging.error(f"Failed to create media session: {str(e)}")
                    raise
            
            return media_session


    @staticmethod
    async def get_location(file_id: FileId) -> Union[raw.types.InputPhotoFileLocation,
                                                     raw.types.InputDocumentFileLocation,
                                                     raw.types.InputPeerPhotoFileLocation,]:
        """
        Returns the file location for the media file.
        """
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ) -> Union[str, None]:
        """
        Custom generator that yields the bytes of the media file.
        Optimized for Koyeb free tier.
        """
        client = self.client
        work_loads[index] += 1
        
        current_part = 1
        retry_count = 0
        max_retries = 2  # Reduced retries to save resources
        
        try:
            media_session = await self.generate_media_session(client, file_id)
            location = await self.get_location(file_id)
            
            while current_part <= part_count and retry_count < max_retries:
                try:
                    # Simple timeout handling
                    r = await asyncio.wait_for(
                        media_session.invoke(
                            raw.functions.upload.GetFile(
                                location=location,
                                offset=offset,
                                limit=chunk_size
                            )
                        ),
                        timeout=20  # Reasonable timeout for Koyeb
                    )
                    
                    if isinstance(r, raw.types.upload.File):
                        chunk = r.bytes
                        if not chunk:
                            break
                            
                        if part_count == 1:
                            yield chunk[first_part_cut:last_part_cut]
                        elif current_part == 1:
                            yield chunk[first_part_cut:]
                        elif current_part == part_count:
                            yield chunk[:last_part_cut]
                        else:
                            yield chunk

                        current_part += 1
                        offset += chunk_size
                        
                        if current_part > part_count:
                            break
                            
                    else:
                        retry_count += 1
                        await asyncio.sleep(1)
                        
                except (asyncio.TimeoutError, TimeoutError) as e:
                    retry_count += 1
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logging.error(f"Error getting chunk: {str(e)}")
                    retry_count += 1
                    await asyncio.sleep(1)
                
        except Exception as e:
            logging.error(f"Error in yield_file: {str(e)}")
        finally:
            work_loads[index] -= 1
    
    async def clean_cache(self) -> None:
        """
        Function to clean the cache to reduce memory usage.
        Optimized for Koyeb free tier.
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            
            # Keep only recently used files based on _last_usage
            current_time = time.time()
            inactive_files = []
            
            for db_id, last_used in self._last_usage.items():
                if current_time - last_used > self.clean_timer:
                    inactive_files.append(db_id)
                    
            for db_id in inactive_files:
                if db_id in self.cached_file_ids:
                    self.cached_file_ids.pop(db_id)
                if db_id in self._last_usage:
                    self._last_usage.pop(db_id)
                if db_id in self._locks:
                    self._locks.pop(db_id)
                    
            logging.debug(f"Cleaned {len(inactive_files)} files from cache")
