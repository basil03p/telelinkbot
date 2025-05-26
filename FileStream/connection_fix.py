import asyncio
import logging
import time

log = logging.getLogger(__name__)

async def maintain_connection(client):
    """
    Maintain the client's connection by periodically checking and reconnecting if needed.
    """
    retry_count = 0
    max_retries = 5
    retry_delay = 5
    
    while True:
        try:
            # Check if client is connected
            if not client.is_connected:
                log.warning(f"Client {client.name} is disconnected. Attempting to reconnect...")
                try:
                    await client.connect()
                    if client.is_connected:
                        log.info(f"Client {client.name} has been reconnected successfully")
                        retry_count = 0  # Reset retry count on successful connection
                    else:
                        retry_count += 1
                        if retry_count >= max_retries:
                            log.error(f"Failed to reconnect after {max_retries} attempts, waiting longer...")
                            await asyncio.sleep(60)  # Wait longer between batches of retries
                            retry_count = 0
                except Exception as e:
                    log.error(f"Error connecting client: {str(e)}")
                    retry_count += 1
            else:
                # Periodically check if the client is still properly connected
                # This can catch cases where is_connected might return True but the connection is actually stale
                try:
                    # A lightweight operation to test connection
                    await client.get_me()
                    log.debug("Connection check successful")
                except Exception as e:
                    log.warning(f"Connection appears stale despite is_connected=True: {str(e)}")
                    try:
                        await client.disconnect()
                        await asyncio.sleep(1)
                        await client.connect()
                        log.info("Reconnected after stale connection detected")
                    except Exception as e2:
                        log.error(f"Failed to reconnect after stale connection: {str(e2)}")
            
            # Wait before checking again
            await asyncio.sleep(60)  # Check connection every minute
        
        except Exception as e:
            log.error(f"Error in connection maintenance: {str(e)}")
            await asyncio.sleep(retry_delay)  # Wait before retrying
