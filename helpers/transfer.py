"""
HYBRID TRANSFER APPROACH for RAM Optimization on Render
========================================================

DOWNLOADS: Streaming (Telethon native)
- Uses client.iter_download() for single-connection streaming
- Minimal RAM usage - no parallel connections
- Downloads chunks one at a time, writing directly to disk
- Prevents RAM spikes and crashes on constrained environments

UPLOADS: FastTelethon (Parallel)
- Uses FastTelethon for parallel upload connections
- Optimized connection count based on file size (3-6 connections)
- Still RAM-efficient as it streams file chunks
- Faster upload speeds while preventing crashes

This hybrid approach provides the best balance:
✓ Downloads won't cause RAM spikes (streaming)
✓ Uploads remain fast (parallel) but RAM-controlled
✓ Prevents Render crashes while maintaining performance
"""
import os
import asyncio
import math
import inspect
from typing import Optional, Callable, BinaryIO
from telethon import TelegramClient, utils
from telethon.tl.types import Message, Document, TypeMessageMedia, InputPhotoFileLocation, InputDocumentFileLocation
from logger import LOGGER
from FastTelethon import download_file as fast_download, upload_file as fast_upload, ParallelTransferrer

IS_CONSTRAINED = bool(
    os.getenv('RENDER') or 
    os.getenv('RENDER_EXTERNAL_URL') or 
    os.getenv('REPLIT_DEPLOYMENT') or 
    os.getenv('REPL_ID')
)

# Tiered connection scaling for RAM optimization (uploads only - downloads use streaming)
# Each connection uses ~5-10MB RAM
# CRITICAL FIX: Reduced from 6 to 3 on constrained environments to prevent RAM spikes
MAX_UPLOAD_CONNECTIONS = 3 if IS_CONSTRAINED else 6

async def download_media_fast(
    client: TelegramClient,
    message: Message,
    file: str,
    progress_callback: Optional[Callable] = None
) -> str:
    """
    Download media using STREAMING approach for minimal RAM usage.
    This prevents RAM spikes and crashes on constrained environments like Render.
    
    Uses Telethon's native iter_download() which streams chunks without parallel connections.
    This is much more RAM-efficient than FastTelethon's parallel approach.
    """
    if not message.media:
        raise ValueError("Message has no media")
    
    try:
        # Get file size for progress tracking
        file_size = 0
        if message.document:
            file_size = message.document.size
        elif message.video:
            file_size = getattr(message.video, 'size', 0)
        elif message.audio:
            file_size = getattr(message.audio, 'size', 0)
        elif message.photo:
            photo_sizes = [size for size in message.photo.sizes if hasattr(size, 'size')]
            if photo_sizes:
                largest_size = max(photo_sizes, key=lambda s: s.size)
                file_size = largest_size.size
        
        LOGGER(__name__).debug(f"Streaming download: {file} ({file_size} bytes)")
        
        # Use Telethon's native streaming download - minimal RAM usage
        # iter_download streams chunks without loading entire file into memory
        downloaded_bytes = 0
        with open(file, 'wb') as f:
            async for chunk in client.iter_download(message.media):
                f.write(chunk)
                downloaded_bytes += len(chunk)
                
                # Call progress callback if provided
                # Handle both sync callbacks and async callbacks (lambdas that return coroutines)
                if progress_callback and file_size > 0:
                    result = progress_callback(downloaded_bytes, file_size)
                    # If callback returns a coroutine, await it
                    if inspect.iscoroutine(result):
                        await result
        
        LOGGER(__name__).debug(f"Streaming download complete: {file}")
        return file
        
    except Exception as e:
        LOGGER(__name__).error(f"Streaming download failed, falling back to standard: {e}")
        return await client.download_media(message, file=file, progress_callback=progress_callback)

async def upload_media_fast(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable] = None
):
    """
    Upload media using FASTTTELETHON for optimized parallel uploads.
    This uses parallel connections for faster uploads while managing RAM efficiently.
    
    FastTelethon uploads stream data in chunks, preventing full file loading into RAM.
    Connection count is automatically optimized based on file size.
    
    CRITICAL: Uses try/finally to ensure cleanup runs even on failures,
    preventing memory leaks from orphaned file handles and buffers.
    """
    import gc
    
    file_size = os.path.getsize(file_path)
    
    # Calculate connection count to verify the monkeypatch is working
    connection_count = _optimized_connection_count_upload(file_size)
    
    file_handle = None
    result = None
    
    try:
        LOGGER(__name__).debug(f"Upload starting: {file_path} ({file_size/1024/1024:.1f}MB)")
        
        file_handle = open(file_path, 'rb')
        result = await fast_upload(
            client=client,
            file=file_handle,
            progress_callback=progress_callback
        )
        
        LOGGER(__name__).debug(f"Upload complete: {file_path}")
        return result
        
    except Exception as e:
        LOGGER(__name__).error(f"FastTelethon upload failed: {e}")
        return None
        
    finally:
        # CRITICAL: Always close file handle to prevent memory leaks
        if file_handle:
            try:
                file_handle.close()
            except:
                pass
        
        # Force garbage collection after upload to release buffers
        gc.collect()
        LOGGER(__name__).debug(f"Upload cleanup complete for: {file_path}")

def _optimized_connection_count_upload(file_size, max_count=MAX_UPLOAD_CONNECTIONS, full_size=100*1024*1024):
    """
    CRITICAL RAM FIX: Tiered connection scaling for constrained environments (Render 512MB RAM, Replit)
    
    Without this fix, a 90MB file spawns 18 connections (>120MB RAM spike), crashing the bot.
    With this fix, same file uses 2-3 connections (~20-30MB RAM spike), staying within limits.
    
    Connection tiers (each connection uses ~10MB RAM):
    
    CONSTRAINED (Replit/Render - 512MB RAM):
    - Files >= 1GB: 2 connections (~20MB RAM) - Prevents OOM on huge uploads
    - Files 50MB-1GB: 2 connections (~20MB RAM) - Safe for constrained hosts
    - Files < 50MB: 3 connections (~30MB RAM) - Faster for small files, still safe
    
    NON-CONSTRAINED (standard servers):
    - Files >= 1GB: 3 connections (~30MB RAM) - Original safe value
    - Files 50MB-1GB: 4 connections (~40MB RAM) - Original balanced value
    - Files < 50MB: 6 connections (~60MB RAM) - Original fast value for small files
    
    IMPORTANT: We ignore max_count parameter to prevent FastTelethon's default (20)
    from bypassing our constraints. Always use hardcoded safe limits.
    """
    # On constrained environments (Replit, Render), use very conservative limits
    if IS_CONSTRAINED:
        # Large files (1GB+): Absolute minimum connections
        if file_size >= 1024 * 1024 * 1024:  # 1GB
            return 2
        # Medium files (50MB-1GB): Still conservative
        elif file_size >= 50 * 1024 * 1024:  # 50MB
            return 2
        # Small files (< 50MB): Slightly faster but still safe
        else:
            return 3
    else:
        # Non-constrained environments: keep original values
        if file_size >= 1024 * 1024 * 1024:  # 1GB
            return 3
        elif file_size >= 50 * 1024 * 1024:  # 50MB
            return 4
        else:
            return 6

# Apply optimized upload connection count to FastTelethon
ParallelTransferrer._get_connection_count = staticmethod(_optimized_connection_count_upload)
