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

# Tiered connection scaling for RAM optimization
# Each connection uses ~5-10MB RAM
# For 1GB+ files: Use only 4 connections to minimize RAM (~20-40MB vs 60-160MB)
# For 200MB-1GB: Use 6 connections for balance (~30-60MB)
# For <200MB: Use 8 connections for speed (~40-80MB)
MAX_DOWNLOAD_CONNECTIONS = 8 if IS_CONSTRAINED else 12
MAX_UPLOAD_CONNECTIONS = 6 if IS_CONSTRAINED else 8

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
        
        LOGGER(__name__).info(f"Streaming download starting: {file} ({file_size} bytes, RAM-optimized)")
        
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
        
        LOGGER(__name__).info(f"Streaming download complete: {file}")
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
    """
    file_size = os.path.getsize(file_path)
    
    # Use FastTelethon for uploads - parallel connections with RAM efficiency
    try:
        LOGGER(__name__).info(f"FastTelethon upload starting: {file_path} ({file_size} bytes, RAM-optimized parallel)")
        
        with open(file_path, 'rb') as f:
            result = await fast_upload(
                client=client,
                file=f,
                progress_callback=progress_callback
            )
        
        LOGGER(__name__).info(f"FastTelethon upload complete: {file_path}")
        return result
        
    except Exception as e:
        LOGGER(__name__).error(f"FastTelethon upload failed: {e}")
        return None

def _optimized_connection_count_download(file_size, max_count=MAX_DOWNLOAD_CONNECTIONS, full_size=100*1024*1024):
    """
    NOTE: This is kept for compatibility but NOT used with streaming downloads.
    Streaming downloads use single connection for maximum RAM efficiency.
    """
    # Large files (1GB+): Minimize connections to save RAM
    if file_size >= 1024 * 1024 * 1024:  # 1GB
        return 4
    # Medium-large files (200MB-1GB): Balanced approach
    elif file_size >= 200 * 1024 * 1024:  # 200MB
        return 6
    # Smaller files: Use more connections for speed (still reasonable RAM)
    else:
        return min(8, max_count)

def _optimized_connection_count_upload(file_size, max_count=MAX_UPLOAD_CONNECTIONS, full_size=100*1024*1024):
    """
    Tiered connection scaling optimized for UPLOAD RAM efficiency on Render
    - Files >= 1GB: Use 3 connections (~15-30MB RAM) - Minimal RAM for large uploads
    - Files 200MB-1GB: Use 4 connections (~20-40MB RAM) - Balanced
    - Files < 200MB: Use 6 connections (~30-60MB RAM) - Faster speed
    This prevents RAM spikes on large uploads while maintaining good performance
    """
    # Large files (1GB+): Minimize connections to save RAM on Render
    if file_size >= 1024 * 1024 * 1024:  # 1GB
        return 3
    # Medium-large files (200MB-1GB): Balanced approach
    elif file_size >= 200 * 1024 * 1024:  # 200MB
        return 4
    # Smaller files: Use moderate connections for speed
    else:
        return min(6, max_count)

# Apply optimized upload connection count to FastTelethon
ParallelTransferrer._get_connection_count = staticmethod(_optimized_connection_count_upload)
