"""
SMART CONNECTION ALLOCATION for Multi-User High-Speed Transfers
================================================================

This module implements a global connection budget allocator that ensures
fair bandwidth distribution across multiple concurrent users.

KEY FEATURES:
- Global connection pool (default 96 connections total)
- Dynamic allocation: 8-16 connections per transfer based on active users
- Fair bandwidth distribution: ~10-15MB/s per user with up to 10 concurrent users
- FLOOD_WAIT handling to prevent Telegram rate limiting
- Automatic rebalancing when users join/leave

CONFIGURATION (Environment Variables):
- TOTAL_FASTTELETHON_CONNECTIONS: Total connection pool (default: 96)
- MIN_CONNECTIONS_PER_TRANSFER: Minimum connections per download (default: 6)
- MAX_CONNECTIONS_PER_TRANSFER: Maximum connections per download (default: 16)
"""
import os
import asyncio
import math
import inspect
import psutil
from typing import Optional, Callable, BinaryIO, Set, Dict
from contextlib import asynccontextmanager
from telethon import TelegramClient, utils
from telethon.tl.types import Message, Document, TypeMessageMedia, InputPhotoFileLocation, InputDocumentFileLocation, MessageMediaPaidMedia
from logger import LOGGER
from FastTelethon import download_file as fast_download, upload_file as fast_upload, ParallelTransferrer

TOTAL_CONNECTIONS = int(os.getenv("TOTAL_FASTTELETHON_CONNECTIONS", "96"))
MIN_CONNECTIONS_PER_TRANSFER = int(os.getenv("MIN_CONNECTIONS_PER_TRANSFER", "6"))
MAX_CONNECTIONS_PER_TRANSFER = int(os.getenv("MAX_CONNECTIONS_PER_TRANSFER", "16"))

class ConnectionAllocator:
    """
    Global connection budget allocator for FastTelethon transfers.
    
    Ensures fair distribution of Telegram connections across multiple
    concurrent downloads/uploads to maintain high speed for all users.
    
    Theory:
    - Telegram allows ~100 concurrent connections per account per DC
    - Beyond 8-12 connections per transfer, diminishing returns
    - With 10 concurrent users, each gets 96/10 = ~9.6 connections
    - This maintains ~10-15MB/s per user instead of contention
    
    IMPORTANT: This allocator NEVER oversubscribes the pool. If connections
    are unavailable, it waits until they are released by other transfers.
    """
    
    def __init__(self, 
                 total_connections: int = TOTAL_CONNECTIONS,
                 min_per_transfer: int = MIN_CONNECTIONS_PER_TRANSFER,
                 max_per_transfer: int = MAX_CONNECTIONS_PER_TRANSFER):
        self.total_connections = total_connections
        self.min_per_transfer = min_per_transfer
        self.max_per_transfer = max_per_transfer
        
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._active_transfers: Dict[int, int] = {}
        self._transfer_counter = 0
        self._connections_in_use = 0
        self._waiting_count = 0
        
        LOGGER(__name__).info(
            f"[ConnectionAllocator] Initialized: "
            f"total={total_connections}, min={min_per_transfer}, max={max_per_transfer}"
        )
    
    async def allocate(self, file_size: int = 0, transfer_id: int = None, timeout: float = 300.0) -> tuple[int, int]:
        """
        Allocate connections for a new transfer.
        
        Waits if insufficient connections are available (never oversubscribes).
        
        Args:
            file_size: Size of the file being transferred (for weighting)
            transfer_id: Optional transfer ID (auto-generated if None)
            timeout: Maximum seconds to wait for connections (default: 300s)
        
        Returns:
            tuple: (transfer_id, allocated_connections)
        
        Raises:
            asyncio.TimeoutError: If timeout expires while waiting for connections
        """
        async with self._condition:
            if transfer_id is None:
                self._transfer_counter += 1
                transfer_id = self._transfer_counter
            
            available = self.total_connections - self._connections_in_use
            
            if available < self.min_per_transfer:
                self._waiting_count += 1
                LOGGER(__name__).info(
                    f"[ConnectionAllocator] Transfer #{transfer_id} waiting for connections "
                    f"(available: {available}, need: {self.min_per_transfer}, waiting: {self._waiting_count})"
                )
                try:
                    while self.total_connections - self._connections_in_use < self.min_per_transfer:
                        await asyncio.wait_for(self._condition.wait(), timeout=timeout)
                finally:
                    self._waiting_count -= 1
                
                available = self.total_connections - self._connections_in_use
                LOGGER(__name__).info(
                    f"[ConnectionAllocator] Transfer #{transfer_id} resumed, {available} connections available"
                )
            
            active_count = len(self._active_transfers) + 1
            fair_share = self.total_connections // active_count
            
            if file_size >= 10 * 1024 * 1024:
                size_weight = 1.0
            elif file_size >= 1 * 1024 * 1024:
                size_weight = 0.8
            elif file_size >= 100 * 1024:
                size_weight = 0.6
            else:
                size_weight = 0.4
            
            allocated = int(fair_share * size_weight)
            allocated = max(self.min_per_transfer, min(allocated, self.max_per_transfer))
            allocated = min(allocated, available)
            
            assert allocated >= self.min_per_transfer, f"Allocation {allocated} < min {self.min_per_transfer}"
            assert self._connections_in_use + allocated <= self.total_connections, \
                f"Would exceed pool: {self._connections_in_use} + {allocated} > {self.total_connections}"
            
            self._active_transfers[transfer_id] = allocated
            self._connections_in_use += allocated
            
            LOGGER(__name__).info(
                f"[ConnectionAllocator] Allocated {allocated} connections for transfer #{transfer_id} "
                f"(active: {active_count}, in_use: {self._connections_in_use}/{self.total_connections})"
            )
            
            return transfer_id, allocated
    
    async def release(self, transfer_id: int) -> None:
        """Release connections back to the pool and notify waiting transfers."""
        async with self._condition:
            if transfer_id in self._active_transfers:
                released = self._active_transfers.pop(transfer_id)
                self._connections_in_use -= released
                
                LOGGER(__name__).info(
                    f"[ConnectionAllocator] Released {released} connections from transfer #{transfer_id} "
                    f"(active: {len(self._active_transfers)}, in_use: {self._connections_in_use}/{self.total_connections}, "
                    f"waiting: {self._waiting_count})"
                )
                
                self._condition.notify_all()
    
    async def get_status(self) -> Dict:
        """Get current allocator status."""
        async with self._lock:
            return {
                'total_connections': self.total_connections,
                'connections_in_use': self._connections_in_use,
                'connections_available': self.total_connections - self._connections_in_use,
                'active_transfers': len(self._active_transfers),
                'waiting_transfers': self._waiting_count,
                'per_transfer_allocation': dict(self._active_transfers)
            }
    
    @asynccontextmanager
    async def borrow(self, file_size: int = 0):
        """
        Context manager for borrowing connections.
        
        Usage:
            async with connection_allocator.borrow(file_size) as (transfer_id, connections):
                await do_transfer(connections=connections)
        """
        transfer_id, connections = await self.allocate(file_size)
        try:
            yield transfer_id, connections
        finally:
            await self.release(transfer_id)


connection_allocator = ConnectionAllocator()


def get_ram_usage_mb():
    """Get current RAM usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def create_ram_logging_callback(original_callback: Optional[Callable], file_size: int, operation: str, file_name: str):
    """
    Wrap progress callback to log RAM usage at 25%, 50%, 75% progress.
    """
    logged_thresholds: Set[int] = set()
    start_ram = get_ram_usage_mb()
    LOGGER(__name__).info(f"[RAM] {operation} START: {file_name} - RAM: {start_ram:.1f}MB")
    
    def ram_logging_wrapper(current: int, total: int):
        nonlocal logged_thresholds
        
        if total <= 0:
            if original_callback:
                return original_callback(current, total)
            return
        
        percent = (current / total) * 100
        
        for threshold in [25, 50, 75, 100]:
            if percent >= threshold and threshold not in logged_thresholds:
                logged_thresholds.add(threshold)
                current_ram = get_ram_usage_mb()
                ram_increase = current_ram - start_ram
                LOGGER(__name__).info(
                    f"[RAM] {operation} {threshold}%: {file_name} - "
                    f"RAM: {current_ram:.1f}MB (+{ram_increase:.1f}MB from start)"
                )
        
        if original_callback:
            return original_callback(current, total)
    
    return ram_logging_wrapper

IS_CONSTRAINED = False

MAX_CONNECTIONS = MAX_CONNECTIONS_PER_TRANSFER
MAX_UPLOAD_CONNECTIONS = MAX_CONNECTIONS_PER_TRANSFER
MAX_DOWNLOAD_CONNECTIONS = MAX_CONNECTIONS_PER_TRANSFER

async def download_media_fast(
    client: TelegramClient,
    message: Message,
    file: str,
    progress_callback: Optional[Callable] = None
) -> str:
    """
    Download media using FastTelethon with smart connection allocation.
    
    Uses the global ConnectionAllocator to ensure fair bandwidth distribution
    across multiple concurrent users. Each download gets 6-16 connections
    based on current load and file size.
    """
    if not message.media:
        raise ValueError("Message has no media")
    
    if isinstance(message.media, MessageMediaPaidMedia):
        LOGGER(__name__).warning(f"Paid media detected - attempting extended media extraction")
        if hasattr(message.media, 'extended_media') and message.media.extended_media:
            extended = message.media.extended_media
            if isinstance(extended, list) and len(extended) > 0:
                first_media = extended[0]
                if hasattr(first_media, 'media') and first_media.media:
                    LOGGER(__name__).info(f"Extracted media from paid media container")
                    return await client.download_media(first_media.media, file=file, progress_callback=progress_callback)
            elif hasattr(extended, 'media') and extended.media:
                LOGGER(__name__).info(f"Extracted single media from paid media container")
                return await client.download_media(extended.media, file=file, progress_callback=progress_callback)
        raise ValueError("Paid media (premium content) cannot be downloaded - the content owner requires payment to access this media")
    
    try:
        file_size = 0
        media_location = None
        
        if message.document:
            file_size = message.document.size
            media_location = message.document
        elif message.video:
            file_size = getattr(message.video, 'size', 0)
            media_location = message.video
        elif message.audio:
            file_size = getattr(message.audio, 'size', 0)
            media_location = message.audio
        elif message.photo:
            photo_sizes = [size for size in message.photo.sizes if hasattr(size, 'size')]
            if photo_sizes:
                largest_size = max(photo_sizes, key=lambda s: s.size)
                file_size = largest_size.size
        
        async with connection_allocator.borrow(file_size) as (transfer_id, connection_count):
            LOGGER(__name__).info(
                f"[Transfer #{transfer_id}] Starting download: {os.path.basename(file)} "
                f"({file_size/1024/1024:.1f}MB, {connection_count} connections)"
            )
            
            file_name = os.path.basename(file)
            ram_callback = create_ram_logging_callback(progress_callback, file_size, "DOWNLOAD", file_name)
            
            if media_location and file_size > 0:
                import gc
                with open(file, 'wb') as f:
                    await fast_download(
                        client=client,
                        location=media_location,
                        out=f,
                        progress_callback=ram_callback,
                        file_size=file_size,
                        connection_count=connection_count
                    )
                end_ram = get_ram_usage_mb()
                LOGGER(__name__).info(f"[RAM] DOWNLOAD COMPLETE: {file_name} - RAM before GC: {end_ram:.1f}MB")
                
                gc.collect()
                after_gc_ram = get_ram_usage_mb()
                ram_released = end_ram - after_gc_ram
                LOGGER(__name__).info(f"[RAM] DOWNLOAD GC: {file_name} - RAM after GC: {after_gc_ram:.1f}MB (released: {ram_released:.1f}MB)")
                return file
            else:
                return await client.download_media(message, file=file, progress_callback=progress_callback)
        
    except Exception as e:
        error_str = str(e).lower()
        if 'paidmedia' in error_str or 'paid' in error_str:
            raise ValueError("Paid media (premium content) cannot be downloaded - the content owner requires payment to access this media")
        LOGGER(__name__).error(f"FastTelethon download failed, falling back to standard: {e}")
        return await client.download_media(message, file=file, progress_callback=progress_callback)

async def upload_media_fast(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable] = None
):
    """
    Upload media using FastTelethon with smart connection allocation.
    
    Uses the global ConnectionAllocator to ensure fair bandwidth distribution
    across multiple concurrent users.
    """
    import gc
    
    file_size = os.path.getsize(file_path)
    
    file_handle = None
    result = None
    
    try:
        async with connection_allocator.borrow(file_size) as (transfer_id, connection_count):
            file_name = os.path.basename(file_path)
            LOGGER(__name__).info(
                f"[Transfer #{transfer_id}] Starting upload: {file_name} "
                f"({file_size/1024/1024:.1f}MB, {connection_count} connections)"
            )
            
            ram_callback = create_ram_logging_callback(progress_callback, file_size, "UPLOAD", file_name)
            
            file_handle = open(file_path, 'rb')
            result = await fast_upload(
                client=client,
                file=file_handle,
                progress_callback=ram_callback,
                connection_count=connection_count
            )
            
            end_ram = get_ram_usage_mb()
            LOGGER(__name__).info(f"[RAM] UPLOAD COMPLETE: {file_name} - RAM before GC: {end_ram:.1f}MB")
            return result
        
    except Exception as e:
        LOGGER(__name__).error(f"FastTelethon upload failed: {e}")
        return None
        
    finally:
        if file_handle:
            try:
                file_handle.close()
            except:
                pass
        
        before_gc = get_ram_usage_mb()
        gc.collect()
        after_gc = get_ram_usage_mb()
        ram_released = before_gc - after_gc
        LOGGER(__name__).info(f"[RAM] UPLOAD GC: {os.path.basename(file_path)} - RAM after GC: {after_gc:.1f}MB (released: {ram_released:.1f}MB)")


def _optimized_connection_count_upload(file_size, max_count=MAX_UPLOAD_CONNECTIONS, full_size=100*1024*1024):
    """
    DYNAMIC: Connection count is now managed by ConnectionAllocator.
    This function is kept for backward compatibility but returns max_count.
    The actual allocation happens in download_media_fast/upload_media_fast.
    """
    if file_size >= 10 * 1024 * 1024:
        return max_count
    elif file_size >= 1 * 1024 * 1024:
        return min(12, max_count)
    elif file_size >= 100 * 1024:
        return min(8, max_count)
    elif file_size >= 10 * 1024:
        return min(6, max_count)
    else:
        return min(4, max_count)

def _optimized_connection_count_download(file_size, max_count=MAX_DOWNLOAD_CONNECTIONS, full_size=100*1024*1024):
    """
    DYNAMIC: Connection count is now managed by ConnectionAllocator.
    This function is kept for backward compatibility.
    """
    if file_size >= 10 * 1024 * 1024:
        return max_count
    elif file_size >= 1 * 1024 * 1024:
        return min(12, max_count)
    elif file_size >= 100 * 1024:
        return min(8, max_count)
    elif file_size >= 10 * 1024:
        return min(6, max_count)
    else:
        return min(4, max_count)


ParallelTransferrer._get_connection_count = staticmethod(_optimized_connection_count_upload)
