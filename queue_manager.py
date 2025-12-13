import os
import asyncio
from datetime import datetime
from typing import Dict, Set, Optional, Tuple, Union
from logger import LOGGER

from database_sqlite import db

from config import PyroConf

class DownloadManager:
    """Simplified download manager - just tracks active downloads and concurrency limits"""
    
    def __init__(self, max_concurrent: int = 3):
        self.max_concurrent = max_concurrent
        
        self.active_downloads: Set[int] = set()
        self._active_download_refs: Dict[int, int] = {}
        self.active_tasks: Dict[int, asyncio.Task] = {}
        
        self.user_cooldowns: Dict[int, float] = {}
        
        self._lock = asyncio.Lock()
        
        LOGGER(__name__).info(f"Download Manager initialized: {max_concurrent} concurrent max")
    
    def add_active_download(self, user_id: int) -> None:
        """
        Add user to active_downloads with reference counting.
        Multiple calls increment the reference count - user is only removed when count reaches 0.
        """
        self.active_downloads.add(user_id)
        self._active_download_refs[user_id] = self._active_download_refs.get(user_id, 0) + 1
        LOGGER(__name__).debug(f"Active download ref added for user {user_id}: count={self._active_download_refs[user_id]}")
    
    def remove_active_download(self, user_id: int) -> None:
        """
        Remove user from active_downloads with reference counting.
        Decrements the reference count - user is only removed from set when count reaches 0.
        """
        if user_id in self._active_download_refs:
            self._active_download_refs[user_id] -= 1
            if self._active_download_refs[user_id] <= 0:
                self.active_downloads.discard(user_id)
                del self._active_download_refs[user_id]
                LOGGER(__name__).debug(f"Active download removed for user {user_id}: no more refs")
            else:
                LOGGER(__name__).debug(f"Active download ref removed for user {user_id}: count={self._active_download_refs[user_id]}")
        else:
            self.active_downloads.discard(user_id)
            LOGGER(__name__).debug(f"Active download removed for user {user_id}: was not ref-counted")
    
    async def start_processor(self):
        """No-op for compatibility"""
        LOGGER(__name__).info("Download manager ready")
    
    async def stop_processor(self):
        """No-op for compatibility"""
        pass
    
    async def start_download(
        self, 
        user_id: int, 
        download_coro, 
        message,
        post_url: str,
        is_premium: bool = False
    ) -> Tuple[bool, Optional[str]]:
        """Start download immediately or reject if user is busy or server is at capacity"""
        async with self._lock:
            if user_id in self.user_cooldowns:
                current_time = datetime.now().timestamp()
                can_download_at = self.user_cooldowns[user_id]
                
                if current_time < can_download_at:
                    remaining = int(can_download_at - current_time)
                    minutes = remaining // 60
                    seconds = remaining % 60
                    
                    tier_name = "PREMIUM" if is_premium else "FREE"
                    time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
                    
                    return False, (
                        f"Download Cooldown Active!\n\n"
                        f"{tier_name} user\n"
                        f"Wait: {time_str}\n\n"
                        f"You can download again after the cooldown ends."
                    )
            
            if user_id in self.active_downloads:
                return False, (
                    "You already have a download in progress!\n\n"
                    "Please wait for it to complete.\n\n"
                    "Want to download this instead?\n"
                    "Use /canceldownload to cancel the current download."
                )
            
            if len(self.active_downloads) >= self.max_concurrent:
                return False, (
                    f"Server is busy!\n\n"
                    f"Active Downloads: {len(self.active_downloads)}/{self.max_concurrent}\n\n"
                    f"Please try again in a few minutes."
                )
            
            self.add_active_download(user_id)
            task = asyncio.create_task(self._execute_download(user_id, download_coro, message))
            self.active_tasks[user_id] = task
            
            return True, None
    
    async def _execute_download(self, user_id: int, download_coro, message):
        import gc
        try:
            from memory_monitor import memory_monitor
            from helpers.session_manager import session_manager
            
            if user_id in session_manager.last_activity:
                from time import time
                session_manager.last_activity[user_id] = time()
                LOGGER(__name__).debug(f"Updated last_activity for user {user_id} at download start")
            
            memory_monitor.log_memory_snapshot("Download Started", f"User {user_id} | Active: {len(self.active_downloads)}", silent=True)
            
            try:
                await download_coro
            except asyncio.CancelledError:
                LOGGER(__name__).info(f"Download cancelled for user {user_id}")
                raise
            
            memory_monitor.log_memory_snapshot("Download Completed", f"User {user_id} | Active: {len(self.active_downloads)}", silent=True)
        except asyncio.CancelledError:
            LOGGER(__name__).info(f"Download task cancelled for user {user_id}")
            try:
                await message.reply("Download cancelled")
            except:
                pass
        except Exception as e:
            LOGGER(__name__).error(f"Download error for user {user_id}: {e}")
            import traceback
            LOGGER(__name__).error(f"Full traceback: {traceback.format_exc()}")
            try:
                await message.reply(f"Download failed: {str(e)}")
            except:
                pass
        finally:
            async with self._lock:
                self.remove_active_download(user_id)
                self.active_tasks.pop(user_id, None)
            
            try:
                from helpers.session_manager import session_manager
                from helpers.transfer import get_ram_usage_mb
                
                before_cleanup = get_ram_usage_mb()
                await session_manager.remove_session(user_id)
                
                gc.collect()
                after_cleanup = get_ram_usage_mb()
                ram_released = before_cleanup - after_cleanup
                
                LOGGER(__name__).info(
                    f"[RAM] SESSION CLEANUP: User {user_id} - "
                    f"RAM after cleanup: {after_cleanup:.1f}MB (released: {ram_released:.1f}MB)"
                )
            except Exception as e:
                LOGGER(__name__).debug(f"Could not cleanup session after download: {e}")
                gc.collect()
            
            LOGGER(__name__).info(f"Download completed for user {user_id}. Active: {len(self.active_downloads)}. Session+GC cleanup done.")
            
            try:
                user_type = db.get_user_type(user_id)
                is_premium = user_type in ['paid', 'admin']
                delay = PyroConf.PREMIUM_DOWNLOAD_DELAY if is_premium else PyroConf.FREE_DOWNLOAD_DELAY
                
                async with self._lock:
                    self.user_cooldowns[user_id] = datetime.now().timestamp() + delay
                
                LOGGER(__name__).info(
                    f"Download cooldown set for user {user_id} ({user_type}): {delay}s until next download allowed"
                )
            except Exception as e:
                LOGGER(__name__).warning(f"Could not set download cooldown for user {user_id}: {e}")
    
    async def get_status(self, user_id: int) -> str:
        async with self._lock:
            if user_id in self.active_downloads:
                return (
                    f"Your download is currently active!\n\n"
                    f"Active Downloads: {len(self.active_downloads)}/{self.max_concurrent}"
                )
            
            return (
                f"No active downloads\n\n"
                f"Active Downloads: {len(self.active_downloads)}/{self.max_concurrent}\n\n"
                f"Send a download link to get started!"
            )
    
    async def get_server_status(self) -> str:
        async with self._lock:
            return (
                f"Download System Status\n"
                f"-------------------\n"
                f"Active Downloads: {len(self.active_downloads)}/{self.max_concurrent}"
            )
    
    async def cancel_user_download(self, user_id: int) -> Tuple[bool, str]:
        async with self._lock:
            if user_id in self.active_downloads:
                task = self.active_tasks.get(user_id)
                if task and not task.done():
                    task.cancel()
                self.remove_active_download(user_id)
                self.active_tasks.pop(user_id, None)
                return True, "Active download cancelled!"
            
            return False, "No active download found."
    
    async def cancel_all_downloads(self) -> int:
        async with self._lock:
            cancelled = 0
            
            for task in self.active_tasks.values():
                if not task.done():
                    task.cancel()
                    cancelled += 1
            
            self.active_downloads.clear()
            self._active_download_refs.clear()
            self.active_tasks.clear()
            
            LOGGER(__name__).info(f"Cancelled all downloads: {cancelled} total")
            return cancelled
    
    async def sweep_stale_items(self, max_age_minutes: int = 60) -> Dict[str, int]:
        """Remove orphaned tasks and expired cooldowns to prevent memory leaks."""
        async with self._lock:
            import gc
            
            task_cleanup_count = 0
            cooldown_cleanup_count = 0
            current_time = datetime.now().timestamp()
            
            for user_id, task in list(self.active_tasks.items()):
                if task.done() or task.cancelled():
                    self.active_tasks.pop(user_id, None)
                    self.remove_active_download(user_id)
                    task_cleanup_count += 1
                    LOGGER(__name__).warning(f"Cleaned up orphaned task for user {user_id}")
            
            expired_cooldowns = [
                user_id for user_id, expire_time in self.user_cooldowns.items()
                if current_time >= expire_time
            ]
            for user_id in expired_cooldowns:
                del self.user_cooldowns[user_id]
                cooldown_cleanup_count += 1
            
            if cooldown_cleanup_count > 0:
                LOGGER(__name__).debug(f"Sweep: cleaned {cooldown_cleanup_count} expired cooldowns")
            
            if task_cleanup_count > 0 or cooldown_cleanup_count > 0:
                LOGGER(__name__).info(f"Sweep: cleaned {task_cleanup_count} orphaned tasks, {cooldown_cleanup_count} expired cooldowns")
                gc.collect()
            
            return {
                'stale_items': 0,
                'orphaned_tasks': task_cleanup_count,
                'expired_cooldowns': cooldown_cleanup_count
            }

IS_CONSTRAINED = bool(
    os.getenv('RENDER') or 
    os.getenv('RENDER_EXTERNAL_URL') or 
    os.getenv('REPLIT_DEPLOYMENT') or 
    os.getenv('REPL_ID')
)

MAX_CONCURRENT = 10 if IS_CONSTRAINED else 20

download_manager = DownloadManager(max_concurrent=MAX_CONCURRENT)
