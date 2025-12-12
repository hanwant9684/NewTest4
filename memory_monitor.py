import os
import psutil
import asyncio
import traceback
from datetime import datetime
from logger import LOGGER

class MemoryMonitor:
    def __init__(self):
        self.process = psutil.Process()
        self.logger = LOGGER(__name__)
        self.last_memory_mb = 0
        self.memory_threshold_mb = 400  # Alert if memory exceeds 400MB on 512MB plan
        self.spike_threshold_mb = 50  # Alert if memory increases by 50MB suddenly
        # Use collections.deque for memory-efficient circular buffer
        from collections import deque
        self.operation_history = deque(maxlen=20)  # Auto-discards old items, saves RAM
        self.max_history = 20
        
        # Dedicated memory log file for debugging OOM issues on Render
        self.memory_log_file = "memory_debug.log"
        self._init_memory_log()
    
    def _init_memory_log(self):
        """Initialize dedicated memory log file"""
        try:
            # Check if file exists (indicates recovery from crash)
            recovering_from_crash = os.path.exists(self.memory_log_file)
            
            if recovering_from_crash:
                # Append recovery message instead of overwriting
                with open(self.memory_log_file, 'a') as f:
                    f.write("\n\n")
                    f.write("=" * 80 + "\n")
                    f.write(f"🔄 BOT RESTARTED at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("Previous session may have crashed - check logs above\n")
                    f.write("=" * 80 + "\n\n")
            else:
                # Write header to new memory log file
                with open(self.memory_log_file, 'w') as f:
                    f.write("=" * 80 + "\n")
                    f.write("MEMORY DEBUG LOG - Telegram Bot\n")
                    f.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("=" * 80 + "\n\n")
        except Exception as e:
            self.logger.error(f"Failed to initialize memory log file: {e}")
    
    def _write_to_memory_log(self, message, force_write=False):
        """Write critical memory events to dedicated log file.
        Only writes when memory is critical or forced.
        """
        try:
            if not force_write:
                mem = self.get_memory_info()
                if mem['rss_mb'] < 400:
                    return
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(self.memory_log_file, 'a') as f:
                f.write(f"[{timestamp}] {message}\n")
                f.flush()
        except Exception as e:
            self.logger.error(f"Failed to write to memory log: {e}")
        
    def get_memory_info(self):
        memory_info = self.process.memory_info()
        rss_mb = memory_info.rss / 1024 / 1024
        vms_mb = memory_info.vms / 1024 / 1024
        
        system_memory = psutil.virtual_memory()
        system_total_mb = system_memory.total / 1024 / 1024
        system_available_mb = system_memory.available / 1024 / 1024
        system_percent = system_memory.percent
        
        return {
            'rss_mb': round(rss_mb, 2),
            'vms_mb': round(vms_mb, 2),
            'system_total_mb': round(system_total_mb, 2),
            'system_available_mb': round(system_available_mb, 2),
            'system_percent': system_percent
        }
    
    def get_detailed_state(self):
        try:
            from helpers.session_manager import session_manager
            active_sessions = len(session_manager.active_sessions) if hasattr(session_manager, 'active_sessions') else 0
        except:
            active_sessions = 0
        
        try:
            from queue_manager import download_manager
            active_downloads = len(download_manager.active_downloads) if hasattr(download_manager, 'active_downloads') else 0
        except:
            active_downloads = 0
        
        try:
            from database_sqlite import db
            cached_items = len(db.cache.cache) if hasattr(db, 'cache') and hasattr(db.cache, 'cache') else 0
            ad_sessions = db.get_ad_sessions_count() if hasattr(db, 'get_ad_sessions_count') else 0
        except:
            cached_items = 0
            ad_sessions = 0
        
        return {
            'active_sessions': active_sessions,
            'active_downloads': active_downloads,
            'cached_items': cached_items,
            'ad_sessions': ad_sessions,
            'thread_count': self.process.num_threads(),
            'open_files': len(self.process.open_files()) if hasattr(self.process, 'open_files') else 0
        }
    
    def log_memory_snapshot(self, operation="", context="", silent=False):
        """Log memory snapshot. Set silent=True for routine operations."""
        mem = self.get_memory_info()
        state = self.get_detailed_state()
        
        # Store operation history
        snapshot = (
            datetime.now().strftime("%H:%M:%S"),
            operation or '',
            round(mem['rss_mb'], 1),
            context or ''
        )
        self.operation_history.append(snapshot)
        
        # Check for critical memory (near crash)
        if mem['rss_mb'] > 480:
            critical_msg = f"🚨 CRITICAL: {mem['rss_mb']:.0f}MB - Sessions:{state['active_sessions']} DLs:{state['active_downloads']} - {operation}"
            self.logger.error(critical_msg)
            self._write_to_memory_log(critical_msg, force_write=True)
            return mem
        
        # Check for memory spike
        memory_increase = mem['rss_mb'] - self.last_memory_mb
        if memory_increase > self.spike_threshold_mb:
            self.logger.warning(f"⚠️ Memory spike: +{memory_increase:.0f}MB ({mem['rss_mb']:.0f}MB total) - {operation}")
            self._write_to_memory_log(f"Memory spike: +{memory_increase:.0f}MB - {operation}")
        elif mem['rss_mb'] > self.memory_threshold_mb:
            self.logger.warning(f"⚠️ High memory: {mem['rss_mb']:.0f}MB - {operation}")
            self._write_to_memory_log(f"High memory: {mem['rss_mb']:.0f}MB - {operation}")
        elif not silent:
            # Only log if not silent and memory is concerning (>300MB)
            if mem['rss_mb'] > 300:
                self.logger.info(f"Memory: {mem['rss_mb']:.0f}MB - {operation}")
        
        self.last_memory_mb = mem['rss_mb']
        return mem
    
    def log_recent_operations(self):
        if not self.operation_history:
            return
        
        self.logger.info("Recent operations:")
        for idx, op in enumerate(list(self.operation_history)[-10:], 1):
            self.logger.info(f"  {idx}. [{op[0]}] {op[1]} - {op[2]:.0f}MB")
    
    async def log_operation(self, operation_name, func, *args, **kwargs):
        user_id = kwargs.get('user_id', 'unknown')
        context = kwargs.pop('memory_context', '')
        
        mem_before = self.get_memory_info()
        
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            mem_after = self.get_memory_info()
            mem_diff = mem_after['rss_mb'] - mem_before['rss_mb']
            
            # Only log if significant memory change
            if abs(mem_diff) > 20:
                self.logger.warning(f"⚠️ Memory change: {mem_diff:+.0f}MB after {operation_name}")
            
            return result
            
        except Exception as e:
            mem_error = self.get_memory_info()
            self.logger.error(f"❌ {operation_name} failed: {str(e)} (Memory: {mem_error['rss_mb']:.0f}MB)")
            raise
    
    def track_download(self, file_size_mb, user_id):
        self.log_memory_snapshot("Download", f"User {user_id}: {file_size_mb:.0f}MB file", silent=True)
    
    def track_upload(self, file_size_mb, user_id):
        self.log_memory_snapshot("Upload", f"User {user_id}: {file_size_mb:.0f}MB file", silent=True)
    
    def track_session_creation(self, user_id):
        self.log_memory_snapshot("Session", f"User {user_id}", silent=True)
    
    def track_session_cleanup(self, user_id):
        self.log_memory_snapshot("Cleanup", f"User {user_id}", silent=True)
    
    def get_memory_state_for_endpoint(self):
        """Get current memory state for /memory-debug endpoint."""
        mem = self.get_memory_info()
        state = self.get_detailed_state()
        
        response = {
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "memory": {
                "ram_usage_mb": mem['rss_mb'],
                "virtual_memory_mb": mem['vms_mb'],
                "system_available_mb": mem['system_available_mb'],
                "system_percent_used": mem['system_percent']
            },
            "state": {
                "sessions": state['active_sessions'],
                "downloads": state['active_downloads'],
                "cache": state['cached_items'],
                "threads": state['thread_count']
            },
            "status": self._get_memory_status(mem['rss_mb']),
            "recent_ops": [
                {"time": op[0], "op": op[1], "mb": op[2]}
                for op in list(self.operation_history)[-10:]
            ]
        }
        
        self._write_to_memory_log(f"/memory-debug: {mem['rss_mb']:.0f}MB", force_write=True)
        return response
    
    def _get_memory_status(self, rss_mb):
        if rss_mb > 480:
            return "CRITICAL"
        elif rss_mb >= 400:
            return "HIGH"
        elif rss_mb >= 300:
            return "ELEVATED"
        else:
            return "OK"
    
    async def periodic_monitor(self, interval=300):
        while True:
            try:
                await asyncio.sleep(interval)
                mem = self.get_memory_info()
                
                # Only log and act if memory is high
                if mem['rss_mb'] > self.memory_threshold_mb:
                    self.logger.warning(f"⚠️ Periodic check: {mem['rss_mb']:.0f}MB - triggering GC")
                    self._write_to_memory_log(f"Periodic: {mem['rss_mb']:.0f}MB - auto GC")
                    
                    import gc
                    collected = gc.collect()
                    mem_after = self.get_memory_info()
                    freed = mem['rss_mb'] - mem_after['rss_mb']
                    
                    if freed > 5:
                        self.logger.info(f"GC freed {freed:.0f}MB ({collected} objects)")
                else:
                    # Silent tracking - just store in history
                    self.log_memory_snapshot("Periodic", "", silent=True)
                    
            except asyncio.CancelledError:
                self.logger.info("Periodic memory monitor task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Periodic monitor error: {e}")

memory_monitor = MemoryMonitor()
