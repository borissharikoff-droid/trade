"""
Cache Manager with Thread-Safe Operations
Provides LRU cache with TTL and size limits
"""

import logging
import time
import threading
from typing import Dict, List, Optional, Any
from collections import OrderedDict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class LRUCache:
    """Thread-safe LRU cache with TTL"""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict = OrderedDict()
        self._timestamps: Dict = {}
        self._lock = threading.RLock()
    
    def get(self, key: Any) -> Optional[Any]:
        """Get value from cache"""
        with self._lock:
            if key not in self._cache:
                return None
            
            # Check TTL
            if key in self._timestamps:
                ttl = self._timestamps.get(key, {}).get('ttl', self.default_ttl)
                age = time.time() - self._timestamps[key]['time']
                if age > ttl:
                    # Expired
                    del self._cache[key]
                    del self._timestamps[key]
                    return None
            
            # Move to end (most recently used)
            value = self._cache.pop(key)
            self._cache[key] = value
            
            return value
    
    def set(self, key: Any, value: Any, ttl: Optional[int] = None):
        """Set value in cache"""
        with self._lock:
            # Remove if exists
            if key in self._cache:
                del self._cache[key]
            
            # Check size limit
            if len(self._cache) >= self.max_size:
                # Remove oldest (first) item
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
                if oldest_key in self._timestamps:
                    del self._timestamps[oldest_key]
            
            # Add new item
            self._cache[key] = value
            self._timestamps[key] = {
                'time': time.time(),
                'ttl': ttl if ttl is not None else self.default_ttl
            }
    
    def delete(self, key: Any):
        """Delete key from cache"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            if key in self._timestamps:
                del self._timestamps[key]
    
    def clear(self):
        """Clear all cache"""
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()
    
    def cleanup_expired(self):
        """Remove expired entries"""
        with self._lock:
            now = time.time()
            expired_keys = []
            
            for key, timestamp_info in self._timestamps.items():
                age = now - timestamp_info['time']
                if age > timestamp_info['ttl']:
                    expired_keys.append(key)
            
            for key in expired_keys:
                if key in self._cache:
                    del self._cache[key]
                del self._timestamps[key]
            
            if expired_keys:
                logger.debug(f"[CACHE] Cleaned up {len(expired_keys)} expired entries")
    
    def size(self) -> int:
        """Get current cache size"""
        with self._lock:
            return len(self._cache)
    
    def keys(self) -> list:
        """Get all keys"""
        with self._lock:
            return list(self._cache.keys())


class ThreadSafeCache:
    """Thread-safe wrapper for dictionary-based caches"""
    
    def __init__(self):
        self._data: Dict = {}
        self._locks: Dict[int, threading.RLock] = {}
        self._global_lock = threading.RLock()
    
    def _get_lock(self, key: int) -> threading.RLock:
        """Get or create lock for a key"""
        with self._global_lock:
            if key not in self._locks:
                self._locks[key] = threading.RLock()
            return self._locks[key]
    
    def get(self, key: int, default: Any = None) -> Any:
        """Get value with per-key locking"""
        lock = self._get_lock(key)
        with lock:
            return self._data.get(key, default)
    
    def set(self, key: int, value: Any):
        """Set value with per-key locking"""
        lock = self._get_lock(key)
        with lock:
            self._data[key] = value
    
    def delete(self, key: int):
        """Delete key with per-key locking"""
        lock = self._get_lock(key)
        with lock:
            if key in self._data:
                del self._data[key]
    
    def pop(self, key: int, default: Any = None) -> Any:
        """Pop key with per-key locking (like dict.pop)"""
        lock = self._get_lock(key)
        with lock:
            if key in self._data:
                value = self._data[key]
                del self._data[key]
                return value
            return default
    
    def items(self):
        """Get all items (read-only, no locking)"""
        return self._data.items()
    
    def keys(self):
        """Get all keys (read-only, no locking)"""
        return self._data.keys()
    
    def clear(self):
        """Clear all data"""
        with self._global_lock:
            self._data.clear()
            self._locks.clear()
    
    def __contains__(self, key: int) -> bool:
        """Check if key exists"""
        return key in self._data


# Global cache instances
users_cache = ThreadSafeCache()
positions_cache = ThreadSafeCache()  # {user_id: List[Dict]}
price_cache = LRUCache(max_size=500, default_ttl=3)  # 3 seconds TTL for prices
stats_cache = LRUCache(max_size=1000, default_ttl=30)  # 30 seconds TTL for user stats


def invalidate_stats_cache(user_id: int):
    """Invalidate stats cache for a user when positions are closed"""
    stats_cache.delete(user_id)


def cleanup_caches():
    """Periodic cleanup of expired cache entries"""
    price_cache.cleanup_expired()
    stats_cache.cleanup_expired()
    logger.debug(f"[CACHE] Cleanup complete. Price cache: {price_cache.size()}, Stats cache: {stats_cache.size()} entries")
