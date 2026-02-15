"""
Database Connection Pooling
Replaces per-query connections with a connection pool for better performance
"""

import logging
import threading
from typing import Optional
from queue import Queue, Empty
import time

logger = logging.getLogger(__name__)

# Import database modules
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    import sqlite3
    SQLITE3_AVAILABLE = True
except ImportError:
    SQLITE3_AVAILABLE = False


class ConnectionPool:
    """Database connection pool"""
    
    def __init__(self, database_url: Optional[str] = None, db_path: str = "bot_data.db", 
                 min_connections: int = 2, max_connections: int = 10):
        self.database_url = database_url
        self.db_path = db_path
        self.use_postgres = database_url is not None
        self.min_connections = min_connections
        self.max_connections = max_connections
        
        self._pool = None
        self._lock = threading.Lock()
        self._initialized = False
        
    def initialize(self):
        """Initialize the connection pool"""
        if self._initialized:
            return
        
        with self._lock:
            if self._initialized:
                return
            
            try:
                if self.use_postgres and PSYCOPG2_AVAILABLE:
                    # PostgreSQL connection pool
                    self._pool = psycopg2.pool.ThreadedConnectionPool(
                        self.min_connections,
                        self.max_connections,
                        self.database_url
                    )
                    logger.info(f"[POOL] PostgreSQL pool initialized: {self.min_connections}-{self.max_connections} connections")
                elif not self.use_postgres and SQLITE3_AVAILABLE:
                    # SQLite doesn't support true pooling, but we'll use a queue
                    self._pool = Queue(maxsize=self.max_connections)
                    # Pre-populate queue with connections
                    for _ in range(self.min_connections):
                        conn = sqlite3.connect(self.db_path, check_same_thread=False)
                        conn.row_factory = sqlite3.Row
                        self._pool.put(conn)
                    logger.info(f"[POOL] SQLite connection queue initialized: {self.min_connections}-{self.max_connections} connections")
                else:
                    logger.error("[POOL] No database driver available")
                    return
                
                self._initialized = True
            except Exception as e:
                logger.error(f"[POOL] Initialization failed: {e}")
                raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        if not self._initialized:
            self.initialize()

        if self.use_postgres:
            # Handle transient pool/network issues with short retries.
            last_error = None
            for attempt in range(3):
                try:
                    if self._pool:
                        conn = self._pool.getconn()
                    else:
                        conn = psycopg2.connect(self.database_url)
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    # psycopg2 starts a transaction even for SELECT when autocommit=False.
                    # Reset state so later set_session/autocommit changes won't fail.
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    return conn
                except Exception as e:
                    last_error = e
                    if not _is_transient_db_error(e) or attempt >= 2:
                        raise
                    time.sleep(0.15 * (2 ** attempt))
            if last_error:
                raise last_error
            raise RuntimeError("Failed to get PostgreSQL connection")
        else:
            # SQLite - get from queue or create new
            try:
                conn = self._pool.get_nowait()
                # Check if connection is still valid
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                except Exception as e:
                    # Connection is dead, create new one
                    logger.debug(f"[POOL] SQLite connection invalid, creating new: {e}")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = sqlite3.connect(self.db_path, check_same_thread=False)
                    conn.row_factory = sqlite3.Row
                return conn
            except Empty:
                # Queue is empty, create new connection
                logger.debug("[POOL] SQLite queue empty, creating new connection")
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                return conn
            except Exception as e:
                # Unexpected error, create fresh connection
                logger.warning(f"[POOL] Error getting SQLite connection: {e}")
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                return conn
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if not self._initialized:
            return
        
        if self.use_postgres:
            if self._pool:
                try:
                    # Always return clean PostgreSQL connection to pool.
                    conn.rollback()
                except Exception:
                    pass
                self._pool.putconn(conn)
        else:
            # SQLite - return to queue if not full
            try:
                self._pool.put_nowait(conn)
            except Exception:
                # Queue is full, close connection
                try:
                    conn.close()
                except Exception as e:
                    logger.debug(f"[POOL] Error closing overflow connection: {e}")
    
    def close_all(self):
        """Close all connections in the pool"""
        if not self._initialized:
            return
        
        with self._lock:
            if self.use_postgres:
                if self._pool:
                    self._pool.closeall()
            else:
                # SQLite - close all connections in queue
                while not self._pool.empty():
                    try:
                        conn = self._pool.get_nowait()
                        conn.close()
                    except Empty:
                        break
            
            self._initialized = False
            logger.info("[POOL] All connections closed")
    
    def get_pool_stats(self) -> dict:
        """Get pool statistics"""
        if not self._initialized:
            return {'initialized': False}
        
        if self.use_postgres:
            return {
                'initialized': True,
                'type': 'postgresql',
                'min_connections': self.min_connections,
                'max_connections': self.max_connections
            }
        else:
            return {
                'initialized': True,
                'type': 'sqlite',
                'queue_size': self._pool.qsize(),
                'max_connections': self.max_connections
            }


# Global pool instance (will be initialized in bot.py)
_connection_pool: Optional[ConnectionPool] = None

# Store database configuration for fallback connections (avoids circular import)
_database_url: Optional[str] = None
_db_path: str = "bot_data.db"


def init_connection_pool(database_url: Optional[str] = None, db_path: str = "bot_data.db",
                        min_connections: int = 2, max_connections: int = 10):
    """Initialize the global connection pool"""
    global _connection_pool, _database_url, _db_path
    
    # Store configuration for fallback use
    _database_url = database_url
    _db_path = db_path
    
    _connection_pool = ConnectionPool(database_url, db_path, min_connections, max_connections)
    _connection_pool.initialize()
    return _connection_pool


def get_pooled_connection():
    """Get a connection from the global pool"""
    if _connection_pool:
        return _connection_pool.get_connection()
    else:
        # Fallback to direct connection (no circular import)
        if _database_url and PSYCOPG2_AVAILABLE:
            return psycopg2.connect(_database_url)
        elif SQLITE3_AVAILABLE:
            conn = sqlite3.connect(_db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
        else:
            raise RuntimeError("No database driver available and connection pool not initialized")


def _is_transient_db_error(error: Exception) -> bool:
    error_text = str(error).lower()
    transient_hints = (
        "timeout",
        "temporar",
        "connection reset",
        "connection refused",
        "connection aborted",
        "too many clients",
        "could not connect",
        "server closed the connection",
        "ssl syscall error",
        "broken pipe",
    )
    return any(h in error_text for h in transient_hints)


def return_pooled_connection(conn):
    """Return a connection to the global pool"""
    if _connection_pool:
        _connection_pool.return_connection(conn)
    else:
        # Fallback - just close it
        try:
            conn.close()
        except Exception:
            pass
