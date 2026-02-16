"""
Error Handling with Retry Logic and Circuit Breakers
Provides resilient error handling for API calls and database operations
"""

import logging
import asyncio
import time
import threading
from typing import Callable, Any, Optional, TypeVar, Tuple
from enum import Enum
from datetime import datetime, timedelta
from functools import wraps
from monitoring import set_circuit_state

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker pattern for API calls (thread-safe)"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60, 
                 expected_exception: type = Exception):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitState.CLOSED
        self._lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker"""
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                else:
                    raise Exception("Circuit breaker is OPEN. Service unavailable.")
        
        try:
            result = func(*args, **kwargs)
            with self._lock:
                self._on_success()
            return result
        except self.expected_exception:
            with self._lock:
                self._on_failure()
            raise
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute async function with circuit breaker"""
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                else:
                    raise Exception("Circuit breaker is OPEN. Service unavailable.")
        
        try:
            result = await func(*args, **kwargs)
            with self._lock:
                self._on_success()
            return result
        except self.expected_exception:
            with self._lock:
                self._on_failure()
            raise
    
    def _on_success(self):
        """Reset on successful call"""
        self.failure_count = 0
        self.state = CircuitState.CLOSED
        # Service name is optional; populated for global breakers below.
        service = getattr(self, "service_name", None)
        if service:
            set_circuit_state(service, self.state.value, self.failure_count)
    
    def _on_failure(self):
        """Handle failure"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning("[CIRCUIT] Opened after %s failures", self.failure_count)
        service = getattr(self, "service_name", None)
        if service:
            set_circuit_state(service, self.state.value, self.failure_count)
    
    def _should_attempt_reset(self) -> bool:
        """Check if we should try to reset"""
        if self.last_failure_time is None:
            return True
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        return elapsed >= self.timeout


def retry_with_backoff(max_retries: int = 3, initial_delay: float = 1.0, 
                      max_delay: float = 60.0, exponential_base: float = 2.0,
                      exceptions: Tuple = (Exception,)):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff
        exceptions: Tuple of exceptions to catch
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        logger.error(f"[RETRY] {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    logger.warning(f"[RETRY] {func.__name__} attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {delay:.2f}s...")
                    await asyncio.sleep(delay)
                    delay = min(delay * exponential_base, max_delay)
            
            # Should never reach here
            raise Exception("Retry logic error")
        
        return wrapper
    return decorator


# Global circuit breakers for different services
binance_circuit = CircuitBreaker(failure_threshold=5, timeout=60)
binance_circuit.service_name = "binance"
bybit_circuit = CircuitBreaker(failure_threshold=5, timeout=60)
bybit_circuit.service_name = "bybit"
database_circuit = CircuitBreaker(failure_threshold=10, timeout=30)
database_circuit.service_name = "database"


def get_circuit_breaker_states() -> dict:
    return {
        "binance": {
            "state": binance_circuit.state.value,
            "failure_count": binance_circuit.failure_count,
            "threshold": binance_circuit.failure_threshold,
            "timeout": binance_circuit.timeout,
        },
        "bybit": {
            "state": bybit_circuit.state.value,
            "failure_count": bybit_circuit.failure_count,
            "threshold": bybit_circuit.failure_threshold,
            "timeout": bybit_circuit.timeout,
        },
        "database": {
            "state": database_circuit.state.value,
            "failure_count": database_circuit.failure_count,
            "threshold": database_circuit.failure_threshold,
            "timeout": database_circuit.timeout,
        },
    }


def isolate_errors(func: Callable) -> Callable:
    """Decorator to isolate errors - prevents one failure from affecting others"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"[ISOLATE] Error in {func.__name__}: {e}", exc_info=True)
            # Return None or empty result instead of raising
            return None
    return wrapper


class ErrorHandler:
    """Centralized error handling"""
    
    @staticmethod
    async def handle_api_error(service: str, error: Exception, context: str = ""):
        """Handle API errors with logging"""
        logger.error(f"[API_ERROR] {service}: {error} {context}")
        # Could add metrics, alerting here
    
    @staticmethod
    async def handle_database_error(error: Exception, query: str = ""):
        """Handle database errors"""
        logger.error(f"[DB_ERROR] {error} Query: {query[:100]}")
        # Could add retry logic, connection reset here
    
    @staticmethod
    def should_retry(error: Exception, attempt: int, max_attempts: int) -> bool:
        """Determine if error should be retried"""
        if attempt >= max_attempts:
            return False
        
        # Retry on transient errors
        retryable_errors = (
            ConnectionError,
            TimeoutError,
            asyncio.TimeoutError,
        )
        
        error_str = str(error).lower()
        retryable_strings = ('timeout', 'connection', 'network', 'temporary')
        
        if isinstance(error, retryable_errors):
            return True
        
        if any(s in error_str for s in retryable_strings):
            return True
        
        return False
