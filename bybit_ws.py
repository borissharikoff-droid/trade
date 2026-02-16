"""
Bybit WebSocket Client для real-time синхронизации позиций.

Заменяет 30-секундный polling на мгновенные обновления через WebSocket.
Обеспечивает 1:1 синхронизацию между ботом и Bybit.
"""

import asyncio
import json
import hmac
import hashlib
import time
import logging
from typing import Optional, Callable, Dict, Any, List
from datetime import datetime
from execution_config import WS_MAX_RECONNECT_DELAY_SECONDS, WS_PING_INTERVAL_SECONDS
from monitoring import set_ws_health, record_api_call

logger = logging.getLogger(__name__)

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    logger.warning("[WS] websockets library not installed, WebSocket sync disabled")


class BybitWebSocket:
    """
    WebSocket клиент для Bybit Private Stream.
    Подписывается на обновления позиций и ордеров в реальном времени.
    """
    
    # WebSocket URLs
    MAINNET_WS = "wss://stream.bybit.com/v5/private"
    TESTNET_WS = "wss://stream-testnet.bybit.com/v5/private"
    DEMO_WS = "wss://stream-demo.bybit.com/v5/private"
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, demo: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.demo = demo
        
        # Выбираем URL
        if demo:
            self.ws_url = self.DEMO_WS
        elif testnet:
            self.ws_url = self.TESTNET_WS
        else:
            self.ws_url = self.MAINNET_WS
        
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._reconnect_delay = 1
        self._max_reconnect_delay = WS_MAX_RECONNECT_DELAY_SECONDS
        self._ping_interval = WS_PING_INTERVAL_SECONDS
        self._disconnects = 0
        
        # Callbacks
        self._on_position_update: Optional[Callable] = None
        self._on_order_update: Optional[Callable] = None
        self._on_execution_update: Optional[Callable] = None
        self._on_wallet_update: Optional[Callable] = None
        
        # State
        self._authenticated = False
        self._subscribed = False
        self._last_ping = 0
        
        logger.info(f"[WS] Initialized for {'demo' if demo else 'testnet' if testnet else 'mainnet'}: {self.ws_url}")
    
    def _generate_signature(self, expires: int) -> str:
        """Генерирует подпись для аутентификации"""
        param_str = f"GET/realtime{expires}"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    async def connect(self) -> bool:
        """Подключается к WebSocket"""
        if not WEBSOCKETS_AVAILABLE:
            logger.error("[WS] websockets library not available")
            return False
        
        try:
            self.ws = await websockets.connect(
                self.ws_url,
                ping_interval=None,  # Отключаем автопинг, делаем свой
                ping_timeout=30,
                close_timeout=10
            )
            logger.info(f"[WS] Connected to {self.ws_url}")
            self._running = True
            self._reconnect_delay = 1  # Reset delay on successful connect
            set_ws_health(True, self._reconnect_delay, self._disconnects)
            record_api_call("bybit_ws_connect", True)
            return True
        except Exception as e:
            logger.error(f"[WS] Connection failed: {e}")
            set_ws_health(False, self._reconnect_delay, self._disconnects)
            record_api_call("bybit_ws_connect", False)
            return False
    
    async def authenticate(self) -> bool:
        """Аутентификация на WebSocket"""
        if not self.ws:
            return False
        
        try:
            expires = int((time.time() + 10) * 1000)  # 10 seconds in future
            signature = self._generate_signature(expires)
            
            auth_msg = {
                "op": "auth",
                "args": [self.api_key, expires, signature]
            }
            
            await self.ws.send(json.dumps(auth_msg))
            
            # Ждём ответ
            response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            if response is None:
                logger.error("[WS] ❌ Authentication: received None response")
                return False
            
            data = json.loads(response)
            
            if data.get("success") == True:
                self._authenticated = True
                logger.info("[WS] ✅ Authenticated successfully")
                return True
            else:
                logger.error(f"[WS] ❌ Authentication failed: {data}")
                return False
        
        except asyncio.TimeoutError:
            logger.error("[WS] Authentication timeout")
            return False
        except websockets.ConnectionClosed as e:
            logger.error(f"[WS] Connection closed during authentication: {e}")
            return False
        except Exception as e:
            logger.error(f"[WS] Authentication error: {e}")
            return False
    
    async def subscribe(self) -> bool:
        """Подписка на приватные каналы"""
        if not self.ws or not self._authenticated:
            logger.error("[WS] Cannot subscribe: not connected or not authenticated")
            return False
        
        try:
            # Подписываемся на все нужные каналы
            subscribe_msg = {
                "op": "subscribe",
                "args": [
                    "position",      # Обновления позиций
                    "execution",     # Исполнения ордеров (TP/SL triggered)
                    "order",         # Обновления ордеров
                    "wallet"         # Обновления баланса
                ]
            }
            
            await self.ws.send(json.dumps(subscribe_msg))
            
            # Ждём подтверждение
            response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            if response is None:
                logger.error("[WS] ❌ Subscription: received None response")
                return False
            
            data = json.loads(response)
            
            if data.get("success") == True:
                self._subscribed = True
                logger.info(f"[WS] ✅ Subscribed to channels: {subscribe_msg['args']}")
                return True
            else:
                logger.error(f"[WS] ❌ Subscription failed: {data}")
                return False
        
        except asyncio.TimeoutError:
            logger.error("[WS] Subscription timeout")
            return False
        except websockets.ConnectionClosed as e:
            logger.error(f"[WS] Connection closed during subscription: {e}")
            return False
        except Exception as e:
            logger.error(f"[WS] Subscription error: {e}")
            return False
    
    async def _send_ping(self):
        """Отправляет ping для поддержания соединения"""
        if self.ws:
            try:
                ping_msg = {"op": "ping"}
                await self.ws.send(json.dumps(ping_msg))
                self._last_ping = time.time()
            except websockets.ConnectionClosed as e:
                logger.warning(f"[WS] Ping failed - connection closed: {e}")
                raise  # Re-raise to trigger reconnect
            except Exception as e:
                logger.warning(f"[WS] Ping failed: {e}")
    
    async def _handle_message(self, message: str):
        """Обрабатывает входящее сообщение"""
        try:
            data = json.loads(message)
            
            # Pong response
            if data.get("op") == "pong":
                return
            
            # Auth response (already handled)
            if data.get("op") == "auth":
                return
            
            # Subscription response (already handled)
            if data.get("op") == "subscribe":
                return
            
            # Data messages
            topic = data.get("topic", "")
            
            if topic == "position":
                await self._handle_position_update(data.get("data", []))
            elif topic == "execution":
                await self._handle_execution_update(data.get("data", []))
            elif topic == "order":
                await self._handle_order_update(data.get("data", []))
            elif topic == "wallet":
                await self._handle_wallet_update(data.get("data", []))
            else:
                logger.debug(f"[WS] Unknown topic: {topic}")
                
        except json.JSONDecodeError:
            logger.warning(f"[WS] Invalid JSON: {message[:100]}")
        except Exception as e:
            logger.error(f"[WS] Message handling error: {e}")
    
    async def _handle_position_update(self, positions: List[Dict]):
        """Обрабатывает обновление позиций"""
        for pos in positions:
            symbol = pos.get("symbol", "")
            side = pos.get("side", "")
            size = float(pos.get("size", 0))
            entry_price = float(pos.get("entryPrice", 0) or 0)
            unrealised_pnl = float(pos.get("unrealisedPnl", 0) or 0)
            realised_pnl = float(pos.get("cumRealisedPnl", 0) or 0)
            
            logger.info(f"[WS] 📊 Position update: {symbol} {side} size={size} entry={entry_price} uPnL={unrealised_pnl}")
            
            # Если позиция закрыта (size=0), это важное событие
            if size == 0 and entry_price == 0:
                logger.info(f"[WS] 🔴 Position CLOSED: {symbol}")
            
            if self._on_position_update:
                try:
                    await self._on_position_update({
                        'symbol': symbol,
                        'side': side,
                        'size': size,
                        'entry_price': entry_price,
                        'unrealised_pnl': unrealised_pnl,
                        'realised_pnl': realised_pnl,
                        'raw': pos
                    })
                except Exception as e:
                    logger.error(f"[WS] Position callback error: {e}")
    
    async def _handle_execution_update(self, executions: List[Dict]):
        """Обрабатывает исполнения (TP/SL triggers)"""
        for exec_data in executions:
            symbol = exec_data.get("symbol", "")
            side = exec_data.get("side", "")
            exec_type = exec_data.get("execType", "")
            exec_price = float(exec_data.get("execPrice", 0) or 0)
            exec_qty = float(exec_data.get("execQty", 0) or 0)
            closed_pnl = float(exec_data.get("closedPnl", 0) or 0)
            order_type = exec_data.get("orderType", "")
            stop_order_type = exec_data.get("stopOrderType", "")
            
            logger.info(f"[WS] ⚡ Execution: {symbol} {side} type={exec_type} qty={exec_qty} pnl={closed_pnl}")
            
            # Определяем причину закрытия
            reason = "TRADE"
            if stop_order_type == "TakeProfit":
                reason = "TP"
            elif stop_order_type == "StopLoss":
                reason = "SL"
            elif exec_type == "Trade" and order_type == "Market":
                reason = "MARKET_CLOSE"
            
            if self._on_execution_update:
                try:
                    await self._on_execution_update({
                        'symbol': symbol,
                        'side': side,
                        'exec_type': exec_type,
                        'exec_price': exec_price,
                        'exec_qty': exec_qty,
                        'closed_pnl': closed_pnl,
                        'reason': reason,
                        'raw': exec_data
                    })
                except Exception as e:
                    logger.error(f"[WS] Execution callback error: {e}")
    
    async def _handle_order_update(self, orders: List[Dict]):
        """Обрабатывает обновления ордеров"""
        for order in orders:
            symbol = order.get("symbol", "")
            order_status = order.get("orderStatus", "")
            order_type = order.get("orderType", "")
            
            logger.debug(f"[WS] 📝 Order update: {symbol} status={order_status} type={order_type}")
            
            if self._on_order_update:
                try:
                    await self._on_order_update({
                        'symbol': symbol,
                        'status': order_status,
                        'type': order_type,
                        'raw': order
                    })
                except Exception as e:
                    logger.error(f"[WS] Order callback error: {e}")
    
    async def _handle_wallet_update(self, wallets: List[Dict]):
        """Обрабатывает обновления баланса"""
        for wallet in wallets:
            coin = wallet.get("coin", "")
            available = float(wallet.get("availableToWithdraw", 0) or 0)
            
            logger.debug(f"[WS] 💰 Wallet update: {coin} available={available}")
            
            if self._on_wallet_update:
                try:
                    await self._on_wallet_update({
                        'coin': coin,
                        'available': available,
                        'raw': wallet
                    })
                except Exception as e:
                    logger.error(f"[WS] Wallet callback error: {e}")
    
    def on_position_update(self, callback: Callable):
        """Устанавливает callback для обновлений позиций"""
        self._on_position_update = callback
    
    def on_execution_update(self, callback: Callable):
        """Устанавливает callback для исполнений"""
        self._on_execution_update = callback
    
    def on_order_update(self, callback: Callable):
        """Устанавливает callback для обновлений ордеров"""
        self._on_order_update = callback
    
    def on_wallet_update(self, callback: Callable):
        """Устанавливает callback для обновлений баланса"""
        self._on_wallet_update = callback
    
    async def run(self):
        """Основной цикл обработки сообщений"""
        while self._running:
            try:
                # Подключаемся если нет соединения
                if not self.ws:
                    if not await self.connect():
                        await asyncio.sleep(self._reconnect_delay)
                        self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)
                        set_ws_health(False, self._reconnect_delay, self._disconnects)
                        continue
                
                # Аутентифицируемся
                if not self._authenticated:
                    if not await self.authenticate():
                        await self.disconnect()
                        await asyncio.sleep(self._reconnect_delay)
                        continue
                
                # Подписываемся
                if not self._subscribed:
                    if not await self.subscribe():
                        await self.disconnect()
                        await asyncio.sleep(self._reconnect_delay)
                        continue
                
                # Обрабатываем сообщения
                try:
                    # Ping каждые 20 секунд
                    if time.time() - self._last_ping > self._ping_interval:
                        await self._send_ping()
                    
                    # Получаем сообщение с таймаутом
                    message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                    
                    # Проверка на None или пустое сообщение
                    if message is None:
                        logger.warning("[WS] Received None message, continuing...")
                        continue
                    
                    await self._handle_message(message)
                    
                except asyncio.TimeoutError:
                    # Таймаут - отправляем ping
                    await self._send_ping()
                except websockets.ConnectionClosed as e:
                    logger.warning(f"[WS] Connection closed ({e.code}): {e.reason}, reconnecting...")
                    self._disconnects += 1
                    await self.disconnect()
                    await asyncio.sleep(self._reconnect_delay)
                except websockets.ConnectionClosedError as e:
                    logger.warning(f"[WS] Connection closed with error ({e.code}): {e.reason}, reconnecting...")
                    self._disconnects += 1
                    await self.disconnect()
                    await asyncio.sleep(self._reconnect_delay)
                    
            except Exception as e:
                logger.error(f"[WS] Run loop error: {e}")
                self._disconnects += 1
                await self.disconnect()
                await asyncio.sleep(self._reconnect_delay)
    
    async def disconnect(self):
        """Отключается от WebSocket"""
        self._authenticated = False
        self._subscribed = False
        
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                logger.debug(f"[WS] Error during disconnect: {e}")
            finally:
                self.ws = None
        
        logger.info("[WS] Disconnected")
        set_ws_health(False, self._reconnect_delay, self._disconnects)
    
    async def stop(self):
        """Останавливает WebSocket клиент"""
        self._running = False
        await self.disconnect()
        logger.info("[WS] Stopped")
    
    @property
    def is_connected(self) -> bool:
        """Проверяет подключение"""
        return self.ws is not None and self._authenticated and self._subscribed


# === ИНТЕГРАЦИЯ С БОТОМ ===

class BybitPositionSync:
    """
    Синхронизатор позиций между Bybit и ботом.
    Использует WebSocket для мгновенных обновлений.
    """
    
    def __init__(self, ws_client: BybitWebSocket):
        self.ws = ws_client
        self._sync_callbacks: List[Callable] = []
        
        # Регистрируем обработчики
        self.ws.on_position_update(self._on_position_update)
        self.ws.on_execution_update(self._on_execution_update)
    
    def add_sync_callback(self, callback: Callable):
        """Добавляет callback для уведомления о синхронизации"""
        self._sync_callbacks.append(callback)
    
    async def _notify_sync(self, event_type: str, data: Dict):
        """Уведомляет все callbacks о событии синхронизации"""
        for callback in self._sync_callbacks:
            try:
                await callback(event_type, data)
            except Exception as e:
                logger.error(f"[SYNC] Callback error: {e}")
    
    async def _on_position_update(self, pos_data: Dict):
        """Обрабатывает обновление позиции"""
        symbol = pos_data['symbol']
        size = pos_data['size']
        
        # Позиция закрыта
        if size == 0:
            await self._notify_sync('POSITION_CLOSED', {
                'symbol': symbol,
                'realised_pnl': pos_data['realised_pnl'],
                'timestamp': datetime.now().isoformat()
            })
        else:
            await self._notify_sync('POSITION_UPDATED', {
                'symbol': symbol,
                'size': size,
                'entry_price': pos_data['entry_price'],
                'unrealised_pnl': pos_data['unrealised_pnl'],
                'timestamp': datetime.now().isoformat()
            })
    
    async def _on_execution_update(self, exec_data: Dict):
        """Обрабатывает исполнение (TP/SL)"""
        await self._notify_sync('EXECUTION', {
            'symbol': exec_data['symbol'],
            'reason': exec_data['reason'],
            'exec_price': exec_data['exec_price'],
            'exec_qty': exec_data['exec_qty'],
            'closed_pnl': exec_data['closed_pnl'],
            'timestamp': datetime.now().isoformat()
        })


# Глобальный экземпляр
_bybit_ws: Optional[BybitWebSocket] = None
_position_sync: Optional[BybitPositionSync] = None


async def init_bybit_websocket(api_key: str, api_secret: str, testnet: bool = False, demo: bool = False) -> BybitWebSocket:
    """Инициализирует глобальный WebSocket клиент"""
    global _bybit_ws, _position_sync
    
    if not WEBSOCKETS_AVAILABLE:
        logger.error("[WS] Cannot initialize: websockets library not available")
        return None
    
    _bybit_ws = BybitWebSocket(api_key, api_secret, testnet, demo)
    _position_sync = BybitPositionSync(_bybit_ws)
    
    return _bybit_ws


def get_bybit_ws() -> Optional[BybitWebSocket]:
    """Возвращает глобальный WebSocket клиент"""
    return _bybit_ws


def get_position_sync() -> Optional[BybitPositionSync]:
    """Возвращает синхронизатор позиций"""
    return _position_sync


async def start_websocket_sync():
    """Запускает WebSocket синхронизацию в фоне"""
    if _bybit_ws:
        asyncio.create_task(_bybit_ws.run())
        logger.info("[WS] WebSocket sync started in background")


async def stop_websocket_sync():
    """Останавливает WebSocket синхронизацию"""
    if _bybit_ws:
        await _bybit_ws.stop()
        logger.info("[WS] WebSocket sync stopped")
