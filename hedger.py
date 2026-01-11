"""
Bybit Hedging Module
Зеркалирует сделки юзеров на Bybit Futures
"""

import os
import logging
import asyncio
import hmac
import hashlib
import time
import json
from typing import Optional, Dict
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)

class BybitHedger:
    """Хеджирование сделок через Bybit"""
    
    BASE_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"
    
    def __init__(self):
        self.api_key = os.getenv("BYBIT_API_KEY", "")
        self.api_secret = os.getenv("BYBIT_API_SECRET", "")
        self.testnet = os.getenv("BYBIT_TESTNET", "").lower() in ("true", "1", "yes")
        self.demo = os.getenv("BYBIT_DEMO", "").lower() in ("true", "1", "yes")
        
        # Demo Trading использует mainnet URL но с особым хедером
        if self.testnet:
            self.base_url = self.TESTNET_URL
        else:
            self.base_url = self.BASE_URL
            
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Маппинг позиций: {user_position_id: bybit_order_id}
        self.hedge_positions: Dict[int, str] = {}
        
        if not self.api_key or not self.api_secret:
            logger.warning("[BYBIT] API ключи не настроены! Хеджирование отключено.")
        else:
            if self.testnet:
                mode = "TESTNET"
            elif self.demo:
                mode = "DEMO TRADING"
            else:
                mode = "LIVE"
            logger.info(f"[BYBIT] Hedger инициализирован ({mode}) - {self.base_url}")
    
    @property
    def enabled(self) -> bool:
        return bool(self.api_key and self.api_secret)
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    def _sign(self, params: dict, is_post: bool = False) -> dict:
        """Подпись запроса для Bybit V5 API"""
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        
        # Для POST - JSON строка, для GET - query string
        if is_post:
            params_str = json.dumps(params) if params else ""
        else:
            params_str = "&".join(f"{k}={v}" for k, v in sorted(params.items())) if params else ""
        
        sign_str = f"{timestamp}{self.api_key}{recv_window}{params_str}"
        
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            sign_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": recv_window
        }
        
        # Для Demo Trading нужен специальный хедер
        if self.demo:
            headers["X-BAPI-DEMO-TRADING"] = "true"
        
        return headers
    
    async def _request(self, method: str, endpoint: str, params: dict = None) -> Optional[dict]:
        """Выполнить запрос к Bybit API"""
        if not self.enabled:
            logger.warning("[BYBIT] Request skipped - not enabled")
            return None
        
        params = params or {}
        url = f"{self.base_url}{endpoint}"
        is_post = method.upper() == "POST"
        headers = self._sign(params, is_post=is_post)
        headers["Content-Type"] = "application/json"
        
        logger.info(f"[BYBIT] {method} {endpoint} params={params}")
        
        try:
            session = await self._get_session()
            
            if method == "GET":
                async with session.get(url, headers=headers, params=params) as resp:
                    data = await resp.json()
            else:
                # POST с JSON body
                async with session.post(url, headers=headers, data=json.dumps(params)) as resp:
                    data = await resp.json()
            
            logger.info(f"[BYBIT] Response: retCode={data.get('retCode')}, retMsg={data.get('retMsg')}")
            
            if data.get("retCode") == 0:
                return data.get("result")
            else:
                logger.error(f"[BYBIT] API Error: {data.get('retMsg')} (code: {data.get('retCode')})")
                return None
                
        except Exception as e:
            logger.error(f"[BYBIT] Request error: {e}")
            return None
    
    async def get_price(self, symbol: str) -> Optional[float]:
        """Получить текущую цену"""
        bybit_symbol = symbol.replace("/", "")  # BTC/USDT -> BTCUSDT
        
        try:
            session = await self._get_session()
            url = f"{self.base_url}/v5/market/tickers"
            params = {"category": "linear", "symbol": bybit_symbol}
            
            async with session.get(url, params=params) as resp:
                data = await resp.json()
                if data.get("retCode") == 0:
                    tickers = data.get("result", {}).get("list", [])
                    if tickers:
                        return float(tickers[0]["lastPrice"])
        except Exception as e:
            logger.error(f"[BYBIT] Price error: {e}")
        
        return None
    
    async def get_balance(self) -> Optional[float]:
        """Получить баланс USDT"""
        # Сначала пробуем UNIFIED (новый тип аккаунта)
        result = await self._request("GET", "/v5/account/wallet-balance", {
            "accountType": "UNIFIED"
        })
        
        if result:
            coins = result.get("list", [{}])[0].get("coin", [])
            for coin in coins:
                if coin.get("coin") == "USDT":
                    return float(coin.get("walletBalance", 0))
        
        # Пробуем CONTRACT (старый тип)
        result = await self._request("GET", "/v5/account/wallet-balance", {
            "accountType": "CONTRACT"
        })
        
        if result:
            coins = result.get("list", [{}])[0].get("coin", [])
            for coin in coins:
                if coin.get("coin") == "USDT":
                    return float(coin.get("walletBalance", 0))
        
        return None
    
    async def open_hedge(self, position_id: int, symbol: str, direction: str, amount_usd: float) -> Optional[str]:
        """
        Открыть хедж-позицию на Bybit
        
        Args:
            position_id: ID позиции юзера в нашей системе
            symbol: Торговая пара (BTC/USDT)
            direction: LONG или SHORT
            amount_usd: Сумма в USD
        
        Returns:
            order_id если успешно, None если ошибка
        """
        if not self.enabled:
            logger.info(f"[HEDGE] Пропуск (Bybit не настроен): {symbol} {direction} ${amount_usd}")
            return None
        
        bybit_symbol = symbol.replace("/", "")
        side = "Buy" if direction == "LONG" else "Sell"
        
        # Получаем цену для расчёта qty
        price = await self.get_price(symbol)
        if not price:
            logger.error(f"[HEDGE] Не удалось получить цену {symbol}")
            return None
        
        # Количество в базовой валюте
        qty = amount_usd / price
        
        # Округляем в зависимости от монеты
        if "BTC" in symbol:
            qty = round(qty, 3)
        elif "ETH" in symbol:
            qty = round(qty, 2)
        else:
            qty = round(qty, 1)
        
        # Минимальный размер
        min_qty = 0.001 if "BTC" in symbol else 0.01
        if qty < min_qty:
            logger.warning(f"[HEDGE] Слишком маленький размер: {qty} < {min_qty}")
            qty = min_qty
        
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": side,
            "orderType": "Market",
            "qty": str(qty),
            "timeInForce": "GTC",
            "positionIdx": 0  # One-way mode
        }
        
        logger.info(f"[HEDGE] Открываем: {bybit_symbol} {side} qty={qty} (${amount_usd})")
        
        result = await self._request("POST", "/v5/order/create", params)
        
        if result:
            order_id = result.get("orderId")
            self.hedge_positions[position_id] = order_id
            logger.info(f"[HEDGE] ✓ Открыто: {order_id}")
            return order_id
        else:
            logger.error(f"[HEDGE] ✗ Ошибка открытия позиции")
            return None
    
    async def close_hedge(self, position_id: int, symbol: str, direction: str) -> bool:
        """
        Закрыть хедж-позицию
        
        Args:
            position_id: ID позиции юзера
            symbol: Торговая пара
            direction: LONG или SHORT (оригинальное направление)
        
        Returns:
            True если успешно
        """
        if not self.enabled:
            return True
        
        bybit_symbol = symbol.replace("/", "")
        # Для закрытия LONG нужен Sell, для SHORT - Buy
        side = "Sell" if direction == "LONG" else "Buy"
        
        # Получаем размер открытой позиции
        pos_result = await self._request("GET", "/v5/position/list", {
            "category": "linear",
            "symbol": bybit_symbol
        })
        
        if not pos_result:
            logger.warning(f"[HEDGE] Не найдена позиция для закрытия: {bybit_symbol}")
            return False
        
        positions = pos_result.get("list", [])
        if not positions:
            logger.warning(f"[HEDGE] Нет открытых позиций: {bybit_symbol}")
            return True  # Уже закрыта
        
        pos = positions[0]
        size = pos.get("size", "0")
        
        if float(size) == 0:
            logger.info(f"[HEDGE] Позиция уже закрыта: {bybit_symbol}")
            return True
        
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": side,
            "orderType": "Market",
            "qty": size,
            "timeInForce": "GTC",
            "positionIdx": 0,
            "reduceOnly": True
        }
        
        logger.info(f"[HEDGE] Закрываем: {bybit_symbol} {side} qty={size}")
        
        result = await self._request("POST", "/v5/order/create", params)
        
        if result:
            # Удаляем из отслеживания
            self.hedge_positions.pop(position_id, None)
            logger.info(f"[HEDGE] ✓ Закрыто: {bybit_symbol}")
            return True
        else:
            logger.error(f"[HEDGE] ✗ Ошибка закрытия")
            return False
    
    async def get_position_pnl(self, symbol: str) -> Optional[float]:
        """Получить PnL по позиции"""
        if not self.enabled:
            return None
        
        bybit_symbol = symbol.replace("/", "")
        
        result = await self._request("GET", "/v5/position/list", {
            "category": "linear",
            "symbol": bybit_symbol
        })
        
        if result:
            positions = result.get("list", [])
            if positions:
                return float(positions[0].get("unrealisedPnl", 0))
        
        return None


# Глобальный экземпляр
hedger = BybitHedger()


async def hedge_open(position_id: int, symbol: str, direction: str, amount: float) -> Optional[str]:
    """Wrapper для открытия хеджа"""
    return await hedger.open_hedge(position_id, symbol, direction, amount)


async def hedge_close(position_id: int, symbol: str, direction: str) -> bool:
    """Wrapper для закрытия хеджа"""
    return await hedger.close_hedge(position_id, symbol, direction)


async def is_hedging_enabled() -> bool:
    """Проверить, включено ли хеджирование"""
    return hedger.enabled
