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
    DEMO_URL = "https://api-demo.bybit.com"  # Отдельный домен для Demo Trading
    
    def __init__(self):
        self.api_key = os.getenv("BYBIT_API_KEY", "")
        self.api_secret = os.getenv("BYBIT_API_SECRET", "")
        self.testnet = os.getenv("BYBIT_TESTNET", "").lower() in ("true", "1", "yes")
        self.demo = os.getenv("BYBIT_DEMO", "").lower() in ("true", "1", "yes")
        
        # Выбираем URL в зависимости от режима
        if self.testnet:
            self.base_url = self.TESTNET_URL
        elif self.demo:
            self.base_url = self.DEMO_URL  # Demo Trading = отдельный домен!
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
        # Demo Trading использует отдельный домен api-demo.bybit.com
        # Header X-BAPI-DEMO-TRADING НЕ нужен
        
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
            
            ret_code = data.get('retCode')
            ret_msg = data.get('retMsg')
            logger.info(f"[BYBIT] Response: retCode={ret_code}, retMsg={ret_msg}")
            
            if ret_code == 0:
                return data.get("result")
            elif ret_code == 10003:
                logger.error(f"[BYBIT] ❌ API KEY INVALID!")
                return None
            elif ret_code == 10004:
                logger.error(f"[BYBIT] ❌ Неверная подпись!")
                return None
            elif ret_code == 110007:
                logger.error(f"[BYBIT] ❌ Недостаточно баланса!")
                return None
            elif ret_code == 110073:
                logger.error(f"[BYBIT] ❌ TP/SL слишком близко к цене! Пробуем без TP/SL...")
                return None
            elif ret_code == 110017:
                logger.error(f"[BYBIT] ❌ Слишком маленький размер ордера!")
                return None
            else:
                logger.error(f"[BYBIT] API Error: {ret_msg} (code: {ret_code})")
                logger.error(f"[BYBIT] Full response: {data}")
                return None
                
        except Exception as e:
            logger.error(f"[BYBIT] Request error: {e}")
            return None
    
    def _to_bybit_symbol(self, symbol: str) -> str:
        """Конвертация символа в формат Bybit"""
        # Маппинг символов (Binance -> Bybit)
        SYMBOL_MAP = {
            "MATIC": "POL",  # MATIC переименован в POL
        }
        
        bybit_symbol = symbol.replace("/", "")
        for old, new in SYMBOL_MAP.items():
            bybit_symbol = bybit_symbol.replace(old, new)
        
        return bybit_symbol
    
    async def get_price(self, symbol: str) -> Optional[float]:
        """Получить текущую цену"""
        bybit_symbol = self._to_bybit_symbol(symbol)
        
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
    
    async def set_leverage(self, symbol: str, leverage: int = 20) -> bool:
        """Установить плечо для символа на Bybit"""
        bybit_symbol = self._to_bybit_symbol(symbol)
        
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage)
        }
        
        logger.info(f"[BYBIT] Устанавливаем плечо x{leverage} для {bybit_symbol}")
        result = await self._request("POST", "/v5/position/set-leverage", params)
        
        if result is not None:
            logger.info(f"[BYBIT] ✓ Плечо установлено: x{leverage}")
            return True
        else:
            # Ошибка 110043 = уже установлено такое плечо (OK)
            logger.info(f"[BYBIT] Плечо уже установлено или ошибка")
            return True  # Продолжаем в любом случае
    
    async def open_hedge(self, position_id: int, symbol: str, direction: str, amount_usd: float, tp: float = None, sl: float = None) -> Optional[str]:
        """
        Открыть хедж-позицию на Bybit с TP/SL
        
        Args:
            position_id: ID позиции юзера в нашей системе
            symbol: Торговая пара (BTC/USDT)
            direction: LONG или SHORT
            amount_usd: Сумма в USD
            tp: Take Profit цена
            sl: Stop Loss цена
        
        Returns:
            order_id если успешно, None если ошибка
        """
        logger.info(f"[HEDGE] === Попытка открыть хедж ===")
        logger.info(f"[HEDGE] symbol={symbol}, direction={direction}, amount=${amount_usd}")
        logger.info(f"[HEDGE] TP=${tp}, SL=${sl}")
        logger.info(f"[HEDGE] API Key: {self.api_key[:8]}... Demo: {self.demo}, Testnet: {self.testnet}")
        
        if not self.enabled:
            logger.info(f"[HEDGE] Пропуск (Bybit не настроен): {symbol} {direction} ${amount_usd}")
            return None
        
        bybit_symbol = self._to_bybit_symbol(symbol)
        side = "Buy" if direction == "LONG" else "Sell"
        
        # Устанавливаем плечо x20 перед открытием
        await self.set_leverage(symbol, 20)
        
        # Получаем цену для расчёта qty
        price = await self.get_price(symbol)
        if not price:
            logger.error(f"[HEDGE] Не удалось получить цену {symbol}")
            return None
        
        # Количество в базовой валюте
        qty = amount_usd / price
        
        # Bybit контракты: точность и минимумы для каждой монеты
        # https://bybit-exchange.github.io/docs/v5/enum#symbol
        BYBIT_SPECS = {
            "BTC": {"precision": 3, "min_qty": 0.001},
            "ETH": {"precision": 2, "min_qty": 0.01},
            "SOL": {"precision": 1, "min_qty": 0.1},
            "BNB": {"precision": 2, "min_qty": 0.01},
            "XRP": {"precision": 0, "min_qty": 1},
            "DOGE": {"precision": 0, "min_qty": 1},
            "AVAX": {"precision": 1, "min_qty": 0.1},
            "LINK": {"precision": 1, "min_qty": 0.1},
            "MATIC": {"precision": 0, "min_qty": 1},  # POL на Bybit
            "ARB": {"precision": 0, "min_qty": 1},
            "OP": {"precision": 1, "min_qty": 0.1},
            "APT": {"precision": 1, "min_qty": 0.1},
        }
        
        # Определяем спецификации для монеты
        coin = symbol.split("/")[0] if "/" in symbol else symbol.replace("USDT", "")
        spec = BYBIT_SPECS.get(coin, {"precision": 1, "min_qty": 0.1})
        
        qty = round(qty, spec["precision"])
        min_qty = spec["min_qty"]
        
        if qty < min_qty:
            logger.warning(f"[HEDGE] Размер {qty} < min {min_qty}, увеличиваем")
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
        
        # Добавляем TP/SL если указаны
        if tp is not None:
            params["takeProfit"] = str(round(tp, 2))
        if sl is not None:
            params["stopLoss"] = str(round(sl, 2))
        
        logger.info(f"[HEDGE] Открываем: {bybit_symbol} {side} qty={qty} (${amount_usd}) TP={tp} SL={sl}")
        
        result = await self._request("POST", "/v5/order/create", params)
        
        # Если ошибка с TP/SL - пробуем без них
        if result is None and (tp is not None or sl is not None):
            logger.warning(f"[HEDGE] Пробуем открыть без TP/SL...")
            params_no_tpsl = {
                "category": "linear",
                "symbol": bybit_symbol,
                "side": side,
                "orderType": "Market",
                "qty": str(qty),
                "timeInForce": "GTC",
                "positionIdx": 0
            }
            result = await self._request("POST", "/v5/order/create", params_no_tpsl)
        
        if result:
            order_id = result.get("orderId")
            self.hedge_positions[position_id] = order_id
            logger.info(f"[HEDGE] ✓ Открыто: {order_id}")
            
            # Если TP/SL не были установлены при открытии - ставим через trading-stop
            if tp is not None or sl is not None:
                await self.set_trading_stop(bybit_symbol, direction, tp, sl)
            
            return order_id
        else:
            logger.error(f"[HEDGE] ✗ Ошибка открытия позиции")
            return None
    
    async def set_trading_stop(self, bybit_symbol: str, direction: str, tp: float = None, sl: float = None) -> bool:
        """Установить TP/SL для открытой позиции"""
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "positionIdx": 0
        }
        
        if tp is not None:
            params["takeProfit"] = str(round(tp, 4))
        if sl is not None:
            params["stopLoss"] = str(round(sl, 4))
        
        logger.info(f"[HEDGE] Устанавливаем TP/SL: {bybit_symbol} TP={tp} SL={sl}")
        
        result = await self._request("POST", "/v5/position/trading-stop", params)
        
        if result is not None:
            logger.info(f"[HEDGE] ✓ TP/SL установлены")
            return True
        else:
            logger.warning(f"[HEDGE] ⚠️ Не удалось установить TP/SL")
            return False
    
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
        
        bybit_symbol = self._to_bybit_symbol(symbol)
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
        pos_side = pos.get("side", "")  # Buy = LONG, Sell = SHORT
        
        if float(size) == 0:
            logger.info(f"[HEDGE] Позиция уже закрыта: {bybit_symbol}")
            return True
        
        # Определяем сторону для закрытия по реальной позиции на Bybit
        # Если позиция Buy (LONG) - закрываем Sell, если Sell (SHORT) - закрываем Buy
        close_side = "Sell" if pos_side == "Buy" else "Buy"
        
        logger.info(f"[HEDGE] Bybit position: {bybit_symbol} side={pos_side} size={size}")
        
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": size,
            "timeInForce": "GTC",
            "positionIdx": 0,
            "reduceOnly": True
        }
        
        logger.info(f"[HEDGE] Закрываем: {bybit_symbol} {close_side} qty={size}")
        
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
        data = await self.get_position_data(symbol)
        if data:
            return data.get('pnl')
        return None
    
    async def get_position_data(self, symbol: str) -> Optional[Dict]:
        """Получить полные данные позиции с Bybit"""
        if not self.enabled:
            return None
        
        bybit_symbol = self._to_bybit_symbol(symbol)
        
        result = await self._request("GET", "/v5/position/list", {
            "category": "linear",
            "symbol": bybit_symbol
        })
        
        if result:
            positions = result.get("list", [])
            if positions and float(positions[0].get("size", 0)) > 0:
                pos = positions[0]
                
                # Парсим данные
                pnl_str = pos.get("unrealisedPnl", "0")
                if pnl_str == "" or pnl_str is None:
                    pnl_str = "0"
                
                entry_str = pos.get("avgPrice", "0")
                if entry_str == "" or entry_str is None:
                    entry_str = "0"
                
                mark_str = pos.get("markPrice", "0")
                if mark_str == "" or mark_str is None:
                    mark_str = "0"
                
                logger.info(f"[BYBIT] Position {bybit_symbol}: entry={entry_str}, mark={mark_str}, pnl={pnl_str}")
                
                return {
                    'symbol': bybit_symbol,
                    'entry': float(entry_str),
                    'current': float(mark_str),
                    'pnl': float(pnl_str),
                    'size': float(pos.get("size", 0)),
                    'side': pos.get("side", ""),
                    'leverage': pos.get("leverage", "1")
                }
        
        return None


# Глобальный экземпляр
hedger = BybitHedger()


async def hedge_open(position_id: int, symbol: str, direction: str, amount: float, tp: float = None, sl: float = None) -> Optional[str]:
    """Wrapper для открытия хеджа с TP/SL"""
    return await hedger.open_hedge(position_id, symbol, direction, amount, tp, sl)


async def hedge_close(position_id: int, symbol: str, direction: str) -> bool:
    """Wrapper для закрытия хеджа"""
    return await hedger.close_hedge(position_id, symbol, direction)


async def is_hedging_enabled() -> bool:
    """Проверить, включено ли хеджирование"""
    return hedger.enabled
