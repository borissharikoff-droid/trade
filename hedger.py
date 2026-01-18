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
from typing import Optional, Dict, List
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)

# #region agent log - Debug instrumentation
import os as _debug_os
DEBUG_LOG_PATH = _debug_os.path.join(_debug_os.path.dirname(_debug_os.path.abspath(__file__)), ".cursor", "debug.log")
def debug_log(hypothesis_id: str, location: str, message: str, data: dict = None):
    """Write NDJSON debug log entry"""
    try:
        _debug_os.makedirs(_debug_os.path.dirname(DEBUG_LOG_PATH), exist_ok=True)
        entry = {"timestamp": datetime.now().isoformat(), "hypothesisId": hypothesis_id, "location": location, "message": message, "data": data or {}, "sessionId": "debug-session"}
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[DEBUG_LOG_ERROR] {e}")
# #endregion

class BybitHedger:
    """Хеджирование сделок через Bybit"""
    
    BASE_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"
    DEMO_URL = "https://api-demo.bybit.com"  # Отдельный домен для Demo Trading
    
    # Bybit контракты: точность и минимумы для каждой монеты
    # https://bybit-exchange.github.io/docs/v5/enum#symbol
    BYBIT_SPECS = {
        # Major
        "BTC": {"precision": 3, "min_qty": 0.001},
        "ETH": {"precision": 2, "min_qty": 0.01},
        "SOL": {"precision": 1, "min_qty": 0.1},
        "BNB": {"precision": 2, "min_qty": 0.01},
        "XRP": {"precision": 0, "min_qty": 1},
        "DOGE": {"precision": 0, "min_qty": 1},
        "AVAX": {"precision": 1, "min_qty": 0.1},
        "LINK": {"precision": 1, "min_qty": 0.1},
        "MATIC": {"precision": 0, "min_qty": 1},
        "POL": {"precision": 0, "min_qty": 1},
        "ARB": {"precision": 0, "min_qty": 1},
        "OP": {"precision": 1, "min_qty": 0.1},
        "APT": {"precision": 1, "min_qty": 0.1},
        "SUI": {"precision": 0, "min_qty": 1},
        "SEI": {"precision": 0, "min_qty": 1},
        "TIA": {"precision": 1, "min_qty": 0.1},
        "INJ": {"precision": 1, "min_qty": 0.1},
        "FTM": {"precision": 0, "min_qty": 1},
        "NEAR": {"precision": 0, "min_qty": 1},
        "ATOM": {"precision": 1, "min_qty": 0.1},
        "DOT": {"precision": 1, "min_qty": 0.1},
        "ADA": {"precision": 0, "min_qty": 1},
        "LTC": {"precision": 2, "min_qty": 0.01},
        # Memecoins
        "PEPE": {"precision": 0, "min_qty": 1000},
        "SHIB": {"precision": 0, "min_qty": 1000},
        "FLOKI": {"precision": 0, "min_qty": 1},
        "BONK": {"precision": 0, "min_qty": 1000},
        "WIF": {"precision": 0, "min_qty": 1},
        "MEME": {"precision": 0, "min_qty": 1},
        "DEGEN": {"precision": 0, "min_qty": 1},
        "FARTCOIN": {"precision": 0, "min_qty": 1},
        "AI16Z": {"precision": 1, "min_qty": 0.1},
        # NFT/Gaming
        "BLUR": {"precision": 0, "min_qty": 1},
        "IMX": {"precision": 0, "min_qty": 1},
        "GALA": {"precision": 0, "min_qty": 1},
        "AXS": {"precision": 1, "min_qty": 0.1},
        "SAND": {"precision": 0, "min_qty": 1},
        "MANA": {"precision": 0, "min_qty": 1},
        # DeFi
        "UNI": {"precision": 1, "min_qty": 0.1},
        "AAVE": {"precision": 2, "min_qty": 0.01},
        "MKR": {"precision": 3, "min_qty": 0.001},
        "SNX": {"precision": 0, "min_qty": 1},
        "CRV": {"precision": 0, "min_qty": 1},
        "LDO": {"precision": 0, "min_qty": 1},
        "PENDLE": {"precision": 0, "min_qty": 1},
        "JUP": {"precision": 0, "min_qty": 1},
        "JTO": {"precision": 1, "min_qty": 0.1},
        # AI
        "FET": {"precision": 0, "min_qty": 1},
        "AGIX": {"precision": 0, "min_qty": 1},
        "RNDR": {"precision": 1, "min_qty": 0.1},
        "TAO": {"precision": 3, "min_qty": 0.001},
        "FHE": {"precision": 0, "min_qty": 1},
        "VIRTUAL": {"precision": 0, "min_qty": 1},
        # L2/Infra
        "STRK": {"precision": 0, "min_qty": 1},
        "ZK": {"precision": 0, "min_qty": 1},
        "MANTA": {"precision": 0, "min_qty": 1},
        "TRX": {"precision": 0, "min_qty": 1},
        "TON": {"precision": 1, "min_qty": 0.1},
        "STX": {"precision": 0, "min_qty": 1},
        # Others
        "WLD": {"precision": 0, "min_qty": 1},
        "PYTH": {"precision": 0, "min_qty": 1},
        "ORDI": {"precision": 1, "min_qty": 0.1},
        "SATS": {"precision": 0, "min_qty": 1000},
        "1000SATS": {"precision": 0, "min_qty": 1},
        "BOME": {"precision": 0, "min_qty": 1},
        "ENA": {"precision": 0, "min_qty": 1},
        "W": {"precision": 0, "min_qty": 1},
        "AEVO": {"precision": 1, "min_qty": 0.1},
        "ETHFI": {"precision": 1, "min_qty": 0.1},
        "JELLYJELLY": {"precision": 0, "min_qty": 1},
        "BARD": {"precision": 0, "min_qty": 1},
        "XPL": {"precision": 0, "min_qty": 1},
    }
    
    # Дефолтные спецификации для неизвестных монет
    DEFAULT_SPEC = {"precision": 1, "min_qty": 1}
    
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
            
            # #region agent log
            debug_log("D", "bybit_request:response", "Bybit API response", {"endpoint": endpoint, "ret_code": ret_code, "ret_msg": ret_msg, "method": method})
            # #endregion
            
            if ret_code == 0:
                return data.get("result")
            elif ret_code == 10003:
                logger.error(f"[BYBIT] ❌ API KEY INVALID!")
                # #region agent log
                debug_log("D", "bybit_request:error", "API KEY INVALID", {"ret_code": ret_code})
                # #endregion
                return None
            elif ret_code == 10004:
                logger.error(f"[BYBIT] ❌ Неверная подпись!")
                # #region agent log
                debug_log("D", "bybit_request:error", "Invalid signature", {"ret_code": ret_code})
                # #endregion
                return None
            elif ret_code == 110007:
                logger.error(f"[BYBIT] ❌ Недостаточно баланса!")
                # #region agent log
                debug_log("D", "bybit_request:error", "Insufficient balance", {"ret_code": ret_code})
                # #endregion
                return None
            elif ret_code == 110073:
                logger.error(f"[BYBIT] ❌ TP/SL слишком близко к цене! Пробуем без TP/SL...")
                return None
            elif ret_code == 110017:
                logger.error(f"[BYBIT] ❌ Слишком маленький размер ордера!")
                # #region agent log
                debug_log("D", "bybit_request:error", "Order size too small", {"ret_code": ret_code})
                # #endregion
                return None
            else:
                logger.error(f"[BYBIT] API Error: {ret_msg} (code: {ret_code})")
                logger.error(f"[BYBIT] Full response: {data}")
                # #region agent log
                debug_log("D", "bybit_request:error", "Unknown API error", {"ret_code": ret_code, "ret_msg": ret_msg})
                # #endregion
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
    
    async def open_hedge(self, position_id: int, symbol: str, direction: str, amount_usd: float, 
                         tp: float = None, sl: float = None,
                         tp1: float = None, tp2: float = None, tp3: float = None) -> Optional[Dict]:
        """
        Открыть хедж-позицию на Bybit с частичными TP
        
        Args:
            position_id: ID позиции юзера в нашей системе
            symbol: Торговая пара (BTC/USDT)
            direction: LONG или SHORT
            amount_usd: Сумма в USD
            tp: Take Profit цена (legacy, используется как TP1 если tp1 не указан)
            sl: Stop Loss цена
            tp1, tp2, tp3: Частичные тейки (50%, 30%, 20%)
        
        Returns:
            {'order_id': str, 'qty': float} если успешно, None если ошибка
        """
        # #region agent log
        debug_log("D", "open_hedge:entry", "Opening hedge position", {"position_id": position_id, "symbol": symbol, "direction": direction, "amount_usd": amount_usd, "tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": sl, "enabled": self.enabled, "demo": self.demo, "testnet": self.testnet})
        # #endregion
        
        # Если tp1 не указан, используем tp как tp1
        if tp1 is None and tp is not None:
            tp1 = tp
        
        logger.info(f"[HEDGE] === Попытка открыть хедж ===")
        logger.info(f"[HEDGE] symbol={symbol}, direction={direction}, amount=${amount_usd}")
        logger.info(f"[HEDGE] TP1=${tp1}, TP2=${tp2}, TP3=${tp3}, SL=${sl}")
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
        
        # Определяем спецификации для монеты из класса
        coin = symbol.split("/")[0] if "/" in symbol else symbol.replace("USDT", "")
        spec = self.BYBIT_SPECS.get(coin, self.DEFAULT_SPEC)
        logger.info(f"[HEDGE] Coin {coin}: precision={spec['precision']}, min_qty={spec['min_qty']}")
        
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
            logger.info(f"[HEDGE] ✓ Открыто: {order_id}, qty={qty}")
            
            # #region agent log
            debug_log("D", "open_hedge:success", "Hedge opened successfully", {"order_id": order_id, "qty": qty, "symbol": bybit_symbol})
            # #endregion
            
            # Устанавливаем SL через trading-stop
            if sl is not None:
                await self.set_trading_stop(bybit_symbol, direction, sl=sl)
            
            # Создаём частичные TP ордера если все три указаны
            if tp1 is not None and tp2 is not None and tp3 is not None:
                await self.set_partial_take_profits(symbol, direction, qty, tp1, tp2, tp3)
            elif tp1 is not None:
                # Fallback: один TP через лимитный ордер
                close_side = "Sell" if direction == "LONG" else "Buy"
                await self._request("POST", "/v5/order/create", {
                    "category": "linear",
                    "symbol": bybit_symbol,
                    "side": close_side,
                    "orderType": "Limit",
                    "qty": str(qty),
                    "price": str(round(tp1, 2)),
                    "timeInForce": "GTC",
                    "positionIdx": 0,
                    "reduceOnly": True
                })
            
            # Возвращаем Dict с order_id и реальным qty
            return {'order_id': order_id, 'qty': qty}
        else:
            # #region agent log
            debug_log("D", "open_hedge:failed", "Failed to open hedge", {"symbol": bybit_symbol, "side": side, "qty": qty})
            # #endregion
            logger.error(f"[HEDGE] ✗ Ошибка открытия позиции")
            return None
    
    async def set_trading_stop(self, bybit_symbol: str, direction: str, tp: float = None, sl: float = None) -> bool:
        """Установить TP/SL для открытой позиции (только SL, TP через partial orders)"""
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "positionIdx": 0
        }
        
        # Ставим только SL через trading-stop
        # TP теперь через partial_take_profits
        if sl is not None:
            params["stopLoss"] = str(round(sl, 4))
        
        if tp is not None and sl is None:
            # Если передан только TP без SL - используем старый метод
            params["takeProfit"] = str(round(tp, 4))
        
        logger.info(f"[HEDGE] Устанавливаем SL: {bybit_symbol} SL={sl}")
        
        result = await self._request("POST", "/v5/position/trading-stop", params)
        
        if result is not None:
            logger.info(f"[HEDGE] ✓ SL установлен")
            return True
        else:
            logger.warning(f"[HEDGE] ⚠️ Не удалось установить SL")
            return False
    
    async def set_partial_take_profits(self, symbol: str, direction: str, qty: float, tp1: float, tp2: float, tp3: float) -> bool:
        """
        Создать частичные Take Profit ордера
        
        TP1: 40% позиции
        TP2: 40% позиции  
        TP3: 20% позиции
        """
        bybit_symbol = self._to_bybit_symbol(symbol)
        
        # Сторона для закрытия (противоположная)
        close_side = "Sell" if direction == "LONG" else "Buy"
        
        # Определяем precision для монеты из класса
        coin = symbol.split("/")[0] if "/" in symbol else symbol.replace("USDT", "")
        spec = self.BYBIT_SPECS.get(coin, self.DEFAULT_SPEC)
        precision = spec["precision"]
        min_qty = spec["min_qty"]
        
        # Рассчитываем qty для каждого TP
        qty1 = round(qty * 0.50, precision)  # 50% - быстро забрать
        qty2 = round(qty * 0.30, precision)  # 30%
        qty3 = round(qty * 0.20, precision)  # 20% - moonbag
        
        # Проверяем минимумы
        if qty1 < min_qty:
            qty1 = min_qty
        if qty2 < min_qty:
            qty2 = min_qty
        if qty3 < min_qty:
            qty3 = min_qty
        
        logger.info(f"[HEDGE] Создаём частичные TP для {bybit_symbol}: TP1={tp1} ({qty1}), TP2={tp2} ({qty2}), TP3={tp3} ({qty3})")
        
        success_count = 0
        
        # === TP1: 40% ===
        params1 = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": close_side,
            "orderType": "Limit",
            "qty": str(qty1),
            "price": str(round(tp1, 2)),
            "timeInForce": "GTC",
            "positionIdx": 0,
            "reduceOnly": True
        }
        
        result1 = await self._request("POST", "/v5/order/create", params1)
        if result1:
            logger.info(f"[HEDGE] ✓ TP1 ордер создан: {tp1} qty={qty1}")
            success_count += 1
        else:
            logger.warning(f"[HEDGE] ✗ Не удалось создать TP1 ордер")
        
        # === TP2: 40% ===
        params2 = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": close_side,
            "orderType": "Limit",
            "qty": str(qty2),
            "price": str(round(tp2, 2)),
            "timeInForce": "GTC",
            "positionIdx": 0,
            "reduceOnly": True
        }
        
        result2 = await self._request("POST", "/v5/order/create", params2)
        if result2:
            logger.info(f"[HEDGE] ✓ TP2 ордер создан: {tp2} qty={qty2}")
            success_count += 1
        else:
            logger.warning(f"[HEDGE] ✗ Не удалось создать TP2 ордер")
        
        # === TP3: 20% ===
        params3 = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": close_side,
            "orderType": "Limit",
            "qty": str(qty3),
            "price": str(round(tp3, 2)),
            "timeInForce": "GTC",
            "positionIdx": 0,
            "reduceOnly": True
        }
        
        result3 = await self._request("POST", "/v5/order/create", params3)
        if result3:
            logger.info(f"[HEDGE] ✓ TP3 ордер создан: {tp3} qty={qty3}")
            success_count += 1
        else:
            logger.warning(f"[HEDGE] ✗ Не удалось создать TP3 ордер")
        
        logger.info(f"[HEDGE] Частичные TP: {success_count}/3 успешно")
        return success_count > 0
    
    async def cancel_all_tp_orders(self, symbol: str) -> bool:
        """Отменить все лимитные ордера (TP) для символа"""
        bybit_symbol = self._to_bybit_symbol(symbol)
        
        params = {
            "category": "linear",
            "symbol": bybit_symbol
        }
        
        result = await self._request("POST", "/v5/order/cancel-all", params)
        
        if result is not None:
            logger.info(f"[HEDGE] ✓ Все ордера отменены для {bybit_symbol}")
            return True
        else:
            logger.warning(f"[HEDGE] ⚠️ Не удалось отменить ордера")
            return False
    
    async def close_hedge(self, position_id: int, symbol: str, direction: str, bybit_qty: float = None) -> bool:
        """
        Закрыть хедж-позицию (частично или полностью)
        
        Args:
            position_id: ID позиции юзера
            symbol: Торговая пара
            direction: LONG или SHORT (оригинальное направление)
            bybit_qty: Количество в базовой валюте для закрытия (None = закрыть всю позицию на Bybit)
        
        Returns:
            True если успешно
        """
        if not self.enabled:
            return True
        
        bybit_symbol = self._to_bybit_symbol(symbol)
        
        # Получаем данные открытой позиции
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
        total_size = float(pos.get("size", "0"))
        pos_side = pos.get("side", "")  # Buy = LONG, Sell = SHORT
        
        if total_size == 0:
            logger.info(f"[HEDGE] Позиция уже закрыта: {bybit_symbol}")
            return True
        
        # Определяем сторону для закрытия
        close_side = "Sell" if pos_side == "Buy" else "Buy"
        
        # Определяем qty для закрытия
        if bybit_qty and bybit_qty > 0:
            # Используем переданное qty напрямую
            close_qty = bybit_qty
            
            # Не закрываем больше чем есть
            if close_qty > total_size:
                close_qty = total_size
                logger.info(f"[HEDGE] Закрываем всю позицию (запрошено {bybit_qty}, есть {total_size})")
            
            close_qty_str = str(close_qty)
        else:
            # Полное закрытие всей позиции
            close_qty_str = str(total_size)
        
        logger.info(f"[HEDGE] Bybit: {bybit_symbol} total={total_size}, closing={close_qty_str}")
        
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": close_qty_str,
            "timeInForce": "GTC",
            "positionIdx": 0,
            "reduceOnly": True
        }
        
        logger.info(f"[HEDGE] Закрываем: {bybit_symbol} {close_side} qty={close_qty_str}")
        
        result = await self._request("POST", "/v5/order/create", params)
        
        if result:
            self.hedge_positions.pop(position_id, None)
            logger.info(f"[HEDGE] ✓ Закрыто: {bybit_symbol} qty={close_qty_str}")
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
    
    async def get_all_positions(self) -> List[Dict]:
        """Получить все открытые позиции на Bybit"""
        if not self.enabled:
            return []
        
        result = await self._request("GET", "/v5/position/list", {
            "category": "linear",
            "settleCoin": "USDT"
        })
        
        positions = []
        if result:
            for pos in result.get("list", []):
                size = float(pos.get("size", 0))
                if size > 0:
                    pnl_str = pos.get("unrealisedPnl", "0") or "0"
                    entry_str = pos.get("avgPrice", "0") or "0"
                    mark_str = pos.get("markPrice", "0") or "0"
                    
                    positions.append({
                        'symbol': pos.get("symbol", ""),
                        'entry': float(entry_str),
                        'current': float(mark_str),
                        'pnl': float(pnl_str),
                        'size': size,
                        'side': pos.get("side", ""),  # Buy = LONG, Sell = SHORT
                        'leverage': pos.get("leverage", "1")
                    })
        
        logger.info(f"[BYBIT] All positions: {len(positions)} open")
        return positions
    
    async def get_closed_pnl(self, symbol: str = None, limit: int = 50) -> List[Dict]:
        """
        Получить закрытые позиции с Bybit за последние 7 дней
        
        Args:
            symbol: Конкретный символ (None = все)
            limit: Максимум записей
        
        Returns:
            Список закрытых позиций с PnL
        """
        if not self.enabled:
            return []
        
        params = {
            "category": "linear",
            "limit": limit
        }
        
        if symbol:
            params["symbol"] = self._to_bybit_symbol(symbol)
        
        result = await self._request("GET", "/v5/position/closed-pnl", params)
        
        closed_positions = []
        if result:
            for item in result.get("list", []):
                closed_positions.append({
                    'symbol': item.get("symbol", ""),
                    'side': item.get("side", ""),  # Buy = закрытие шорта, Sell = закрытие лонга
                    'qty': float(item.get("qty", 0)),
                    'entry_price': float(item.get("avgEntryPrice", 0)),
                    'exit_price': float(item.get("avgExitPrice", 0)),
                    'closed_pnl': float(item.get("closedPnl", 0)),
                    'leverage': item.get("leverage", "1"),
                    'order_id': item.get("orderId", ""),
                    'created_time': int(item.get("createdTime", 0)),
                    'updated_time': int(item.get("updatedTime", 0))
                })
        
        logger.info(f"[BYBIT] Closed PnL: {len(closed_positions)} records")
        return closed_positions
    
    async def get_last_order_price(self, symbol: str, side: str = None) -> Optional[Dict]:
        """
        Получить цену последнего исполненного ордера
        
        Args:
            symbol: Торговая пара (BTC/USDT)
            side: 'Sell' для закрытия LONG, 'Buy' для закрытия SHORT (опционально)
        
        Returns:
            {'price': float, 'qty': float, 'order_id': str} или None
        """
        if not self.enabled:
            return None
        
        bybit_symbol = self._to_bybit_symbol(symbol)
        
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "orderStatus": "Filled",
            "limit": 5
        }
        
        result = await self._request("GET", "/v5/order/history", params)
        
        if result:
            orders = result.get("list", [])
            for order in orders:
                # Если указан side, фильтруем
                if side and order.get("side") != side:
                    continue
                
                avg_price = order.get("avgPrice", "0")
                if avg_price and float(avg_price) > 0:
                    logger.info(f"[BYBIT] Last order {bybit_symbol}: price=${avg_price}, qty={order.get('qty')}")
                    return {
                        'price': float(avg_price),
                        'qty': float(order.get("qty", 0)),
                        'order_id': order.get("orderId", ""),
                        'side': order.get("side", "")
                    }
        
        return None


# Глобальный экземпляр
hedger = BybitHedger()


async def hedge_open(position_id: int, symbol: str, direction: str, amount: float, 
                     tp: float = None, sl: float = None,
                     tp1: float = None, tp2: float = None, tp3: float = None) -> Optional[Dict]:
    """Wrapper для открытия хеджа с частичными TP. Возвращает {'order_id': str, 'qty': float}"""
    return await hedger.open_hedge(position_id, symbol, direction, amount, tp, sl, tp1, tp2, tp3)


async def hedge_set_partial_tps(symbol: str, direction: str, qty: float, tp1: float, tp2: float, tp3: float) -> bool:
    """Wrapper для установки частичных TP"""
    return await hedger.set_partial_take_profits(symbol, direction, qty, tp1, tp2, tp3)


async def hedge_close(position_id: int, symbol: str, direction: str, bybit_qty: float = None) -> bool:
    """Wrapper для закрытия хеджа. bybit_qty - количество в базовой валюте (None = закрыть всё)"""
    return await hedger.close_hedge(position_id, symbol, direction, bybit_qty)


async def is_hedging_enabled() -> bool:
    """Проверить, включено ли хеджирование"""
    return hedger.enabled
