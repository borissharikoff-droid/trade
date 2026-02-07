"""
AI Trading Analyzer powered by DeepSeek
Анализирует сделки, учится на истории, понимает влияние новостей

ВАЖНО: Данные теперь хранятся в PostgreSQL для сохранения между деплоями!
"""

import os
import json
import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from collections import deque
import hashlib

logger = logging.getLogger(__name__)

# Price tracking for news impact
_news_price_tracker: Dict[str, Dict] = {}  # news_id -> {timestamp, coins, prices_before, checked}

# DeepSeek API Configuration - key must be set via env, no default
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"  # или deepseek-coder для технического анализа

# Memory storage - теперь используем PostgreSQL если доступен
DATABASE_URL = os.getenv("DATABASE_URL")
USE_POSTGRES_MEMORY = DATABASE_URL is not None

# Fallback to local files only if no PostgreSQL
MEMORY_PATH = "ai_memory.json"
INSIGHTS_PATH = "ai_insights.json"

try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("[AI] OpenAI library not installed. Run: pip install openai")


@dataclass
class TradeAnalysis:
    """Результат анализа сделки"""
    trade_id: str
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    pnl: float
    is_win: bool
    reason: str  # TP/SL/MANUAL etc
    
    # AI Analysis
    win_factors: List[str]  # Факторы успеха
    loss_factors: List[str]  # Факторы провала
    market_conditions: str  # Состояние рынка
    entry_quality: int  # 1-10
    exit_quality: int  # 1-10
    lessons_learned: List[str]  # Уроки
    recommendations: List[str]  # Рекомендации
    confidence_score: float  # 0-1


@dataclass 
class NewsImpact:
    """Анализ влияния новости на рынок"""
    news_id: str
    title: str
    source: str
    timestamp: datetime
    affected_coins: List[str]
    
    # AI Analysis
    predicted_direction: str  # BULLISH/BEARISH/NEUTRAL
    actual_direction: str  # Что произошло на самом деле
    price_change_percent: float
    prediction_accuracy: float  # Насколько точно предсказали
    patterns_identified: List[str]  # Найденные паттерны
    similar_news_outcomes: List[Dict]  # Похожие новости и их исходы


class AIMemory:
    """
    Долгосрочная память AI - хранит знания и инсайты.
    
    ВАЖНО: Теперь использует PostgreSQL для сохранения между деплоями!
    При первом запуске создаёт таблицу ai_memory.
    """
    
    def __init__(self):
        self.trade_patterns: Dict[str, Dict] = {}  # Паттерны успешных/неуспешных сделок
        self.news_patterns: Dict[str, Dict] = {}  # Паттерны влияния новостей
        self.market_insights: List[Dict] = []  # Общие инсайты о рынке
        self.symbol_knowledge: Dict[str, Dict] = {}  # Знания о конкретных монетах
        self.learned_rules: List[str] = []  # Выученные правила
        self.performance_history: List[Dict] = []  # История performance
        self.recent_analyses: List[Dict] = []  # Последние анализы сделок (для дашборда)
        
        # Расширенные паттерны для глубокого обучения
        self.time_patterns: Dict[str, Dict[str, int]] = {}  # hour_utc -> {"wins": n, "losses": n}
        self.regime_patterns: Dict[str, Dict[str, int]] = {}  # regime -> {"wins": n, "losses": n}
        self.correlation_patterns: Dict[str, Dict[str, int]] = {}  # symbol -> btc_correlation outcomes
        
        # Обратная связь: точность решений AI по символу и последнее решение до закрытия
        self.symbol_accuracy: Dict[str, float] = {}  # symbol -> EMA accuracy 0-1
        self.last_signal_decisions: Dict[str, Dict] = {}  # symbol -> {confidence, should_trade, timestamp}
        
        # Статистика
        self.total_trades_analyzed: int = 0
        self.total_news_analyzed: int = 0
        self.prediction_accuracy: float = 0.5
        self._prediction_accuracy_ema_n: int = 0  # для EMA
        
        self._db_initialized = False
        self._init_db()
        self._load()
    
    def _get_db_connection(self):
        """Получить подключение к PostgreSQL"""
        if not USE_POSTGRES_MEMORY:
            return None
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            logger.error(f"[AI_MEMORY] DB connection error: {e}")
            return None
    
    def _init_db(self):
        """Создать таблицу для AI памяти если не существует"""
        if not USE_POSTGRES_MEMORY:
            logger.info("[AI_MEMORY] Using local file storage (no PostgreSQL)")
            return
        
        conn = self._get_db_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ai_memory (
                        id SERIAL PRIMARY KEY,
                        key VARCHAR(100) UNIQUE NOT NULL,
                        data JSONB NOT NULL,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                conn.commit()
            self._db_initialized = True
            logger.info("[AI_MEMORY] ✓ PostgreSQL table initialized")
        except Exception as e:
            logger.error(f"[AI_MEMORY] DB init error: {e}")
        finally:
            conn.close()
    
    def _load(self):
        """Загрузить память из PostgreSQL или файла"""
        # Сначала пробуем PostgreSQL
        if USE_POSTGRES_MEMORY and self._db_initialized:
            if self._load_from_db():
                return
        
        # Fallback: загрузка из файла
        self._load_from_file()
    
    def _load_from_db(self) -> bool:
        """Загрузить из PostgreSQL"""
        conn = self._get_db_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT key, data FROM ai_memory")
                rows = cur.fetchall()
                
                for key, data in rows:
                    if key == 'trade_patterns':
                        self.trade_patterns = data or {}
                    elif key == 'news_patterns':
                        self.news_patterns = data or {}
                    elif key == 'market_insights':
                        self.market_insights = data or []
                    elif key == 'symbol_knowledge':
                        self.symbol_knowledge = data or {}
                    elif key == 'learned_rules':
                        self.learned_rules = data or []
                    elif key == 'performance_history':
                        self.performance_history = data or []
                    elif key == 'recent_analyses':
                        self.recent_analyses = data or []
                    elif key == 'time_patterns':
                        self.time_patterns = data or {}
                    elif key == 'regime_patterns':
                        self.regime_patterns = data or {}
                    elif key == 'correlation_patterns':
                        self.correlation_patterns = data or {}
                    elif key == 'symbol_accuracy':
                        self.symbol_accuracy = data or {}
                    elif key == 'stats':
                        self.total_trades_analyzed = data.get('total_trades_analyzed', 0)
                        self.total_news_analyzed = data.get('total_news_analyzed', 0)
                        self.prediction_accuracy = data.get('prediction_accuracy', 0.5)
                        self._prediction_accuracy_ema_n = data.get('prediction_accuracy_ema_n', 0)
                
                logger.info(f"[AI_MEMORY] ✓ Loaded from PostgreSQL: {self.total_trades_analyzed} trades, {len(self.recent_analyses)} recent analyses")
                return True
        except Exception as e:
            logger.error(f"[AI_MEMORY] DB load error: {e}")
            return False
        finally:
            conn.close()
    
    def _load_from_file(self):
        """Загрузить из локального файла (fallback)"""
        try:
            if os.path.exists(MEMORY_PATH):
                with open(MEMORY_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.trade_patterns = data.get('trade_patterns', {})
                    self.news_patterns = data.get('news_patterns', {})
                    self.market_insights = data.get('market_insights', [])
                    self.symbol_knowledge = data.get('symbol_knowledge', {})
                    self.learned_rules = data.get('learned_rules', [])
                    self.performance_history = data.get('performance_history', [])
                    self.recent_analyses = data.get('recent_analyses', [])
                    self.time_patterns = data.get('time_patterns', {})
                    self.regime_patterns = data.get('regime_patterns', {})
                    self.correlation_patterns = data.get('correlation_patterns', {})
                    self.symbol_accuracy = data.get('symbol_accuracy', {})
                    self.total_trades_analyzed = data.get('total_trades_analyzed', 0)
                    self.total_news_analyzed = data.get('total_news_analyzed', 0)
                    self.prediction_accuracy = data.get('prediction_accuracy', 0.5)
                    self._prediction_accuracy_ema_n = data.get('prediction_accuracy_ema_n', 0)
                logger.info(f"[AI_MEMORY] Loaded from file: {self.total_trades_analyzed} trades, {len(self.recent_analyses)} recent analyses")
                
                # Мигрируем в PostgreSQL если доступен
                if USE_POSTGRES_MEMORY and self._db_initialized:
                    logger.info("[AI_MEMORY] Migrating file data to PostgreSQL...")
                    self.save()
        except Exception as e:
            logger.error(f"[AI_MEMORY] File load error: {e}")
    
    def save(self):
        """Сохранить память в PostgreSQL (с fallback на файл)"""
        # Сначала пробуем PostgreSQL
        if USE_POSTGRES_MEMORY and self._db_initialized:
            if self._save_to_db():
                return
        
        # Fallback: сохранение в файл
        self._save_to_file()
    
    def _save_to_db(self) -> bool:
        """Сохранить в PostgreSQL"""
        conn = self._get_db_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cur:
                # Используем UPSERT для каждого ключа
                data_items = [
                    ('trade_patterns', self.trade_patterns),
                    ('news_patterns', self.news_patterns),
                    ('market_insights', self.market_insights[-1000:]),
                    ('symbol_knowledge', self.symbol_knowledge),
                    ('learned_rules', self.learned_rules[-500:]),
                    ('performance_history', self.performance_history[-100:]),
                    ('recent_analyses', self.recent_analyses[-100:]),
                    ('time_patterns', self.time_patterns),
                    ('regime_patterns', self.regime_patterns),
                    ('correlation_patterns', self.correlation_patterns),
                    ('symbol_accuracy', self.symbol_accuracy),
                    ('stats', {
                        'total_trades_analyzed': self.total_trades_analyzed,
                        'total_news_analyzed': self.total_news_analyzed,
                        'prediction_accuracy': self.prediction_accuracy,
                        'prediction_accuracy_ema_n': self._prediction_accuracy_ema_n,
                        'last_saved': datetime.now().isoformat()
                    })
                ]
                
                for key, data in data_items:
                    cur.execute("""
                        INSERT INTO ai_memory (key, data, updated_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (key) DO UPDATE SET
                            data = EXCLUDED.data,
                            updated_at = NOW()
                    """, (key, json.dumps(data, default=str)))
                
                conn.commit()
                logger.debug("[AI_MEMORY] ✓ Saved to PostgreSQL")
                return True
        except Exception as e:
            logger.error(f"[AI_MEMORY] DB save error: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def _save_to_file(self):
        """Сохранить в локальный файл (fallback)"""
        try:
            data = {
                'trade_patterns': self.trade_patterns,
                'news_patterns': self.news_patterns,
                'market_insights': self.market_insights[-1000:],
                'symbol_knowledge': self.symbol_knowledge,
                'learned_rules': self.learned_rules[-500:],
                'performance_history': self.performance_history[-100:],
                'recent_analyses': self.recent_analyses[-100:],
                'time_patterns': self.time_patterns,
                'regime_patterns': self.regime_patterns,
                'correlation_patterns': self.correlation_patterns,
                'symbol_accuracy': self.symbol_accuracy,
                'total_trades_analyzed': self.total_trades_analyzed,
                'total_news_analyzed': self.total_news_analyzed,
                'prediction_accuracy': self.prediction_accuracy,
                'prediction_accuracy_ema_n': self._prediction_accuracy_ema_n,
                'last_saved': datetime.now().isoformat()
            }
            with open(MEMORY_PATH, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            logger.error(f"[AI_MEMORY] File save error: {e}")
    
    def add_trade_pattern(self, symbol: str, pattern: Dict):
        """Добавить паттерн сделки и обновить time/regime паттерны"""
        if symbol not in self.trade_patterns:
            self.trade_patterns[symbol] = {'wins': [], 'losses': []}
        
        key = 'wins' if pattern.get('is_win') else 'losses'
        self.trade_patterns[symbol][key].append(pattern)
        
        # Ограничиваем размер
        if len(self.trade_patterns[symbol][key]) > 100:
            self.trade_patterns[symbol][key] = self.trade_patterns[symbol][key][-100:]
        
        self.total_trades_analyzed += 1
        
        # Обновляем time_patterns (час UTC по timestamp)
        try:
            ts = pattern.get('timestamp', '')
            if ts:
                if isinstance(ts, str):
                    ts = ts.replace('Z', '+00:00')
                    dt = datetime.fromisoformat(ts)
                else:
                    dt = ts
                hour_key = str(dt.hour)
                if hour_key not in self.time_patterns:
                    self.time_patterns[hour_key] = {'wins': 0, 'losses': 0}
                self.time_patterns[hour_key][key] = self.time_patterns[hour_key].get(key, 0) + 1
        except Exception:
            pass
        
        # Обновляем regime_patterns
        regime = (pattern.get('market_regime_at_entry') or '').strip().upper() or 'UNKNOWN'
        if regime not in self.regime_patterns:
            self.regime_patterns[regime] = {'wins': 0, 'losses': 0}
        self.regime_patterns[regime][key] = self.regime_patterns[regime].get(key, 0) + 1
    
    def add_news_pattern(self, news_hash: str, pattern: Dict):
        """Добавить паттерн новости"""
        self.news_patterns[news_hash] = pattern
        
        # Ограничиваем размер
        if len(self.news_patterns) > 500:
            # Удаляем самые старые
            oldest_keys = list(self.news_patterns.keys())[:100]
            for k in oldest_keys:
                del self.news_patterns[k]
        
        self.total_news_analyzed += 1
    
    def add_insight(self, insight: str, category: str = "general"):
        """Добавить инсайт"""
        self.market_insights.append({
            'insight': insight,
            'category': category,
            'timestamp': datetime.now().isoformat()
        })
    
    def _rule_similarity(self, a: str, b: str) -> float:
        """Word overlap ratio 0-1. Used to reject near-duplicate rules."""
        if not a or not b:
            return 0.0
        wa, wb = set(a.lower().split()), set(b.lower().split())
        if not wa:
            return 0.0
        return len(wa & wb) / len(wa)
    
    def add_rule(self, rule: str):
        """Добавить выученное правило (отклоняем только при >85% совпадении по словам)."""
        rule = (rule or '').strip()
        if not rule:
            return
        if rule in self.learned_rules:
            logger.debug("[AI_MEMORY] Rule rejected as exact duplicate")
            return
        for existing in self.learned_rules[-500:]:
            if self._rule_similarity(rule, existing) > 0.85:
                logger.debug(f"[AI_MEMORY] Rule rejected as similar to existing: {rule[:60]}...")
                return
        self.learned_rules.append(rule)
    
    def get_symbol_context(self, symbol: str) -> str:
        """Получить контекст знаний о символе"""
        patterns = self.trade_patterns.get(symbol, {'wins': [], 'losses': []})
        knowledge = self.symbol_knowledge.get(symbol, {})
        
        context = f"Symbol: {symbol}\n"
        context += f"Historical wins: {len(patterns['wins'])}, losses: {len(patterns['losses'])}\n"
        
        if patterns['wins']:
            recent_wins = patterns['wins'][-5:]
            context += "Recent winning patterns:\n"
            for w in recent_wins:
                context += f"  - {w.get('summary', 'N/A')}\n"
        
        if patterns['losses']:
            recent_losses = patterns['losses'][-5:]
            context += "Recent losing patterns:\n"
            for l in recent_losses:
                context += f"  - {l.get('summary', 'N/A')}\n"
        
        if knowledge:
            context += f"Known characteristics: {knowledge}\n"
        
        return context
    
    def get_enhanced_context(self, symbol: str, direction: str = '', market_regime: str = '') -> str:
        """Расширенный контекст для AI: символ + time/regime/correlation паттерны."""
        base = self.get_symbol_context(symbol)
        parts = [base]
        
        # Лучшие/худшие часы (UTC)
        if self.time_patterns:
            by_hour = []
            for hour, counts in sorted(self.time_patterns.items(), key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0):
                w, l = counts.get('wins', 0), counts.get('losses', 0)
                total = w + l
                wr = (w / total * 100) if total else 0
                by_hour.append(f"  UTC {hour}:00 — WR {wr:.0f}% ({w}W/{l}L)")
            if by_hour:
                parts.append("Time patterns (by hour UTC):\n" + "\n".join(by_hour[-12:]))
        
        # Режим рынка: где лучше/хуже
        if self.regime_patterns:
            by_regime = []
            for regime, counts in self.regime_patterns.items():
                w, l = counts.get('wins', 0), counts.get('losses', 0)
                total = w + l
                wr = (w / total * 100) if total else 0
                by_regime.append(f"  {regime}: WR {wr:.0f}% ({w}W/{l}L)")
            if by_regime:
                parts.append("Regime patterns:\n" + "\n".join(by_regime))
        
        # Correlation с BTC по символу (если есть данные)
        if symbol in self.correlation_patterns:
            corr = self.correlation_patterns[symbol]
            parts.append(f"BTC correlation outcomes: {corr}")
        
        return "\n---\n".join(parts)
    
    def record_signal_decision(self, symbol: str, decision: Dict):
        """Записать решение AI по сигналу (вызывать после should_take_trade перед открытием сделки)."""
        self.last_signal_decisions[symbol] = {
            'confidence': decision.get('confidence', 0.5),
            'should_trade': decision.get('should_trade', True),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_pending_feedback(self, symbol: str, max_age_hours: float = 24.0) -> Optional[Dict]:
        """Получить последнее решение AI по символу для обратной связи (и удалить если устарело)."""
        rec = self.last_signal_decisions.get(symbol)
        if not rec:
            return None
        try:
            ts = datetime.fromisoformat(rec['timestamp'].replace('Z', '+00:00'))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            if age_h > max_age_hours or age_h < 0:
                del self.last_signal_decisions[symbol]
                return None
        except Exception:
            pass
        return rec
    
    def clear_signal_decision(self, symbol: str):
        """Очистить сохранённое решение по символу после учёта обратной связи."""
        self.last_signal_decisions.pop(symbol, None)
    
    def update_prediction_accuracy(self, actual_win: bool, predicted_confidence: float, symbol: Optional[str] = None, alpha: float = 0.1):
        """Обновить точность предсказаний EMA; при указании symbol — также обновить symbol_accuracy."""
        correct = 1.0 if (actual_win and predicted_confidence >= 0.5) or (not actual_win and predicted_confidence < 0.5) else 0.0
        self._prediction_accuracy_ema_n += 1
        n = self._prediction_accuracy_ema_n
        self.prediction_accuracy = self.prediction_accuracy * (1 - alpha) + correct * alpha if n > 1 else correct
        if symbol:
            prev = self.symbol_accuracy.get(symbol, 0.5)
            self.symbol_accuracy[symbol] = prev * (1 - alpha) + correct * alpha
        logger.debug(f"[AI_MEMORY] Prediction accuracy EMA: {self.prediction_accuracy:.3f} (n={n})" + (f", {symbol}: {self.symbol_accuracy.get(symbol, 0):.3f}" if symbol else ""))
    
    def get_relevant_rules(self, context: str) -> List[str]:
        """Релевантные правила по символу, направлению и режиму рынка с оценкой по релевантности."""
        context_lower = context.lower().strip()
        tokens = set(context_lower.split())
        # Извлекаем символ (первый токен может быть BTC/USDT или BTC)
        symbol_part = (context_lower.split() or [''])[0].replace('/usdt', '').strip()
        symbol_bases = [symbol_part]
        if '/' in context_lower:
            symbol_bases.append(context_lower.split('/')[0].strip())
        # Направление и режим
        direction_words = {'long', 'short', 'buy', 'sell'}
        regime_words = {'uptrend', 'downtrend', 'ranging', 'trend', 'volatility', 'regime'}
        context_directions = tokens & direction_words
        context_regimes = tokens & regime_words
        
        scored = []
        for rule in self.learned_rules[-100:]:
            rule_lower = rule.lower()
            rule_tokens = set(rule_lower.split())
            score = 0.0
            # Совпадение по символу (BTC, ETH и т.д.)
            for base in symbol_bases:
                if len(base) >= 2 and base in rule_lower:
                    score += 2.0
                    break
            # Совпадение по направлению (LONG/SHORT)
            if context_directions and (context_directions & rule_tokens):
                score += 1.5
            # Совпадение по режиму рынка
            if context_regimes and (context_regimes & rule_tokens):
                score += 1.0
            # Общие значимые слова (не стоп-слова)
            stop = {'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or', 'is', 'if', 'no'}
            overlap = (tokens & rule_tokens) - stop
            score += 0.3 * len(overlap)
            if score >= 0.3:
                scored.append((score, rule))
        scored.sort(key=lambda x: -x[0])
        return [r for _, r in scored[:10]]


class DeepSeekAnalyzer:
    """Основной AI анализатор на DeepSeek"""
    
    def __init__(self):
        self.memory = AIMemory()
        self.client = None
        # recent_analyses теперь хранится в memory для персистентности
        self.pending_trades = {}  # Сделки ожидающие анализа
        self._initialized = False
        
        # Rate limiting
        self._last_request = datetime.min
        self._min_interval = 1.0  # Минимум 1 секунда между запросами
    
    @property
    def recent_analyses(self):
        """Последние анализы - теперь из персистентной памяти"""
        return self.memory.recent_analyses
        
    async def initialize(self):
        """Инициализация клиента"""
        if not OPENAI_AVAILABLE:
            logger.error("[AI] OpenAI library not available")
            return False
        if not DEEPSEEK_API_KEY:
            logger.error("[AI] DEEPSEEK_API_KEY not set in environment")
            return False
        try:
            self.client = AsyncOpenAI(
                api_key=DEEPSEEK_API_KEY,
                base_url=DEEPSEEK_BASE_URL
            )
            self._initialized = True
            logger.info("[AI] DeepSeek analyzer initialized")
            return True
        except Exception as e:
            logger.error(f"[AI] Failed to initialize: {e}")
            return False
    
    async def _rate_limit(self):
        """Rate limiting"""
        now = datetime.now()
        elapsed = (now - self._last_request).total_seconds()
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request = datetime.now()
    
    async def _call_deepseek(self, messages: List[Dict], max_tokens: int = 1000) -> Optional[str]:
        """Вызов DeepSeek API"""
        if not self._initialized:
            await self.initialize()
        
        if not self.client:
            return None
        
        await self._rate_limit()
        
        try:
            response = await self.client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=messages,
                max_tokens=max_tokens,
                temperature=0.7
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"[AI] DeepSeek API error: {e}")
            return None
    
    # ==================== TRADE ANALYSIS ====================
    
    async def analyze_closed_trade(self, trade: Dict) -> Optional[TradeAnalysis]:
        """
        Анализ закрытой сделки - почему выиграли/проиграли
        """
        try:
            symbol = trade.get('symbol', 'UNKNOWN')
            pnl = float(trade.get('pnl', 0))
            is_win = pnl > 0
            
            # Получаем контекст из памяти
            symbol_context = self.memory.get_symbol_context(symbol)
            relevant_rules = self.memory.get_relevant_rules(symbol)
            
            system_prompt = """Ты - эксперт-трейдер с 20-летним опытом в крипто-трейдинге.
Твоя задача - глубоко анализировать сделки и извлекать уроки для повышения win rate.

МНОГОФАКТОРНЫЙ АНАЛИЗ (обязательно затронь):
- Технический: RSI, объём, тренд на таймфрейме входа, структура рынка
- Режим рынка в момент входа (тренд/флэт/волатильность)
- Контекст новостей/событий если релевантен
- Время суток и день недели (паттерны ликвидности)
- Качество входа: вовремя ли вошли относительно движения

Формат ответа (JSON):
{
    "win_factors": ["фактор1", "фактор2"],
    "loss_factors": ["фактор1", "фактор2"],
    "market_conditions": "подробное описание рынка в момент сделки",
    "market_regime_at_entry": "UPTREND/DOWNTREND/RANGING/HIGH_VOLATILITY",
    "pattern_type": "breakout/reversal/trend_continuation/pullback/range/other",
    "entry_timing_score": 1-10,
    "market_alignment_score": 1-10,
    "entry_quality": 1-10,
    "exit_quality": 1-10,
    "lessons_learned": ["урок1", "урок2"],
    "recommendations": ["рекомендация1"],
    "confidence_score": 0.0-1.0,
    "new_rule": "одно КОНКРЕТНОЕ правило для запоминания: обязательно укажи символ, направление (LONG/SHORT), режим рынка или время — не давай общих фраз. Или пустая строка если нового правила нет."
}"""

            user_prompt = f"""Проанализируй эту сделку:

Символ: {symbol}
Направление: {trade.get('direction', 'UNKNOWN')}
Вход: ${trade.get('entry', 0):.4f}
Выход: ${trade.get('exit_price', 0):.4f}
PnL: ${pnl:.2f} ({'ПРИБЫЛЬ ✅' if is_win else 'УБЫТОК ❌'})
Причина закрытия: {trade.get('reason', 'UNKNOWN')}
Время в позиции: {trade.get('duration', 'N/A')}

--- Контекст из памяти ---
{symbol_context}

--- Известные правила ---
{chr(10).join(relevant_rules[:5]) if relevant_rules else 'Нет релевантных правил'}
---

Проанализируй:
1. Почему сделка {'была прибыльной' if is_win else 'была убыточной'}?
2. Какие факторы повлияли на результат?
3. Что можно улучшить?
4. Какое новое правило можно извлечь?"""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            response = await self._call_deepseek(messages, max_tokens=1200)
            
            if not response:
                return None
            
            # Парсим JSON ответ
            try:
                # Извлекаем JSON из ответа
                json_start = response.find('{')
                json_end = response.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    analysis_data = json.loads(response[json_start:json_end])
                else:
                    logger.warning(f"[AI] Could not parse JSON from response")
                    analysis_data = {}
            except json.JSONDecodeError:
                logger.warning(f"[AI] Invalid JSON in response")
                analysis_data = {}
            
            # Создаём TradeAnalysis
            analysis = TradeAnalysis(
                trade_id=str(trade.get('id', hash(str(trade)))),
                symbol=symbol,
                direction=trade.get('direction', 'UNKNOWN'),
                entry_price=float(trade.get('entry', 0)),
                exit_price=float(trade.get('exit_price', 0)),
                pnl=pnl,
                is_win=is_win,
                reason=trade.get('reason', 'UNKNOWN'),
                win_factors=analysis_data.get('win_factors', []),
                loss_factors=analysis_data.get('loss_factors', []),
                market_conditions=analysis_data.get('market_conditions', ''),
                entry_quality=analysis_data.get('entry_quality', 5),
                exit_quality=analysis_data.get('exit_quality', 5),
                lessons_learned=analysis_data.get('lessons_learned', []),
                recommendations=analysis_data.get('recommendations', []),
                confidence_score=analysis_data.get('confidence_score', 0.5)
            )
            
            # Сохраняем в память (включая режим и тип паттерна для обучения)
            pattern = {
                'is_win': is_win,
                'direction': trade.get('direction'),
                'pnl': pnl,
                'reason': trade.get('reason'),
                'entry_quality': analysis.entry_quality,
                'summary': analysis_data.get('market_conditions', '')[:100],
                'lessons': analysis.lessons_learned[:2],
                'market_regime_at_entry': analysis_data.get('market_regime_at_entry', ''),
                'pattern_type': analysis_data.get('pattern_type', ''),
                'entry_timing_score': analysis_data.get('entry_timing_score'),
                'market_alignment_score': analysis_data.get('market_alignment_score'),
                'timestamp': datetime.now().isoformat()
            }
            self.memory.add_trade_pattern(symbol, pattern)
            
            # Добавляем новое правило если есть
            if analysis_data.get('new_rule'):
                self.memory.add_rule(analysis_data['new_rule'])
            
            # Добавляем в recent_analyses с временем закрытия сделки
            analysis_dict = asdict(analysis)
            analysis_dict['timestamp'] = trade.get('closed_at') or datetime.now().isoformat()
            self.memory.recent_analyses.append(analysis_dict)
            if len(self.memory.recent_analyses) > 100:
                self.memory.recent_analyses = self.memory.recent_analyses[-100:]
            
            # Сохраняем память (включая recent_analyses)
            self.memory.save()
            
            # Обратная связь: обновить точность предсказаний по факту сделки
            pending = self.memory.get_pending_feedback(symbol)
            if pending:
                self.memory.update_prediction_accuracy(is_win, pending.get('confidence', 0.5), symbol=symbol)
                self.memory.clear_signal_decision(symbol)
                self.memory.save()
            
            logger.info(f"[AI] Analyzed trade {symbol}: {'WIN' if is_win else 'LOSS'}, quality={analysis.entry_quality}/{analysis.exit_quality}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"[AI] Error analyzing trade: {e}", exc_info=True)
            return None
    
    # ==================== LIVE TRADING DECISION ====================
    
    async def should_take_trade(self, signal: Dict, market_data: Dict = None) -> Dict:
        """
        Принятие решения о сделке в реальном времени
        
        Returns:
            {
                'should_trade': bool,
                'confidence': float,
                'reasoning': str,
                'adjusted_sl': float or None,
                'adjusted_tp': float or None,
                'position_size_multiplier': float
            }
        """
        try:
            symbol = signal.get('symbol', 'UNKNOWN')
            
            # Расширенный контекст: символ + time/regime/correlation паттерны
            market_regime = (market_data or {}).get('market_regime', '') or (market_data or {}).get('regime', '')
            symbol_context = self.memory.get_enhanced_context(symbol, signal.get('direction', ''), market_regime)
            relevant_rules = self.memory.get_relevant_rules(f"{symbol} {signal.get('direction', '')}")
            
            # Статистика из памяти
            patterns = self.memory.trade_patterns.get(symbol, {'wins': [], 'losses': []})
            win_count = len(patterns['wins'])
            loss_count = len(patterns['losses'])
            historical_wr = win_count / (win_count + loss_count) if (win_count + loss_count) > 0 else 0.5
            
            system_prompt = """Ты - AI торговый советник. Принимай решения о сделках на основе взвешенной оценки.

КРИТЕРИИ (оцени каждый и выведи итог):
1. Технический анализ (40%): качество сетапа, R/R, структура, объём
2. Историческая успешность (25%): WR по символу, похожие сетапы из памяти и их исходы
3. Рынок и новости (20%): корреляция с BTC, funding rate, open interest, последние новости
4. Риск (15%): волатильность, зона входа, динамический SL/TP на основе ATR

Учитывай похожие исторические сетапы: если по символу/направлению были убытки в таком же режиме — снижай уверенность. Указывай оптимальную зону входа и предложения по SL/TP в процентах если нужно скорректировать. Учитывай BOB index (1-10: Bearish/Neutral/Bullish) из рыночных данных — при расхождении направления сделки с BOB снижай уверенность.

Формат ответа (JSON):
{
    "should_trade": true/false,
    "confidence": 0.0-1.0,
    "reasoning": "краткое обоснование с учётом весов",
    "risk_level": "low/medium/high",
    "adjusted_sl_percent": null или число (процент от входа),
    "adjusted_tp_percent": null или число,
    "position_size_multiplier": 0.5-1.5,
    "optimal_entry_zone": "описание зоны входа если есть",
    "key_concerns": ["беспокойство1"],
    "key_strengths": ["сильная сторона1"]
}"""

            user_prompt = f"""Оцени этот торговый сигнал:

=== СИГНАЛ ===
Символ: {symbol}
Направление: {signal.get('direction', 'UNKNOWN')}
Цена входа: ${signal.get('entry', 0)}
Stop Loss: ${signal.get('sl', 0)} ({signal.get('sl_percent', 0):.1f}%)
Take Profit: ${signal.get('tp', 0)} ({signal.get('tp_percent', 0):.1f}%)
Качество сигнала: {signal.get('quality', 'N/A')}
Winrate провайдера: {signal.get('provider_winrate', 'N/A')}%

=== ИСТОРИЯ СИМВОЛА ===
{symbol_context}
Исторический WR для {symbol}: {historical_wr*100:.1f}% ({win_count}W/{loss_count}L)

=== ВЫУЧЕННЫЕ ПРАВИЛА ===
{chr(10).join(relevant_rules[:7]) if relevant_rules else 'Нет специфичных правил'}

=== РЫНОЧНЫЕ ДАННЫЕ ===
{json.dumps(market_data, indent=2) if market_data else 'Нет дополнительных данных'}

Дай рекомендацию: входить в сделку или нет?"""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            response = await self._call_deepseek(messages, max_tokens=1000)
            
            if not response:
                return {
                    'should_trade': True,  # Default: берём сделку
                    'confidence': 0.5,
                    'reasoning': 'AI unavailable, using default',
                    'position_size_multiplier': 1.0
                }
            
            # Парсим ответ
            try:
                json_start = response.find('{')
                json_end = response.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    decision = json.loads(response[json_start:json_end])
                else:
                    decision = {'should_trade': True, 'confidence': 0.5}
            except json.JSONDecodeError:
                decision = {'should_trade': True, 'confidence': 0.5}
            
            result = {
                'should_trade': decision.get('should_trade', True),
                'confidence': decision.get('confidence', 0.5),
                'reasoning': decision.get('reasoning', ''),
                'risk_level': decision.get('risk_level', 'medium'),
                'adjusted_sl': decision.get('adjusted_sl_percent'),
                'adjusted_tp': decision.get('adjusted_tp_percent'),
                'position_size_multiplier': decision.get('position_size_multiplier', 1.0),
                'optimal_entry_zone': decision.get('optimal_entry_zone', ''),
                'key_concerns': decision.get('key_concerns', []),
                'key_strengths': decision.get('key_strengths', [])
            }
            
            logger.info(f"[AI] Trade decision for {symbol}: {'✅ TAKE' if result['should_trade'] else '❌ SKIP'} (conf={result['confidence']:.2f})")
            
            # Сохраняем решение для обратной связи после закрытия сделки
            self.memory.record_signal_decision(symbol, result)
            
            return result
            
        except Exception as e:
            logger.error(f"[AI] Error in trade decision: {e}")
            return {
                'should_trade': True,
                'confidence': 0.5,
                'reasoning': f'Error: {e}',
                'position_size_multiplier': 1.0
            }
    
    # ==================== NEWS ANALYSIS ====================
    
    async def analyze_news_impact(self, news: Dict, price_before: float, price_after: float) -> Optional[NewsImpact]:
        """
        Анализ влияния новости на рынок
        """
        try:
            title = news.get('title', '')
            source = news.get('source', '')
            coins = news.get('affected_coins', [])
            
            price_change = ((price_after - price_before) / price_before) * 100 if price_before > 0 else 0
            actual_direction = "BULLISH" if price_change > 0.5 else "BEARISH" if price_change < -0.5 else "NEUTRAL"
            
            # Ищем похожие новости в памяти
            similar_outcomes = []
            for news_hash, pattern in list(self.memory.news_patterns.items())[-50:]:
                if any(coin in pattern.get('coins', []) for coin in coins):
                    similar_outcomes.append({
                        'title': pattern.get('title', '')[:50],
                        'direction': pattern.get('actual_direction'),
                        'change': pattern.get('price_change', 0)
                    })
            
            system_prompt = """Ты - аналитик крипто-новостей с глубоким пониманием влияния новостей на рынок.

Анализируй новость и её фактическое влияние на рынок.
Извлекай паттерны и правила для будущих предсказаний.

Формат ответа (JSON):
{
    "patterns_identified": ["паттерн1", "паттерн2"],
    "why_this_impact": "почему новость повлияла так",
    "future_prediction_rule": "правило для похожих новостей",
    "confidence_in_pattern": 0.0-1.0,
    "category": "regulatory/partnership/technical/market/other"
}"""

            user_prompt = f"""Проанализируй влияние новости на рынок:

=== НОВОСТЬ ===
Заголовок: {title}
Источник: {source}
Монеты: {', '.join(coins[:5])}
Время: {news.get('timestamp', 'N/A')}

=== ФАКТИЧЕСКОЕ ВЛИЯНИЕ ===
Цена ДО: ${price_before:.4f}
Цена ПОСЛЕ: ${price_after:.4f}
Изменение: {price_change:+.2f}%
Направление: {actual_direction}

=== ПОХОЖИЕ НОВОСТИ ИЗ ПАМЯТИ ===
{json.dumps(similar_outcomes[:5], indent=2, ensure_ascii=False) if similar_outcomes else 'Нет похожих'}

Вопросы:
1. Какие паттерны можно выделить?
2. Почему новость вызвала такое движение?
3. Какое правило можно создать для похожих новостей?"""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            response = await self._call_deepseek(messages, max_tokens=600)
            
            analysis_data = {}
            if response:
                try:
                    json_start = response.find('{')
                    json_end = response.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        analysis_data = json.loads(response[json_start:json_end])
                except json.JSONDecodeError:
                    pass
            
            # Создаём NewsImpact
            news_hash = hashlib.md5(title.encode()).hexdigest()[:16]
            
            impact = NewsImpact(
                news_id=news_hash,
                title=title,
                source=source,
                timestamp=datetime.now(),
                affected_coins=coins,
                predicted_direction=news.get('predicted_direction', 'NEUTRAL'),
                actual_direction=actual_direction,
                price_change_percent=price_change,
                prediction_accuracy=1.0 if news.get('predicted_direction') == actual_direction else 0.0,
                patterns_identified=analysis_data.get('patterns_identified', []),
                similar_news_outcomes=similar_outcomes[:5]
            )
            
            # Сохраняем в память
            pattern = {
                'title': title[:100],
                'source': source,
                'coins': coins,
                'actual_direction': actual_direction,
                'price_change': price_change,
                'patterns': analysis_data.get('patterns_identified', []),
                'category': analysis_data.get('category', 'other'),
                'timestamp': datetime.now().isoformat()
            }
            self.memory.add_news_pattern(news_hash, pattern)
            
            # Добавляем правило если есть
            if analysis_data.get('future_prediction_rule'):
                rule = f"[NEWS] {analysis_data['future_prediction_rule']}"
                self.memory.add_rule(rule)
            
            self.memory.save()
            
            logger.info(f"[AI] Analyzed news impact: {title[:50]}... -> {actual_direction} ({price_change:+.2f}%)")
            
            return impact
            
        except Exception as e:
            logger.error(f"[AI] Error analyzing news: {e}")
            return None
    
    def _news_keyword_weight(self, word: str) -> float:
        """Вес ключевого слова для TF-IDF-подобного сопоставления новостей."""
        w = word.lower().strip()
        if not w or len(w) < 2:
            return 0.0
        high = {'sec', 'etf', 'fed', 'rate', 'ban', 'approval', 'rejection', 'hack', 'exploit',
                 'bitcoin', 'btc', 'ethereum', 'eth', 'regulation', 'inflation', 'tariff', 'trump'}
        if w in high:
            return 2.0
        if any(x in w for x in ('crypto', 'bull', 'bear', 'pump', 'dump', 'long', 'short')):
            return 1.5
        return 1.0
    
    async def predict_news_impact(self, news: Dict) -> Dict:
        """
        Предсказать влияние новости ДО того как она повлияет.
        Улучшенное сопоставление по весу ключевых слов, учёт отложенного эффекта и типа события.
        """
        try:
            title = news.get('title', '')
            coins = news.get('affected_coins', [])
            title_words = set(title.lower().split())
            
            # Взвешенное сопоставление с прошлыми новостями
            scored = []
            for pattern in self.memory.news_patterns.values():
                pattern_title = pattern.get('title', '') or ''
                pattern_words = set(pattern_title.lower().split())
                overlap_words = title_words & pattern_words
                score = sum(self._news_keyword_weight(w) for w in overlap_words)
                if any(c in (pattern.get('coins') or []) for c in coins):
                    score += 3.0  # совпадение монет сильно повышает релевантность
                if score >= 1.5:  # порог вместо фиксированного overlap >= 2
                    scored.append((score, pattern))
            scored.sort(key=lambda x: -x[0])
            similar_patterns = [p for _, p in scored[:25]]
            
            # Считаем статистику (в т.ч. отложенный эффект из сохранённых паттернов)
            if similar_patterns:
                bullish_count = sum(1 for p in similar_patterns if p.get('actual_direction') == 'BULLISH')
                bearish_count = sum(1 for p in similar_patterns if p.get('actual_direction') == 'BEARISH')
                avg_change = sum(p.get('price_change', 0) for p in similar_patterns) / len(similar_patterns)
                
                if bullish_count > bearish_count * 1.5:
                    predicted = 'BULLISH'
                    confidence = bullish_count / len(similar_patterns)
                elif bearish_count > bullish_count * 1.5:
                    predicted = 'BEARISH'
                    confidence = bearish_count / len(similar_patterns)
                else:
                    predicted = 'NEUTRAL'
                    confidence = 0.5
            else:
                predicted = 'NEUTRAL'
                confidence = 0.3
                avg_change = 0
            
            system_prompt = """Ты - эксперт по крипто-новостям. Предскажи влияние новости на рынок.

Учитывай:
- Отложенный эффект: часть новостей влияет через 1-24 часа (регуляторы, макро).
- Тип события: one_time (разовый скачок) или trend_start (начало тренда) — от этого зависит сила и длительность реакции.

Формат ответа (JSON):
{
    "predicted_direction": "BULLISH/BEARISH/NEUTRAL",
    "confidence": 0.0-1.0,
    "expected_price_change_percent": число,
    "time_to_impact_hours": число (0 = сразу, 1-24 = отложенный эффект),
    "event_type": "one_time или trend_start или uncertain",
    "reasoning": "краткое обоснование",
    "trading_recommendation": "buy/sell/hold/wait"
}"""

            user_prompt = f"""Предскажи влияние этой новости:

Заголовок: {title}
Затронутые монеты: {', '.join(coins[:5])}

Статистика похожих новостей из памяти (взвешенное совпадение):
- Найдено похожих: {len(similar_patterns)}
- Среднее изменение цены: {avg_change:+.2f}%
- Предварительный анализ: {predicted} (conf: {confidence:.2f})

Оцени время до влияния (сразу / через часы) и тип: разовое событие или старт тренда. Дай прогноз."""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            response = await self._call_deepseek(messages, max_tokens=400)
            
            prediction = {
                'predicted_direction': predicted,
                'confidence': confidence,
                'expected_change': avg_change,
                'similar_count': len(similar_patterns),
                'reasoning': '',
                'time_to_impact_hours': 0,
                'event_type': 'uncertain'
            }
            
            if response:
                try:
                    json_start = response.find('{')
                    json_end = response.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        ai_pred = json.loads(response[json_start:json_end])
                        prediction.update({
                            'predicted_direction': ai_pred.get('predicted_direction', predicted),
                            'confidence': ai_pred.get('confidence', confidence),
                            'expected_change': ai_pred.get('expected_price_change_percent', avg_change),
                            'reasoning': ai_pred.get('reasoning', ''),
                            'recommendation': ai_pred.get('trading_recommendation', 'hold'),
                            'time_to_impact_hours': ai_pred.get('time_to_impact_hours', 0),
                            'event_type': ai_pred.get('event_type', 'uncertain')
                        })
                except json.JSONDecodeError:
                    pass
            
            logger.info(f"[AI] News prediction: {title[:40]}... -> {prediction['predicted_direction']} (conf={prediction['confidence']:.2f})")
            
            return prediction
            
        except Exception as e:
            logger.error(f"[AI] Error predicting news: {e}")
            return {'predicted_direction': 'NEUTRAL', 'confidence': 0.3}
    
    # ==================== LEARNING & INSIGHTS ====================
    
    async def generate_daily_insights(self, trades: List[Dict], news: List[Dict]) -> str:
        """
        Генерация ежедневных инсайтов на основе сделок и новостей
        """
        try:
            # Статистика за день
            total_trades = len(trades)
            wins = sum(1 for t in trades if float(t.get('pnl', 0)) > 0)
            total_pnl = sum(float(t.get('pnl', 0)) for t in trades)
            
            # Топ символы
            symbol_pnl = {}
            for t in trades:
                sym = t.get('symbol', 'UNK')
                symbol_pnl[sym] = symbol_pnl.get(sym, 0) + float(t.get('pnl', 0))
            
            top_symbols = sorted(symbol_pnl.items(), key=lambda x: x[1], reverse=True)[:5]
            worst_symbols = sorted(symbol_pnl.items(), key=lambda x: x[1])[:5]
            
            system_prompt = """Ты - трейдинг-коуч. Сформируй дневное саммари для команды.

Включи в ответ:
1. Что за сегодня узнало ИИ: новые правила, паттерны, выводы из сделок.
2. Сделки за день: количество, выигрыши/проигрыши, как меняется стиль торговли.
3. Рост и динамика: PnL за день, сравнение с предыдущими днями по тону.
4. Конкретные рекомендации на завтра и новые правила для запоминания."""

            wr_pct = (wins / total_trades * 100) if total_trades else 0
            user_prompt = f"""Дневное саммари (сегодня):

=== СТАТИСТИКА ДНЯ ===
Сделок: {total_trades} | Прибыльных: {wins} | Win rate: {wr_pct:.1f}%
Общий PnL за день: ${total_pnl:.2f}

=== ЛУЧШИЕ СИМВОЛЫ ===
{chr(10).join(f'{s}: ${p:.2f}' for s, p in top_symbols) if top_symbols else 'Нет'}

=== ХУДШИЕ СИМВОЛЫ ===
{chr(10).join(f'{s}: ${p:.2f}' for s, p in worst_symbols) if worst_symbols else 'Нет'}

=== НОВОСТИ ДНЯ ===
{chr(10).join(n.get('title', '')[:60] for n in news[:10]) if news else 'Нет'}

=== ВЫУЧЕННЫЕ ПРАВИЛА (всего: {len(self.memory.learned_rules)}) ===
{chr(10).join(self.memory.learned_rules[-10:])}

Опиши: что ИИ узнало сегодня, как мы растем, как меняется стиль торговли. Дай рекомендации."""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            response = await self._call_deepseek(messages, max_tokens=1000)
            
            if response:
                # Сохраняем инсайт
                self.memory.add_insight(response[:500], 'daily_summary')
                self.memory.save()
            
            return response or "Не удалось сгенерировать инсайты"
            
        except Exception as e:
            logger.error(f"[AI] Error generating insights: {e}")
            return f"Ошибка: {e}"
    
    def get_stats(self) -> Dict:
        """Получить статистику AI"""
        return {
            'initialized': self._initialized,
            'total_trades_analyzed': self.memory.total_trades_analyzed,
            'total_news_analyzed': self.memory.total_news_analyzed,
            'learned_rules_count': len(self.memory.learned_rules),
            'trade_patterns_count': sum(len(v['wins']) + len(v['losses']) for v in self.memory.trade_patterns.values()),
            'news_patterns_count': len(self.memory.news_patterns),
            'market_insights_count': len(self.memory.market_insights),
            'recent_analyses_count': len(self.recent_analyses),
            'prediction_accuracy': self.memory.prediction_accuracy
        }


# Глобальный экземпляр
_analyzer: Optional[DeepSeekAnalyzer] = None


def get_ai_analyzer() -> DeepSeekAnalyzer:
    """Получить глобальный AI анализатор"""
    global _analyzer
    if _analyzer is None:
        _analyzer = DeepSeekAnalyzer()
    return _analyzer


async def init_ai_analyzer() -> bool:
    """Инициализировать AI анализатор"""
    analyzer = get_ai_analyzer()
    return await analyzer.initialize()


# ==================== CONVENIENCE FUNCTIONS ====================

async def analyze_trade(trade: Dict) -> Optional[TradeAnalysis]:
    """Анализировать закрытую сделку"""
    analyzer = get_ai_analyzer()
    return await analyzer.analyze_closed_trade(trade)


async def should_take_signal(signal: Dict, market_data: Dict = None) -> Dict:
    """Принять решение о сделке"""
    analyzer = get_ai_analyzer()
    return await analyzer.should_take_trade(signal, market_data)


async def analyze_news(news: Dict, price_before: float, price_after: float) -> Optional[NewsImpact]:
    """Анализировать влияние новости"""
    analyzer = get_ai_analyzer()
    return await analyzer.analyze_news_impact(news, price_before, price_after)


async def predict_news(news: Dict) -> Dict:
    """Предсказать влияние новости"""
    analyzer = get_ai_analyzer()
    return await analyzer.predict_news_impact(news)


async def daily_insights(trades: List[Dict], news: List[Dict]) -> str:
    """Генерировать ежедневные инсайты"""
    analyzer = get_ai_analyzer()
    return await analyzer.generate_daily_insights(trades, news)


def get_ai_stats() -> Dict:
    """Получить статистику AI"""
    analyzer = get_ai_analyzer()
    return analyzer.get_stats()


# ==================== NEWS PRICE TRACKING ====================

async def fetch_coin_price(symbol: str) -> Optional[float]:
    """Получить текущую цену монеты через Binance API"""
    try:
        # Нормализуем символ
        clean_symbol = symbol.upper().replace('/', '').replace('USDT', '') + 'USDT'
        
        async with aiohttp.ClientSession() as session:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={clean_symbol}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get('price', 0))
    except Exception as e:
        logger.debug(f"[AI] Price fetch error for {symbol}: {e}")
    return None


async def fetch_btc_price() -> Optional[float]:
    """Получить цену BTC как основной индикатор рынка"""
    return await fetch_coin_price('BTCUSDT')


async def track_news_for_impact(news_event) -> None:
    """
    Начать отслеживание новости для последующего анализа влияния на цену.
    Вызывается когда приходит новая новость из RSS.
    """
    global _news_price_tracker
    
    try:
        news_id = news_event.id if hasattr(news_event, 'id') else str(hash(str(news_event)))
        
        # Проверяем, не отслеживаем ли уже эту новость
        if news_id in _news_price_tracker:
            return
        
        # Получаем затронутые монеты
        coins = []
        if hasattr(news_event, 'affected_coins'):
            coins = news_event.affected_coins[:5]  # Максимум 5 монет
        
        # Если нет конкретных монет, используем BTC как индикатор
        if not coins:
            coins = ['BTC']
        
        # Получаем текущие цены
        prices_before = {}
        for coin in coins:
            price = await fetch_coin_price(coin)
            if price:
                prices_before[coin] = price
        
        # Всегда добавляем BTC для общего контекста
        if 'BTC' not in prices_before:
            btc_price = await fetch_btc_price()
            if btc_price:
                prices_before['BTC'] = btc_price
        
        if not prices_before:
            logger.debug(f"[AI] Could not fetch prices for news tracking: {news_id[:8]}")
            return
        
        # Сохраняем для отслеживания
        _news_price_tracker[news_id] = {
            'news_id': news_id,
            'title': news_event.title if hasattr(news_event, 'title') else str(news_event),
            'source': news_event.source if hasattr(news_event, 'source') else 'Unknown',
            'sentiment': news_event.sentiment.name if hasattr(news_event, 'sentiment') else 'NEUTRAL',
            'coins': coins,
            'prices_before': prices_before,
            'timestamp': datetime.now().isoformat(),
            'checked': False
        }
        
        logger.info(f"[AI] 📰 Tracking news impact: {_news_price_tracker[news_id]['title'][:50]}... ({len(prices_before)} coins)")
        
        # Ограничиваем размер трекера
        if len(_news_price_tracker) > 100:
            # Удаляем самые старые
            oldest_keys = sorted(_news_price_tracker.keys(), 
                               key=lambda k: _news_price_tracker[k]['timestamp'])[:20]
            for k in oldest_keys:
                del _news_price_tracker[k]
                
    except Exception as e:
        logger.warning(f"[AI] Error tracking news: {e}")


async def check_news_impacts() -> List[Dict]:
    """
    Проверить влияние отслеживаемых новостей (вызывается периодически).
    Сравнивает цены до и после новости, отправляет на AI анализ.
    """
    global _news_price_tracker
    
    analyzed = []
    analyzer = get_ai_analyzer()
    
    try:
        current_time = datetime.now()
        to_remove = []
        
        for news_id, data in list(_news_price_tracker.items()):
            try:
                # Пропускаем уже проверенные
                if data.get('checked'):
                    # Удаляем старые проверенные (>1 час)
                    news_time = datetime.fromisoformat(data['timestamp'])
                    if (current_time - news_time).total_seconds() > 3600:
                        to_remove.append(news_id)
                    continue
                
                # Проверяем только новости старше 15-30 минут
                news_time = datetime.fromisoformat(data['timestamp'])
                age_minutes = (current_time - news_time).total_seconds() / 60
                
                if age_minutes < 15:
                    continue  # Слишком рано
                
                if age_minutes > 120:
                    # Слишком старые - удаляем
                    to_remove.append(news_id)
                    continue
                
                # Получаем текущие цены
                prices_after = {}
                for coin in data['coins']:
                    price = await fetch_coin_price(coin)
                    if price:
                        prices_after[coin] = price
                
                if 'BTC' not in prices_after:
                    btc_price = await fetch_btc_price()
                    if btc_price:
                        prices_after['BTC'] = btc_price
                
                if not prices_after:
                    continue
                
                # Рассчитываем изменения цен
                price_changes = {}
                for coin, price_before in data['prices_before'].items():
                    if coin in prices_after:
                        price_after = prices_after[coin]
                        change_pct = ((price_after - price_before) / price_before) * 100
                        price_changes[coin] = {
                            'before': price_before,
                            'after': price_after,
                            'change_percent': round(change_pct, 2)
                        }
                
                if not price_changes:
                    continue
                
                # Определяем основное изменение (по первой монете или BTC)
                main_coin = data['coins'][0] if data['coins'] else 'BTC'
                if main_coin not in price_changes:
                    main_coin = 'BTC'
                
                main_change = price_changes.get(main_coin, {})
                price_before = main_change.get('before', 0)
                price_after = main_change.get('after', 0)
                
                if price_before <= 0:
                    continue
                
                # Отправляем на AI анализ
                news_dict = {
                    'title': data['title'],
                    'source': data['source'],
                    'affected_coins': data['coins'],
                    'timestamp': data['timestamp'],
                    'predicted_direction': data['sentiment']  # Исходный сентимент как предсказание
                }
                
                impact = await analyzer.analyze_news_impact(news_dict, price_before, price_after)
                
                if impact:
                    analyzed.append({
                        'news_id': news_id,
                        'title': data['title'][:80],
                        'source': data['source'],
                        'price_changes': price_changes,
                        'main_change': main_change.get('change_percent', 0),
                        'ai_analysis': {
                            'actual_direction': impact.actual_direction,
                            'patterns': impact.patterns_identified[:3]
                        }
                    })
                    logger.info(f"[AI] 📊 News impact analyzed: {data['title'][:40]}... -> {impact.actual_direction} ({main_change.get('change_percent', 0):+.2f}%)")
                
                # Помечаем как проверенную
                _news_price_tracker[news_id]['checked'] = True
                _news_price_tracker[news_id]['prices_after'] = prices_after
                _news_price_tracker[news_id]['impact_analyzed'] = True
                
            except Exception as e:
                logger.warning(f"[AI] Error checking news impact {news_id[:8]}: {e}")
        
        # Удаляем старые записи
        for news_id in to_remove:
            del _news_price_tracker[news_id]
            
    except Exception as e:
        logger.error(f"[AI] Error in check_news_impacts: {e}")
    
    return analyzed


def get_tracked_news_count() -> int:
    """Получить количество отслеживаемых новостей"""
    return len(_news_price_tracker)


def get_pending_news_analysis() -> List[Dict]:
    """Получить список новостей ожидающих анализа"""
    pending = []
    for news_id, data in _news_price_tracker.items():
        if not data.get('checked'):
            pending.append({
                'news_id': news_id[:8],
                'title': data['title'][:60],
                'source': data['source'],
                'coins': data['coins'],
                'timestamp': data['timestamp']
            })
    return pending
