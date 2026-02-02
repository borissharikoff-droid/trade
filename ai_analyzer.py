"""
AI Trading Analyzer powered by DeepSeek
Анализирует сделки, учится на истории, понимает влияние новостей
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from collections import deque
import hashlib

logger = logging.getLogger(__name__)

# DeepSeek API Configuration
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-b2c365a48862461fb4f9ea74887a3f5c")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"  # или deepseek-coder для технического анализа

# Memory storage path
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
    """Долгосрочная память AI - хранит знания и инсайты"""
    
    def __init__(self):
        self.trade_patterns: Dict[str, Dict] = {}  # Паттерны успешных/неуспешных сделок
        self.news_patterns: Dict[str, Dict] = {}  # Паттерны влияния новостей
        self.market_insights: List[Dict] = []  # Общие инсайты о рынке
        self.symbol_knowledge: Dict[str, Dict] = {}  # Знания о конкретных монетах
        self.learned_rules: List[str] = []  # Выученные правила
        self.performance_history: List[Dict] = []  # История performance
        
        # Статистика
        self.total_trades_analyzed: int = 0
        self.total_news_analyzed: int = 0
        self.prediction_accuracy: float = 0.5
        
        self._load()
    
    def _load(self):
        """Загрузить память из файла"""
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
                    self.total_trades_analyzed = data.get('total_trades_analyzed', 0)
                    self.total_news_analyzed = data.get('total_news_analyzed', 0)
                    self.prediction_accuracy = data.get('prediction_accuracy', 0.5)
                logger.info(f"[AI_MEMORY] Loaded: {self.total_trades_analyzed} trades, {self.total_news_analyzed} news analyzed")
        except Exception as e:
            logger.error(f"[AI_MEMORY] Error loading: {e}")
    
    def save(self):
        """Сохранить память в файл"""
        try:
            data = {
                'trade_patterns': self.trade_patterns,
                'news_patterns': self.news_patterns,
                'market_insights': self.market_insights[-1000:],  # Последние 1000
                'symbol_knowledge': self.symbol_knowledge,
                'learned_rules': self.learned_rules[-500:],  # Последние 500 правил
                'performance_history': self.performance_history[-100:],
                'total_trades_analyzed': self.total_trades_analyzed,
                'total_news_analyzed': self.total_news_analyzed,
                'prediction_accuracy': self.prediction_accuracy,
                'last_saved': datetime.now().isoformat()
            }
            with open(MEMORY_PATH, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            logger.error(f"[AI_MEMORY] Error saving: {e}")
    
    def add_trade_pattern(self, symbol: str, pattern: Dict):
        """Добавить паттерн сделки"""
        if symbol not in self.trade_patterns:
            self.trade_patterns[symbol] = {'wins': [], 'losses': []}
        
        key = 'wins' if pattern.get('is_win') else 'losses'
        self.trade_patterns[symbol][key].append(pattern)
        
        # Ограничиваем размер
        if len(self.trade_patterns[symbol][key]) > 100:
            self.trade_patterns[symbol][key] = self.trade_patterns[symbol][key][-100:]
        
        self.total_trades_analyzed += 1
    
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
    
    def add_rule(self, rule: str):
        """Добавить выученное правило"""
        if rule not in self.learned_rules:
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
    
    def get_relevant_rules(self, context: str) -> List[str]:
        """Получить релевантные правила для контекста"""
        # Простой поиск по ключевым словам
        relevant = []
        context_lower = context.lower()
        
        for rule in self.learned_rules[-100:]:  # Последние 100
            rule_lower = rule.lower()
            # Если есть общие слова
            if any(word in context_lower for word in rule_lower.split()[:5]):
                relevant.append(rule)
        
        return relevant[:10]  # Максимум 10


class DeepSeekAnalyzer:
    """Основной AI анализатор на DeepSeek"""
    
    def __init__(self):
        self.memory = AIMemory()
        self.client = None
        self.recent_analyses = deque(maxlen=100)  # Последние анализы
        self.pending_trades = {}  # Сделки ожидающие анализа
        self._initialized = False
        
        # Rate limiting
        self._last_request = datetime.min
        self._min_interval = 1.0  # Минимум 1 секунда между запросами
        
    async def initialize(self):
        """Инициализация клиента"""
        if not OPENAI_AVAILABLE:
            logger.error("[AI] OpenAI library not available")
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
Твоя задача - анализировать сделки и извлекать уроки.

ВАЖНО:
- Будь конкретным и практичным
- Указывай точные факторы успеха/провала
- Давай actionable рекомендации
- Учитывай рыночные условия

Формат ответа (JSON):
{
    "win_factors": ["фактор1", "фактор2"],
    "loss_factors": ["фактор1", "фактор2"],
    "market_conditions": "описание рынка",
    "entry_quality": 7,
    "exit_quality": 8,
    "lessons_learned": ["урок1", "урок2"],
    "recommendations": ["рекомендация1"],
    "confidence_score": 0.8,
    "new_rule": "если есть новое правило для запоминания"
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
            
            response = await self._call_deepseek(messages, max_tokens=800)
            
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
            
            # Сохраняем в память
            pattern = {
                'is_win': is_win,
                'direction': trade.get('direction'),
                'pnl': pnl,
                'reason': trade.get('reason'),
                'entry_quality': analysis.entry_quality,
                'summary': analysis_data.get('market_conditions', '')[:100],
                'lessons': analysis.lessons_learned[:2],
                'timestamp': datetime.now().isoformat()
            }
            self.memory.add_trade_pattern(symbol, pattern)
            
            # Добавляем новое правило если есть
            if analysis_data.get('new_rule'):
                self.memory.add_rule(analysis_data['new_rule'])
            
            # Сохраняем память
            self.memory.save()
            
            self.recent_analyses.append(asdict(analysis))
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
            
            # Получаем контекст
            symbol_context = self.memory.get_symbol_context(symbol)
            relevant_rules = self.memory.get_relevant_rules(f"{symbol} {signal.get('direction', '')}")
            
            # Статистика из памяти
            patterns = self.memory.trade_patterns.get(symbol, {'wins': [], 'losses': []})
            win_count = len(patterns['wins'])
            loss_count = len(patterns['losses'])
            historical_wr = win_count / (win_count + loss_count) if (win_count + loss_count) > 0 else 0.5
            
            system_prompt = """Ты - AI торговый советник. Твоя задача - принимать решения о сделках.

КРИТЕРИИ ОЦЕНКИ:
1. Качество сигнала (технический анализ)
2. Историческая успешность символа
3. Текущие рыночные условия
4. Соотношение риск/прибыль
5. Выученные правила из прошлых сделок

Формат ответа (JSON):
{
    "should_trade": true/false,
    "confidence": 0.0-1.0,
    "reasoning": "краткое обоснование",
    "risk_level": "low/medium/high",
    "adjusted_sl_percent": null или число (процент от входа),
    "adjusted_tp_percent": null или число,
    "position_size_multiplier": 0.5-1.5,
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
            
            response = await self._call_deepseek(messages, max_tokens=600)
            
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
                'key_concerns': decision.get('key_concerns', []),
                'key_strengths': decision.get('key_strengths', [])
            }
            
            logger.info(f"[AI] Trade decision for {symbol}: {'✅ TAKE' if result['should_trade'] else '❌ SKIP'} (conf={result['confidence']:.2f})")
            
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
    
    async def predict_news_impact(self, news: Dict) -> Dict:
        """
        Предсказать влияние новости ДО того как она повлияет
        """
        try:
            title = news.get('title', '')
            coins = news.get('affected_coins', [])
            
            # Ищем похожие паттерны
            similar_patterns = []
            for pattern in self.memory.news_patterns.values():
                # Простое сравнение по ключевым словам
                title_words = set(title.lower().split())
                pattern_words = set(pattern.get('title', '').lower().split())
                overlap = len(title_words & pattern_words)
                
                if overlap >= 2 or any(coin in pattern.get('coins', []) for coin in coins):
                    similar_patterns.append(pattern)
            
            # Считаем статистику
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

Формат ответа (JSON):
{
    "predicted_direction": "BULLISH/BEARISH/NEUTRAL",
    "confidence": 0.0-1.0,
    "expected_price_change_percent": число,
    "time_to_impact_hours": число,
    "reasoning": "краткое обоснование",
    "trading_recommendation": "buy/sell/hold/wait"
}"""

            user_prompt = f"""Предскажи влияние этой новости:

Заголовок: {title}
Затронутые монеты: {', '.join(coins[:5])}

Статистика похожих новостей из памяти:
- Найдено похожих: {len(similar_patterns)}
- Среднее изменение цены: {avg_change:+.2f}%
- Мой предварительный анализ: {predicted} (conf: {confidence:.2f})

Дай свой прогноз."""

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
                'reasoning': ''
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
                            'recommendation': ai_pred.get('trading_recommendation', 'hold')
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
            
            system_prompt = """Ты - трейдинг-коуч. Проанализируй дневную торговую сессию и дай ценные инсайты.

Формат ответа:
1. Краткий обзор дня
2. Что работало хорошо
3. Что нужно улучшить
4. Конкретные рекомендации на завтра
5. Новые правила для запоминания"""

            user_prompt = f"""Анализ торговой сессии:

=== СТАТИСТИКА ===
Всего сделок: {total_trades}
Прибыльных: {wins} ({wins/total_trades*100:.1f}% WR)
Общий PnL: ${total_pnl:.2f}

=== ЛУЧШИЕ СИМВОЛЫ ===
{chr(10).join(f'{s}: ${p:.2f}' for s, p in top_symbols)}

=== ХУДШИЕ СИМВОЛЫ ===
{chr(10).join(f'{s}: ${p:.2f}' for s, p in worst_symbols)}

=== НОВОСТИ ДНЯ ===
{chr(10).join(n.get('title', '')[:60] for n in news[:10])}

=== ВЫУЧЕННЫЕ ПРАВИЛА (всего: {len(self.memory.learned_rules)}) ===
{chr(10).join(self.memory.learned_rules[-10:])}

Дай анализ и рекомендации."""

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
