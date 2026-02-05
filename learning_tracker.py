"""
Learning Analytics Tracker
Отслеживает прогресс обучения AI системы, формирует отчёты о темпе обучения и заработке.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from collections import defaultdict

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
DATABASE_URL = os.environ.get("DATABASE_URL")
USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor


# ==================== DATA CLASSES ====================
@dataclass
class DailySnapshot:
    """Ежедневный снимок метрик обучения"""
    date: str
    
    # Трейдинг метрики
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    winrate: float = 0.0
    total_pnl: float = 0.0
    avg_pnl_per_trade: float = 0.0
    
    # AI метрики
    trades_analyzed: int = 0
    news_analyzed: int = 0
    rules_learned: int = 0
    patterns_discovered: int = 0
    prediction_accuracy: float = 0.5
    
    # Качество сделок (по AI оценке)
    avg_entry_quality: float = 0.0
    avg_exit_quality: float = 0.0
    
    # Топ/худшие символы
    best_symbol: str = ""
    best_symbol_pnl: float = 0.0
    worst_symbol: str = ""
    worst_symbol_pnl: float = 0.0


@dataclass 
class LearningReport:
    """Отчёт о прогрессе обучения"""
    generated_at: str
    period: str  # "daily", "weekly", "monthly"
    
    # Текущие метрики
    current_winrate: float = 0.0
    current_avg_pnl: float = 0.0
    current_prediction_accuracy: float = 0.5
    
    # Тренды (сравнение с предыдущим периодом)
    winrate_trend: float = 0.0  # +/- процентов
    pnl_trend: float = 0.0
    accuracy_trend: float = 0.0
    
    # Прогресс обучения
    new_rules_learned: int = 0
    new_patterns_discovered: int = 0
    total_knowledge_items: int = 0
    
    # Оценка
    learning_score: float = 0.0  # 0-100
    learning_status: str = "unknown"  # "improving", "stable", "degrading"
    
    # Рекомендации
    recommendations: List[str] = None
    
    # Детали
    top_performing_rules: List[str] = None
    weak_areas: List[str] = None


class LearningTracker:
    """Трекер прогресса обучения AI системы"""
    
    def __init__(self):
        self.snapshots: List[DailySnapshot] = []
        self._db_initialized = False
        self._init_db()
        self._load_snapshots()
    
    def _get_db_connection(self):
        """Получить подключение к PostgreSQL"""
        if not USE_POSTGRES:
            return None
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            logger.error(f"[LEARNING] DB connection error: {e}")
            return None
    
    def _init_db(self):
        """Создать таблицу для трекинга"""
        if not USE_POSTGRES:
            return
        
        conn = self._get_db_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS learning_snapshots (
                        id SERIAL PRIMARY KEY,
                        date DATE UNIQUE NOT NULL,
                        data JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                conn.commit()
            self._db_initialized = True
            logger.info("[LEARNING] ✓ Database table initialized")
        except Exception as e:
            logger.error(f"[LEARNING] DB init error: {e}")
        finally:
            conn.close()
    
    def _load_snapshots(self):
        """Загрузить исторические снимки"""
        if not USE_POSTGRES or not self._db_initialized:
            return
        
        conn = self._get_db_connection()
        if not conn:
            return
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT date, data FROM learning_snapshots 
                    ORDER BY date DESC LIMIT 90
                """)
                rows = cur.fetchall()
                
                for row in rows:
                    data = row['data']
                    snapshot = DailySnapshot(
                        date=str(row['date']),
                        total_trades=data.get('total_trades', 0),
                        wins=data.get('wins', 0),
                        losses=data.get('losses', 0),
                        winrate=data.get('winrate', 0.0),
                        total_pnl=data.get('total_pnl', 0.0),
                        avg_pnl_per_trade=data.get('avg_pnl_per_trade', 0.0),
                        trades_analyzed=data.get('trades_analyzed', 0),
                        news_analyzed=data.get('news_analyzed', 0),
                        rules_learned=data.get('rules_learned', 0),
                        patterns_discovered=data.get('patterns_discovered', 0),
                        prediction_accuracy=data.get('prediction_accuracy', 0.5),
                        avg_entry_quality=data.get('avg_entry_quality', 0.0),
                        avg_exit_quality=data.get('avg_exit_quality', 0.0),
                        best_symbol=data.get('best_symbol', ''),
                        best_symbol_pnl=data.get('best_symbol_pnl', 0.0),
                        worst_symbol=data.get('worst_symbol', ''),
                        worst_symbol_pnl=data.get('worst_symbol_pnl', 0.0)
                    )
                    self.snapshots.append(snapshot)
                
                self.snapshots.reverse()  # Oldest first
                logger.info(f"[LEARNING] ✓ Loaded {len(self.snapshots)} historical snapshots")
        except Exception as e:
            logger.error(f"[LEARNING] Load error: {e}")
        finally:
            conn.close()
    
    def _save_snapshot(self, snapshot: DailySnapshot):
        """Сохранить снимок в БД"""
        if not USE_POSTGRES or not self._db_initialized:
            return
        
        conn = self._get_db_connection()
        if not conn:
            return
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO learning_snapshots (date, data)
                    VALUES (%s, %s)
                    ON CONFLICT (date) DO UPDATE SET
                        data = EXCLUDED.data,
                        created_at = NOW()
                """, (snapshot.date, json.dumps(asdict(snapshot))))
                conn.commit()
            logger.debug(f"[LEARNING] Saved snapshot for {snapshot.date}")
        except Exception as e:
            logger.error(f"[LEARNING] Save error: {e}")
        finally:
            conn.close()
    
    def collect_daily_metrics(self, run_sql_func) -> DailySnapshot:
        """Собрать метрики за сегодня"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        snapshot = DailySnapshot(date=today)
        
        try:
            # === Трейдинг метрики из history ===
            if USE_POSTGRES:
                trades = run_sql_func("""
                    SELECT symbol, direction, pnl, reason
                    FROM history 
                    WHERE DATE(closed_at) = CURRENT_DATE
                """, fetch="all") or []
            else:
                trades = run_sql_func("""
                    SELECT symbol, direction, pnl, reason
                    FROM history 
                    WHERE date(closed_at) = date('now')
                """, fetch="all") or []
            
            if trades:
                snapshot.total_trades = len(trades)
                snapshot.wins = sum(1 for t in trades if float(t.get('pnl', 0)) > 0)
                snapshot.losses = snapshot.total_trades - snapshot.wins
                snapshot.winrate = round(snapshot.wins / snapshot.total_trades * 100, 1) if snapshot.total_trades > 0 else 0
                snapshot.total_pnl = round(sum(float(t.get('pnl', 0)) for t in trades), 2)
                snapshot.avg_pnl_per_trade = round(snapshot.total_pnl / snapshot.total_trades, 2) if snapshot.total_trades > 0 else 0
                
                # Топ/худший символ
                symbol_pnl = defaultdict(float)
                for t in trades:
                    symbol_pnl[t['symbol']] += float(t.get('pnl', 0))
                
                if symbol_pnl:
                    best = max(symbol_pnl.items(), key=lambda x: x[1])
                    worst = min(symbol_pnl.items(), key=lambda x: x[1])
                    snapshot.best_symbol = best[0]
                    snapshot.best_symbol_pnl = round(best[1], 2)
                    snapshot.worst_symbol = worst[0]
                    snapshot.worst_symbol_pnl = round(worst[1], 2)
            
            # === AI метрики из ai_memory ===
            try:
                if USE_POSTGRES:
                    ai_data = run_sql_func("""
                        SELECT key, data FROM ai_memory
                    """, fetch="all") or []
                    
                    ai_stats = {}
                    for row in ai_data:
                        ai_stats[row['key']] = row['data']
                    
                    stats = ai_stats.get('stats', {})
                    snapshot.trades_analyzed = stats.get('total_trades_analyzed', 0)
                    snapshot.news_analyzed = stats.get('total_news_analyzed', 0)
                    snapshot.prediction_accuracy = stats.get('prediction_accuracy', 0.5)
                    
                    learned_rules = ai_stats.get('learned_rules', [])
                    snapshot.rules_learned = len(learned_rules) if isinstance(learned_rules, list) else 0
                    
                    trade_patterns = ai_stats.get('trade_patterns', {})
                    snapshot.patterns_discovered = sum(
                        len(v.get('wins', [])) + len(v.get('losses', []))
                        for v in trade_patterns.values()
                    ) if isinstance(trade_patterns, dict) else 0
                    
                    # Качество из recent_analyses
                    recent = ai_stats.get('recent_analyses', [])
                    if recent and isinstance(recent, list):
                        today_analyses = [a for a in recent if a.get('timestamp', '').startswith(today)]
                        if today_analyses:
                            snapshot.avg_entry_quality = round(
                                sum(a.get('entry_quality', 0) for a in today_analyses) / len(today_analyses), 1
                            )
                            snapshot.avg_exit_quality = round(
                                sum(a.get('exit_quality', 0) for a in today_analyses) / len(today_analyses), 1
                            )
            except Exception as e:
                logger.warning(f"[LEARNING] Could not get AI metrics: {e}")
            
        except Exception as e:
            logger.error(f"[LEARNING] Error collecting metrics: {e}")
        
        # Сохраняем и добавляем в историю
        self._save_snapshot(snapshot)
        
        # Обновляем или добавляем в snapshots
        existing_idx = next((i for i, s in enumerate(self.snapshots) if s.date == today), None)
        if existing_idx is not None:
            self.snapshots[existing_idx] = snapshot
        else:
            self.snapshots.append(snapshot)
        
        return snapshot
    
    def generate_report(self, period: str = "daily") -> LearningReport:
        """Генерировать отчёт о прогрессе обучения"""
        report = LearningReport(
            generated_at=datetime.now().isoformat(),
            period=period,
            recommendations=[],
            top_performing_rules=[],
            weak_areas=[]
        )
        
        if not self.snapshots:
            report.learning_status = "no_data"
            report.recommendations = ["Недостаточно данных для анализа. Подождите накопления истории."]
            return report
        
        # Определяем периоды для сравнения
        if period == "daily":
            current_period = self.snapshots[-1:] if self.snapshots else []
            previous_period = self.snapshots[-2:-1] if len(self.snapshots) > 1 else []
        elif period == "weekly":
            current_period = self.snapshots[-7:] if len(self.snapshots) >= 7 else self.snapshots
            previous_period = self.snapshots[-14:-7] if len(self.snapshots) >= 14 else []
        else:  # monthly
            current_period = self.snapshots[-30:] if len(self.snapshots) >= 30 else self.snapshots
            previous_period = self.snapshots[-60:-30] if len(self.snapshots) >= 60 else []
        
        # === Текущие метрики ===
        if current_period:
            total_trades = sum(s.total_trades for s in current_period)
            total_wins = sum(s.wins for s in current_period)
            total_pnl = sum(s.total_pnl for s in current_period)
            
            report.current_winrate = round(total_wins / total_trades * 100, 1) if total_trades > 0 else 0
            report.current_avg_pnl = round(total_pnl / total_trades, 2) if total_trades > 0 else 0
            report.current_prediction_accuracy = round(
                sum(s.prediction_accuracy for s in current_period) / len(current_period), 2
            )
            
            report.new_rules_learned = current_period[-1].rules_learned if current_period else 0
            report.new_patterns_discovered = current_period[-1].patterns_discovered if current_period else 0
            report.total_knowledge_items = report.new_rules_learned + report.new_patterns_discovered
        
        # === Тренды (сравнение с предыдущим периодом) ===
        if previous_period:
            prev_trades = sum(s.total_trades for s in previous_period)
            prev_wins = sum(s.wins for s in previous_period)
            prev_pnl = sum(s.total_pnl for s in previous_period)
            
            prev_winrate = prev_wins / prev_trades * 100 if prev_trades > 0 else 0
            prev_avg_pnl = prev_pnl / prev_trades if prev_trades > 0 else 0
            prev_accuracy = sum(s.prediction_accuracy for s in previous_period) / len(previous_period)
            
            report.winrate_trend = round(report.current_winrate - prev_winrate, 1)
            report.pnl_trend = round(report.current_avg_pnl - prev_avg_pnl, 2)
            report.accuracy_trend = round(report.current_prediction_accuracy - prev_accuracy, 2)
        
        # === Оценка обучения ===
        score = 50  # Базовый скор
        
        # Winrate
        if report.current_winrate >= 55:
            score += 15
        elif report.current_winrate >= 50:
            score += 5
        elif report.current_winrate < 45:
            score -= 10
        
        # PnL тренд
        if report.pnl_trend > 0:
            score += 10
        elif report.pnl_trend < -1:
            score -= 10
        
        # Winrate тренд
        if report.winrate_trend > 2:
            score += 10
        elif report.winrate_trend < -2:
            score -= 10
        
        # Accuracy
        if report.current_prediction_accuracy > 0.6:
            score += 10
        elif report.current_prediction_accuracy < 0.4:
            score -= 10
        
        # Knowledge accumulation
        if report.total_knowledge_items > 100:
            score += 5
        
        report.learning_score = max(0, min(100, score))
        
        # Статус
        if report.winrate_trend > 2 and report.pnl_trend > 0:
            report.learning_status = "improving"
        elif report.winrate_trend < -2 or report.pnl_trend < -1:
            report.learning_status = "degrading"
        else:
            report.learning_status = "stable"
        
        # === Рекомендации ===
        if report.current_winrate < 50:
            report.recommendations.append("⚠️ Winrate ниже 50% - пересмотрите критерии входа в сделки")
            report.weak_areas.append("Low winrate")
        
        if report.winrate_trend < -3:
            report.recommendations.append("📉 Winrate падает - возможно рынок изменился, нужна адаптация")
        
        if report.current_avg_pnl < 0:
            report.recommendations.append("💰 Средний PnL отрицательный - проверьте соотношение риск/прибыль")
            report.weak_areas.append("Negative average PnL")
        
        if report.current_prediction_accuracy < 0.5:
            report.recommendations.append("🎯 Точность предсказаний низкая - AI нужно больше данных для обучения")
        
        if report.total_knowledge_items < 50:
            report.recommendations.append("📚 Мало накопленных знаний - продолжайте торговать для обучения AI")
        
        if not report.recommendations:
            report.recommendations.append("✅ Система работает стабильно. Продолжайте в том же духе!")
        
        return report
    
    def get_learning_summary(self) -> Dict[str, Any]:
        """Получить сводку для дашборда"""
        if not self.snapshots:
            return {
                'status': 'no_data',
                'message': 'Нет данных для анализа',
                'snapshots_count': 0
            }
        
        latest = self.snapshots[-1]
        report = self.generate_report("daily")
        
        # Графики трендов (последние 7 дней)
        last_7 = self.snapshots[-7:] if len(self.snapshots) >= 7 else self.snapshots
        
        return {
            'status': 'active',
            'snapshots_count': len(self.snapshots),
            
            # Текущие метрики
            'today': {
                'date': latest.date,
                'trades': latest.total_trades,
                'winrate': latest.winrate,
                'pnl': latest.total_pnl,
                'best_symbol': latest.best_symbol,
                'worst_symbol': latest.worst_symbol
            },
            
            # Обучение
            'learning': {
                'score': report.learning_score,
                'status': report.learning_status,
                'rules_count': latest.rules_learned,
                'patterns_count': latest.patterns_discovered,
                'prediction_accuracy': round(report.current_prediction_accuracy * 100, 1)
            },
            
            # Тренды
            'trends': {
                'winrate': report.winrate_trend,
                'pnl': report.pnl_trend,
                'accuracy': report.accuracy_trend
            },
            
            # Данные для графиков
            'chart_data': {
                'dates': [s.date for s in last_7],
                'winrates': [s.winrate for s in last_7],
                'pnls': [s.total_pnl for s in last_7],
                'trades': [s.total_trades for s in last_7]
            },
            
            # Рекомендации
            'recommendations': report.recommendations[:3],
            'weak_areas': report.weak_areas
        }
    
    def format_telegram_report(self) -> str:
        """Форматировать отчёт для Telegram"""
        report = self.generate_report("daily")
        summary = self.get_learning_summary()
        
        if summary['status'] == 'no_data':
            return "📊 <b>Learning Report</b>\n\nНет данных для анализа. Подождите накопления истории."
        
        status_emoji = {
            'improving': '📈',
            'stable': '➡️',
            'degrading': '📉'
        }.get(report.learning_status, '❓')
        
        trend_arrow = lambda x: '🟢' if x > 0 else ('🔴' if x < 0 else '⚪')
        
        text = f"""📊 <b>Learning Analytics Report</b>

{status_emoji} <b>Статус:</b> {report.learning_status.upper()}
🎯 <b>Learning Score:</b> {report.learning_score}/100

<b>━━━ Сегодня ━━━</b>
📈 Сделок: {summary['today']['trades']}
🎯 Winrate: {summary['today']['winrate']}%
💰 PnL: ${summary['today']['pnl']:.2f}
🏆 Лучший: {summary['today']['best_symbol']}
💀 Худший: {summary['today']['worst_symbol']}

<b>━━━ Тренды ━━━</b>
{trend_arrow(report.winrate_trend)} Winrate: {report.winrate_trend:+.1f}%
{trend_arrow(report.pnl_trend)} PnL/trade: {report.pnl_trend:+.2f}$
{trend_arrow(report.accuracy_trend)} Accuracy: {report.accuracy_trend:+.0%}

<b>━━━ Обучение ━━━</b>
📚 Правил выучено: {summary['learning']['rules_count']}
🔍 Паттернов: {summary['learning']['patterns_count']}
🎯 Точность AI: {summary['learning']['prediction_accuracy']}%

<b>━━━ Рекомендации ━━━</b>
"""
        
        for rec in report.recommendations[:3]:
            text += f"• {rec}\n"
        
        return text


# ==================== GLOBAL INSTANCE ====================
_learning_tracker: Optional[LearningTracker] = None

def get_learning_tracker() -> LearningTracker:
    """Получить глобальный экземпляр трекера"""
    global _learning_tracker
    if _learning_tracker is None:
        _learning_tracker = LearningTracker()
    return _learning_tracker

def collect_daily_snapshot(run_sql_func) -> DailySnapshot:
    """Собрать ежедневный снимок"""
    tracker = get_learning_tracker()
    return tracker.collect_daily_metrics(run_sql_func)

def get_learning_report(period: str = "daily") -> LearningReport:
    """Получить отчёт"""
    tracker = get_learning_tracker()
    return tracker.generate_report(period)

def get_learning_summary() -> Dict[str, Any]:
    """Получить сводку для дашборда"""
    tracker = get_learning_tracker()
    return tracker.get_learning_summary()

def get_telegram_report() -> str:
    """Получить отчёт для Telegram"""
    tracker = get_learning_tracker()
    return tracker.format_telegram_report()
