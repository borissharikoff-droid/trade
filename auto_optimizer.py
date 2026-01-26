"""
Auto Optimizer - Automated Trading Strategy Optimization System
Continuously analyzes trading performance and adjusts parameters to improve win rate
"""

import logging
import json
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class OptimizationType(Enum):
    """Types of optimization"""
    PARAMETER_TUNE = "parameter_tune"
    FILTER_ADJUST = "filter_adjust"
    TIMING_OPTIMIZE = "timing_optimize"
    SYMBOL_SELECTION = "symbol_selection"
    RISK_ADJUST = "risk_adjust"


@dataclass
class OptimizationResult:
    """Result of an optimization analysis"""
    parameter: str
    current_value: Any
    recommended_value: Any
    confidence: float  # 0-1
    expected_improvement: float  # Expected win rate improvement
    reason: str
    data_points: int  # Number of trades analyzed
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class TradePattern:
    """Identified trading pattern"""
    pattern_type: str
    conditions: Dict
    win_rate: float
    total_trades: int
    avg_pnl: float
    significance: float  # Statistical significance


class AutoOptimizer:
    """
    Automated trading strategy optimizer
    
    Continuously analyzes:
    - Win rates by symbol, direction, time, market conditions
    - Optimal TP/SL levels
    - Best entry timing
    - Risk management parameters
    
    And automatically adjusts parameters to improve performance
    """
    
    def __init__(self, run_sql_func=None, trade_logger=None):
        self._run_sql = run_sql_func
        self._trade_logger = trade_logger
        
        # Optimization state
        self._last_analysis = None
        self._analysis_interval = timedelta(hours=4)  # Analyze every 4 hours
        self._min_trades_for_analysis = 20
        
        # Current optimized parameters (can be applied to trading)
        self._optimized_params: Dict[str, Any] = {}
        
        # Learning history
        self._optimization_history: List[OptimizationResult] = []
        
        # Performance metrics
        self._metrics = {
            'total_analyses': 0,
            'optimizations_applied': 0,
            'estimated_improvement': 0.0
        }
    
    def set_dependencies(self, run_sql_func, trade_logger=None):
        """Set dependencies after initialization"""
        self._run_sql = run_sql_func
        self._trade_logger = trade_logger
    
    # === Data Gathering ===
    
    def _get_trade_history(self, days: int = 30) -> List[Dict]:
        """Get trade history from database"""
        if not self._run_sql:
            return []
        
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            
            trades = self._run_sql("""
                SELECT * FROM history 
                WHERE closed_at > ?
                ORDER BY closed_at DESC
            """, (cutoff,), fetch="all")
            
            return trades or []
        except Exception as e:
            logger.error(f"[OPTIMIZER] Failed to get trade history: {e}")
            return []
    
    def _get_trading_logs(self, days: int = 7, category: str = None) -> List[Dict]:
        """Get trading logs from database"""
        if not self._run_sql:
            return []
        
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            
            if category:
                logs = self._run_sql("""
                    SELECT * FROM trading_logs 
                    WHERE timestamp > ? AND category = ?
                    ORDER BY timestamp DESC
                """, (cutoff, category), fetch="all")
            else:
                logs = self._run_sql("""
                    SELECT * FROM trading_logs 
                    WHERE timestamp > ?
                    ORDER BY timestamp DESC
                """, (cutoff,), fetch="all")
            
            return logs or []
        except Exception as e:
            logger.error(f"[OPTIMIZER] Failed to get trading logs: {e}")
            return []
    
    # === Analysis Methods ===
    
    def analyze_win_rate_by_symbol(self, trades: List[Dict]) -> Dict[str, Dict]:
        """Analyze win rate by trading symbol"""
        symbol_stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_pnl': 0, 'trades': []})
        
        for trade in trades:
            symbol = trade.get('symbol', 'UNKNOWN')
            pnl = trade.get('pnl', 0) or 0
            
            if pnl > 0:
                symbol_stats[symbol]['wins'] += 1
            else:
                symbol_stats[symbol]['losses'] += 1
            
            symbol_stats[symbol]['total_pnl'] += pnl
            symbol_stats[symbol]['trades'].append(pnl)
        
        result = {}
        for symbol, stats in symbol_stats.items():
            total = stats['wins'] + stats['losses']
            if total > 0:
                result[symbol] = {
                    'win_rate': stats['wins'] / total,
                    'total_trades': total,
                    'total_pnl': stats['total_pnl'],
                    'avg_pnl': stats['total_pnl'] / total,
                    'max_win': max(stats['trades']) if stats['trades'] else 0,
                    'max_loss': min(stats['trades']) if stats['trades'] else 0
                }
        
        return result
    
    def analyze_win_rate_by_direction(self, trades: List[Dict]) -> Dict[str, Dict]:
        """Analyze win rate by trade direction (LONG/SHORT)"""
        direction_stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_pnl': 0})
        
        for trade in trades:
            direction = trade.get('direction', 'UNKNOWN')
            pnl = trade.get('pnl', 0) or 0
            
            if pnl > 0:
                direction_stats[direction]['wins'] += 1
            else:
                direction_stats[direction]['losses'] += 1
            
            direction_stats[direction]['total_pnl'] += pnl
        
        result = {}
        for direction, stats in direction_stats.items():
            total = stats['wins'] + stats['losses']
            if total > 0:
                result[direction] = {
                    'win_rate': stats['wins'] / total,
                    'total_trades': total,
                    'total_pnl': stats['total_pnl'],
                    'avg_pnl': stats['total_pnl'] / total
                }
        
        return result
    
    def analyze_win_rate_by_hour(self, trades: List[Dict]) -> Dict[int, Dict]:
        """Analyze win rate by hour of day (UTC)"""
        hour_stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_pnl': 0})
        
        for trade in trades:
            try:
                closed_at = trade.get('closed_at', '')
                if closed_at:
                    dt = datetime.fromisoformat(str(closed_at).replace('Z', '+00:00'))
                    hour = dt.hour
                    pnl = trade.get('pnl', 0) or 0
                    
                    if pnl > 0:
                        hour_stats[hour]['wins'] += 1
                    else:
                        hour_stats[hour]['losses'] += 1
                    
                    hour_stats[hour]['total_pnl'] += pnl
            except:
                pass
        
        result = {}
        for hour, stats in hour_stats.items():
            total = stats['wins'] + stats['losses']
            if total >= 3:  # Minimum trades for statistical relevance
                result[hour] = {
                    'win_rate': stats['wins'] / total,
                    'total_trades': total,
                    'total_pnl': stats['total_pnl']
                }
        
        return result
    
    def analyze_tp_sl_effectiveness(self, trades: List[Dict]) -> Dict:
        """Analyze TP/SL hit rates and effectiveness"""
        tp_hits = {'TP1': 0, 'TP2': 0, 'TP3': 0, 'SL': 0, 'OTHER': 0}
        tp_pnl = {'TP1': 0, 'TP2': 0, 'TP3': 0, 'SL': 0, 'OTHER': 0}
        
        for trade in trades:
            reason = trade.get('reason', 'OTHER')
            pnl = trade.get('pnl', 0) or 0
            
            if 'TP1' in reason:
                tp_hits['TP1'] += 1
                tp_pnl['TP1'] += pnl
            elif 'TP2' in reason:
                tp_hits['TP2'] += 1
                tp_pnl['TP2'] += pnl
            elif 'TP3' in reason:
                tp_hits['TP3'] += 1
                tp_pnl['TP3'] += pnl
            elif 'SL' in reason or 'STOP' in reason:
                tp_hits['SL'] += 1
                tp_pnl['SL'] += pnl
            else:
                tp_hits['OTHER'] += 1
                tp_pnl['OTHER'] += pnl
        
        total = sum(tp_hits.values())
        
        return {
            'tp1_rate': tp_hits['TP1'] / total if total > 0 else 0,
            'tp2_rate': tp_hits['TP2'] / total if total > 0 else 0,
            'tp3_rate': tp_hits['TP3'] / total if total > 0 else 0,
            'sl_rate': tp_hits['SL'] / total if total > 0 else 0,
            'other_rate': tp_hits['OTHER'] / total if total > 0 else 0,
            'tp_pnl': tp_pnl,
            'total_trades': total
        }
    
    def analyze_holding_time(self, trades: List[Dict]) -> Dict:
        """Analyze trade holding times and correlation with outcomes"""
        winners = []
        losers = []
        
        for trade in trades:
            try:
                opened_at = trade.get('opened_at', '')
                closed_at = trade.get('closed_at', '')
                pnl = trade.get('pnl', 0) or 0
                
                if opened_at and closed_at:
                    opened = datetime.fromisoformat(str(opened_at).replace('Z', '+00:00'))
                    closed = datetime.fromisoformat(str(closed_at).replace('Z', '+00:00'))
                    holding_minutes = (closed - opened).total_seconds() / 60
                    
                    if pnl > 0:
                        winners.append(holding_minutes)
                    else:
                        losers.append(holding_minutes)
            except:
                pass
        
        return {
            'avg_winner_time': np.mean(winners) if winners else 0,
            'avg_loser_time': np.mean(losers) if losers else 0,
            'median_winner_time': np.median(winners) if winners else 0,
            'median_loser_time': np.median(losers) if losers else 0,
            'winner_count': len(winners),
            'loser_count': len(losers)
        }
    
    def analyze_consecutive_patterns(self, trades: List[Dict]) -> Dict:
        """Analyze patterns in consecutive wins/losses"""
        # Sort by time
        sorted_trades = sorted(trades, key=lambda x: x.get('closed_at', ''))
        
        current_streak = 0
        max_win_streak = 0
        max_loss_streak = 0
        streak_type = None
        
        streaks = {'wins': [], 'losses': []}
        
        for trade in sorted_trades:
            pnl = trade.get('pnl', 0) or 0
            is_win = pnl > 0
            
            if streak_type is None:
                streak_type = is_win
                current_streak = 1
            elif streak_type == is_win:
                current_streak += 1
            else:
                # Streak ended
                if streak_type:
                    streaks['wins'].append(current_streak)
                    max_win_streak = max(max_win_streak, current_streak)
                else:
                    streaks['losses'].append(current_streak)
                    max_loss_streak = max(max_loss_streak, current_streak)
                
                streak_type = is_win
                current_streak = 1
        
        # Don't forget the last streak
        if streak_type is not None:
            if streak_type:
                streaks['wins'].append(current_streak)
                max_win_streak = max(max_win_streak, current_streak)
            else:
                streaks['losses'].append(current_streak)
                max_loss_streak = max(max_loss_streak, current_streak)
        
        return {
            'max_win_streak': max_win_streak,
            'max_loss_streak': max_loss_streak,
            'avg_win_streak': np.mean(streaks['wins']) if streaks['wins'] else 0,
            'avg_loss_streak': np.mean(streaks['losses']) if streaks['losses'] else 0
        }
    
    # === Optimization Recommendations ===
    
    def generate_recommendations(self, trades: List[Dict]) -> List[OptimizationResult]:
        """Generate optimization recommendations based on analysis"""
        recommendations = []
        
        if len(trades) < self._min_trades_for_analysis:
            logger.info(f"[OPTIMIZER] Insufficient trades for analysis: {len(trades)}")
            return recommendations
        
        # Analyze all aspects
        symbol_analysis = self.analyze_win_rate_by_symbol(trades)
        direction_analysis = self.analyze_win_rate_by_direction(trades)
        hour_analysis = self.analyze_win_rate_by_hour(trades)
        tp_sl_analysis = self.analyze_tp_sl_effectiveness(trades)
        holding_analysis = self.analyze_holding_time(trades)
        streak_analysis = self.analyze_consecutive_patterns(trades)
        
        # 1. Symbol recommendations
        underperforming_symbols = []
        for symbol, stats in symbol_analysis.items():
            if stats['total_trades'] >= 5 and stats['win_rate'] < 0.4:
                underperforming_symbols.append((symbol, stats['win_rate'], stats['total_trades']))
        
        if underperforming_symbols:
            symbols_str = ', '.join([f"{s[0]}({s[1]:.0%})" for s in underperforming_symbols])
            recommendations.append(OptimizationResult(
                parameter='underperforming_symbols',
                current_value=symbols_str,
                recommended_value='Consider avoiding these symbols',
                confidence=0.7,
                expected_improvement=0.05,
                reason=f'Symbols with <40% win rate: {symbols_str}',
                data_points=sum(s[2] for s in underperforming_symbols)
            ))
        
        # 2. Direction bias
        if 'LONG' in direction_analysis and 'SHORT' in direction_analysis:
            long_wr = direction_analysis['LONG']['win_rate']
            short_wr = direction_analysis['SHORT']['win_rate']
            
            if abs(long_wr - short_wr) > 0.15:  # Significant difference
                better = 'LONG' if long_wr > short_wr else 'SHORT'
                recommendations.append(OptimizationResult(
                    parameter='direction_bias',
                    current_value=f'LONG:{long_wr:.0%}, SHORT:{short_wr:.0%}',
                    recommended_value=f'Favor {better} trades',
                    confidence=0.6,
                    expected_improvement=abs(long_wr - short_wr) * 0.3,
                    reason=f'{better} performs significantly better ({abs(long_wr - short_wr):.0%} difference)',
                    data_points=direction_analysis['LONG']['total_trades'] + direction_analysis['SHORT']['total_trades']
                ))
        
        # 3. Time-based recommendations
        best_hours = sorted(hour_analysis.items(), key=lambda x: x[1]['win_rate'], reverse=True)[:3]
        worst_hours = sorted(hour_analysis.items(), key=lambda x: x[1]['win_rate'])[:3]
        
        if best_hours and worst_hours:
            best_wr = np.mean([h[1]['win_rate'] for h in best_hours])
            worst_wr = np.mean([h[1]['win_rate'] for h in worst_hours])
            
            if best_wr - worst_wr > 0.2:
                recommendations.append(OptimizationResult(
                    parameter='trading_hours',
                    current_value=f'Worst: {[h[0] for h in worst_hours]}',
                    recommended_value=f'Best: {[h[0] for h in best_hours]}',
                    confidence=0.5,
                    expected_improvement=(best_wr - worst_wr) * 0.2,
                    reason=f'Best hours have {(best_wr - worst_wr)*100:.0f}% better win rate',
                    data_points=sum(h[1]['total_trades'] for h in hour_analysis.values())
                ))
        
        # 4. TP/SL optimization
        if tp_sl_analysis['total_trades'] >= 10:
            # Check if SL rate is too high
            if tp_sl_analysis['sl_rate'] > 0.4:
                recommendations.append(OptimizationResult(
                    parameter='stop_loss_distance',
                    current_value=f"SL rate: {tp_sl_analysis['sl_rate']:.0%}",
                    recommended_value='Consider wider SL or better entries',
                    confidence=0.6,
                    expected_improvement=0.03,
                    reason=f"High SL hit rate ({tp_sl_analysis['sl_rate']:.0%}), trades may be stopped out too early",
                    data_points=tp_sl_analysis['total_trades']
                ))
            
            # Check if TP3 is rarely hit
            if tp_sl_analysis['tp1_rate'] > 0.3 and tp_sl_analysis['tp3_rate'] < 0.05:
                recommendations.append(OptimizationResult(
                    parameter='tp_levels',
                    current_value=f"TP1:{tp_sl_analysis['tp1_rate']:.0%}, TP3:{tp_sl_analysis['tp3_rate']:.0%}",
                    recommended_value='Consider closer TP levels',
                    confidence=0.5,
                    expected_improvement=0.02,
                    reason='TP3 rarely hit, most profit taken at TP1',
                    data_points=tp_sl_analysis['total_trades']
                ))
        
        # 5. Risk management based on streaks
        if streak_analysis['max_loss_streak'] >= 4:
            recommendations.append(OptimizationResult(
                parameter='consecutive_loss_limit',
                current_value=f"Max streak: {streak_analysis['max_loss_streak']}",
                recommended_value='Reduce position size after 3 losses',
                confidence=0.7,
                expected_improvement=0.02,
                reason=f"Experienced {streak_analysis['max_loss_streak']} consecutive losses",
                data_points=len(trades)
            ))
        
        return recommendations
    
    # === Main Analysis Loop ===
    
    async def run_analysis(self) -> Dict:
        """Run full optimization analysis"""
        self._metrics['total_analyses'] += 1
        
        logger.info("[OPTIMIZER] Starting optimization analysis...")
        
        # Get trade history
        trades = self._get_trade_history(days=30)
        
        if len(trades) < self._min_trades_for_analysis:
            logger.info(f"[OPTIMIZER] Not enough trades ({len(trades)}) for analysis")
            return {
                'status': 'insufficient_data',
                'trades_analyzed': len(trades),
                'recommendations': []
            }
        
        # Perform analyses
        symbol_analysis = self.analyze_win_rate_by_symbol(trades)
        direction_analysis = self.analyze_win_rate_by_direction(trades)
        hour_analysis = self.analyze_win_rate_by_hour(trades)
        tp_sl_analysis = self.analyze_tp_sl_effectiveness(trades)
        holding_analysis = self.analyze_holding_time(trades)
        streak_analysis = self.analyze_consecutive_patterns(trades)
        
        # Generate recommendations
        recommendations = self.generate_recommendations(trades)
        
        # Calculate overall stats
        total_pnl = sum(t.get('pnl', 0) or 0 for t in trades)
        total_wins = sum(1 for t in trades if (t.get('pnl', 0) or 0) > 0)
        overall_win_rate = total_wins / len(trades) if trades else 0
        
        # Log recommendations
        if self._trade_logger and recommendations:
            for rec in recommendations:
                self._trade_logger.log_optimization(
                    parameter=rec.parameter,
                    old_value=rec.current_value,
                    new_value=rec.recommended_value,
                    reason=rec.reason,
                    expected_improvement=rec.expected_improvement
                )
        
        self._last_analysis = datetime.now(timezone.utc)
        
        result = {
            'status': 'completed',
            'timestamp': self._last_analysis.isoformat(),
            'trades_analyzed': len(trades),
            'overall_stats': {
                'win_rate': overall_win_rate,
                'total_pnl': total_pnl,
                'total_trades': len(trades)
            },
            'symbol_analysis': symbol_analysis,
            'direction_analysis': direction_analysis,
            'hour_analysis': hour_analysis,
            'tp_sl_analysis': tp_sl_analysis,
            'holding_analysis': holding_analysis,
            'streak_analysis': streak_analysis,
            'recommendations': [r.to_dict() for r in recommendations]
        }
        
        logger.info(f"[OPTIMIZER] Analysis complete: {len(trades)} trades, {overall_win_rate:.1%} win rate, {len(recommendations)} recommendations")
        
        return result
    
    def should_run_analysis(self) -> bool:
        """Check if enough time has passed for new analysis"""
        if self._last_analysis is None:
            return True
        
        return datetime.now(timezone.utc) - self._last_analysis >= self._analysis_interval
    
    def get_optimized_params(self) -> Dict[str, Any]:
        """Get currently optimized parameters"""
        return self._optimized_params.copy()
    
    def get_metrics(self) -> Dict:
        """Get optimizer metrics"""
        return {
            **self._metrics,
            'last_analysis': self._last_analysis.isoformat() if self._last_analysis else None,
            'recommendations_count': len(self._optimization_history)
        }
    
    # === Quick Analysis Methods ===
    
    def get_best_symbols(self, min_trades: int = 10) -> List[str]:
        """Get list of best performing symbols"""
        trades = self._get_trade_history(days=14)
        symbol_analysis = self.analyze_win_rate_by_symbol(trades)
        
        # Filter by minimum trades and sort by win rate
        good_symbols = [
            (symbol, stats) for symbol, stats in symbol_analysis.items()
            if stats['total_trades'] >= min_trades and stats['win_rate'] >= 0.55
        ]
        
        good_symbols.sort(key=lambda x: x[1]['win_rate'], reverse=True)
        
        return [s[0] for s in good_symbols[:10]]
    
    def get_worst_symbols(self, min_trades: int = 5) -> List[str]:
        """Get list of worst performing symbols to avoid"""
        trades = self._get_trade_history(days=14)
        symbol_analysis = self.analyze_win_rate_by_symbol(trades)
        
        # Filter by minimum trades and poor win rate
        bad_symbols = [
            (symbol, stats) for symbol, stats in symbol_analysis.items()
            if stats['total_trades'] >= min_trades and stats['win_rate'] < 0.4
        ]
        
        bad_symbols.sort(key=lambda x: x[1]['win_rate'])
        
        return [s[0] for s in bad_symbols[:5]]
    
    def get_recommended_direction(self) -> Optional[str]:
        """Get recommended trade direction based on recent performance"""
        trades = self._get_trade_history(days=7)
        direction_analysis = self.analyze_win_rate_by_direction(trades)
        
        if 'LONG' in direction_analysis and 'SHORT' in direction_analysis:
            long_wr = direction_analysis['LONG']['win_rate']
            short_wr = direction_analysis['SHORT']['win_rate']
            
            # Only recommend if significant difference
            if abs(long_wr - short_wr) > 0.15:
                return 'LONG' if long_wr > short_wr else 'SHORT'
        
        return None
    
    def get_daily_report(self) -> str:
        """Generate a daily performance report"""
        trades = self._get_trade_history(days=1)
        
        if not trades:
            return "No trades in the last 24 hours."
        
        total_pnl = sum(t.get('pnl', 0) or 0 for t in trades)
        total_wins = sum(1 for t in trades if (t.get('pnl', 0) or 0) > 0)
        win_rate = total_wins / len(trades)
        
        report = f"""📊 Daily Report
━━━━━━━━━━━━━━━
Trades: {len(trades)}
Win Rate: {win_rate:.1%}
Total PnL: ${total_pnl:+.2f}
Winners: {total_wins}
Losers: {len(trades) - total_wins}
"""
        
        # Add symbol breakdown
        symbol_stats = self.analyze_win_rate_by_symbol(trades)
        if symbol_stats:
            report += "\nBy Symbol:\n"
            for symbol, stats in sorted(symbol_stats.items(), key=lambda x: x[1]['total_pnl'], reverse=True)[:5]:
                report += f"  {symbol}: {stats['win_rate']:.0%} ({stats['total_trades']} trades, ${stats['total_pnl']:+.2f})\n"
        
        return report


# Global instance
auto_optimizer = AutoOptimizer()


def init_auto_optimizer(run_sql_func, trade_logger=None):
    """Initialize the global auto optimizer"""
    auto_optimizer.set_dependencies(run_sql_func, trade_logger)
    logger.info("[OPTIMIZER] Auto optimizer initialized")
    return auto_optimizer
