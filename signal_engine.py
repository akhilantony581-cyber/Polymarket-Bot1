"""
signal_engine.py
Evaluates trade signals by combining Binance real-world data with Polymarket state.
Produces a reversal risk score (0–100) and recommends trade mode.
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from binance_feed import BinanceFeed, SymbolData
from polymarket_listener import PolymarketMarket

logger = logging.getLogger(__name__)


class TradeMode(Enum):
    NONE = "none"
    STANDARD = "standard"
    SNIPER = "sniper"
    MAKER = "maker"


@dataclass
class SignalResult:
    market_id: str
    coin: str
    timeframe: str
    yes_price: float
    strike: float
    binance_price: float
    reversal_score: float       # 0 = no reversal risk, 100 = certain reversal
    mode: TradeMode
    kelly_fraction: float       # 0.06 / 0.10 / 0.15
    reason: str                 # human-readable explanation
    timestamp: float = 0.0

    def is_tradeable(self) -> bool:
        return self.mode != TradeMode.NONE

    def to_dict(self) -> dict:
        return {
            "market_id": self.market_id,
            "coin": self.coin,
            "timeframe": self.timeframe,
            "yes_price": self.yes_price,
            "strike": self.strike,
            "binance_price": self.binance_price,
            "reversal_score": self.reversal_score,
            "mode": self.mode.value,
            "kelly_fraction": self.kelly_fraction,
            "reason": self.reason,
            "timestamp": self.timestamp,
        }


class SignalEngine:
    """
    Core signal evaluation logic.
    Combines Binance price/momentum/volatility with Polymarket market state
    to determine if a trade meets the certainty threshold.
    """

    def __init__(self, config: dict, binance_feed: BinanceFeed):
        self.config = config
        self.binance = binance_feed
        self._load_config()

    def _load_config(self):
        sig = self.config.get("signal", {})
        self.reversal_threshold = sig.get("reversal_score_threshold", 10)
        self.buffer_pct = sig.get("buffer_from_strike_pct", 0.003)
        self.stability_window = sig.get("stability_window_seconds", 30)

        price_cfg = self.config.get("price", {})
        self.min_entry = price_cfg.get("min_entry", 0.98)
        self.standard_max = price_cfg.get("standard_max", 0.989)
        self.sniper_min = price_cfg.get("sniper_min", 0.99)

        sniper_cfg = self.config.get("sniper", {})
        self.sniper_expiry_threshold = sniper_cfg.get("time_to_expiry_threshold", 40)
        self.sniper_strike_buffer = sniper_cfg.get("oracle_strike_buffer", 0.003)

        ob_cfg = self.config.get("order_book", {})
        self.min_imbalance = ob_cfg.get("min_bid_ask_ratio", 1.2)
        self.min_depth = ob_cfg.get("min_depth_at_price", 100.0)

        kelly_cfg = self.config.get("risk", {}).get("kelly", {})
        self.kelly_tiers = [
            (95, kelly_cfg.get("score_95_100", 0.15)),
            (90, kelly_cfg.get("score_90_94", 0.10)),
            (85, kelly_cfg.get("score_85_89", 0.06)),
        ]

        vol_cfg = self.config.get("risk", {})
        self.vol_spike_threshold = vol_cfg.get("volatility_spike_threshold", 2.5)

    def reload_config(self, config: dict):
        self.config = config
        self._load_config()

    def evaluate(self, market: PolymarketMarket) -> SignalResult:
        """
        Main evaluation entry point.
        Returns a SignalResult with mode=NONE if no trade should be taken.
        """
        yes_price = market.yes_price
        coin = market.coin
        tte = market.seconds_to_expiry

        # Hard floor check
        if yes_price < self.min_entry:
            return self._reject(market, "below_min_price",
                                f"YES price {yes_price:.4f} < {self.min_entry}")

        binance_data = self.binance.get(coin)
        if not binance_data or not self.binance.is_ready(coin):
            return self._reject(market, "binance_not_ready",
                                f"Binance data not ready for {coin}")

        # Route to correct mode
        if yes_price >= self.sniper_min and tte <= self.sniper_expiry_threshold:
            return self._evaluate_sniper(market, binance_data)
        elif yes_price >= self.min_entry:
            return self._evaluate_standard(market, binance_data)

        return self._reject(market, "no_mode_match", "No mode criteria met")

    def _is_updown_market(self, market: PolymarketMarket) -> bool:
        """True for 'Up or Down' directional markets with no fixed strike."""
        return market.strike == 0.0

    # ------------------------------------------------------------------
    # STANDARD MODE: full reversal analysis
    # ------------------------------------------------------------------
    def _evaluate_standard(self, market: PolymarketMarket,
                            bd: SymbolData) -> SignalResult:
        score_components = []
        reasons = []

        # 1. Price buffer from strike (skip for Up/Down markets — no strike)
        if not self._is_updown_market(market):
            distance = self._distance_from_strike(market, bd.price)
            if distance < self.buffer_pct:
                return self._reject(market, "insufficient_buffer",
                                    f"Price {bd.price} too close to strike {market.strike} "
                                    f"(buffer {distance:.4f} < {self.buffer_pct})")
        else:
            # For Up/Down markets require at least 1m momentum to align
            m1 = bd.momentum(60)
            if m1 is not None:
                expected_up = market.direction in ("above", "up")
                if expected_up and m1 < 0:
                    return self._reject(market, "momentum_against_direction",
                                        f"1m momentum {m1:.3f}% negative for UP market")
                elif not expected_up and m1 > 0:
                    return self._reject(market, "momentum_against_direction",
                                        f"1m momentum {m1:.3f}% positive for DOWN market")

        # 2. Price held beyond strike for stability_window
        if not self._held_beyond_strike(market, bd):
            score_components.append(20)
            reasons.append("price_not_held")
        else:
            score_components.append(0)

        # 3. Momentum aligned (1m, 3m, 5m all pointing same direction)
        momentum_score = self._momentum_score(market, bd)
        score_components.append(momentum_score)
        if momentum_score > 5:
            reasons.append(f"momentum_weak({momentum_score})")

        # 4. Volatility not spiking
        vol_score = self._volatility_score(bd)
        score_components.append(vol_score)
        if vol_score > 5:
            reasons.append(f"volatility_elevated({vol_score})")

        # 5. Price moving away from strike (not toward it)
        direction_score = self._direction_score(market, bd)
        score_components.append(direction_score)
        if direction_score > 5:
            reasons.append(f"price_moving_toward_strike({direction_score})")

        # 6. Order book health
        ob_score = self._order_book_score(market)
        score_components.append(ob_score)
        if ob_score > 5:
            reasons.append(f"weak_order_book({ob_score})")

        total_score = sum(score_components)
        reason_str = ", ".join(reasons) if reasons else "all_clear"

        if total_score >= self.reversal_threshold:
            return self._reject(market, "reversal_risk_too_high",
                                f"Score {total_score} >= threshold {self.reversal_threshold}: {reason_str}")

        # Compute certainty score (inverse of reversal) for Kelly sizing
        certainty = 100 - total_score
        kelly = self._kelly_fraction(certainty)

        return SignalResult(
            market_id=market.market_id,
            coin=market.coin,
            timeframe=market.timeframe,
            yes_price=market.yes_price,
            strike=market.strike,
            binance_price=bd.price,
            reversal_score=total_score,
            mode=TradeMode.STANDARD,
            kelly_fraction=kelly,
            reason=f"standard_ok: certainty={certainty:.0f} {reason_str}",
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # SNIPER MODE: lean checks only — time is the arbiter
    # ------------------------------------------------------------------
    def _evaluate_sniper(self, market: PolymarketMarket,
                         bd: SymbolData) -> SignalResult:
        tte = market.seconds_to_expiry

        # Oracle must still be clearly beyond strike
        if not self._held_beyond_strike(market, bd, window=min(30, int(tte))):
            return self._reject(market, "oracle_not_confirmed",
                                "Binance price not confirmed above/below strike")

        # No sudden approach to strike (skip for Up/Down — no fixed strike)
        if not self._is_updown_market(market):
            distance = self._distance_from_strike(market, bd.price)
            if distance < self.sniper_strike_buffer:
                return self._reject(market, "too_close_to_strike",
                                    f"Distance {distance:.4f} < sniper buffer {self.sniper_strike_buffer}")

        # Order book must show no sudden sell wall
        ob = market.order_book
        imbalance = ob.imbalance_ratio(market.yes_price)
        if imbalance < self.min_imbalance:
            return self._reject(market, "sell_pressure",
                                f"Order book imbalance {imbalance:.2f} < {self.min_imbalance}")

        # Price stability: YES token not dropping
        # (use Polymarket order book best bid as proxy)
        best_bid = ob.best_bid()
        if best_bid and best_bid < market.yes_price - 0.008:
            return self._reject(market, "yes_price_falling",
                                f"Best bid {best_bid:.4f} significantly below YES price")

        distance_note = "updown" if self._is_updown_market(market) else \
            f"{self._distance_from_strike(market, bd.price):.4f}"
        return SignalResult(
            market_id=market.market_id,
            coin=market.coin,
            timeframe=market.timeframe,
            yes_price=market.yes_price,
            strike=market.strike,
            binance_price=bd.price,
            reversal_score=2.0,
            mode=TradeMode.SNIPER,
            kelly_fraction=self._kelly_fraction(98),
            reason=f"sniper_ok: tte={tte:.0f}s distance={distance_note}",
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # MAKER MODE: evaluate if worth posting a passive order
    # ------------------------------------------------------------------
    def evaluate_maker(self, market: PolymarketMarket) -> Optional[SignalResult]:
        """
        Returns a signal for posting a maker order at 0.93–0.96
        if the market shows high eventual certainty.
        """
        bd = self.binance.get(market.coin)
        if not bd or not self.binance.is_ready(market.coin):
            return None

        if market.seconds_to_expiry < 120:
            return None  # too close to expiry for maker

        maker_cfg = self.config.get("maker", {})
        min_certainty = maker_cfg.get("min_certainty_score", 80)

        if self._is_updown_market(market):
            # For Up/Down markets: use multi-window momentum alignment as certainty proxy
            windows = [60, 180, 300]
            expected_up = market.direction in ("above", "up")
            aligned = sum(
                1 for w in windows
                if (m := bd.momentum(w)) is not None and
                   ((expected_up and m > 0) or (not expected_up and m < 0))
            )
            certainty = aligned / len(windows) * 100
            distance_note = f"momentum_aligned={aligned}/{len(windows)}"
        else:
            distance = self._distance_from_strike(market, bd.price)
            if distance < self.buffer_pct * 2:
                return None  # not enough conviction yet
            certainty = min(100, distance / self.buffer_pct * 25)
            distance_note = f"distance={distance:.4f}"

        if certainty < min_certainty:
            return None

        kelly = self._kelly_fraction(certainty)
        return SignalResult(
            market_id=market.market_id,
            coin=market.coin,
            timeframe=market.timeframe,
            yes_price=market.yes_price,
            strike=market.strike,
            binance_price=bd.price,
            reversal_score=100 - certainty,
            mode=TradeMode.MAKER,
            kelly_fraction=kelly,
            reason=f"maker_ok: certainty={certainty:.0f} {distance_note}",
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Scoring helpers
    # ------------------------------------------------------------------
    def _distance_from_strike(self, market: PolymarketMarket, price: float) -> float:
        """Returns fractional distance of Binance price from strike.
        Returns a large positive value for Up/Down markets (no fixed strike)."""
        if market.strike == 0.0:
            return 1.0  # Up/Down market — no strike, treat as well clear
        if market.direction == "above":
            return (price - market.strike) / market.strike
        else:
            return (market.strike - price) / market.strike

    def _held_beyond_strike(self, market: PolymarketMarket,
                             bd: SymbolData, window: int = None) -> bool:
        w = window or self.stability_window
        if market.strike == 0.0:
            # Up/Down: verify momentum direction has been consistent
            expected_up = market.direction in ("above", "up")
            m = bd.momentum(w)
            if m is None:
                return bd.is_stable(w)
            return (expected_up and m > 0) or (not expected_up and m < 0)
        if market.direction == "above":
            return bd.held_above(market.strike, w)
        else:
            return bd.held_below(market.strike, w)

    def _momentum_score(self, market: PolymarketMarket, bd: SymbolData) -> float:
        """0 = perfectly aligned, higher = more reversal risk."""
        score = 0.0
        expected_positive = market.direction in ("above", "up")
        windows = [60, 180, 300]
        for w in windows:
            m = bd.momentum(w)
            if m is None:
                score += 3
                continue
            if expected_positive and m < 0:
                score += 4
            elif not expected_positive and m > 0:
                score += 4
        return min(score, 15)

    def _volatility_score(self, bd: SymbolData) -> float:
        """0 = stable, higher = more volatile."""
        vol = bd.volatility(60)
        if vol is None:
            return 5.0
        # Compare against recent baseline
        vol_5m = bd.volatility(300)
        if vol_5m is None or vol_5m == 0:
            return 0.0
        ratio = vol / vol_5m
        if ratio > self.vol_spike_threshold:
            return 15.0
        elif ratio > 1.5:
            return 8.0
        elif ratio > 1.2:
            return 3.0
        return 0.0

    def _direction_score(self, market: PolymarketMarket, bd: SymbolData) -> float:
        """0 = moving away from strike, higher = moving toward it."""
        m1 = bd.momentum(60)
        if m1 is None:
            return 5.0
        expected_positive = market.direction in ("above", "up")
        if expected_positive and m1 > 0:
            return 0.0
        elif not expected_positive and m1 < 0:
            return 0.0
        elif abs(m1) < 0.01:
            return 2.0  # flat — slight concern
        return 8.0  # moving wrong way

    def _order_book_score(self, market: PolymarketMarket) -> float:
        """0 = healthy book, higher = weak/dangerous book."""
        ob = market.order_book
        if not ob.bids and not ob.asks:
            return 10.0  # no data

        imbalance = ob.imbalance_ratio(market.yes_price)
        depth = ob.bid_depth_at_or_above(market.yes_price - 0.01)

        score = 0.0
        if imbalance < self.min_imbalance:
            score += 8.0
        if depth < self.min_depth:
            score += 5.0
        return min(score, 15.0)

    def _kelly_fraction(self, certainty: float) -> float:
        for threshold, fraction in self.kelly_tiers:
            if certainty >= threshold:
                return fraction
        return 0.0

    def _reject(self, market: PolymarketMarket, code: str, detail: str) -> SignalResult:
        logger.debug(f"Signal rejected [{code}] {market.market_id}: {detail}")
        return SignalResult(
            market_id=market.market_id,
            coin=market.coin,
            timeframe=market.timeframe,
            yes_price=market.yes_price,
            strike=market.strike,
            binance_price=self.binance.get(market.coin).price
            if self.binance.get(market.coin) else 0.0,
            reversal_score=100.0,
            mode=TradeMode.NONE,
            kelly_fraction=0.0,
            reason=f"{code}: {detail}",
            timestamp=time.time(),
        )
