"""
Volume PairList provider

Provides dynamic pair list based on trade volumes
"""
import logging
from datetime import timedelta
from typing import Any, Dict, List

from cachetools import TTLCache

from freqtrade.constants import Config, ListPairsWithTimeframes
from freqtrade.exceptions import OperationalException
from freqtrade.exchange import timeframe_to_minutes, timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.plugins.pairlist.IPairList import IPairList, PairlistParameter
from freqtrade.resolvers.strategy_resolver import StrategyResolver
from freqtrade.strategy.interface import IStrategy
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import dt_now


logger = logging.getLogger(__name__)


SORT_VALUES = ["quoteVolume"]


class IndicatorFilter(IPairList):
    def __init__(
        self,
        exchange,
        pairlistmanager,
        config: Config,
        pairlistconfig: Dict[str, Any],
        pairlist_pos: int,
    ) -> None:
        super().__init__(exchange, pairlistmanager, config, pairlistconfig, pairlist_pos)

        self._strategy_name = self._pairlistconfig.get("strategy_name", "")
        self._lookback_timeframe = self._pairlistconfig.get("lookback_timeframe", "1d")
        self._lookback_period = self._pairlistconfig.get("lookback_period", 0)

        self._def_candletype = self._config["candle_type_def"]

        self._tf_in_min = timeframe_to_minutes(self._lookback_timeframe)
        self._tf_in_sec = timeframe_to_seconds(self._lookback_timeframe)

        self._pair_cache: TTLCache = TTLCache(maxsize=1000, ttl=self._tf_in_sec)

        candle_limit = exchange.ohlcv_candle_limit(
            self._lookback_timeframe, self._config["candle_type_def"]
        )
        if self._lookback_period <= 0:
            raise OperationalException("IndicatorFilter requires lookback_period to be > 0")
        if self._lookback_period > candle_limit:
            raise OperationalException(
                "IndicatorFilter requires lookback_period to not "
                f"exceed exchange max request size ({candle_limit})"
            )
        if self._strategy_name == "":
            raise OperationalException("IndicatorFilter requires a strategy to function")

        self._strategy: IStrategy = StrategyResolver._load_strategy(self._strategy_name, config)

    @property
    def needstickers(self) -> bool:
        """
        Boolean property defining if tickers are necessary.
        If no Pairlist requires tickers, an empty List is passed
        as tickers argument to filter_pairlist
        """
        return False

    def short_desc(self) -> str:
        """
        Short whitelist method description - used for startup-messages
        """
        return f"{self.name} - filtering pairs by condition from indicator_filter strategy callback"

    @staticmethod
    def description() -> str:
        return "Filter pairs by condition from indicator_filter strategy callback"

    @staticmethod
    def available_parameters() -> Dict[str, PairlistParameter]:
        return {
            "strategy_name": {
                "type": "string",
                "default": "",
                "description": "Strategy",
                "help": "Strategy to use for the desired callback function",
            },
            "lookback_timeframe": {
                "type": "string",
                "default": "",
                "description": "Lookback Timeframe",
                "help": "Timeframe to use for lookback.",
            },
            "lookback_period": {
                "type": "number",
                "default": 0,
                "description": "Lookback Period",
                "help": "Number of periods to look back at.",
            },
        }

    def filter_pairlist(self, pairlist: List[str], tickers: Dict) -> List[str]:
        """
        Validate trading range
        :param pairlist: pairlist to filter or sort
        :param tickers: Tickers (from exchange.get_tickers). May be cached.
        :return: new allowlist
        """
        filtered_tickers: List[Dict[str, Any]] = [{"symbol": k} for k in pairlist]
        pairs: List[str] = []
        since_ms = (
            int(
                timeframe_to_prev_date(
                    self._lookback_timeframe,
                    dt_now()
                    + timedelta(
                        minutes=-(self._lookback_period * self._tf_in_min) - self._tf_in_min
                    ),
                ).timestamp()
            )
            * 1000
        )

        needed_pairs: ListPairsWithTimeframes = [
            (p, self._lookback_timeframe, self._def_candletype)
            for p in [s["symbol"] for s in filtered_tickers]
            if p not in self._pair_cache
        ]

        candles = {}
        if needed_pairs:
            candles = self._exchange.refresh_latest_ohlcv(
                needed_pairs, since_ms=since_ms, cache=False
            )

        for i, p in enumerate(filtered_tickers):
            dataframe = (
                candles[(p["symbol"], self._lookback_timeframe, self._def_candletype)]
                if (p["symbol"], self._lookback_timeframe, self._def_candletype) in candles
                else None
            )
            if dataframe is None:
                continue

            if strategy_safe_wrapper(self._strategy.indicator_filter)(dataframe):
                pairs.append(p["symbol"])
            else:
                logger.info(
                    f"{p['symbol']} doesn't satisfy defined condition, removing from whitelist."
                )

        return pairs
