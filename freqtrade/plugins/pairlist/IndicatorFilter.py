"""
Indicator PairList filter
"""
import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Dict, List

from cachetools import TTLCache
from pandas import DataFrame

from freqtrade.constants import Config, ListPairsWithTimeframes
from freqtrade.exceptions import OperationalException
from freqtrade.exchange import timeframe_to_minutes, timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.plugins.pairlist.IPairList import IPairList, PairlistParameter
from freqtrade.util import dt_now


logger = logging.getLogger(__name__)


class IndicatorFilter(IPairList, ABC):
    def __init__(
        self,
        exchange,
        pairlistmanager,
        config: Config,
        pairlistconfig: Dict[str, Any],
        pairlist_pos: int,
    ) -> None:
        super().__init__(exchange, pairlistmanager, config, pairlistconfig, pairlist_pos)

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
        return f"{self.name} - filtering pairs by condition from filter_pairlist method"

    @staticmethod
    def description() -> str:
        return "Filter pairs by condition from filter_pairs method"

    @staticmethod
    def available_parameters() -> Dict[str, PairlistParameter]:
        return {
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

            if self.filter_dataframe(dataframe=dataframe):
                pairs.append(p["symbol"])

        logger.info(f"{len(pairs)}/{len(pairlist)} pairs matched IndicatorFilter condition")
        return pairs

    @abstractmethod
    def filter_dataframe(self, dataframe: DataFrame) -> bool:
        pass
