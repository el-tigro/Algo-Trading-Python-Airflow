import pandas as pd
from datetime import datetime, timedelta
from utils import tinkoff_functions, db
import math
from tinkoff.invest.utils import  quotation_to_decimal
import logging

def create_limit_order_by_signals(
        connector: str,
        ticker: str,
        weights: dict,
        threshold: float = 0.5
) -> None:

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    signals = db.get_data_from_signal_table(
        connector,
        f"WHERE time = (SELECT max(time) FROM signal) and lower(ticker) = lower('{ticker}')"
    )
    logger.info("get_data_from_signal_table")
    if signals.empty:
        return
    logger.info("put weights")
    signals['weights'] = signals['strategy_type'].map(weights)
    signals['weighted_position'] = signals['weights'] * signals['position']
    position = tinkoff_functions.get_position_by_ticker(ticker)
    last_price = db.get_last_price_from_price_table(connector, ticker.lower())

    if signals['weighted_position'].sum() > threshold:
        logger.info("'weighted_position > threshold")
        if not position:
            tinkoff_functions.create_limit_order_by_figi(
                tinkoff_functions.get_figi_from_ticker(ticker),
                int(tinkoff_functions.get_current_balance('usd') * 0.1 // last_price),
                math.ceil(last_price * 100) / 100,
                'Buy'
            )
    elif signals['weighted_position'].sum() < -threshold:
        logger.info("'weighted_position < threshold")
        if position:
            tinkoff_functions.create_limit_order_by_figi(
                position.figi,
                int(quotation_to_decimal(position.quantity_lots)),
                math.floor(last_price * 100) / 100,
                'Sell'
            )