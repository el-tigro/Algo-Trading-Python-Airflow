import pandas as pd
from datetime import datetime, timedelta
from configparser import ConfigParser
from typing import Optional
import uuid

from tinkoff.invest import (
    Client,
    PortfolioPosition,
    CandleInterval,
    MoneyValue,
    OrderDirection,
    OrderExecutionReportStatus,
    OrderType,
    PostOrderResponse,
    StopOrderDirection,
    StopOrderExpirationType,
    StopOrderType,
)
import tinkoff.invest.schemas
from tinkoff.invest.constants import INVEST_GRPC_API, INVEST_GRPC_API_SANDBOX
from tinkoff.invest.services import Services
from tinkoff.invest.utils import  quotation_to_decimal, decimal_to_quotation, money_to_decimal

def _get_api_params_from_config() -> dict:
    config_parser = ConfigParser()
    config_parser.read('/usr/local/airflow/tinkoff.cfg')

    if config_parser.get('core', 'USE_SANDBOX') == 'True':
        return {
            'token': config_parser.get('core', 'TOKEN_TINKOFF'),
            'target': INVEST_GRPC_API_SANDBOX
        }
    else:
        return {
            'token': config_parser.get('core', 'TOKEN_TINKOFF'),
            'target': INVEST_GRPC_API
        }

def get_figi_from_ticker(ticker: str) -> str:
    with Client(**_get_api_params_from_config()) as client:
    # with Client(TOKEN, target=INVEST_GRPC_API_SANDBOX) as client:
        ticker_data = client.instruments.find_instrument(query=ticker)
        figi = ticker_data.instruments[0].figi
    return figi


def get_data_by_ticker_and_period(
        ticker: str,
        period_in_days: int = 365,
        freq: CandleInterval = CandleInterval.CANDLE_INTERVAL_DAY
) -> pd.DataFrame:
    with Client(**_get_api_params_from_config()) as client:
        figi = get_figi_from_ticker(ticker)

        raw_data = client.market_data.get_candles(
            figi=figi,
            from_=datetime.now() - timedelta(days=period_in_days),
            to=datetime.now() - timedelta(days=1),
            interval=freq
        )

    return pd.DataFrame(
        data=(
            (
                candle.time,
                quotation_to_decimal(candle.open),
                quotation_to_decimal(candle.high),
                quotation_to_decimal(candle.low),
                quotation_to_decimal(candle.close),
                candle.volume
            ) for candle in raw_data.candles
        ),
        columns=['time',
                 'open',
                 'high',
                 'low',
                 'close',
                 'volume'
                 ]
    )

def get_position_by_ticker(ticker: str) -> Optional[PortfolioPosition]:
    with Client(**_get_api_params_from_config()) as client:

        response = client.users.get_accounts()
        account, *_ = response.accounts
        account_id = account.id

        positions = client.operations.get_portfolio(account_id = account_id).positions

        filtered_position = list(filter(lambda x: x.figi.lower() == get_figi_from_ticker(ticker).lower(), positions))

        if len(filtered_position) > 0:
            return filtered_position[0]
        return None

def create_limit_order_by_figi(
        figi: str,
        lots: int,
        price: float,
        op_type: str = 'Buy'
) -> None:
    if op_type not in ('Buy', 'Sell'):
        raise ValueError('Operation type must be Sell or Buy with upper-case first letter')
    if op_type=='Buy':
        direction = OrderDirection.ORDER_DIRECTION_BUY
    if op_type=='Sell':
        direction = OrderDirection.ORDER_DIRECTION_SELL

    with Client(**_get_api_params_from_config()) as client:

        response = client.users.get_accounts()
        account, *_ = response.accounts
        account_id = account.id

        order_id = uuid.uuid4().hex

        post_order_response: PostOrderResponse = client.orders.post_order(
            figi=figi,
            quantity=lots,
            direction=direction,
            account_id=account_id,
            order_type=OrderType.ORDER_TYPE_LIMIT,
            order_id=order_id,
            price=price
            # instrument_id=INSTRUMENT_ID,
        )

        status = post_order_response.execution_report_status

def get_current_balance(currency_type: str) -> float:
    with Client(**_get_api_params_from_config()) as client:

        response = client.users.get_accounts()
        account, *_ = response.accounts
        account_id = account.id

        for money_value in client.operations.get_positions(account_id=account_id).money:
            currency = money_value.currency
            balance = quotation_to_decimal(money_value)
            if (currency==currency_type):
                return float(balance)

    return 0.0

