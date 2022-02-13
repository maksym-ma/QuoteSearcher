import sys
from datetime import datetime
from binance.client import Client
import os

if not os.environ.get("api_key"):
    import config

key = os.environ.get("api_key") if os.environ.get("api_key") else config.api_key
secret = os.environ.get("api_secret") if os.environ.get("api_secret") else config.api_secret

pairs_list = {
                "ETHUSDT": 3,
                "BTCUSDT": 6,
                "ETHBTC": 3,
                "BTCBUSD": 6,
            }


def get_avg_price(pair):
    client = Client()
    price = client.get_avg_price(symbol=pair)['price']

    return price


def get_all_orders(pair):
    client = Client(key, secret)
    try:
        buy_order_info = client.get_all_orders(symbol=pair)
    except:
        print(str(sys.exc_info()))
        return str(sys.exc_info())

    return buy_order_info


def get_active_limit_orders(pair):
    client = Client(key, secret)
    try:
        buy_order_info = client.get_open_orders(symbol=pair)
    except:
        print(str(sys.exc_info()))
        return str(sys.exc_info())

    return buy_order_info


def cancel_order(pair, orderId):
    client = Client(key, secret)
    try:
        buy_order_info = client.cancel_order(symbol=pair, orderId=orderId)
    except:
        print(str(sys.exc_info()))
        return str(sys.exc_info())

    return "success"


def buy(mult, pair):
    client = Client(key, secret)
    try:
        buy_order_info = client.order_market_buy(symbol=pair, quantity=round(0.01*mult, pairs_list[pair]))
    except:
        print(str(sys.exc_info()))
        return str(sys.exc_info())

    return "success"


def limit_buy(mult, pair, price):
    client = Client(key, secret)
    try:
        sell_order_info = client.order_limit_buy(
            symbol=pair,
            quantity=round(0.01*mult, pairs_list[pair]),
            price=round(price, 2)
        )
    except:
        print(str(sys.exc_info()))
        return str(sys.exc_info())

    return "success"


def sell(mult, pair):
    client = Client(key, secret)
    try:
        sell_order_info = client.order_market_sell(symbol=pair, quantity=round(0.01*mult, pairs_list[pair]))
    except:
        print(str(sys.exc_info()))
        return str(sys.exc_info())

    return "success"


def limit_sell(mult, pair, price):
    client = Client(key, secret)
    try:
        sell_order_info = client.order_limit_sell(
            symbol=pair,
            quantity=round(0.01*mult, pairs_list[pair]),
            price=round(price, 2)
        )
    except:
        print(str(sys.exc_info()))
        return str(sys.exc_info())

    return "success"


def get_custom_klines(from_time, to_time, pair):
    """
    :param pair: coin pair symbol
    :param from_time: '%d.%m.%Y %H:%M:%S'
    :param to_time: '%d.%m.%Y %H:%M:%S'
    :return: klines list
    """
    client = Client()

    start_baseline_time = datetime.strptime(from_time, '%d.%m.%Y %H:%M:%S')
    start_millisec = start_baseline_time.timestamp() * 1000

    end_baseline_time = datetime.strptime(to_time, '%d.%m.%Y %H:%M:%S')
    end_millisec = end_baseline_time.timestamp() * 1000

    end_time = end_millisec
    start_time = start_millisec

    candles = client.get_historical_klines(pair, Client.KLINE_INTERVAL_1MINUTE, str(start_time), str(end_time))

    return candles


def get_custom_trades(from_time, to_time, pair):
    """
    :param pair: coin pair symbol
    :param from_time: '%d.%m.%Y %H:%M:%S'
    :param to_time: '%d.%m.%Y %H:%M:%S'
    :return: klines list
    """
    client = Client()

    start_baseline_time = datetime.strptime(from_time, '%d.%m.%Y %H:%M:%S')
    start_millisec = start_baseline_time.timestamp() * 1000

    end_baseline_time = datetime.strptime(to_time, '%d.%m.%Y %H:%M:%S')
    end_millisec = end_baseline_time.timestamp() * 1000

    end_time = end_millisec
    start_time = start_millisec

    trades = client.get_aggregate_trades(symbol=pair, startTime=int(start_time), endTime=int(end_time))

    return trades


def get_short_klines(pair, minutes):
    client = Client()

    current_baseline_time = datetime.now()
    current_millisec = current_baseline_time.timestamp() * 1000

    end_time = current_millisec
    start_time = end_time - (minutes * 60 * 1000) + 1
    # print(datetime.fromtimestamp(start_time / 1000.0), datetime.fromtimestamp(end_time / 1000.0))
    candles = client.get_klines(symbol=pair, interval=Client.KLINE_INTERVAL_1MINUTE, startTime=int(start_time),
                                endTime=int(end_time))
    if len(candles) > 0:
        return candles[0]


def get_short_trades(pair, minutes):
    client = Client()

    current_baseline_time = datetime.now()
    current_millisec = current_baseline_time.timestamp() * 1000

    end_time = current_millisec
    start_time = end_time - (minutes * 60 * 1000) + 1

    trades = client.get_aggregate_trades(symbol=pair, startTime=int(start_time), endTime=int(end_time))

    return trades


def show_info(pair):
    client = Client()
    return client.get_symbol_info(pair)


def show_balance(asset):
    client = Client(key, secret)
    return float(client.get_asset_balance(asset)['free'])
