import bigquery.service as bs
import binance_parser.service as bps
import datetime


def cancel_order(pair, orderId):
    return bps.cancel_order(pair, orderId)


def get_all_orders(pair):
    return bps.get_all_orders(pair)


def get_active_orders(pair):
    return bps.get_active_limit_orders(pair)


def buy(mult, pair):
    return bps.buy(mult, pair)


def sell(mult, pair):
    return bps.sell(mult, pair)


def limit_buy(mult, pair, price):
    return bps.limit_buy(mult, pair, price)


def limit_sell(mult, pair, price):
    return bps.limit_sell(mult, pair, price)


def write_custom_trade_data(datefrom, dateto):
    data = bps.get_custom_trades(datefrom, dateto, 'ETHUSDT')
    if len(data) > 0:
        bs.write_log_data("tech", "api_requests_log", "old_agg_trades", str(data[:10]))
        keylist = ['a', 'p', 'q', 'f', 'l', 'T', 'm', 'M']
        errs = bs.write_raw_dict_data("raw", "old_agg_trades", "agg_trades", data, keylist)
        return str(len(data)), str(errs)
    else:
        return "empty"


def write_short_trade_data():
    data = bps.get_short_trades('ETHUSDT', 15)
    if len(data) > 0:
        bs.write_log_data("tech", "api_requests_log", "short_agg_trades", str(data[:10]))
        keylist = ['a', 'p', 'q', 'f', 'l', 'T', 'm', 'M']
        errs = bs.write_raw_dict_data("raw", "short_agg_trades", "agg_trades", data, keylist)
        return str(len(data)), str(errs)
    else:
        return "empty"


def write_custom_kline_data(datefrom, dateto):
    data = bps.get_custom_klines(datefrom, dateto, 'ETHUSDT')
    if len(data) > 0:
        bs.write_log_data("tech", "api_requests_log", "old_klines", str(data[:10]))
        errs = bs.write_raw_data("raw", "old_klines", "klines", data)
        return str(len(data)), str(errs)
    else:
        return "empty"


def write_short_kline_data(pair):
    data = bps.get_short_klines(pair, 1)
    if not data:
        return False
    print(data)
    data.append(pair)
    if len(data) > 0:
        bs.write_log_data("tech", "api_requests_log", "short_klines", str(pair + " - " + str(data[:10])))
        errs = bs.write_raw_data("raw", "short_klines", "klines", [data])
        if len(errs) == 0:
            return True
        return False
    return False


def get_short_kline_data(pair):
    data = bps.get_short_klines(pair, 1)
    return data


def write_average_price():
    data = bps.get_avg_price('ETHUSDT')
    if len(data) > 0:
        bs.write_log_data("tech", "api_requests_log", "avg_price", str(data))
        keylist = ['mins', 'price', 'Time']
        data['Time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        print(data)
        errs = bs.write_raw_dict_data("raw", "avg_price", "average_price", [data], keylist)
        return str(len(data)), str(errs)
    else:
        return "empty"


def show_info(pair):
    info = bps.show_info(pair)
    return info


def get_avg_price(pair):
    return bps.get_avg_price(pair)
