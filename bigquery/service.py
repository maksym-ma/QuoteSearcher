import datetime
import json
import ast
import re
import logging
import numpy as np
from datetime import date
import pytz

from google.cloud import bigquery
from binance_parser import service as bps


def get_peak_signal(prices_list):
    input_pr = np.array(prices_list)

    signal_grow = (input_pr > np.roll(input_pr, 1)) & \
                  (input_pr > sum(
                      [np.roll(input_pr, 1), np.roll(input_pr, 2), np.roll(input_pr, 3), np.roll(input_pr, 4),
                       np.roll(input_pr, 5)]) / 5)

    signal_fall = (input_pr > np.roll(input_pr, -1)) & \
                  (input_pr > sum(
                      [np.roll(input_pr, -1), np.roll(input_pr, -2), np.roll(input_pr, -3), np.roll(input_pr, -4),
                       np.roll(input_pr, -5)]) / 5)

    gr_fa_list = []
    for x in range(len(input_pr)):
        if signal_fall[x]:
            gr_fa_list.append(-1)
        elif signal_grow[x]:
            gr_fa_list.append(+1)
        else:
            gr_fa_list.append(0)

    return gr_fa_list


def get_eval_prediction(pair):
    client = bigquery.Client()

    f = open("bigquery/queries/eval_prediction.sql").read()
    query_job = client.query(f.format(pair=pair, l_pair=pair.lower()))

    results = query_job.result()  # Waits for job to complete.

    eth = 0.5
    usdt = 1500
    thr = 0.85
    price = 0
    tax = 0

    for row in reversed(list(results)):
        price = row['price']

        print(row['Time'], price)
        coeff_buy, coeff_sell, custom, prob, soft_average_price = run_prediction(row, price)

        if prob + coeff_buy >= thr:
            # buy
            if usdt <= 0:
                continue
            buy_value = 450
            if buy_value > usdt:
                buy_value = usdt

            eth += buy_value / price
            usdt -= buy_value
            tax += buy_value * 0.01 * 0.1

        elif prob - coeff_sell <= 1 - thr:
            # sell
            if eth <= 0:
                continue
            sell_value = 0.15
            if sell_value > eth:
                sell_value = eth

            eth -= sell_value
            usdt += sell_value * price
            tax += sell_value * price * 0.01 * 0.1

        else:
            continue

    return {"ETH": eth,
            "USDT": usdt,
            "USDT_BALANCE": eth*price + usdt,
            "ETH_BALANCE": eth + usdt/price,
            "USDT_IDLE": 0.5*price + 1500,
            "ETH_IDLE": 0.5 + 1500/price,
            "TAX": tax
            }


def get_quick_prediction(thr, price, pair):
    client = bigquery.Client()

    f = open("bigquery/queries/quick_prediction.sql").read()
    query_job = client.query(f.format(pair=pair, l_pair=pair.lower()))

    results = query_job.result()  # Waits for job to complete.

    price = float(price)

    eth_balance = bps.show_balance("ETH")
    usdt_balance = bps.show_balance("USDT")

    total_balance = (eth_balance * price) + usdt_balance

    for row in results:
        coeff_buy, coeff_sell, custom, prob, soft_average_price = run_prediction(row, price)

        if prob + coeff_buy >= thr:
            # if usdt_balance / total_balance <= 0.1 and price > row['avg_sell_l8'] * 0.97:
            #    return -3, prob, row['avg_buy_l8'], row['avg_sell_l8'], row[
            #        'avg_price_l30'], coeff_buy, coeff_sell, total_balance
            return 1, prob, row['avg_buy_l8'], row['avg_sell_l3'], row['avg_price_l12'], coeff_buy, coeff_sell, \
                   row['avg_price_l7'], row['avg_price_l12'], row['avg_price_l25'], soft_average_price, \
                   total_balance, eth_balance, usdt_balance, custom
        elif prob - coeff_sell <= 1 - thr:
            # if eth_balance / total_balance <= 0.1 and price < row['avg_buy_l8'] * 1.03:
            #    return -2, prob, row['avg_buy_l8'], row['avg_sell_l8'], row[
            #        'avg_price_l30'], coeff_buy, coeff_sell, total_balance
            return -1, prob, row['avg_buy_l8'], row['avg_sell_l3'], row['avg_price_l12'], coeff_buy, coeff_sell, \
                   row['avg_price_l7'], row['avg_price_l12'], row['avg_price_l25'], soft_average_price, \
                   total_balance, eth_balance, usdt_balance, custom
        else:
            return 0, prob, row['avg_buy_l8'], row['avg_sell_l3'], row['avg_price_l12'], coeff_buy, coeff_sell, \
                   row['avg_price_l7'], row['avg_price_l12'], row['avg_price_l25'], soft_average_price, \
                   total_balance, eth_balance, usdt_balance, custom


def run_prediction(row, price):
    # prob = 0.5
    coeff_buy = 0.0
    coeff_sell = 0.0

    prob = row['prob']
    spikes = get_peak_signal(row['prices_list'])
    prev = object()
    shortspikes = []
    for v in spikes:
        if prev != v:
            shortspikes.append(v)
        prev = v
    custom = 0
    drops = price - 3 < (row['prices_list'][1] + row['prices_list'][2] + row['prices_list'][3]) / 3 and price - 1 < \
            row['prices_list'][1] and min(row['prices_list'][:25]) + 25 < max(row['prices_list'][:25])
    grows = price + 3 > (row['prices_list'][1] + row['prices_list'][2] + row['prices_list'][3]) / 3 and price + 1 > \
            row['prices_list'][1] and min(row['prices_list'][:25]) + 25 < max(row['prices_list'][:25])

    if grows:
        custom += 10
    if drops:
        custom += 1

    if price < row['avg_price_l7'] < row['avg_price_l50'] - 10 and grows:
        coeff_buy += 0.05
        coeff_sell -= 0.05
    if price > row['avg_price_l7'] > row['avg_price_l50'] + 10 and drops:
        coeff_sell += 0.05
        coeff_buy -= 0.05

    if price < row['avg_price_l7'] < row['avg_price_l25'] < row['avg_price_l50'] and grows:
        coeff_buy += 0.2
        coeff_sell -= 0.1
    if price > row['avg_price_l7'] > row['avg_price_l25'] > row['avg_price_l50'] and drops:
        coeff_sell += 0.2
        coeff_buy -= 0.1

    if row['avg_price_l7'] > row['avg_price_l50']:
        coeff_buy -= 0.1
    if row['avg_price_l7'] < row['avg_price_l50']:
        coeff_sell -= 0.1

    if row['min_price_l60'] * 1.01 > price and grows:
        coeff_buy += 0.05
    if row['min_price_l120'] * 1.01 > price and grows:
        coeff_buy += 0.05
    if row['min_price_l240'] * 1.01 > price and grows:
        coeff_buy += 0.05
    if row['min_price_l720'] * 1.01 > price and grows:
        coeff_buy += 0.05
    if row['min_price_l1440'] * 1.01 > price and grows:
        coeff_buy += 0.05

    if sum(spikes[-25:]) < -14 and grows:
        coeff_buy += 0.05
        coeff_sell -= 0.05
    if sum(spikes[-25:]) > 14 and drops:
        coeff_buy -= 0.05
        coeff_sell += 0.05

    if sum(spikes[-12:]) < -5 and grows:
        coeff_buy += 0.05
        coeff_sell -= 0.05
    if sum(spikes[-12:]) > 5 and drops:
        coeff_buy -= 0.05
        coeff_sell += 0.05



    if row['max_price_l60'] / 1.01 < price and drops:
        coeff_sell += 0.05
    if row['max_price_l120'] / 1.01 < price and drops:
        coeff_sell += 0.05
    if row['max_price_l240'] / 1.01 < price and drops:
        coeff_sell += 0.05
    if row['max_price_l720'] / 1.01 < price and drops:
        coeff_sell += 0.05
    if row['max_price_l1440'] / 1.01 < price and drops:
        coeff_sell += 0.05

    if price < min(row['prices_list'][:30]) * 1.003 and grows:
        coeff_buy += 0.1
    if price < min(row['prices_list'][:60]) * 1.003 and grows:
        coeff_buy += 0.1
    if price > max(row['prices_list'][:30]) * 0.997 and drops:
        coeff_sell += 0.1

    if price > row['avg_buy_l8'] * 1.005 and drops:
        coeff_sell += 0.1
    if price < row['avg_buy_l8'] * 0.995 and grows:
        coeff_sell -= 0.1

    if price > row['avg_buy_l8'] * 1.01 and drops:
        coeff_sell += 0.1
    if price < row['avg_buy_l8'] * 0.99 and drops:
        coeff_sell -= 0.1

    if price < row['avg_buy_l8'] * 0.982 and grows:
        coeff_sell -= 0.1
    if price > row['avg_buy_l8'] * 1.018 and drops:
        coeff_sell += 0.1

    if price > max(row['prices_list'][:30]) * 0.992 and drops:
        coeff_sell += 0.05
    if price < min(row['prices_list'][:30]) * 1.008 and grows:
        coeff_buy += 0.05

    if row['prices_list'][0] < row['prices_list'][1] < row['prices_list'][2] < row['prices_list'][3] \
            and row['prices_list'][3] - row['prices_list'][0] > 7 and drops:
        coeff_sell += 0.05
    if row['prices_list'][0] > row['prices_list'][1] > row['prices_list'][2] > row['prices_list'][3] \
            and row['prices_list'][0] - row['prices_list'][3] > 7 and grows:
        coeff_buy += 0.05

    if row['avg_price_l4'] <= row['avg_price_l7'] <= row['avg_price_l12'] <= row['avg_price_l25'] and grows:
        coeff_buy += 0.05
    if row['avg_price_l4'] >= row['avg_price_l7'] >= row['avg_price_l12'] >= row['avg_price_l25'] and drops:
        coeff_sell += 0.05

    if prob > 0.7:
        prob = 0.7
    if prob < 0.3:
        prob = 0.3

    sorted_prices = row['prices_list'][:40]
    sorted_prices.sort()
    soft_average_price = sum(sorted_prices[10:-10]) / 20
    """
        if price < row['prices_list'][60] * 1.025:
            coeff_buy -= 0.15

        if price > row['prices_list'][60] * 0.975:
            coeff_sell -= 0.15
        """
    if min(row['prices_list']) == min(row['prices_list'][50:]) and max(row['prices_list']) == max(row['prices_list'][:10]):
        coeff_buy -= 0.15
    if max(row['prices_list']) == max(row['prices_list'][50:]) and min(row['prices_list']) == min(row['prices_list'][:10]):
        coeff_sell -= 0.15

    if price > soft_average_price - 10:
        coeff_buy = 0.001
    else:
        coeff_buy += 0.05
    if price < soft_average_price + 10:
        coeff_sell = 0.001
    else:
        coeff_sell += 0.05

    if price < row['prices_list'][1] < row['prices_list'][2] \
            and row['prices_list'][2] - price > 12:
        coeff_sell += 0.3
    if price > row['prices_list'][1] > row['prices_list'][2] \
            and row['prices_list'][2] - price < -12:
        coeff_buy += 0.3

    avgdif = 0
    avg30 = sum(row['prices_list'][1:31])/30
    for lprice in row['prices_list'][1:31]:
        avgdif += abs(lprice - avg30)

    if abs(price - avg30) > avgdif + 21 and price > avg30:
        coeff_buy += 0.5

    if abs(price - avg30) > avgdif + 21 and price < avg30:
        coeff_sell += 0.5

    if sum(spikes[-12:]) < -9 and row['prices_list'][10] - price < -15:
        coeff_sell += 0.15
        coeff_buy -= 0.15
    if sum(spikes[-12:]) > 9 and row['prices_list'][10] - price > 15:
        coeff_buy += 0.15
        coeff_sell -= 0.15

    if sum(spikes[-8:]) < -6 and row['prices_list'][7] - price < -12:
        coeff_sell += 0.15
        coeff_buy -= 0.15
    if sum(spikes[-8:]) > 6 and row['prices_list'][7] - price > 12:
        coeff_buy += 0.15
        coeff_sell -= 0.15

    if sum(spikes[-3:]) == -3 and row['prices_list'][2] - price < -10:
        coeff_sell += 0.3
        coeff_buy -= 0.15
    if sum(spikes[-3:]) == 3 and row['prices_list'][2] - price > 10:
        coeff_buy += 0.3
        coeff_sell -= 0.15

    if coeff_buy > 0.6:
        coeff_buy = 0.6
    if coeff_sell > 0.6:
        coeff_sell = 0.6
    if coeff_buy < -0.6:
        coeff_buy = -0.6
    if coeff_sell < -0.6:
        coeff_sell = -0.6

    return coeff_buy, coeff_sell, custom, prob, soft_average_price


def get_1h_prediction(pair):
    client = bigquery.Client()

    f = open("bigquery/queries/1h_stats.sql").read()
    query_job = client.query(f.format(pair=pair, l_pair=pair.lower()))

    results = query_job.result()  # Waits for job to complete.
    coeff_buy = 0
    coeff_sell = 0

    price = float(bps.get_avg_price(pair))

    for row in results:
        if row['min_price_l1440'] * 1.005 > price:
            coeff_buy += 1

        if row['max_price_l1440'] / 1.005 < price:
            coeff_sell += 1

    return coeff_buy, coeff_sell


def write_prediction(dataset, table, data):
    client = bigquery.Client()
    rows = []

    try:
        dt = datetime.datetime.now(pytz.timezone('Europe/Kiev'))
        data['Time'] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        rows.append(data)
    except:
        print("Error filling the array to write to BQ")
    errors = client.insert_rows_json(
        client.get_table(dataset + "." + table),
        rows
    )
    return errors


def write_log_data(dataset, table, request, data):
    client = bigquery.Client()
    rows = []

    js_row = {"Time": "", "API_name": "binance", "request": request, "response": data}
    try:
        dt = datetime.datetime.now(pytz.timezone('Europe/Kiev'))
        js_row['Time'] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        rows.append(js_row)
    except:
        print("Error filling the array to write to BQ")
    errors = client.insert_rows_json(
        client.get_table(dataset + "." + table),
        rows
    )
    return errors


def write_raw_dict_data(dataset, table, request, data, keylist):
    client = bigquery.Client()
    rows = [[row[key] for key in keylist] for row in data]
    errors = client.insert_rows(
        client.get_table(dataset + "." + table),
        rows
    )
    return errors


def write_raw_data(dataset, table, request, data):
    client = bigquery.Client()
    errors = client.insert_rows(
        client.get_table(dataset + "." + table),
        data
    )
    return errors


def thresholding_algo(y, lag, threshold, influence):
    signals = np.zeros(len(y))
    filteredY = np.array(y)
    avgFilter = [0] * len(y)
    stdFilter = [0] * len(y)
    avgFilter[lag - 1] = np.mean(y[0:lag])
    stdFilter[lag - 1] = np.std(y[0:lag])
    for i in range(lag, len(y)):
        if abs(y[i] - avgFilter[i - 1]) > threshold * stdFilter[i - 1]:
            if y[i] > avgFilter[i - 1]:
                signals[i] = 1
            else:
                signals[i] = -1

            filteredY[i] = influence * y[i] + (1 - influence) * filteredY[i - 1]
            avgFilter[i] = np.mean(filteredY[(i - lag + 1):i + 1])
            stdFilter[i] = np.std(filteredY[(i - lag + 1):i + 1])
        else:
            signals[i] = 0
            filteredY[i] = y[i]
            avgFilter[i] = np.mean(filteredY[(i - lag + 1):i + 1])
            stdFilter[i] = np.std(filteredY[(i - lag + 1):i + 1])

    return dict(signals=np.asarray(signals),
                avgFilter=np.asarray(avgFilter),
                stdFilter=np.asarray(stdFilter))
