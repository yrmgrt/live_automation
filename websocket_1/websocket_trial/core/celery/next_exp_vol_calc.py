import time

from celery import shared_task, app
from websocket_trial.models.next_expiry_live import next_expiry_live
# from websocket_trial.models.sample_day_data import sample_data_gdfl
from django.db import OperationalError
from django.db.models import Q
import requests
import datetime
import logging
import py_vollib.black_scholes.implied_volatility
import py_vollib.black_scholes.greeks.numerical
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from websocket_trial.models.next_exp_vol_table import next_exp_vol_table
from sklearn.metrics import r2_score
strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")

daily_morn_df = pd.read_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")
dte = daily_morn_df['next_dte_monthly'].item()
def calc_vol(close, fut_close, strike, dte, opt_type):
    try:
        return py_vollib.black_scholes.implied_volatility.implied_volatility(
                close,
                fut_close,
                strike,
                dte/365, 0,
                opt_type)
    except:
        return 0


# dte = 18

def calc_delta(fut_close, strike, dte, opt_type, iv):
    iv = iv/100
    try:
        return py_vollib.black_scholes.greeks.numerical.delta(opt_type,fut_close,
                                                            strike,
                                                           dte/365, 0, iv)
    except:
        return 0

logger = logging.getLogger('manager')

@shared_task
def next_exp_vol_calc(symbol):
    # dte = 32
    # print('calc b start')
    # print(symbol)
    # print(dte)

    # while True:
    k = next_expiry_live.objects.filter(symbol=symbol, instrument_type='FUT').last()
    fut_close = k.close
    # print(fut_close)
    # fut_close_time = k.time
    # print(fut_close_time)
    strike_diff = strike_diff_df[strike_diff_df['symbol']==symbol]['strike_diff'].item()
    atm_strike = round(fut_close / strike_diff) * strike_diff
    # if symbol == 'TATASTEEL':
    #
    #     atm_strike = atm_strike + 0.4
    lower_threshold = fut_close - 0.12* fut_close
    higher_threshold = fut_close + 0.12* fut_close
    latest_concerned = next_expiry_live.objects.filter(Q(symbol=symbol) & Q(strike_price__gt=lower_threshold) &
                                    (Q(strike_price__lt=higher_threshold)))

    data = list(latest_concerned.values())
    data_df = pd.DataFrame(data)
    # print(symbol, fut_close)
    # print(data_df)
    data_df = data_df.sort_values(by='time', ascending=True)
    grouped = data_df.groupby(['strike_price','instrument_type'])
    last_df_list = []
    for strike, grouped_df in grouped:

        # print(type(grouped_df.iloc[-1]))
        last_df_list.append(grouped_df.iloc[-1])

    last_df = pd.DataFrame(last_df_list)
    # print(last_df)
    last_df['instrument_type'] = (last_df['instrument_type'].str.replace("E", "")).map(str.lower)
    # straddle_value = last_df[last_df['strike_price'] == atm_strike]['close'].sum()
    # rounded_straddle_value = math.ceil(straddle_value/strike_diff)*strike_diff
    #
    # down_round_strad = math.floor((atm_strike - 1.6 * straddle_value)/strike_diff)*strike_diff
    # up_round_strad = math.ceil((atm_strike + 1.6 * straddle_value) / strike_diff)*strike_diff
    # print(up_round_strad)

    # put_df = last_df[(last_df['strike_price'] >= (down_round_strad)) &
    #                  (last_df['strike_price'] <= atm_strike)]
    #
    # put_df = put_df[put_df['instrument_type'] == 'p']

    put_df = last_df[last_df['instrument_type'] == 'p']
    put_df = put_df[put_df['strike_price'] <= atm_strike]

    # call_df = last_df[(last_df['strike_price'] >= atm_strike) &
    #                  (last_df['strike_price'] <= (up_round_strad))]
    #
    # call_df = call_df[call_df['instrument_type'] == 'c']

    call_df = last_df[last_df['instrument_type'] == 'c']
    call_df = call_df[call_df['strike_price'] >= atm_strike]

    put_df['bid_iv'] = put_df.apply(lambda x: calc_vol(x['bid_price'], fut_close, x['strike_price'],
                                                     dte, x['instrument_type']), axis=1)
    put_df['ask_iv'] = put_df.apply(lambda x: calc_vol(x['ask_price'], fut_close, x['strike_price'],
                                                       dte, x['instrument_type']), axis=1)

    call_df['bid_iv'] = call_df.apply(lambda x: calc_vol(x['bid_price'], fut_close, x['strike_price'],
                                                     dte, x['instrument_type']), axis=1)
    call_df['ask_iv'] = call_df.apply(lambda x: calc_vol(x['ask_price'], fut_close, x['strike_price'],
                                                         dte, x['instrument_type']), axis=1)

    put_df['bid_iv'] = put_df['bid_iv'] * 100
    put_df['ask_iv'] = put_df['ask_iv'] * 100

    put_df['ask_iv'] = np.where(put_df['ask_price'] == 0, -1, put_df['ask_iv'])

    if symbol == 'BERGEPAINT':
        print(put_df[['strike_price', 'instrument_type', 'ask_iv']])

    call_df['bid_iv'] = call_df['bid_iv'] * 100
    call_df['ask_iv'] = call_df['ask_iv'] * 100

    call_df['ask_iv'] = np.where(call_df['ask_price'] == 0, -1, call_df['ask_iv'])

    # print(call_df)
    #
    put_df = put_df[(put_df['strike_price'] % strike_diff == 0)]
    # put_df = put_df[put_df['iv'] != 0]

    call_df = call_df[(call_df['strike_price'] % strike_diff == 0)]
    # call_df = call_df[call_df['iv'] != 0]

    # put_df['delta'] = put_df.apply(lambda x: calc_delta(fut_close, x['strike_price'],
    #                                                  dte, x['instrument_type'], x['iv']), axis=1)
    #
    # call_df['delta'] = call_df.apply(lambda x: calc_delta(fut_close, x['strike_price'],
    #                                                     dte, x['instrument_type'], x['iv']), axis=1)

    sym_df = pd.concat([put_df, call_df])



    # objects_to_update = next_exp_vol_table.objects.filter(symbol=symbol)
    next_exp_vol_table.objects.filter(symbol=symbol).delete()

    for i, row in sym_df.iterrows():
        inserts = []
        temp = dict()
        temp['time'] = datetime.datetime.now()
        temp['symbol'] = symbol
        temp['strike_price'] = row['strike_price']
        temp['instrument_type'] = row['instrument_type']

        temp['bid_iv'] = row['bid_iv']
        temp['ask_iv'] = row['ask_iv']

        temp['fut_close'] = fut_close

        inserts.append(next_exp_vol_table(**temp))
        next_exp_vol_table.objects.bulk_create(inserts, batch_size=500)



