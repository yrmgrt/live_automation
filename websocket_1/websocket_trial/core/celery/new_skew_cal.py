import time

from celery import shared_task, app
from websocket_trial.models.temp_table_live import live_before_df
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
from websocket_trial.models.new_skew_tb import new_skew_table
from sklearn.metrics import r2_score
strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols_2.csv")

daily_morn_df = pd.read_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")
dte = daily_morn_df['dte_monthly'].item()
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
previous_time = datetime.datetime(2023, 6, 13, 9, 15)
@shared_task
def calc_ab(symbol):
    global previous_time
    # dte = 32
    # print('calc b start')
    # print(symbol)
    # print(dte)

    # while True:
    k = live_before_df.objects.filter(symbol=symbol, instrument_type='FUT').last()
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
    latest_concerned = live_before_df.objects.filter(Q(symbol=symbol) & Q(strike_price__gt=lower_threshold) &
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

    put_df['iv'] = put_df.apply(lambda x: calc_vol(x['close'], fut_close, x['strike_price'],
                                                     dte, x['instrument_type']), axis=1)
    call_df['iv'] = call_df.apply(lambda x: calc_vol(x['close'], fut_close, x['strike_price'],
                                                     dte, x['instrument_type']), axis=1)
    put_df['iv'] = put_df['iv'] * 100
    call_df['iv'] = call_df['iv'] * 100

    # print(call_df)
    #
    put_df = put_df[(put_df['strike_price'] % strike_diff == 0)]
    put_df = put_df[put_df['iv'] != 0]

    call_df = call_df[(call_df['strike_price'] % strike_diff == 0)]
    call_df = call_df[call_df['iv'] != 0]

    put_df['delta'] = put_df.apply(lambda x: calc_delta(fut_close, x['strike_price'],
                                                     dte, x['instrument_type'], x['iv']), axis=1)

    call_df['delta'] = call_df.apply(lambda x: calc_delta(fut_close, x['strike_price'],
                                                        dte, x['instrument_type'], x['iv']), axis=1)

    atm_c = call_df[call_df['strike_price'] == atm_strike]
    atm_p = put_df[put_df['strike_price'] == atm_strike]

    if not atm_c.empty:
        atm_c_close = atm_c.iloc[-1].close.item()
        atm_c_iv = py_vollib.black_scholes.implied_volatility.implied_volatility(
            atm_c_close,
            fut_close,
            atm_strike,
            dte / 365, 0,
            'c')

    if not atm_p.empty:
        atm_p_close = atm_p.iloc[-1].close.item()
        atm_p_iv = py_vollib.black_scholes.implied_volatility.implied_volatility(
            atm_p_close,
            fut_close,
            atm_strike,
            dte / 365, 0,
            'p')

    if atm_p.empty and atm_c.empty:
        atm_c_iv = 0
        atm_p_iv = 0
        atm_vol = 0
    if atm_c.empty and not atm_p.empty:
        atm_vol = atm_p_iv * 100
        atm_c_iv = 0
    elif atm_p.empty and not atm_c.empty:
        atm_vol = atm_c_iv * 100
        atm_p_iv = 0
    else:
        atm_vol = (atm_c_iv + atm_p_iv) / 2 * 100

    try:
        put_strike = put_df[put_df['delta'] >= -0.3]
        put_strike = put_strike[put_strike['delta'] <= -0.2]
        put_strike['atm_diff'] = (-1)*(put_strike['strike_price'] - atm_strike)
        # print(put_strike)

        call_strike = call_df[call_df['delta'] <= 0.5]
        call_strike = call_strike[call_strike['delta'] >= 0.1]
        call_strike['atm_diff'] = call_strike['strike_price'] - atm_strike
        # print('---------------------------------')
        # print(call_strike)
        equi_strikes = np.intersect1d(put_strike['atm_diff'], call_strike['atm_diff'])
        # print(equi_strikes)
        put_strike = put_strike[(put_strike['atm_diff'] == equi_strikes[0])]
        call_strike = call_strike[(call_strike['atm_diff'] == equi_strikes[0])]

        skew_atm_diff = put_strike.atm_diff.item()

        b_value = (put_strike.iv.item() - call_strike.iv.item()) / (2*equi_strikes[0])

        put_skew_strike = put_strike.strike_price.item()
        put_skew_iv = put_strike.iv.item()

        call_skew_strike = call_strike.strike_price.item()
        call_skew_iv = call_strike.iv.item()

    except:
        b_value = -99999
        put_skew_strike = -999
        put_skew_iv = -9

        call_skew_strike = -999
        call_skew_iv = -9

        skew_atm_diff = -99

    try:
        put_strike = put_df[put_df['delta'] >= -0.2]
        put_strike = put_strike[put_strike['delta'] <= -0.1]
        put_strike['atm_diff'] = (-1) * (put_strike['strike_price'] - atm_strike)

        call_strike = call_df[call_df['delta'] <= 0.5]
        call_strike = call_strike[call_strike['delta'] >= 0]
        call_strike['atm_diff'] = call_strike['strike_price'] - atm_strike

        equi_strikes = np.intersect1d(put_strike['atm_diff'], call_strike['atm_diff'])

        put_strike = put_strike[(put_strike['atm_diff'] == equi_strikes[0])]
        call_strike = call_strike[(call_strike['atm_diff'] == equi_strikes[0])]

        kurt_atm_diff = put_strike.atm_diff.item()

        # put_atm = put_df[put_df['strike_price'] == atm_strike]

        atm_iv = atm_vol

        a_value = (put_strike.iv.item() + call_strike.iv.item() - 2*atm_iv) / (2 * (equi_strikes[0])**2)

        put_kurt_strike = put_strike.strike_price.item()
        put_kurt_iv = put_strike.iv.item()

        call_kurt_strike = call_strike.strike_price.item()
        call_kurt_iv = call_strike.iv.item()
    except:
        a_value = -99999
        put_kurt_strike = -999
        put_kurt_iv = -9

        call_kurt_strike = -999
        call_kurt_iv = -9
        atm_iv = atm_vol

        kurt_atm_diff = -99

    try:
        put_strike1 = put_df[put_df['delta'] >= -0.45]
        put_strike1 = put_strike1[put_strike1['delta'] <= -0.35]
        put_strike1 = put_strike1.iloc[(put_strike1['delta'] + 0.4).abs().argsort()[:1]]
        put_strike1['atm_diff'] = (-1) * (put_strike1['strike_price'] - atm_strike)
        # print(put_strike)
        put_strike2 = put_df[put_df['delta'] >= -0.25]
        put_strike2 = put_strike2[put_strike2['delta'] <= -0.15]
        put_strike2 = put_strike2.iloc[(put_strike2['delta'] + 0.2).abs().argsort()[:1]]
        put_strike2['atm_diff'] = (-1) * (put_strike2['strike_price'] - atm_strike)


        put_strike1['iv1'] = put_strike1['iv'] - atm_vol
        put_strike2['iv1'] = put_strike2['iv'] - atm_vol

        # print(put_strike1)
        # print(put_strike2)

        put_skew_atm_diff1 = put_strike1.atm_diff.item()
        put_skew_atm_diff2 = put_strike2.atm_diff.item()

        put_b_value = (put_strike1['iv1'].item()*(put_skew_atm_diff2)**2 - put_strike2['iv1'].item()*(put_skew_atm_diff1)**2)/((put_skew_atm_diff2-put_skew_atm_diff1)*put_skew_atm_diff1*put_skew_atm_diff2)
        # print(put_b_value)
        put_skew_strike1 = put_strike1.strike_price.item()
        put_skew_strike2 = put_strike2.strike_price.item()

        put_skew_iv1 = put_strike1.iv.item()
        put_skew_iv2 = put_strike2.iv.item()

        put_a_value = (put_strike1['iv1'].item()*put_skew_atm_diff2 - put_strike2['iv1'].item()*put_skew_atm_diff1)/((put_skew_atm_diff1-put_skew_atm_diff2)*put_skew_atm_diff1*put_skew_atm_diff2)

    except:
        put_b_value = -99999
        put_skew_strike1 = -999
        put_skew_strike2 = -999

        put_skew_iv1 = -9
        put_skew_iv2 = -9

        put_a_value = -99999

        put_skew_atm_diff1 = -99
        put_skew_atm_diff2 = -99

    try:
        call_strike1 = call_df[call_df['delta'] <= 0.45]
        call_strike1 = call_strike1[call_strike1['delta'] >= 0.35]
        call_strike1 = call_strike1.iloc[(call_strike1['delta'] - 0.4).abs().argsort()[:1]]
        call_strike1['atm_diff'] = (call_strike1['strike_price'] - atm_strike)
        # print(put_strike)
        call_strike2 = call_df[call_df['delta'] <= 0.25]
        call_strike2 = call_strike2[call_strike2['delta'] >= 0.15]
        call_strike2 = call_strike2.iloc[(call_strike2['delta'] - 0.2).abs().argsort()[:1]]
        call_strike2['atm_diff'] = (call_strike2['strike_price'] - atm_strike)


        call_strike1['iv1'] = call_strike1['iv'] - atm_vol
        call_strike2['iv1'] = call_strike2['iv'] - atm_vol

        # print(put_strike1)
        # print(put_strike2)

        call_skew_atm_diff1 = call_strike1.atm_diff.item()
        call_skew_atm_diff2 = call_strike2.atm_diff.item()

        call_b_value = (call_strike1['iv1'].item()*(call_skew_atm_diff2)**2 - call_strike2['iv1'].item()*(call_skew_atm_diff1)**2)/((call_skew_atm_diff1-call_skew_atm_diff2)*call_skew_atm_diff1*call_skew_atm_diff2)
        # print(put_b_value)
        call_skew_strike1 = call_strike1.strike_price.item()
        call_skew_strike2 = call_strike2.strike_price.item()

        call_skew_iv1 = call_strike1.iv.item()
        call_skew_iv2 = call_strike2.iv.item()

        call_a_value = (call_strike1['iv1'].item()*call_skew_atm_diff2 - call_strike2['iv1'].item()*call_skew_atm_diff1)/((call_skew_atm_diff1-call_skew_atm_diff2)*call_skew_atm_diff1*call_skew_atm_diff2)

    except:
        call_b_value = -99999
        call_skew_strike1 = -999
        call_skew_strike2 = -999

        call_skew_iv1 = -9
        call_skew_iv2 = -9

        call_a_value = -99999

        call_skew_atm_diff1 = -99
        call_skew_atm_diff2 = -99



    inserts = []

    objects_to_update = new_skew_table.objects.filter(symbol=symbol)

    # if (symbol == 'NIFTY') or (symbol == 'BANKNIFTY'):
    #     print(symbol, put_b_value * 10000, put_a_value * 100000000)

    ##############when to add new symbol
    if not objects_to_update.exists():
        temp = dict()
        #
        temp['time'] = datetime.datetime.now()
        temp['symbol'] = symbol
        temp['b_value'] = b_value*10000
        temp['a_value'] = a_value*100000000

        temp['put_b_value'] = put_b_value*10000
        temp['put_a_value'] = put_a_value*100000000

        temp['call_b_value'] = call_b_value * 10000
        temp['call_a_value'] = call_a_value * 100000000


        temp['put_skew_strike'] = put_skew_strike

        temp['put_skew_strike1'] = put_skew_strike1
        temp['put_skew_strike2'] = put_skew_strike2

        temp['call_skew_strike'] = call_skew_strike

        temp['call_skew_strike1'] = call_skew_strike1
        temp['call_skew_strike2'] = call_skew_strike2

        temp['put_skew_iv'] = put_skew_iv

        temp['put_skew_iv1'] = put_skew_iv1
        temp['put_skew_iv2'] = put_skew_iv2

        temp['call_skew_iv'] = call_skew_iv

        temp['call_skew_iv1'] = call_skew_iv1
        temp['call_skew_iv2'] = call_skew_iv2

        temp['put_kurt_strike'] = put_kurt_strike
        temp['call_kurt_strike'] = call_kurt_strike
        temp['put_kurt_iv'] = put_kurt_iv
        temp['call_kurt_iv'] = call_kurt_iv
        temp['atm_strike'] = atm_strike
        temp['atm_iv'] = atm_iv

        temp['p_c_skew'] = call_skew_iv - put_skew_iv
        temp['skew_atm_diff'] = skew_atm_diff

        temp['put_skew_atm_diff1'] = put_skew_atm_diff1
        temp['put_skew_atm_diff2'] = put_skew_atm_diff2

        temp['call_skew_atm_diff1'] = call_skew_atm_diff1
        temp['call_skew_atm_diff2'] = call_skew_atm_diff2

        temp['kurt_atm_diff'] = kurt_atm_diff


        inserts.append(new_skew_table(**temp))
        new_skew_table.objects.bulk_create(inserts, batch_size=500)


    for obj in objects_to_update:

        obj.time = datetime.datetime.now()
        obj.symbol = symbol
        obj.b_value = b_value*10000
        obj.a_value = a_value*100000000

        obj.put_b_value = put_b_value * 10000
        obj.put_a_value = put_a_value * 100000000

        obj.call_b_value = call_b_value * 10000
        obj.call_a_value = call_a_value * 100000000

        obj.put_skew_strike = put_skew_strike

        obj.put_skew_strike1 = put_skew_strike1
        obj.put_skew_strike2 = put_skew_strike2

        obj.call_skew_strike = call_skew_strike

        obj.call_skew_strike1 = call_skew_strike1
        obj.call_skew_strike2 = call_skew_strike2

        obj.put_skew_iv = put_skew_iv

        obj.put_skew_iv1 = put_skew_iv1
        obj.put_skew_iv2 = put_skew_iv2

        obj.call_skew_iv = call_skew_iv

        obj.call_skew_iv1 = call_skew_iv1
        obj.call_skew_iv2 = call_skew_iv2

        obj.put_kurt_strike = put_kurt_strike
        obj.call_kurt_strike = call_kurt_strike
        obj.put_kurt_iv = put_kurt_iv
        obj.call_kurt_iv = call_kurt_iv
        obj.atm_strike = atm_strike
        obj.atm_iv = atm_iv

        obj.p_c_skew = call_skew_iv - put_skew_iv

        obj.skew_atm_diff = skew_atm_diff

        obj.put_skew_atm_diff1 = put_skew_atm_diff1
        obj.put_skew_atm_diff2 = put_skew_atm_diff2

        obj.call_skew_atm_diff1 = call_skew_atm_diff1
        obj.call_skew_atm_diff2 = call_skew_atm_diff2

        obj.kurt_atm_diff = kurt_atm_diff

        # Update other fields as needed
        obj.save()

