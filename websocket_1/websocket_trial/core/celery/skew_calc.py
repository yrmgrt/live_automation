import time

from celery import shared_task, app
from websocket_trial.models.temp_table_live import live_before_df
# from websocket_trial.models.sample_day_data import sample_data_gdfl
from websocket_trial.models.skew_tb import skew_table
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
from websocket_trial.models.skew_tb import skew_table
from sklearn.metrics import r2_score
strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")


logger = logging.getLogger('manager')
previous_time = datetime.datetime(2023, 6, 13, 9, 15)




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
                                                           dte, 0, iv)
    except:
        return 0

@shared_task
def calc_ab(symbol):
    global previous_time
    dte = 32
    print('calc b start')
    print(symbol)
    print(dte)

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
    lower_threshold = fut_close - 0.2* fut_close
    higher_threshold = fut_close + 0.2* fut_close
    latest_concerned = live_before_df.objects.filter(Q(symbol=symbol) & Q(strike_price__gt=lower_threshold) &
                                    (Q(strike_price__lt=higher_threshold)))

    data = list(latest_concerned.values())
    data_df = pd.DataFrame(data)
    data_df = data_df.sort_values(by='time', ascending=True)
    grouped = data_df.groupby(['strike_price','instrument_type'])
    last_df_list = []
    for strike, grouped_df in grouped:

        # print(type(grouped_df.iloc[-1]))
        last_df_list.append(grouped_df.iloc[-1])

    last_df = pd.DataFrame(last_df_list)
    # print(last_df)
    last_df['instrument_type'] = (last_df['instrument_type'].str.replace("E", "")).map(str.lower)
    straddle_value = last_df[last_df['strike_price'] == atm_strike]['close'].sum()
    rounded_straddle_value = math.ceil(straddle_value/strike_diff)*strike_diff

    down_round_strad = math.floor((atm_strike - 1.6 * straddle_value)/strike_diff)*strike_diff
    up_round_strad = math.ceil((atm_strike + 1.6 * straddle_value) / strike_diff)*strike_diff
    # print(up_round_strad)

    put_df = last_df[(last_df['strike_price'] >= (down_round_strad)) &
                     (last_df['strike_price'] <= atm_strike)]

    put_df = put_df[put_df['instrument_type'] == 'p']

    call_df = last_df[(last_df['strike_price'] >= atm_strike) &
                     (last_df['strike_price'] <= (up_round_strad))]

    call_df = call_df[call_df['instrument_type'] == 'c']

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




    put_strikes_ratio = len(put_df)/(((atm_strike-down_round_strad)/strike_diff) + 1)
    call_strikes_ratio = len(call_df)/(((up_round_strad-atm_strike)/strike_diff) + 1)

    # print('call_df_len:', len(call_df))
    # print('value:', (((up_round_strad-atm_strike)/strike_diff) + 1))



    put_plot_df = pd.DataFrame()
    put_plot_df['strike'] = put_df['strike_price'] - atm_strike
    put_plot_df['skew'] = put_df['iv'] - put_df[put_df['strike_price'] == atm_strike]['iv'].item()
    put_plot_df = put_plot_df.sort_values(by=['strike'], ascending=True)
    # if not symbol == 'TATASTEEL':
    #     put_plot_df = put_plot_df[(put_plot_df['strike']%strike_diff == 0)]
    put_model_fit = np.poly1d(np.polyfit(put_plot_df['strike'],put_plot_df['skew'],2))

    call_plot_df = pd.DataFrame()
    call_plot_df['strike'] = call_df['strike_price'] - atm_strike
    call_plot_df['skew'] = call_df['iv'] - call_df[call_df['strike_price'] == atm_strike]['iv'].item()
    call_plot_df = call_plot_df.sort_values(by=['strike'], ascending=True)
    # if not symbol == 'TATASTEEL':
    #     call_plot_df = call_plot_df[(call_plot_df['strike'] % strike_diff == 0)]
    call_model_fit = np.poly1d(np.polyfit(call_plot_df['strike'], call_plot_df['skew'], 2))


    call_df = call_df[call_df['strike_price'] != atm_strike]

    put_call_df = pd.concat([put_df, call_df], ignore_index=True)
    put_call_df = put_call_df[put_call_df['iv'] != 0]

    put_call_strikes_ratio = len(put_call_df)/(((up_round_strad-down_round_strad)/strike_diff) + 1)
    # print('ratios: ', put_strikes_ratio, call_strikes_ratio, put_call_strikes_ratio)
    put_call_plot_df = pd.DataFrame()
    put_call_plot_df['strike'] = put_call_df['strike_price'] - atm_strike
    put_call_plot_df['skew'] = put_call_df['iv'] - put_call_df[put_call_df['strike_price'] == atm_strike]['iv'].item()
    put_call_plot_df = put_call_plot_df.sort_values(by=['strike'], ascending=True)
    if not symbol == 'TATASTEEL':
        put_call_plot_df = put_call_plot_df[(put_call_plot_df['strike'] % strike_diff == 0)]
    # print(put_call_plot_df)
    put_call_model_fit = np.poly1d(np.polyfit(put_call_plot_df['strike'], put_call_plot_df['skew'], 2))

    inserts = []

    # temp['put_b_value'] = put_model_fit[1] * (-10000)
    # temp['call_b_value'] = call_model_fit[1] * (-10000)
    # temp['put_call_b_value'] = put_call_model_fit[1] * (-10000)
    # temp['put_call_a_value'] = put_call_model_fit[2] * (100000000)
    # temp['put_straddle_skew'] = put_model_fit(rounded_straddle_value*(-1))
    # temp['call_straddle_skew'] = call_model_fit(rounded_straddle_value)
    # temp['put_call_straddle_skew'] = put_call_model_fit(rounded_straddle_value * (-1))-put_call_model_fit(rounded_straddle_value)
    # temp['put_call_straddle_kurt'] = put_call_model_fit(1.5*rounded_straddle_value * (-1))+put_call_model_fit(1.5*rounded_straddle_value)

    objects_to_update = skew_table.objects.filter(symbol=symbol)



    ##############when to add new symbol
    if not objects_to_update.exists():
        temp = dict()
        #
        temp['symbol'] = symbol
        temp['time'] = datetime.datetime.now()
        temp['put_b_value'] = put_model_fit[1] * (-10000)
        temp['call_b_value'] = call_model_fit[1] * (-10000)
        temp['put_call_b_value'] = put_call_model_fit[1] * (-10000)
        temp['put_call_a_value'] = put_call_model_fit[2] * (100000000)
        temp['put_straddle_skew'] = put_df[put_df['strike_price'] == (atm_strike)].iv.item() - \
            put_df[put_df['strike_price'] == (atm_strike-rounded_straddle_value)].iv.item()
        temp['call_straddle_skew'] = call_model_fit(rounded_straddle_value)
        temp['put_call_straddle_skew'] = put_call_model_fit(rounded_straddle_value * (-1))-put_call_model_fit(rounded_straddle_value)
        temp['put_call_straddle_kurt'] = put_call_model_fit(1.5*rounded_straddle_value * (-1))+put_call_model_fit(1.5*rounded_straddle_value)
        temp['R2_put_call'] = r2_score(put_call_plot_df['skew'], put_call_model_fit(put_call_plot_df['strike']))
        temp['R2_call'] = r2_score(call_plot_df['skew'], call_model_fit(call_plot_df['strike']))
        temp['strike_1_put_call'] = (atm_strike-rounded_straddle_value)
        temp['strike_2_put_call'] = (atm_strike+rounded_straddle_value)
        temp['strike_1_call'] = (atm_strike)
        temp['strike_2_call'] = (atm_strike+rounded_straddle_value)
        temp['put_call_strikes_ratio'] = put_call_strikes_ratio
        temp['call_strikes_ratio'] = call_strikes_ratio

        inserts.append(skew_table(**temp))
        skew_table.objects.bulk_create(inserts, batch_size=500)

    # if symbol == 'BAJAJ-AUTO':
    #     print(atm_strike)
    #     print(straddle_value)
    #     print(rounded_straddle_value)
    #     print(put_call_df[['time','symbol', 'strike_price', 'close', 'iv']])
    #     print(fut_close_time)


    for obj in objects_to_update:
        obj.time = datetime.datetime.now()
        obj.put_b_value = put_model_fit[1] * (-10000)
        obj.call_b_value = call_model_fit[1] * (-10000)
        obj.put_call_b_value = put_call_model_fit[1] * (-10000)
        obj.put_call_a_value = put_call_model_fit[2] * (100000000)
        obj.put_straddle_skew = put_df[put_df['strike_price'] == (atm_strike)].iv.item() - \
            put_df[put_df['strike_price'] == (atm_strike-rounded_straddle_value)].iv.item()
        try:
            obj.call_straddle_skew = call_df[call_df['strike_price'] == (atm_strike+strike_diff)].iv.item() - \
            call_df[call_df['strike_price'] == (atm_strike+rounded_straddle_value)].iv.item()
        except:
            obj.call_straddle_skew = -99999
        try:
            obj.put_call_straddle_skew = \
            put_call_df[put_call_df['strike_price'] == (atm_strike+rounded_straddle_value)].iv.item() - \
            put_call_df[put_call_df['strike_price'] == (atm_strike-rounded_straddle_value)].iv.item()
        except:
            obj.put_call_straddle_skew = -99999
        obj.put_call_straddle_kurt = put_call_model_fit(1.5*rounded_straddle_value * (-1))+put_call_model_fit(1.5*rounded_straddle_value)
        obj.R2_put_call = r2_score(put_call_plot_df['skew'], put_call_model_fit(put_call_plot_df['strike']))
        obj.R2_call = r2_score(call_plot_df['skew'], call_model_fit(call_plot_df['strike']))
        obj.strike_1_put_call = (atm_strike-rounded_straddle_value)
        obj.strike_2_put_call = (atm_strike+rounded_straddle_value)
        obj.strike_1_put = (atm_strike-rounded_straddle_value)
        obj.strike_2_put = (atm_strike)
        obj.strike_1_call = (atm_strike) + strike_diff
        obj.strike_2_call = (atm_strike+rounded_straddle_value)
        obj.put_call_strikes_ratio = put_call_strikes_ratio
        obj.call_strikes_ratio = call_strikes_ratio
        # Update other fields as needed
        obj.save()

    current_time = datetime.datetime.now()

    # if (current_time - previous_time) > datetime.timedelta(minutes=15):
    #     live_before_df.objects.filter(Q(time__lt=previous_time)).delete()
    #
    #     previous_time = current_time






    # if symbol == 'BRITANNIA':
    #     print('------------fut_close----------', fut_close)
    #     print('------------dte------------', dte)
    #
    #     # print('----------------------------------------------------------------------')
    #     # print(put_df[['symbol', 'strike_price', 'close', 'iv']])
    #     print('-------------------------------CALL DF')
    #     print(call_plot_df)
    #     print('----------------------------------------------------------------')
    # time.sleep(5)
    # print('b_value: ', model_fit[1])


    # ###########from here
    # polyline = np.linspace(0-1.6*straddle_value-20,0+20,30)
    # plt.scatter(put_plot_df['strike'],put_plot_df['skew'], label=symbol + ' ' + 'put')
    # plt.plot(polyline, put_model_fit(polyline))
    # plt.legend()
    # plt.show()
    #
    # if symbol == 'ITC':
    #     polyline = np.linspace(0 - 1.6 * straddle_value, 0 + 20, 30)
    #     plt.scatter(put_plot_df['strike'], put_plot_df['skew'], label=symbol + ' ' + 'call')
    #     plt.plot(polyline, put_model_fit(polyline))
    #     plt.legend()
    #     plt.show()
    #     polyline = np.linspace(0 - 20, 0 + 1.6 * straddle_value, 30)
    #     plt.scatter(call_plot_df['strike'], call_plot_df['skew'], label=symbol + ' ' + 'call')
    #     plt.plot(polyline, call_model_fit(polyline))
    #     plt.legend()
    #     plt.show()
    #     polyline = np.linspace(0 - 1.6 * straddle_value, 0 + 1.6 * straddle_value, 30)
    #     plt.scatter(put_call_plot_df['strike'], put_call_plot_df['skew'], label=symbol + ' ' + 'call')
    #     plt.plot(polyline, put_call_model_fit(polyline))
    #     plt.legend()
    #     plt.show()
    #
    # polyline = np.linspace(0-1.6*straddle_value-20, 0 + 1.6 * straddle_value + 20, 30)
    # plt.scatter(put_call_plot_df['strike'], put_call_plot_df['skew'], label=symbol + ' ' + 'put_call')
    # plt.plot(polyline, put_call_model_fit(polyline))
    # plt.legend()
    # plt.show()
    #
    # #####to here






    # print(data_df['strike_price'])

    # for row in latest_concerned:
    #
    #     test_tb_instance = test_tb(close=row.close, strike_price=row.strike_price)
    #     # Set values for other fields if needed
    #     test_tb_instance.save()






    # # fut_value = k.object.filter(instrument_type='FUT').last().close
    # #
    # # atm_value = 1000
    # # strad_value = k.object.filter(strike_price=atm_value, instrument_type='CE').last().close +\
    # #               k.object.filter(strike_price=atm_value, instrument_type='PE').last().close
    # #########
    # ########query strikes less than greater than 1.4
    # # strikes_df = m
    # # strikes_df_with_iv
    # # calculate a and b
    # except:
    #     print('Varun')






