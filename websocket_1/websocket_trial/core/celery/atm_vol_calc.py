import datetime

from celery import shared_task
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models.atm_vol_table import vol_table
import pandas as pd
strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")

# long_short_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
import py_vollib.black_scholes.implied_volatility
import py_vollib.black_scholes.greeks.numerical

daily_morn_df = pd.read_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")
dte = daily_morn_df['dte_monthly'].item()
@shared_task()
def atm_vol_calc(symbol):

    temp = dict()
    inserts = []
    # print(dte)
    k = live_before_df.objects.filter(symbol=symbol, instrument_type='FUT').last()
    # print(symbol)

    fut_close = k.close


    fut_close_time = k.time
    strike_diff = strike_diff_df[strike_diff_df['symbol'] == symbol]['strike_diff'].item()
    atm_strike = round(fut_close / strike_diff) * strike_diff
    # if symbol == 'TATASTEEL':
    #     atm_strike = atm_strike + 0.4
    latest_concerned = live_before_df.objects.filter(symbol=symbol)
    data = list(latest_concerned.values())
    data_df = pd.DataFrame(data)
    data_df = data_df.sort_values(by='time', ascending=True)

    atm_c = data_df[(data_df['strike_price'] == atm_strike) & (data_df['instrument_type'] == 'CE')]
    atm_p = data_df[(data_df['strike_price'] == atm_strike) & (data_df['instrument_type'] == 'PE')]
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
        atm_vol = (atm_c_iv + atm_p_iv)/2 * 100

    # if symbol == 'ABFRL':
    #     print(symbol)
    #     print(atm_strike)
    #     print(atm_c_iv)
    #     print(atm_p_iv)
    #     print(atm_c)
    #     print(atm_p)

    # atm_vol = (atm_c_iv + atm_p_iv)/2 * 100
    objects_to_update = vol_table.objects.filter(symbol=symbol)
    # if symbol == 'SHREECEM':
    #     print(fut_close)
    #     print(atm_vol)
    # if atm_c_iv is None:
    #     atm_c_iv = 0
    # if atm_p_iv is None:
    #     atm_p_iv = 0
    ##############when to add new symbol
    if not objects_to_update.exists():
        temp = dict()
        temp['time'] = datetime.datetime.now()
        temp['symbol'] = symbol
        temp['current_iv'] = atm_vol
        temp['fut_close'] = fut_close
        temp['current_atm'] = atm_strike
        temp['current_call_iv'] = atm_c_iv * 100
        temp['current_put_iv'] = atm_p_iv * 100
        inserts = []

        inserts.append(vol_table(**temp))
        vol_table.objects.bulk_create(inserts, batch_size=500)
    else:
        for obj in objects_to_update:
            obj.time = datetime.datetime.now()
            obj.symbol = symbol
            obj.current_iv = atm_vol
            obj.fut_close = fut_close
            obj.current_atm = atm_strike
            obj.current_call_iv = atm_c_iv * 100
            obj.current_put_iv = atm_p_iv * 100
            obj.save()