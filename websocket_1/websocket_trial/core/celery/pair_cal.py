import datetime

from celery import shared_task
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models.long_short_tb import long_short_tb
import pandas as pd
strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Desktop\strike_diff.csv")
corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\correlation.csv")
import py_vollib.black_scholes.implied_volatility
import py_vollib.black_scholes.greeks.numerical
def calc_vol(symbol):
    dte = 32
    k = live_before_df.objects.filter(symbol=symbol, instrument_type='FUT').last()
    fut_close = k.close
    fut_close_time = k.time
    strike_diff = strike_diff_df[strike_diff_df['symbol'] == symbol]['strike_diff'].item()
    atm_strike = round(fut_close / strike_diff) * strike_diff
    latest_concerned = live_before_df.objects.filter(symbol=symbol)
    data = list(latest_concerned.values())
    data_df = pd.DataFrame(data)
    data_df = data_df.sort_values(by='time', ascending=True)
    atm_c = data_df[(data_df['strike_price'] == atm_strike) & (data_df['instrument_type'] == 'CE')].iloc[-1].close.item()
    atm_p = data_df[(data_df['strike_price'] == atm_strike) & (data_df['instrument_type'] == 'PE')].iloc[-1].close.item()
    atm_c_iv = py_vollib.black_scholes.implied_volatility.implied_volatility(
        atm_c,
        fut_close,
        atm_strike,
        dte / 365, 0,
        'c')
    atm_p_iv = py_vollib.black_scholes.implied_volatility.implied_volatility(
        atm_p,
        fut_close,
        atm_strike,
        dte / 365, 0,
        'p')
    atm_vol = (atm_c_iv + atm_p_iv) / 2 * 100
    return atm_vol


@shared_task()
def pair_cal(symbol_list):
    symbol_1 = symbol_list[0]
    symbol_2 = symbol_list[1]
    temp = dict()
    inserts = []
    iv_1 = calc_vol(symbol_1)
    iv_2 = calc_vol(symbol_2)
    diff = iv_2 - iv_1
    conc_df = corr_df[(corr_df['stock_1'] == symbol_1) & (corr_df['stock_2'] == symbol_2)]
    avg_dif = conc_df.avg_diff.item()
    if abs(avg_dif-diff) > 2:
        temp['time'] = datetime.datetime.now()
        temp['symbol_1'] = symbol_1
        temp['iv_1'] = iv_1
        temp['avg_iv_1'] = conc_df.stock_1_avg_iv.item()
        temp['avg_iv_2'] = conc_df.stock_2_avg_iv.item()
        temp['symbol_2'] = symbol_2
        temp['iv_2'] = iv_2




