import datetime

from celery import shared_task
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models.long_short_tb import long_short_tb
import pandas as pd
strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Desktop\strike_diff.csv")
long_short_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
import py_vollib.black_scholes.implied_volatility
import py_vollib.black_scholes.greeks.numerical

@shared_task()
def long_cal(symbol):
    long_short_tb.objects.all().delete()
    temp = dict()
    inserts = []
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
                dte/365, 0,
                'c')
    atm_p_iv = py_vollib.black_scholes.implied_volatility.implied_volatility(
                atm_p,
                fut_close,
                atm_strike,
                dte/365, 0,
                'p')
    atm_vol = (atm_c_iv + atm_p_iv)/2 * 100
    avg_vol = long_short_df[long_short_df['symbol'] == symbol]['average_iv'].item()
    if atm_vol > avg_vol + 1:
        temp['time'] = datetime.datetime.now()
        temp['symbol'] = symbol
        temp['current_iv'] = atm_vol
        temp['avg_iv'] = avg_vol
        temp['high_iv'] = long_short_df[long_short_df['symbol'] == symbol].highest_iv.item()
        temp['low_iv'] = long_short_df[long_short_df['symbol'] == symbol].lowest_iv.item()
        temp['signal'] = 'SHORT'

        inserts.append(long_short_tb(**temp))
        long_short_tb.objects.bulk_create(inserts, batch_size=500)
    if atm_vol < avg_vol - 1:
        temp['time'] = datetime.datetime.now()
        temp['symbol'] = symbol
        temp['current_iv'] = atm_vol
        temp['avg_iv'] = avg_vol
        temp['high_iv'] = long_short_df[long_short_df['symbol'] == symbol].highest_iv.item()
        temp['low_iv'] = long_short_df[long_short_df['symbol'] == symbol].lowest_iv.item()
        temp['signal'] = 'LONG'

        inserts.append(long_short_tb(**temp))
        long_short_tb.objects.bulk_create(inserts, batch_size=500)

