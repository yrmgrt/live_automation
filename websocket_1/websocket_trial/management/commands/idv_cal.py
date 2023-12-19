import time
import datetime
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")

import http.client, urllib

# create connection


# make POST request to send message
def delta_ping_ios(symbol):
    conn = http.client.HTTPSConnection("api.pushover.net:443")
    conn.request("POST", "/1/messages.json",
      urllib.parse.urlencode({
        "token": "abybcozibie6ae11hjhmvsen1h3d7t",
        "user": "u29fptbwczie7vy72y9s3wnsc5xp4e",
        "title": "Delta",
        "message": f"{symbol}",
        "url": "",
        "priority": "0"
      }), { "Content-type": "application/x-www-form-urlencoded" })

    # get response
    conn.getresponse()

import math


def zptile(z_score):
    return .5 * (math.erf(z_score / 2 ** .5) + 1)



# idv_df['bench_fut_close']['RELIANCE'] = 0
# print(idv_df['bench_fut_close']['RELIANCE'])
# exit()
# idv_df[idv_df['symbol'] == 'RELIANCE']['bench_fut_close'] = 0
# print(idv_df[idv_df['symbol'] == 'RELIANCE']['bench_fut_close'])
# exit()
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models.idv_cal_tb import idv_cal
symbols = df.symbol.unique()
all_symbols = df.symbol.unique()
print(all_symbols)

benchmark_fut = dict()
symbols = list(set(all_symbols) and set(symbols))


while True:

    idv_df = pd.read_csv(r"C:\Users\Administrator\Desktop\idv_cal.csv")
    idv_df.set_index('symbol', inplace=True)
    # idv_df['recent_change'] = 'no'

    for symbol in all_symbols:
        try:
            current_fut = live_before_df.objects.filter(symbol=symbol, instrument_type='FUT').last()
            # fut_close = k.close
            # current_fut = vol_table.objects.filter(symbol=symbol).first().fut_close
            current_fut = current_fut.close
            current_iv = vol_table.objects.filter(symbol=symbol).first().current_iv
            avg_iv = df[df['symbol'] == symbol]['avg_normal_iv'].item()
            stddev_iv = df[df['symbol'] == symbol]['stddev_normal_iv'].item()

            iv_z_s = (current_iv - avg_iv)/stddev_iv
            ivp = round(zptile(iv_z_s)*100)

            idv_df['current_iv'][symbol] = round(current_iv,2)
            idv_df['IVP'][symbol] = ivp
            # print(symbol, current_fut)
            benchmark_fut_val = idv_df['bench_fut_close'][symbol].item()
            yest_fut_val = idv_df['yest_fut_close'][symbol].item()
            idv_df['pct_change'][symbol] = ((current_fut - yest_fut_val)/yest_fut_val)*100
            long_move_val = df[df['symbol'] == symbol]['long_move'].item()
            long_moves = idv_df['long_moves'][symbol].item()
            # full_move = df[df['symbol'] == symbol]['full_move']
            # benchmark_fut_val = df[df['symbol'] == symbol]['fut_close']
            if (abs(current_fut - benchmark_fut_val) > long_move_val):
                # k = idv_cal.objects.filter(symbol=symbol)

                long_moves = long_moves + round(abs(current_fut - benchmark_fut_val)/long_move_val)
                idv_df['long_moves'][symbol] = long_moves
                idv_df['bench_fut_close'][symbol] = current_fut
                idv_df['change_time'][symbol] = datetime.datetime.now()
                # idv_df['recent_change'][symbol] = 'yes'
                # print(symbol)
            # if abs(current_fut- benchmark_fut_val) > full_move:
            #     current_full_move = k.first().full_move
            #     k.full_move = current_full_move + 1
            #     benchmark_fut['symbol'] = current_fut
            #     print(symbol)
        except Exception as error:
            print(symbol, error)

    idv_df.to_csv(r"C:\Users\Administrator\Desktop\idv_cal.csv")
    # time.sleep(50)

