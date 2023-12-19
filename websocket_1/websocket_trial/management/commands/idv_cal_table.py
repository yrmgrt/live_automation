import time
import datetime
import pandas as pd
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.fwd_vol import forward_vol_tb
from django.core.management.base import BaseCommand
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.idv_cal_tb import idv_cal
df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
symbols = df.symbol.unique()
all_symbols = df.symbol.unique()
benchmark_fut = dict()
symbols = list(set(all_symbols) and set(symbols))


daily_morn_df = pd.read_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")
today = daily_morn_df['today'].item()

forward_df = pd.read_csv(r"C:\Users\Administrator\Downloads\forward_vol_{}.csv".format(today))
long_short_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
idv_prev = pd.read_csv(r"C:\Users\Administrator\Desktop\idv_cal.csv")
prev_long_moves = 0

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:

            for symbol in all_symbols:
                try:
                    current_fut = vol_table.objects.filter(symbol=symbol).first().fut_close
                    current_iv = vol_table.objects.filter(symbol=symbol).first().current_iv

                    print(current_fut)
                    benchmark_fut_val = idv_prev['bench_fut_close'][symbol].item()
                    prev_long_moves = idv_prev['long_moves'][symbol].item()
                    long_move_val = df[df['symbol'] == symbol]['long_move'].item()

                    # full_move = df[df['symbol'] == symbol]['full_move']
                    # benchmark_fut_val = df[df['symbol'] == symbol]['fut_close']
                    if (abs(current_fut - benchmark_fut_val) > long_move_val):


                        benchmark_fut_val = current_fut
                        # print(symbol)

                except:
                    print(symbol)