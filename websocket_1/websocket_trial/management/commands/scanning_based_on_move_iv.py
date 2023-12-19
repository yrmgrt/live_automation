import time
import datetime
from django.core.management.base import BaseCommand

from websocket_trial.models.scanner import scanner_table_skew
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.long_short_tb import long_short_tb
from websocket_trial.models.move_iv_tb import move_iv_tb

from websocket_trial.models.skew_tb import skew_table


import pandas as pd
# df = pd.read_csv(r"C:\Users\Administrator\Downloads\Final_skew_file.csv")
move_iv_df = pd.read_csv(r"C:\Users\Administrator\Downloads\long_move_based_iv.csv")
long_short_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\spot_iv_correlation.csv")
df = pd.read_csv(r"C:\Users\Administrator\Downloads\Final_skew_remark_file.csv")
front_spread = []
back_spread = []

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:
            table_values = vol_table.objects.all()

            move_iv_tb.objects.all().delete()
            for row in table_values:
                inserts = []
                atm_vol = row.current_iv
                print(row.symbol)
                sym_vol_row = move_iv_df[move_iv_df['symbol'] == row.symbol]
                avg_vol_row = long_short_df[long_short_df['symbol'] == row.symbol]
                if sym_vol_row.empty:
                    continue
                if avg_vol_row.empty:
                    continue
                move_based_iv = sym_vol_row.move_iv.item()
                temp = dict()
                sym_df = df[df['symbol'] == row.symbol]
                temp['symbol'] = row.symbol
                temp['current_iv'] = round(atm_vol, 2)
                temp['move_based_iv'] = round(move_based_iv,2)
                temp['high_iv'] = round(long_short_df[long_short_df['symbol'] == row.symbol].higest_normal_iv.item(), 2)
                temp['low_iv'] = round(long_short_df[long_short_df['symbol'] == row.symbol].lowest_normal_iv.item(), 2)
                temp['iv_diff'] =  round(atm_vol, 2) - round(move_based_iv, 2)
                temp['current_call_iv'] = round(row.current_call_iv, 2)
                temp['current_put_iv'] = round(row.current_put_iv, 2)
                filtered = skew_table.objects.filter(symbol=row.symbol).all()
                if not filtered:
                    temp['call_std'] = 0
                    temp['four_leg_std'] = 0
                    temp['put_std'] = 0


                else:
                    pass
                inserts.append(move_iv_tb(**temp))
                move_iv_tb.objects.bulk_create(inserts, batch_size=500)
            time.sleep(10)