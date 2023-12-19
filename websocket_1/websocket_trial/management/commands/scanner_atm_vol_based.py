import time
import datetime
from django.core.management.base import BaseCommand

from websocket_trial.models.scanner import scanner_table_skew
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.long_short_tb import long_short_tb
import pandas as pd
# df = pd.read_csv(r"C:\Users\Administrator\Downloads\Final_skew_file.csv")
long_short_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\spot_iv_correlation.csv")
front_spread = []
back_spread = []

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:
            table_values = vol_table.objects.all()
            long_short_tb.objects.all().delete()

            for row in table_values:
                inserts = []
                atm_vol = row.current_iv
                print(row.symbol)
                avg_vol_row = long_short_df[long_short_df['symbol'] == row.symbol]
                if avg_vol_row.empty:
                    continue
                avg_vol = avg_vol_row.avg_normal_iv.item()
                if atm_vol > avg_vol + 1:

                    temp = dict()
                    temp['time'] = datetime.datetime.now()
                    temp['symbol'] = row.symbol
                    temp['current_iv'] = round(atm_vol, 2)
                    temp['avg_iv'] = round(avg_vol, 2)
                    temp['high_iv'] = round(long_short_df[long_short_df['symbol'] == row.symbol].higest_normal_iv.item(), 2)
                    temp['low_iv'] = round(long_short_df[long_short_df['symbol'] == row.symbol].lowest_normal_iv.item(),2)
                    temp['signal'] = 'SHORT'
                    temp['short_moves_prev'] = round(long_short_df[long_short_df['symbol'] == row.symbol].avg_short_move.item(), 2)
                    temp['long_moves_prev'] = round(
                        long_short_df[long_short_df['symbol'] == row.symbol].avg_long_move.item(), 2)
                    temp['benchmark_iv'] = round(long_short_df[long_short_df['symbol'] == row.symbol].bench_mark_iv.item(), 2)
                    inserts.append(long_short_tb(**temp))
                    long_short_tb.objects.bulk_create(inserts, batch_size=500)
                if atm_vol < avg_vol - 0.5:
                    temp = dict()
                    temp['time'] = datetime.datetime.now()
                    temp['symbol'] = row.symbol
                    temp['current_iv'] = round(atm_vol, 2)
                    temp['avg_iv'] = round(avg_vol, 2)
                    temp['high_iv'] = round(long_short_df[long_short_df['symbol'] == row.symbol].higest_normal_iv.item(),2)
                    temp['low_iv'] = round(long_short_df[long_short_df['symbol'] == row.symbol].lowest_normal_iv.item(),2)
                    temp['signal'] = 'LONG'
                    temp['long_moves_prev'] = round(long_short_df[long_short_df['symbol'] == row.symbol].avg_long_move.item(),2)
                    temp['short_moves_prev'] = round(
                        long_short_df[long_short_df['symbol'] == row.symbol].avg_short_move.item(), 2)
                    temp['benchmark_iv'] = round(long_short_df[long_short_df['symbol'] == row.symbol].bench_mark_iv.item(), 2)

                    inserts.append(long_short_tb(**temp))
                    long_short_tb.objects.bulk_create(inserts, batch_size=500)
            # time.sleep(10)