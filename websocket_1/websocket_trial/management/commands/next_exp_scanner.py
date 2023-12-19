import time
import datetime
from django.core.management.base import BaseCommand

from websocket_trial.models.next_exp_vol_table import next_exp_vol_table
from websocket_trial.models.next_exp_scanner_tb import next_exp_scanner_tb

import math


def zptile(z_score):
    return .5 * (math.erf(z_score / 2 ** .5) + 1)

import pandas as pd
# df = pd.read_csv(r"C:\Users\Administrator\Downloads\Final_skew_file.csv")
iv_stats_next_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats_next.csv")

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:
            table_values = next_exp_vol_table.objects.all()
            next_exp_scanner_tb.objects.all().delete()
            for row in table_values:
                lower_iv_bar = iv_stats_next_df[iv_stats_next_df['symbol'] == row.symbol]['lower_iv_bar'].item()
                upper_iv_bar = iv_stats_next_df[iv_stats_next_df['symbol'] == row.symbol]['upper_iv_bar'].item()

                temp = dict()
                temp['time'] = datetime.datetime.now()
                temp['symbol'] = row.symbol
                temp['strike_price'] = row.strike_price
                temp['instrument_type'] = row.instrument_type
                if row.bid_iv > upper_iv_bar:
                    inserts = []
                    temp['iv'] = round(row.bid_iv, 2)
                    temp['signal'] = 'sell'
                    inserts.append(next_exp_scanner_tb(**temp))
                    next_exp_scanner_tb.objects.bulk_create(inserts, batch_size=500)
                if (row.ask_iv < lower_iv_bar) and (row.ask_iv >= 0):
                    inserts = []
                    temp['iv'] = round(row.ask_iv, 2)
                    temp['signal'] = 'buy'
                    inserts.append(next_exp_scanner_tb(**temp))
                    next_exp_scanner_tb.objects.bulk_create(inserts, batch_size=500)
                # filtered = skew_table.objects.filter(symbol=row.symbol).all()

            # time.sleep(0)