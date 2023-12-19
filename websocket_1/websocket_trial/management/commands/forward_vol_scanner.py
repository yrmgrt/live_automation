import time
import datetime
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.fwd_vol import forward_vol_tb
from django.core.management.base import BaseCommand

import pandas as pd

import math


def zptile(z_score):
    return .5 * (math.erf(z_score / 2 ** .5) + 1)


daily_morn_df = pd.read_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")
today = daily_morn_df['today'].item()

forward_df = pd.read_csv(r"C:\Users\Administrator\Downloads\forward_vol_{}.csv".format(today))
long_short_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")

hv_current_df = pd.read_csv(r"C:\Users\Administrator\Downloads\long_move_based_iv.csv")
class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:
            table_values = vol_table.objects.all()
            # forward_vol_tb.objects.all().delete()
            for row in table_values:

                atm_vol = row.current_iv
                # print(row.symbol)
                sym_vol_row = forward_df[forward_df['symbol'] == row.symbol]
                sym_avg_row = long_short_df[long_short_df['symbol'] == row.symbol]
                try:
                    hv_current = hv_current_df[hv_current_df['symbol'] == row.symbol]['move_iv'].item()
                except:
                    continue
                fwd_vol = sym_vol_row.forward_vol.item()

                iv_z_s = (atm_vol - sym_avg_row.avg_normal_iv.item())/sym_avg_row.stddev_normal_iv.item()

                ivp = round(zptile(iv_z_s)*100)

                # print(fwd_vol, atm_vol)
                if fwd_vol == 0:
                    continue
                if row.current_iv == 0:
                    continue
                # if fwd_vol < atm_vol:
                if True:
                    objects_to_update = forward_vol_tb.objects.filter(symbol=row.symbol)
                    if not objects_to_update.exists():
                        inserts = []
                        temp = dict()
                        temp['symbol'] = row.symbol
                        temp['current_iv'] = round(atm_vol, 2)
                        temp['fwd_vol'] = round(fwd_vol,2)
                        temp['fut_close'] = row.fut_close
                        temp['current_atm'] = row.current_atm
                        temp['current_call_iv'] = round(row.current_call_iv, 2)
                        temp['current_put_iv'] = round(row.current_put_iv, 2)
                        temp['fair_vol'] = round(sym_avg_row.avg_normal_iv.item(), 2)
                        temp['ivp'] = ivp
                        temp['hv_current'] = hv_current
                        inserts.append(forward_vol_tb(**temp))
                        forward_vol_tb.objects.bulk_create(inserts, batch_size=500)
                    else:
                        objects_to_update = objects_to_update.last()
                        objects_to_update.symbol = row.symbol
                        objects_to_update.current_iv = round(atm_vol, 2)
                        objects_to_update.fwd_vol = round(fwd_vol,2)
                        objects_to_update.fut_close = row.fut_close
                        objects_to_update.current_atm = row.current_atm
                        objects_to_update.current_call_iv = round(row.current_call_iv, 2)
                        objects_to_update.current_put_iv = round(row.current_put_iv, 2)
                        objects_to_update.fair_vol = round(sym_avg_row.avg_normal_iv.item(), 2)
                        objects_to_update.ivp = ivp
                        objects_to_update.hv_current = hv_current

                        # Update other fields as needed
                        objects_to_update.save()
            time.sleep(10)