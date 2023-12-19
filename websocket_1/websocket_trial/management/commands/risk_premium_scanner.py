import time
import datetime
from django.core.management.base import BaseCommand

from websocket_trial.models.scanner import scanner_table_skew
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.long_short_tb import long_short_tb
from websocket_trial.models.move_iv_tb import move_iv_tb

from websocket_trial.models.skew_tb import skew_table
import math

today = datetime.datetime.today().date()

def zptile(z_score):
    return .5 * (math.erf(z_score / 2 ** .5) + 1)

import pandas as pd
# df = pd.read_csv(r"C:\Users\Administrator\Downloads\Final_skew_file.csv")
move_iv_df = pd.read_csv(r"C:\Users\Administrator\Downloads\long_move_based_iv.csv")
long_short_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
forward_vol_df_read = pd.read_csv(r"C:\Users\Administrator\Downloads\forward_vol_{}.csv".format(today))
# corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\spot_iv_correlation.csv")
# df = pd.read_csv(r"C:\Users\Administrator\Downloads\Final_skew_remark_file.csv")
risk_prem_df = pd.read_csv(r"C:\Users\Administrator\Downloads\avg_risk_prem.csv")
back_spread = []

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:
            table_values = vol_table.objects.all()
            nifty_vol_row = vol_table.objects.filter(symbol='NIFTY').last()
            try:
                nifty_vol = nifty_vol_row.current_iv
            except:
                continue

            # long_short_tb.objects.all().delete()
            for row in table_values:
                inserts = []
                atm_vol = row.current_iv
                if atm_vol == 0:
                    continue
                # print(row.symbol)
                sym_vol_row = move_iv_df[move_iv_df['symbol'] == row.symbol]
                avg_vol_row = long_short_df[long_short_df['symbol'] == row.symbol]
                fwd_vol_row = forward_vol_df_read[forward_vol_df_read['symbol'] == row.symbol]
                if sym_vol_row.empty:
                    continue
                if avg_vol_row.empty:
                    continue
                move_based_iv = sym_vol_row.move_iv.item()
                fair_vol = avg_vol_row.avg_normal_iv.item()
                fwd_vol = fwd_vol_row.forward_vol.item()
                stddev_fair_vol = avg_vol_row.stddev_normal_iv.item()

                iv_z_s = (atm_vol - fair_vol)/stddev_fair_vol

                ivp = round(zptile(iv_z_s)*100)

                temp = dict()
                # sym_df = df[df['symbol'] == row.symbol]
                sym_risk_prem_row = risk_prem_df[risk_prem_df['symbol'] == row.symbol]
                avg_risk_prem = sym_risk_prem_row.avg_risk_prem.item()

                objects_to_update = long_short_tb.objects.filter(symbol=row.symbol)

                if not objects_to_update.exists():

                    temp['symbol'] = row.symbol
                    temp['current_iv'] = round(atm_vol, 2)
                    temp['fwd_iv'] = round(fwd_vol, 2)
                    temp['hv_current'] = round(move_based_iv,2)
                    temp['risk_prem'] = round((atm_vol - move_based_iv), 2)
                    temp['risk_prem_historical'] = round(avg_risk_prem,2)
                    temp['rp_diff'] = round(temp['risk_prem'] - temp['risk_prem_historical'],2)
                    temp['vol_ratio'] = round((atm_vol/nifty_vol), 2)
                    temp['fair_vol'] = round(fair_vol,2)
                    temp['ivp'] = ivp
                    # filtered = skew_table.objects.filter(symbol=row.symbol).all()

                    inserts.append(long_short_tb(**temp))
                    long_short_tb.objects.bulk_create(inserts, batch_size=500)

                else:
                    objects_to_update = objects_to_update.last()
                    objects_to_update.symbol = row.symbol
                    objects_to_update.current_iv = round(atm_vol, 2)
                    objects_to_update.fwd_iv = round(fwd_vol, 2)
                    objects_to_update.hv_current = round(move_based_iv,2)
                    objects_to_update.risk_prem = round((atm_vol - move_based_iv), 2)
                    objects_to_update.risk_prem_historical = round(avg_risk_prem,2)
                    objects_to_update.rp_diff = round(objects_to_update.risk_prem - objects_to_update.risk_prem_historical,2)
                    objects_to_update.vol_ratio = round((atm_vol/nifty_vol), 2)
                    objects_to_update.fair_vol = round(fair_vol,2)
                    objects_to_update.ivp = ivp

                    objects_to_update.save()

            time.sleep(10)