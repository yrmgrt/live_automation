import datetime

import pandas as pd
from django.core.management.base import BaseCommand

from websocket_trial.models.kite import KiteInstrument

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])

    def handle(self, *args, **options):

        latest_instruments = KiteInstrument.objects.all()
        latest_instruments = list(latest_instruments.values())
        latest_instruments = pd.DataFrame(latest_instruments)
        latest_instruments.to_csv(r"C:\Users\Administrator\latest_instrument.csv")

        today = datetime.datetime.today().date()
        monthly_expiry = datetime.datetime(2023, 12, 28).date()
        next_monthly_expiry = datetime.datetime(2024, 1, 25).date()

        dte_monthly = (monthly_expiry - today).days + 1
        next_dte_monthly = (next_monthly_expiry - today).days + 1

        bnk_weekly_expiry = datetime.datetime(2023, 12, 20).date()
        nfy_weekly_expiry = datetime.datetime(2023, 12, 21).date()

        dte_weekly_bnk = (bnk_weekly_expiry - today).days + 1
        dte_weekly_nfy = (nfy_weekly_expiry - today).days + 1

        today_less_15_days = today - datetime.timedelta(days=15)
        today_more_15_days = today + datetime.timedelta(days=15)

        today_less_8_days = today - datetime.timedelta(days=9)
        today_more_8_days = today + datetime.timedelta(days=9)

        bnk_round_strike = 42500
        nfy_round_strike = 19000

        temp_dict = dict()

        temp_dict['today'] = today
        temp_dict['monthly_expiry'] = monthly_expiry
        temp_dict['next_monthly_expiry'] = next_monthly_expiry
        temp_dict['dte_monthly'] = dte_monthly
        temp_dict['next_dte_monthly'] = next_dte_monthly
        temp_dict['bnk_weekly_expiry'] = bnk_weekly_expiry
        temp_dict['nfy_weekly_expiry'] = nfy_weekly_expiry
        temp_dict['dte_weekly_bnk'] = dte_weekly_bnk
        temp_dict['dte_weekly_nfy'] = dte_weekly_nfy
        temp_dict['today_less_15_days'] = today_less_15_days
        temp_dict['today_more_15_days'] = today_more_15_days
        temp_dict['today_less_8_days'] = today_less_8_days
        temp_dict['today_more_8_days'] = today_more_8_days
        temp_dict['bnk_round_strike'] = bnk_round_strike
        temp_dict['nfy_round_strike'] = nfy_round_strike

        # print(temp_dict)

        list_for_df = []
        list_for_df.append(temp_dict)

        daily_morn_df = pd.DataFrame(list_for_df)
        # print(daily_morn_df)
        daily_morn_df.to_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")

        prev_idv_df = pd.read_csv(r"C:\Users\Administrator\Desktop\idv_cal.csv")
        prev_idv_df['long_moves_yest'] = prev_idv_df['long_moves']
        prev_idv_df.to_csv(r"C:\Users\Administrator\Desktop\idv_cal_yest.csv")

        idv_df_read = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
        hv_df_read = pd.read_csv(r"C:\Users\Administrator\Downloads\long_move_based_iv.csv")
        forward_vol_df_read = pd.read_csv(r"C:\Users\Administrator\Downloads\forward_vol_{}.csv".format(today))
        idv_df_read = pd.merge(idv_df_read, hv_df_read, on='symbol', how='left')
        idv_df_read = pd.merge(idv_df_read, forward_vol_df_read, on='symbol', how='left')
        idv_df_read['long_moves'] = 0
        idv_df_read['change_time'] = datetime.datetime.now()
        idv_df_read['IVP'] = 0
        idv_df_read['bench_fut_close'] = idv_df_read['fut_close']
        idv_df_read['yest_fut_close'] = idv_df_read['fut_close']
        idv_df_read['pct_change'] = 0
        idv_df_read['current_iv'] = 0
        idv_df_read['avg_iv'] = idv_df_read['avg_normal_iv']

        idv_df = idv_df_read[['symbol', 'long_moves', 'change_time', 'bench_fut_close', 'yest_fut_close', 'pct_change',
                              'forward_vol', 'current_iv', 'IVP', 'avg_iv', 'move_iv', 'bench_mark_iv']]

        idv_df.to_csv(r"C:\Users\Administrator\Desktop\idv_cal.csv")

