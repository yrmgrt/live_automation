
from django.core.management.base import BaseCommand

from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models.next_expiry_live import next_expiry_live
from websocket_trial.models.new_skew_tb import new_skew_table
from websocket_trial.models.skew_scanner import skew_scanner
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.next_exp_vol_table import next_exp_vol_table
from websocket_trial.models.long_short_tb import long_short_tb
from websocket_trial.models.pair_table import pair_table
from websocket_trial.models.fwd_vol import forward_vol_tb
from websocket_trial.models.overwatch_db import overwatch_db
from websocket_trial.models.weekly_expiry_live import weekly_expiry_live
from websocket_trial.models.weekly_skew_table import weekly_skew_table
from websocket_trial.models.weekly_skew_scanner import weekly_skew_scanner
from websocket_trial.models.next_exp_scanner_tb import next_exp_scanner_tb


class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])

    def handle(self, *args, **options):

        tables_to_del = [live_before_df, next_expiry_live, new_skew_table, skew_scanner,
                         vol_table, long_short_tb, pair_table, forward_vol_tb,
                         next_exp_vol_table, overwatch_db, weekly_expiry_live, weekly_skew_table,
                         weekly_skew_scanner, next_exp_scanner_tb]

        for table in tables_to_del:

            table.objects.all().delete()

