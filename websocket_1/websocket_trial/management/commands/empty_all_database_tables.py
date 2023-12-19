from websocket_trial.models.scanner import scanner_table_skew
from websocket_trial.models.skew_tb import skew_table
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.skew_scanner import skew_scanner
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models.next_expiry_live import next_expiry_live
from websocket_trial.models.new_skew_tb import new_skew_table
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.next_exp_vol_table import next_exp_vol_table
from websocket_trial.models.weekly_expiry_live import weekly_expiry_live

import time
import datetime
from django.db.models import Q

from django.core.management.base import BaseCommand
class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):

        previous_time = datetime.datetime.now() - datetime.timedelta(minutes=15)
        while True:
            live_before_df.objects.filter(Q(time__lt=previous_time)).delete()
            next_expiry_live.objects.filter(Q(time__lt=previous_time)).delete()
            new_skew_table.objects.filter(Q(time__lt=previous_time)).delete()
            vol_table.objects.filter(Q(time__lt=previous_time)).delete()
            next_exp_vol_table.objects.filter(Q(time__lt=previous_time)).delete()
            weekly_expiry_live.objects.filter(Q(time__lt=previous_time)).delete()

            current_time = datetime.datetime.now()
            previous_time = current_time
            print(previous_time)
            time.sleep(600)


