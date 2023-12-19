import time

from django.core.management.base import BaseCommand
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models.scanner import scanner_table_skew
from websocket_trial.models.skew_tb import skew_table
import pandas as pd
all_symbols = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")
all_symbols = all_symbols.symbol.unique()


class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:

            for symbol in all_symbols:

                fut_data = live_before_df.objects.filter(symbol=symbol, instrument_type='FUT')
                data = list(fut_data.values())
                data_df = pd.DataFrame(data)
                data_df = data_df.sort_values(by='time', ascending=True)

                data_df = data_df.set_index('time')
                print(data_df)
                resamp_data = data_df.resample('5s').last()
                # print(data_df)
                # print(resamp_data)
                time.sleep(5)
            self.stdout.write(self.style.SUCCESS('Scanning complete.'))
