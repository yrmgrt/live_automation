import datetime
import time
import pandas as pd
from django.core.management.base import BaseCommand
from django.db.models import Q

from websocket_trial.models.put_call_OI_tb import put_call_OI_tb
from websocket_trial.models.temp_table_live import live_before_df

all_symbols = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")
all_symbols = all_symbols.symbol.unique()

strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:

            # put_call_OI_tb.objects.all().delete()

            for symbol in all_symbols:
                print(symbol)
                k = live_before_df.objects.filter(symbol=symbol, instrument_type='FUT').last()
                fut_close = k.close
                # print(fut_close)
                # fut_close_time = k.time
                # print(fut_close_time)
                strike_diff = strike_diff_df[strike_diff_df['symbol'] == symbol]['strike_diff'].item()
                atm_strike = round(fut_close / strike_diff) * strike_diff
                # if symbol == 'TATASTEEL':
                #
                #     atm_strike = atm_strike + 0.4
                lower_threshold = fut_close - 0.12 * fut_close
                higher_threshold = fut_close + 0.12 * fut_close
                latest_concerned = live_before_df.objects.filter(
                    Q(symbol=symbol) & Q(strike_price__gt=lower_threshold) &
                    (Q(strike_price__lt=higher_threshold)))

                data = list(latest_concerned.values())
                data_df = pd.DataFrame(data)
                # print(symbol, fut_close)
                # print(data_df)
                if data_df.empty:
                    continue
                data_df = data_df.sort_values(by='time', ascending=True)
                grouped = data_df.groupby(['strike_price', 'instrument_type'])
                last_df_list = []
                for strike, grouped_df in grouped:
                    # print(type(grouped_df.iloc[-1]))
                    last_df_list.append(grouped_df.iloc[-1])

                last_df = pd.DataFrame(last_df_list)
                # print(last_df)
                last_df['instrument_type'] = (last_df['instrument_type'].str.replace("E", "")).map(str.lower)

                if len(last_df[last_df['strike_price'] == atm_strike]) == 2:
                    straddle_value = last_df[last_df['strike_price'] == atm_strike]['close'].sum()
                else:
                    continue

                put_df = last_df[last_df['instrument_type'] == 'p']
                put_df = put_df[(put_df['strike_price'] <= atm_strike) & (put_df['strike_price'] >= atm_strike-2*straddle_value)]

                call_df = last_df[last_df['instrument_type'] == 'c']
                call_df = call_df[(call_df['strike_price'] >= atm_strike) & (call_df['strike_price'] <= atm_strike+2*straddle_value)]

                put_df['OI_val'] = put_df['open_interest']*put_df['close']
                call_df['OI_val'] = call_df['open_interest'] * call_df['close']

                put_OI_val = put_df['OI_val'].sum()
                call_OI_val = call_df['OI_val'].sum()

                inserts = []

                objects_to_update = put_call_OI_tb.objects.filter(symbol=symbol)

                # if (symbol == 'NIFTY') or (symbol == 'BANKNIFTY'):
                #     print(symbol, put_b_value * 10000, put_a_value * 100000000)

                ##############when to add new symbol
                if not objects_to_update.exists():
                    temp = dict()
                    temp['time'] = datetime.datetime.now()
                    temp['symbol'] = symbol
                    temp['put_OI_val'] = round(put_OI_val / 100000, 2)
                    temp['call_OI_val'] = round(call_OI_val / 100000, 2)
                    temp['put_call_ratio'] = round(put_OI_val / call_OI_val, 2)

                    inserts.append(put_call_OI_tb(**temp))
                    # print(symbol)
                    put_call_OI_tb.objects.bulk_create(inserts, batch_size=500)

                for obj in objects_to_update:
                    obj.time = datetime.datetime.now()
                    obj.symbol = symbol
                    obj.put_OI_val = round(put_OI_val / 100000, 2)
                    obj.call_OI_val = round(call_OI_val / 100000, 2)
                    obj.put_call_ratio = round(put_OI_val / call_OI_val, 2)

                    # Update other fields as needed
                    obj.save()

            time.sleep(10)