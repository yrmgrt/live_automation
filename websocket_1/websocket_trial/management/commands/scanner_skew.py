import time

from django.core.management.base import BaseCommand

from websocket_trial.models.scanner import scanner_table_skew
from websocket_trial.models.skew_tb import skew_table
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.skew_scanner import skew_scanner
import pandas as pd
df = pd.read_csv(r"C:\Users\Administrator\Downloads\Final_skew_remark_file.csv")
corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\spot_iv_correlation.csv")
front_spread = []
back_spread = []
results_df = pd.read_csv(r"C:\Users\Administrator\Downloads\results_date.csv")
results_df = results_df[results_df['date'] >= '2023-01-01']

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:
            table_values = skew_table.objects.all()

            # Perform scanning actions
            scanner_table_skew.objects.all().delete()
            num = 0
            for row in table_values:
                num = num + 1

                sym = row.symbol
                # if sym == 'FEDE'
                # print(sym)
                filtered_results_dates = results_df[results_df['symbol'] == sym]['date'].unique()
                if not sym in ['NIFTY', 'BANKNIFTY']:
                    pass
                    # latest_result_date = filtered_results_dates[-1]
                    # if latest_result_date <= '2023-08-15' and latest_result_date >= '2023-08-01':
                    #     continue
                filtered = vol_table.objects.filter(symbol=sym).all()
                if not filtered:
                    print("Query result is empty")
                    continue
                else:
                    # Process the result
                    for key in filtered:
                        vol = key.current_iv



                #vol = filtered.current_iv
                sym_df = df[df['symbol'] == sym]
                if sym_df.empty:
                    continue

                put_call_bench_mark = sym_df.put_call_std_down_b.item()
                put_call_average = sym_df.put_call_avg_b.item()

                put_call_sigma = put_call_average - put_call_bench_mark

                sym_corr_df = df[df['symbol'] == sym]
                if sym_corr_df.empty:
                    continue



                call_call_bench_mark = sym_df.call_std_up_b.item()
                call_call_average = sym_df.call_avg_b.item()

                call_call_sigma = call_call_bench_mark - call_call_average


                call_front_benchmark = sym_df.call_std_down_b.item()
                call_front_average = sym_df.call_avg_b.item()
                call_front_sigma = call_front_average -call_front_benchmark

                call_call_average = sym_df.call_avg_b.item()

                call_call_sigma = call_call_bench_mark - call_call_average


                put_call_a_benchmark = sym_df.put_call_std_down_a.item()
                put_call_a_average = sym_df.put_call_avg_a.item()
                put_call_a_sigma = put_call_a_average - put_call_a_benchmark


                if (row.call_b_value < call_front_benchmark) and (row.call_strikes_ratio >= 0.7)\
                        and (row.R2_call >= 0.75):
                    inserts = []
                    temp = dict()
                    temp['num'] = num
                    temp['spread'] = 'Call Front Spread'

                    temp['symbol'] = sym
                    temp['z_score'] = round(((row.call_b_value - call_front_average) / call_front_sigma)/0.01)*0.01
                    temp['straddle_skew'] = round(row.call_straddle_skew/0.01)*0.01 * -1

                    temp['entry'] = round(sym_df.call_std_up_straddle_skew.item()/0.01)*0.01
                    temp['exit'] = round(sym_df.call_std_down_straddle_skew.item()/0.01)*0.01

                    temp['r2'] = round(row.R2_call/0.01)*0.01
                    temp['strike_1'] = row.strike_1_call
                    temp['strike_2'] = row.strike_2_call
                    temp['current_iv'] = round(vol, 2)
                    temp['correlation_last_day'] = round(sym_corr_df['up-remark'].item(), 2)
                    temp['correlation_last_week'] = round(sym_corr_df['down-remark'].item(), 2)

                    back_spread.append(temp)
                    inserts.append(scanner_table_skew(**temp))
                    scanner_table_skew.objects.bulk_create(inserts, batch_size=500)

                if (row.put_call_b_value < put_call_bench_mark) and (row.put_call_strikes_ratio >= 0.7)\
                        and (row.R2_put_call >= 0.8):
                    inserts = []
                    temp = dict()
                    temp['num'] = num
                    temp['spread'] = 'Front Spread'
                    temp['symbol'] = sym
                    temp['z_score'] = round(((row.put_call_b_value - put_call_average)/put_call_sigma),2)
                    temp['straddle_skew'] = round(row.put_call_straddle_skew,2)
                    temp['entry'] = round(sym_df.put_call_std_up_straddle_skew.item(),2)
                    temp['exit'] = round(sym_df.put_call_std_down_straddle_skew.item(),2)
                    temp['r2'] = round(row.R2_put_call,2)
                    temp['strike_1'] = row.strike_1_put_call
                    temp['strike_2'] = row.strike_2_put_call
                    temp['current_iv'] = round(vol, 2)
                    temp['correlation_last_day'] = round(sym_corr_df['up-remark'].item(), 2)
                    temp['correlation_last_week'] = round(sym_corr_df['down-remark'].item(), 2)
                    front_spread.append(temp)
                    inserts.append(scanner_table_skew(**temp))
                    scanner_table_skew.objects.bulk_create(inserts, batch_size=500)


                if (row.call_b_value > call_call_bench_mark) and (row.call_strikes_ratio >= 0.7)\
                        and (row.R2_call >= 0.75):
                    inserts = []
                    temp = dict()
                    temp['num'] = num
                    temp['spread'] = 'Back Spread'

                    temp['symbol'] = sym
                    temp['z_score'] = round(((row.call_b_value - call_call_average) / call_call_sigma)/0.01)*0.01
                    temp['straddle_skew'] = round(row.call_straddle_skew/0.01)*0.01

                    temp['entry'] = round(sym_df.call_std_up_straddle_skew.item()/0.01)*0.01
                    temp['exit'] = round(sym_df.call_std_down_straddle_skew.item()/0.01)*0.01

                    temp['r2'] = round(row.R2_call/0.01)*0.01
                    temp['strike_1'] = row.strike_1_call
                    temp['strike_2'] = row.strike_2_call
                    temp['current_iv'] = round(vol, 2)
                    temp['correlation_last_day'] = round(sym_corr_df['up-remark'].item(), 2)
                    temp['correlation_last_week'] = round(sym_corr_df['down-remark'].item(), 2)
                    back_spread.append(temp)
                    inserts.append(scanner_table_skew(**temp))
                    scanner_table_skew.objects.bulk_create(inserts, batch_size=500)
                if (row.put_call_a_value < put_call_a_benchmark) and(row.R2_put_call >= 0.8):
                    inserts = []
                    temp = dict()
                    temp['spread'] = 'Four_leg'

                    temp['symbol'] = sym
                    temp['z_score'] = round(((row.put_call_a_value - put_call_a_average) / put_call_a_sigma) / 0.01) * 0.01
                    temp['straddle_skew'] = round(row.put_straddle_skew,2) + round(row.call_straddle_skew,2)

                    temp['entry'] = round(sym_df.put_call_std_up_straddle_kurt,2)
                    temp['exit'] = round(sym_df.put_call_std_down_straddle_kurt,2)

                    temp['r2'] = round(row.R2_call / 0.01) * 0.01
                    temp['strike_1'] = row.strike_1_put

                    temp['strike_2'] = row.strike_2_put

                    temp['current_iv'] = round(vol, 2)
                    temp['correlation_last_day'] = row.strike_1_call
                    temp['correlation_last_week'] = row.strike_2_call

                    back_spread.append(temp)
                    inserts.append(scanner_table_skew(**temp))
                    scanner_table_skew.objects.bulk_create(inserts, batch_size=500)

                    # back_spread.append(sym)
            # front_df = pd.DataFrame(front_spread)
            # front_df = front_df[front_df['r2'] > 0.8]
            # front_df = front_df.sort_values(by='z_score')
            #
            # print('-----------------------------------------')
            # print('Front spread')
            #
            # print(front_df)
            # print('---------------------------------')
            # # print('Front Spread', front_spread)
            #
            #
            # back_df = pd.DataFrame(back_spread)
            # # if not back_df.empty:
            # back_df = back_df[back_df['r2'] > 0.8]
            # back_df = back_df.sort_values(by='z_score')
            #
            # print('Back Spread')
            # print(back_df)
            # print('-----------------------------------------')




            # self.stdout.write(self.style.SUCCESS('Scanning complete.'))
            time.sleep(10)