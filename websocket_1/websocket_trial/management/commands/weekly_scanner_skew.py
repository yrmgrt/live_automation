import time

from django.core.management.base import BaseCommand

# from websocket_trial.models.scanner import scanner_table_skew
from websocket_trial.models.weekly_skew_table import weekly_skew_table
# from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.weekly_skew_scanner import weekly_skew_scanner
import math
import pandas as pd

df = pd.read_csv(r"C:\Users\Administrator\Downloads\Weekly_Final_skew_remark_file.csv")

def zptile(z_score):
    return .5 * (math.erf(z_score / 2 ** .5) + 1)


long_short_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
# corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\spot_iv_correlation.csv")
# front_spread = []
# back_spread = []
# results_df = pd.read_csv(r"C:\Users\Administrator\Downloads\results_date.csv")
# results_df = results_df[results_df['date'] >= '2023-01-01']

class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:
            table_values = weekly_skew_table.objects.all()

            # Perform scanning actions
            weekly_skew_scanner.objects.all().delete()
            # num = 0
            for row in table_values:
                # num = num + 1

                sym = row.symbol
                # if sym == 'FEDE'
                # print(sym)
                # filtered_results_dates = results_df[results_df['symbol'] == sym]['date'].unique()
                # if not sym in ['NIFTY', 'BANKNIFTY']:
                #     pass
                    # latest_result_date = filtered_results_dates[-1]
                    # if latest_result_date <= '2023-08-15' and latest_result_date >= '2023-08-01':
                    #     continue

                #vol = filtered.current_iv
                sym_df = df[df['symbol'] == sym]

                if sym_df.empty:
                    continue

                strike_diff = sym_df['strike_diff'].item()

                put_call_bench_mark = sym_df.put_call_std_down_b.item()
                put_call_average = sym_df.put_call_avg_b.item()
                put_call_sigma = put_call_average - put_call_bench_mark

                put_call_a_benchmark = sym_df.put_call_std_down_a.item()
                put_call_a_average = sym_df.put_call_avg_a.item()
                put_call_a_sigma = put_call_a_average - put_call_a_benchmark

                zs_b = round((row.b_value - put_call_average)/put_call_sigma,2)
                zs_a = round((row.a_value - put_call_a_average)/put_call_a_sigma,2)

                if row.put_skew_atm_diff1 != -99:
                    put_x1_x2_sum = row.put_skew_atm_diff1 + row.put_skew_atm_diff2
                    # print(strike_diff, row.put_skew_atm_diff1, row.put_skew_atm_diff2)
                    try:
                        put_put_benchmark = (row.put_skew_atm_diff2-row.put_skew_atm_diff1)*sym_df[f'pp{int(put_x1_x2_sum/strike_diff)}std_down'].item()
                        put_put_average = (row.put_skew_atm_diff2-row.put_skew_atm_diff1)*sym_df[f'pp{int(put_x1_x2_sum/strike_diff)}avg'].item()
                    except:
                        continue
                    put_put_sigma = put_put_average - put_put_benchmark
                    # if sym == 'NIFTY':
                    #     print(row.put_skew_iv2 - row.put_skew_iv1, put_put_benchmark, put_put_average, put_put_sigma)
                    pp_zs = round(((row.put_skew_iv2 - row.put_skew_iv1) - put_put_average)/put_put_sigma,2)
                    print(sym, pp_zs)
                else:
                    put_put_average = -999
                    put_put_sigma = -999
                    pp_zs = -999

                if row.call_skew_atm_diff1 != -99:
                    call_x1_x2_sum = row.call_skew_atm_diff1 + row.call_skew_atm_diff2
                    # print(strike_diff, row.call_skew_atm_diff1, row.call_skew_atm_diff2)
                    try:
                        call_call_benchmark = (row.call_skew_atm_diff1-row.call_skew_atm_diff2)*sym_df[f'cc{int(call_x1_x2_sum/strike_diff)}std_up'].item()
                        call_call_average = (row.call_skew_atm_diff1-row.call_skew_atm_diff2)*sym_df[f'cc{int(call_x1_x2_sum/strike_diff)}avg'].item()
                    except:
                        continue
                    call_call_sigma = call_call_benchmark - call_call_average
                    # if sym == 'NIFTY':
                    #     print(row.call_skew_iv1 - row.call_skew_iv2,call_call_benchmark, call_call_average, call_call_sigma)
                    cc_zs = round(((row.call_skew_iv1 - row.call_skew_iv2) - call_call_average) / call_call_sigma,2)

                else:
                    call_call_average = -999
                    call_call_sigma = -999
                    cc_zs = -999

                # up_rem = sym_df['up-remark'].item()
                # down_rem = sym_df['down-remark'].item()
                up_rem = 0
                down_rem = 0

                # print(zs_b, sym)
                if (row.b_value != -999990000):# and (row.b_value < (put_call_average - put_call_sigma*0)):
                    inserts = []
                    temp = dict()
                    temp['time'] = row.time
                    temp['symbol'] = row.symbol
                    temp['spread'] = 'PCF'
                    temp['z'] = zs_b
                    temp['b_value'] = row.b_value
                    temp['a_value'] = row.a_value
                    temp['put_strike'] = row.put_skew_strike
                    temp['call_strike'] = row.call_skew_strike
                    temp['put_iv'] = round(row.put_skew_iv, 2)
                    temp['call_iv'] = round(row.call_skew_iv, 2)
                    temp['atm_strike'] = row.atm_strike
                    temp['atm_iv'] = round(row.atm_iv, 2)
                    temp['up_rem'] = round(up_rem, 2)
                    temp['down_rem'] = round(down_rem, 2)
                    temp['skew'] = round(row.call_skew_iv - row.put_skew_iv,2)
                    if row.skew_atm_diff is not None:
                        temp['target'] = round((-2) * put_call_average/10000 * (row.skew_atm_diff),2)
                    else:
                        print("Error in ", row.symbol, 'Front')
                    inserts.append(weekly_skew_scanner(**temp))
                    weekly_skew_scanner.objects.bulk_create(inserts, batch_size=500)

                if (row.b_value != -999990000):# and (row.b_value > (put_call_average + put_call_sigma*0)):
                    inserts = []
                    temp = dict()
                    temp['time'] = row.time
                    temp['symbol'] = row.symbol
                    temp['spread'] = 'PCB'
                    temp['z'] = zs_b
                    temp['b_value'] = row.b_value
                    temp['a_value'] = row.a_value
                    temp['put_strike'] = row.put_skew_strike
                    temp['call_strike'] = row.call_skew_strike
                    temp['put_iv'] = round(row.put_skew_iv, 2)
                    temp['call_iv'] = round(row.call_skew_iv, 2)
                    temp['atm_strike'] = row.atm_strike
                    temp['atm_iv'] = round(row.atm_iv, 2)
                    temp['up_rem'] = round(up_rem, 2)
                    temp['down_rem'] = round(down_rem, 2)

                    temp['skew'] = round(row.put_skew_iv - row.call_skew_iv,2)

                    if row.skew_atm_diff is not None:
                        temp['target'] = round(2 * put_call_average/10000 * (row.skew_atm_diff),2)
                    else:
                        print("Error in ", row.symbol, 'Back')

                    inserts.append(weekly_skew_scanner(**temp))
                    weekly_skew_scanner.objects.bulk_create(inserts, batch_size=500)



                if (row.a_value != -9999900000000):# and (row.a_value < put_call_a_average - put_call_a_sigma*0):
                    inserts = []
                    temp = dict()
                    temp['time'] = row.time
                    temp['symbol'] = row.symbol
                    temp['spread'] = '4'
                    temp['z'] = zs_a
                    temp['b_value'] = row.b_value
                    temp['a_value'] = row.a_value
                    temp['put_strike'] = row.put_kurt_strike
                    temp['call_strike'] = row.call_kurt_strike
                    temp['put_iv'] = round(row.put_kurt_iv,2)
                    temp['call_iv'] = round(row.call_kurt_iv,2)
                    temp['atm_strike'] = row.atm_strike
                    temp['atm_iv'] = round(row.atm_iv,2)
                    temp['up_rem'] = round(up_rem, 2)
                    temp['down_rem'] = round(down_rem, 2)

                    temp['skew'] = round((2 * row.atm_iv) - (row.call_kurt_iv + row.put_kurt_iv),2)
                    if row.kurt_atm_diff is not None:
                        temp['target'] = round((-2) * put_call_a_average / 100000000 * (row.kurt_atm_diff)**2,2)
                    else:
                        print("Error in ", row.symbol, 'Four Leg')


                    inserts.append(weekly_skew_scanner(**temp))
                    weekly_skew_scanner.objects.bulk_create(inserts, batch_size=500)

                if (row.put_b_value != -999990000):# and ((row.put_skew_iv2 - row.put_skew_iv1) < (put_put_average - put_put_sigma*0)):
                    inserts = []
                    temp = dict()
                    temp['time'] = row.time
                    temp['symbol'] = row.symbol
                    temp['spread'] = 'PPF'
                    temp['z'] = pp_zs
                    temp['b_value'] = row.put_b_value
                    temp['a_value'] = row.put_a_value
                    temp['put_strike'] = row.put_skew_strike2
                    temp['call_strike'] = row.put_skew_strike1
                    temp['put_iv'] = round(row.put_skew_iv2, 2)
                    temp['call_iv'] = round(row.put_skew_iv1, 2)
                    temp['atm_strike'] = row.atm_strike
                    temp['atm_iv'] = round(row.atm_iv, 2)
                    temp['up_rem'] = round(up_rem, 2)
                    temp['down_rem'] = round(down_rem, 2)
                    temp['skew'] = round(row.put_skew_iv1 - row.put_skew_iv2,2)
                    if row.skew_atm_diff is not None:
                        temp['target'] = round((-1) * put_put_average,2)
                    else:
                        print("Error in ", row.symbol, 'PPFront')
                    inserts.append(weekly_skew_scanner(**temp))
                    weekly_skew_scanner.objects.bulk_create(inserts, batch_size=500)

                if (row.call_b_value != -999990000):# and ((row.call_skew_iv1 - row.call_skew_iv2) > (call_call_average + call_call_sigma*0)):
                    inserts = []
                    temp = dict()
                    temp['time'] = row.time
                    temp['symbol'] = row.symbol
                    temp['spread'] = 'CCB'
                    temp['z'] = cc_zs
                    temp['b_value'] = row.call_b_value
                    temp['a_value'] = row.call_a_value
                    temp['put_strike'] = row.call_skew_strike1
                    temp['call_strike'] = row.call_skew_strike2
                    temp['put_iv'] = round(row.call_skew_iv1, 2)
                    temp['call_iv'] = round(row.call_skew_iv2, 2)
                    temp['atm_strike'] = row.atm_strike
                    temp['atm_iv'] = round(row.atm_iv, 2)
                    temp['up_rem'] = round(up_rem, 2)
                    temp['down_rem'] = round(down_rem, 2)
                    temp['skew'] = round(row.call_skew_iv1 - row.call_skew_iv2,2)
                    if row.skew_atm_diff is not None:
                        temp['target'] = round(call_call_average,2)
                    else:
                        print("Error in ", row.symbol, 'CCBack')
                    inserts.append(weekly_skew_scanner(**temp))
                    weekly_skew_scanner.objects.bulk_create(inserts, batch_size=500)

            # self.stdout.write(self.style.SUCCESS('Scanning complete.'))
            time.sleep(10)