import time

import pandas as pd
import datetime
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.pair_table import pair_table
from django.core.management.base import BaseCommand
from django.db.models import Q
import math

def zptile(z_score):
    return .5 * (math.erf(z_score / 2 ** .5) + 1)


today = datetime.datetime.now().date()

corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\correlation.csv")
iv_stats_df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
forward_vol_df_read = pd.read_csv(r"C:\Users\Administrator\Downloads\forward_vol_{}.csv".format(today))
hv_df_read = pd.read_csv(r"C:\Users\Administrator\Downloads\long_move_based_iv.csv")
all_symbols_df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")
all_symbols = all_symbols_df.symbol.unique()
pairs = [[corr_df['stock_1'][i], corr_df['stock_2'][i]] for i in range(len(corr_df))]
class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'
    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        while True:
            # try:
            for pair_list in pairs:
                inserts = []
                symbol_1 = pair_list[0]
                symbol_2 = pair_list[1]
                if not symbol_1 in all_symbols:
                    # print(symbol_1)
                    continue
                if not symbol_2 in all_symbols:
                    continue

                sym_1 = vol_table.objects.filter(symbol=symbol_1).last()
                sym_2 = vol_table.objects.filter(symbol=symbol_2).last()
                objects_to_update = pair_table.objects.filter(symbol_1=symbol_1, symbol_2=symbol_2)
                objects_to_update1 = pair_table.objects.filter(symbol_1=symbol_2, symbol_2=symbol_1)

                if (symbol_1 == 'AXISBANK' or symbol_2 == 'AXISBANK') and (symbol_1 == 'INDUSINDBK' or symbol_2 == 'INDUSINDBK'):
                    print(objects_to_update)
                    print(objects_to_update1)


                try:
                    if sym_1.current_iv == 0 or sym_2.current_iv == 0:
                        continue
                except:
                    continue

                iv_1 = sym_1.current_iv
                iv_2 = sym_2.current_iv

                avg_iv_stat_1 = iv_stats_df[iv_stats_df['symbol'] == symbol_1]['avg_normal_iv'].item()
                avg_iv_stat_2 = iv_stats_df[iv_stats_df['symbol'] == symbol_2]['avg_normal_iv'].item()

                fwd_iv_1 = forward_vol_df_read[forward_vol_df_read['symbol'] == symbol_1]['forward_vol'].item()
                fwd_iv_2 = forward_vol_df_read[forward_vol_df_read['symbol'] == symbol_2]['forward_vol'].item()

                stddev_iv_stat_1 = iv_stats_df[iv_stats_df['symbol'] == symbol_1]['stddev_normal_iv'].item()
                stddev_iv_stat_2 = iv_stats_df[iv_stats_df['symbol'] == symbol_2]['stddev_normal_iv'].item()

                diff_std_diff = corr_df[(corr_df['stock_1'] == symbol_1) & (corr_df['stock_2'] == symbol_2)].std_diff.item()
                try:

                    hv_current_1 = round(hv_df_read[hv_df_read['symbol'] == symbol_1]['move_iv'].item(), 2)
                    hv_current_2 = round(hv_df_read[hv_df_read['symbol'] == symbol_2]['move_iv'].item(), 2)
                except:
                    hv_current_1 = 0
                    hv_current_2 = 0





                z_s_iv_1 = (iv_1 - avg_iv_stat_1) / stddev_iv_stat_1
                z_s_iv_2 = (iv_2 - avg_iv_stat_2) / stddev_iv_stat_2

                ivp_1 = round(zptile(z_s_iv_1) * 100)
                ivp_2 = round(zptile(z_s_iv_2) * 100)



                ##############when to add new symbol
                if not objects_to_update.exists() and not objects_to_update1.exists():

                    temp = dict()
                    inserts = []

                    diff = iv_2 - iv_1
                    conc_df = corr_df[(corr_df['stock_1'] == symbol_1) & (corr_df['stock_2'] == symbol_2)]
                    avg_dif = conc_df.avg_diff.item()

                    # z_score = (round(-diff,2) - round(-avg_dif,2))/diff_std_diff

                    if diff-avg_dif > 0.5:
                        z_score = (round(-diff,2) - round(-avg_dif,2))/diff_std_diff
                        temp = dict()
                        temp['time'] = datetime.datetime.now()
                        temp['signal'] = 'short_1'
                        temp['symbol_1'] = symbol_2
                        temp['iv_1'] = round(iv_2, 2)
                        temp['ivp1'] = int(ivp_2)
                        temp['fwd_iv_1'] = fwd_iv_2
                        temp['hv_current_1'] = hv_current_2
                        temp['avg_iv_1'] = round(conc_df.stock_2_avg_iv.item(), 2)
                        temp['avg_iv_2'] = round(conc_df.stock_1_avg_iv.item(), 2)
                        temp['symbol_2'] = symbol_1
                        temp['iv_2'] = round(iv_1, 2)
                        temp['ivp2'] = int(ivp_1)
                        temp['fwd_iv_2'] = fwd_iv_1
                        temp['hv_current_2'] = hv_current_1
                        temp['target'] = round(-avg_dif,2)
                        temp['short_movement'] = round(conc_df.short_move_2.item(), 2)
                        temp['long_movement'] = round(conc_df.long_move_1.item(), 2)
                        temp['current_diff'] = round(-diff,2)
                        temp['z_score'] = z_score
                        inserts.append(pair_table(**temp))
                        pair_table.objects.bulk_create(inserts, batch_size=500)
                    elif diff-avg_dif < -0.5:
                        z_score = (round(diff,2) - round(avg_dif,2))/diff_std_diff
                        temp = dict()
                        temp['time'] = datetime.datetime.now()
                        temp['signal'] = 'short_1'
                        temp['symbol_1'] = symbol_1
                        temp['iv_1'] = round(iv_1, 2)
                        temp['ivp1'] = int(ivp_1)
                        temp['fwd_iv_1'] = fwd_iv_1
                        temp['hv_current_1'] = hv_current_1
                        temp['avg_iv_1'] = round(conc_df.stock_1_avg_iv.item(), 2)
                        temp['avg_iv_2'] = round(conc_df.stock_2_avg_iv.item(), 2)
                        temp['symbol_2'] = symbol_2
                        temp['iv_2'] = round(iv_2, 2)
                        temp['ivp2'] = int(ivp_2)
                        temp['fwd_iv_2'] = fwd_iv_2
                        temp['hv_current_2'] = hv_current_2
                        temp['short_movement'] = round(conc_df.short_move_1.item(), 2)
                        temp['long_movement'] = round(conc_df.long_move_2.item(), 2)
                        temp['target'] = round(avg_dif,2)
                        temp['current_diff'] = round(diff,2)
                        temp['z_score'] = z_score
                        inserts.append(pair_table(**temp))
                        pair_table.objects.bulk_create(inserts, batch_size=500)
                else:

                    if not objects_to_update.exists():
                        objects_to_update = objects_to_update1
                    diff = iv_2 - iv_1
                    conc_df = corr_df[(corr_df['stock_1'] == symbol_1) & (corr_df['stock_2'] == symbol_2)]
                    avg_dif = conc_df.avg_diff.item()
                    if diff-avg_dif > 0.5:
                        z_score = (round(-diff,2) - round(-avg_dif,2))/diff_std_diff

                        for obj in objects_to_update:
                            obj.time = datetime.datetime.now()
                            obj.signal = 'long_1'
                            obj.symbol_1 = symbol_1
                            obj.iv_1 = round(iv_1, 2)
                            obj.ivp1 = int(ivp_1)
                            obj.fwd_iv_1 = fwd_iv_1
                            obj.hv_current_1 = hv_current_1
                            obj.avg_iv_1 = round(conc_df.stock_1_avg_iv.item(), 2)
                            obj.avg_iv_2 = round(conc_df.stock_2_avg_iv.item(), 2)
                            obj.symbol_2 = symbol_2
                            obj.iv_2 = round(iv_2, 2)
                            obj.ivp2 = int(ivp_2)
                            obj.fwd_iv_2 = fwd_iv_2
                            obj.hv_current_2 = hv_current_2
                            obj.short_movement = round(conc_df.short_move_1.item(), 2)
                            obj.long_movement = round(conc_df.long_move_2.item(), 2)
                            obj.target = round(-avg_dif,2)
                            obj.current_diff = round(-diff,2)
                            obj.z_score = z_score
                            obj.save()
                    elif diff-avg_dif < -0.5:
                        z_score = (round(diff,2) - round(avg_dif,2))/diff_std_diff
                        for obj in objects_to_update:
                            obj.time = datetime.datetime.now()
                            obj.signal = 'short_1'
                            obj.symbol_1 = symbol_1
                            obj.iv_1 = round(iv_1, 2)
                            obj.ivp1 = int(ivp_1)
                            obj.fwd_iv_1 = fwd_iv_1
                            obj.hv_current_1 = hv_current_1
                            obj.avg_iv_1 = round(conc_df.stock_1_avg_iv.item(), 2)
                            obj.avg_iv_2 = round(conc_df.stock_2_avg_iv.item(), 2)
                            obj.symbol_2 = symbol_2
                            obj.iv_2 = round(iv_2, 2)
                            obj.ivp2 = int(ivp_2)
                            obj.fwd_iv_2 = fwd_iv_2
                            obj.hv_current_2 = hv_current_2
                            obj.short_movement = round(conc_df.short_move_2.item(), 2)
                            obj.long_movement = round(conc_df.long_move_1.item(), 2)
                            obj.target = round(avg_dif,2)
                            obj.current_diff = round(diff,2)
                            obj.z_score = z_score
                            obj.save()
                    else:
                        objects_to_update.delete()
                        # objects_to_update1.delete()
            # except:
            #     print('hi')

            time.sleep(10)
            # pair_table.objects.all().delete()
