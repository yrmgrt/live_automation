from django.core.management.base import BaseCommand

# from websocket_trial.models.scanner import scanner_table_skew
from websocket_trial.models.skew_scanner import skew_scanner
# from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.skew_scanner import skew_scanner
from websocket_trial.models.long_short_tb import long_short_tb
from websocket_trial.models.pair_table import pair_table
from websocket_trial.models.fwd_vol import forward_vol_tb
from websocket_trial.models.put_call_OI_tb import put_call_OI_tb
from websocket_trial.models.weekly_skew_scanner import weekly_skew_scanner

from websocket_trial.models.overwatch_db import overwatch_db

import pandas as pd

import tkinter as tk
from tkinter import ttk
from tkinter import *
from tkinter import messagebox
import random
import datetime
import requests
import numpy as np
from scipy.stats import zscore
import warnings
import datetime
import math

warnings.filterwarnings("ignore")
import py_vollib.black_scholes.implied_volatility
import py_vollib.black_scholes.greeks.numerical
import time

def zptile(z_score):
    return .5 * (math.erf(z_score / 2 ** .5) + 1)


daily_morn_df = pd.read_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")
today = daily_morn_df['today'].item()
today_less = daily_morn_df['today_less_15_days'].item()
today_more = daily_morn_df['today_more_15_days'].item()

today_less_8 = daily_morn_df['today_less_8_days'].item()
today_more_8 = daily_morn_df['today_more_8_days'].item()

yest_idv_df = pd.read_csv(r"C:\Users\Administrator\Desktop\idv_cal_yest.csv")

today_date_10_30 = datetime.datetime.now().replace(hour=10, minute=30, second=0, microsecond=0)


class Command(BaseCommand):
    help = 'Scan the PostgreSQL table and perform actions'

    # print(df.iloc[0]['put_std_down_b'])
    def handle(self, *args, **options):
        # Retrieve table values
        future_results = pd.read_csv(r"C:\Users\Administrator\Downloads\results_dates.csv")
        future_results = list(future_results[(future_results['date'] >= today)]['symbol'])

        future_results_1 = pd.read_csv(r"C:\Users\Administrator\Downloads\results_dates.csv")
        future_results_1 = list(
            future_results_1[(future_results_1['date'] >= today) & (future_results_1['date'] <= today_more_8)][
                'symbol'])

        past_results = pd.read_csv(r"C:\Users\Administrator\Downloads\results_dates.csv")
        past_results = list(past_results[(past_results['date'] < today)]['symbol'])

        past_results_1 = pd.read_csv(r"C:\Users\Administrator\Downloads\results_dates.csv")
        past_results_1 = list(
            past_results_1[(past_results_1['date'] >= today_less_8) & (past_results_1['date'] < today)]['symbol'])

        root = tk.Tk()
        root.title("Overwatch")
        root.geometry('800x600')
        root.configure(bg='gray28')

        total_no_of_frames = 7

        frames = []
        for i in range(total_no_of_frames):
            frame = tk.Frame(root, bg='gray28', borderwidth=0, highlightthickness=0, padx=0, pady=0)
            frames.append(frame)

        # Create a frame to hold the data table
        (frame1, frame2, frame3, frame4, frame5, frame6, frame7) = tuple(frames[0:7])

        my_frames = []
        my_frames = my_frames + [frame1, frame2, frame3, frame4, frame5, frame6, frame7]

        centre_orient = tk.N + tk.E + tk.S + tk.W
        left_orient = tk.NW

        coord_place = []
        coord_place = coord_place + [[0,1,i,1,centre_orient,0,0] for i in range(7)]

        for i, frame in enumerate(my_frames):
            frame.grid(row=coord_place[i][0], rowspan=coord_place[i][1], column=coord_place[i][2],
                       columnspan=coord_place[i][3], sticky=coord_place[i][4],
                       padx=coord_place[i][5], pady=coord_place[i][6], ipadx=0, ipady=0)
            frame.configure(borderwidth=0, highlightthickness=0)

        columns1 = ('time',)
        columns2 = ('type', 'Symbol1', 'Symbol2')
        columns3 = ('iv1',)
        columns4 = ('iv2',)
        columns5 = ('curr_val',)
        columns6 = ('prev_time',)
        columns7 = ('prev_val',)

        table_columns = []
        table_columns = table_columns + [columns1, columns2, columns3, columns4, columns5, columns6, columns7]

        table_heights = []
        for i in range(7):
            table_heights.append(40)

        tree_tables = []

        for i, height in enumerate(table_heights):
            table = ttk.Treeview(my_frames[i], columns=table_columns[i], show='headings', height=height)
            tree_tables.append(table)

        (table1, table2, table3, table4, table5, table6, table7) = tuple(tree_tables[0:7])

        my_tables = []
        my_tables = my_tables + [table1, table2, table3, table4, table5, table6, table7]

        style = ttk.Style()
        style.configure("Treeview", font=("Arial", 8), rowheight=16)
        style.configure("Treeview.Heading", height=16)

        for table in my_tables:
            table.tag_configure('future_result', background='light blue')
            table.tag_configure('past_result', background='bisque')
            table.tag_configure('recent_change', background='IndianRed1')
            table.tag_configure('long_highlight', background='aquamarine')
            table.tag_configure('short_highlight', background='LightPink1')
            table.tag_configure('z_highlight', background='plum1')
            table.tag_configure('%_neg_highlight', background='coral1')
            table.tag_configure('%_pos_highlight', background='medium spring green')

        widths = []

        widths = widths + [[70], [40, 70, 70], [30], [30], [50], [70], [50]]

        # print(my_tables)
        # print(table_columns)

        for i, table in enumerate(my_tables):
            # print(table)
            # print(table_columns[i])
            for j, col in enumerate(table_columns[i]):
                # print(col)
                table.heading(col, text=col)
                if widths[i][j] == 100:
                    table.column(col, width=widths[i][j], anchor=tk.W)
                else:
                    table.column(col, width=widths[i][j], anchor=tk.CENTER)

            table.pack()

        while True:

            for table in my_tables:
                for item in table.get_children():
                    table.delete(item)

            table_df1 = skew_scanner.objects.all()
            table_df1 = list(table_df1.values())
            table_df1 = pd.DataFrame(table_df1)

            # print(table_df1.columns)
            table_df1_1 = pd.DataFrame()
            table_df1_2 = pd.DataFrame()
            table_df1_3 = pd.DataFrame()
            table_df1_4 = pd.DataFrame()

            table_df2 = long_short_tb.objects.all()
            table_df2 = list(table_df2.values())
            table_df2 = pd.DataFrame(table_df2)

            table_df2_1 = pd.DataFrame()
            table_df2_2 = pd.DataFrame()
            # print(table_df2.columns)

            table_df3 = pair_table.objects.all()
            table_df3 = list(table_df3.values())
            table_df3 = pd.DataFrame(table_df3)
            if not table_df3.empty:
                table_df3['diff_val'] = table_df3['current_diff'] - table_df3['target']

            # print(table_df3.columns)

            table_df4 = forward_vol_tb.objects.all()
            table_df4 = list(table_df4.values())
            table_df4 = pd.DataFrame(table_df4)
            if not table_df4.empty:
                table_df4['diff_val'] = table_df4['current_iv'] - table_df4['fwd_vol']
            # print(table_df4.columns)

            table_df4_1 = pd.DataFrame()
            table_df4_2 = pd.DataFrame()

            try:
                table_df5 = pd.read_csv(r"C:\Users\Administrator\Desktop\idv_cal.csv")
            except:
                pass

            table_df6 = weekly_skew_scanner.objects.all()
            table_df6 = list(table_df6.values())
            table_df6 = pd.DataFrame(table_df6)

            table_df7 = table_df2.copy()
            table_df8 = table_df2.copy()

            if not table_df7.empty:
                table_df7['sort_ratio'] = table_df7['current_iv']/table_df7['hv_current']

            # table_df6 = put_call_OI_tb.objects.all()
            # table_df6 = list(table_df6.values())
            # table_df6 = pd.DataFrame(table_df6)

            if not table_df1.empty:
                table_df1_1 = table_df1[table_df1['spread'] == '4']
                table_df1_1.sort_values(by=['z'], ascending=True, inplace=True)
                table_df1_2 = table_df1[table_df1['spread'] == 'PCF']
                table_df1_2.sort_values(by=['z'], ascending=True, inplace=True)
                table_df1_3 = table_df1[table_df1['spread'] == 'PPF']
                table_df1_3.sort_values(by=['z'], ascending=True, inplace=True)
                table_df1_4 = table_df1[table_df1['spread'] == 'CCB']
                table_df1_4.sort_values(by=['z'], ascending=False, inplace=True)

            if not table_df2.empty:
                table_df2_1 = table_df2.sort_values(by=['ivp', 'rp_diff'], ascending=[False, False])
                table_df2_2 = table_df2.sort_values(by=['ivp'], ascending=True)
            if not table_df3.empty:
                table_df3.sort_values(by=['z_score'], ascending=True, inplace=True)
            if not table_df4.empty:
                table_df4_1 = table_df4[table_df4['diff_val'] >= 0]
                table_df4_1['diff_val_pct'] = table_df4_1['diff_val']/table_df4_1['fwd_vol']
                table_df4_1.sort_values(by=['diff_val_pct'], ascending=False, inplace=True)
                table_df4_2 = table_df4[table_df4['diff_val'] < 0]
                table_df4_2['diff_val_pct'] = (table_df4_2['diff_val']/table_df4_2['fwd_vol'])*(-1)
                table_df4_2.sort_values(by=['diff_val_pct'], ascending=False, inplace=True)

            if not table_df5.empty:
                table_df5.sort_values(by=['long_moves','IVP'], ascending=[False,True], inplace=True)

            # if not table_df6.empty:
            #     table_df6.sort_values(by=['put_call_ratio'], ascending=False, inplace=True)

            if not table_df6.empty:
                table_df6.sort_values(by=['symbol','spread'], ascending=[True,False], inplace=True)


            if not table_df1.empty and not table_df5.empty:
                table_df1_1 = pd.merge(table_df1_1, table_df5[['symbol', 'pct_change']], on='symbol',
                                       how='left')
                table_df1_2 = pd.merge(table_df1_2, table_df5[['symbol', 'pct_change']], on='symbol',
                                       how='left')
                table_df1_3 = pd.merge(table_df1_3, table_df5[['symbol', 'pct_change']], on='symbol',
                                       how='left')
                table_df1_4 = pd.merge(table_df1_4, table_df5[['symbol', 'pct_change']], on='symbol',
                                       how='left')

            if not table_df2.empty and not table_df5.empty:
                table_df2_1 = pd.merge(table_df2_1, table_df5[['symbol', 'long_moves', 'pct_change']], on='symbol',
                                       how='left')
                table_df2_1['days_theta'] = table_df2_1['long_moves'] / 2
                table_df2_2 = pd.merge(table_df2_2, table_df5[['symbol', 'long_moves', 'pct_change']], on='symbol',
                                       how='left')
                table_df2_2['days_theta'] = table_df2_2['long_moves'] / 2

            if not table_df3.empty and not table_df5.empty:
                table_df3['symbol'] = table_df3['symbol_1']
                table_df3 = pd.merge(table_df3, table_df5[['symbol', 'long_moves', 'pct_change']], on='symbol', how='left')
                # print(table_df3)
                # print(table_df3_temp)
                table_df3['days_theta_1'] = table_df3['long_moves'] / 2
                table_df3['pct_change_1'] = table_df3['pct_change']

                table_df3.drop(['long_moves'], axis=1, inplace=True)
                table_df3.drop(['pct_change'], axis=1, inplace=True)

                table_df3['symbol'] = table_df3['symbol_2']
                table_df3 = pd.merge(table_df3, table_df5[['symbol', 'long_moves', 'pct_change']], on='symbol', how='left')
                table_df3['days_theta_2'] = table_df3['long_moves'] / 2
                table_df3['pct_change_2'] = table_df3['pct_change']

            if not table_df4.empty and not table_df5.empty:
                table_df4_1 = pd.merge(table_df4_1, table_df5[['symbol', 'long_moves', 'pct_change']], on='symbol', how='left')
                table_df4_1['days_theta'] = table_df4_1['long_moves'] / 2
                table_df4_2 = pd.merge(table_df4_2, table_df5[['symbol', 'long_moves', 'pct_change']], on='symbol', how='left')
                table_df4_2['days_theta'] = table_df4_2['long_moves'] / 2

            if not table_df7.empty and not table_df5.empty:

                table_df7 = pd.merge(table_df7, table_df5[['symbol', 'long_moves', 'pct_change', 'bench_mark_iv']], on='symbol',
                                       how='left')
                table_df7['days_theta'] = table_df7['long_moves'] / 2

                table_df7 = pd.merge(table_df7, yest_idv_df[['symbol', 'long_moves_yest']], on='symbol',
                                       how='left')
                table_df7.sort_values(by=['sort_ratio'], ascending=False, inplace=True)
                table_df8 = table_df7.sort_values(by=['sort_ratio'], ascending=True)

            for i, row in table_df1_1.head(14).iterrows():

                z_percentile = round(row['z'],1)

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='4_L')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(z_percentile - objects_exist.curr_percentile) >= 0.5:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = '4_L'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['atm_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = row['skew']
                        temp['curr_percentile'] = z_percentile
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = '4_L'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['atm_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = row['skew']
                    temp['curr_percentile'] = z_percentile
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = row['skew']
                    temp['prev_percentile'] = z_percentile

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            for i, row in table_df1_2.head(14).iterrows():

                z_percentile = round(row['z'],1)

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='PCF')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(z_percentile - objects_exist.curr_percentile) >= 0.5:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'PCF'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['atm_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = row['skew']
                        temp['curr_percentile'] = z_percentile
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'PCF'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['atm_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = row['skew']
                    temp['curr_percentile'] = z_percentile
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = row['skew']
                    temp['prev_percentile'] = z_percentile

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            for i, row in table_df1_3.head(14).iterrows():
                z_percentile = round(row['z'],1)

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='PPF')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(z_percentile - objects_exist.curr_percentile) >= 0.5:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'PPF'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['atm_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = row['skew']
                        temp['curr_percentile'] = z_percentile
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'PPF'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['atm_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = row['skew']
                    temp['curr_percentile'] = z_percentile
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = row['skew']
                    temp['prev_percentile'] = z_percentile

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            for i, row in table_df1_4.head(14).iterrows():
                z_percentile = round(row['z'],1)

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='CCB')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(z_percentile - objects_exist.curr_percentile) >= 0.5:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'CCB'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['atm_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = row['skew']
                        temp['curr_percentile'] = z_percentile
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'CCB'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['atm_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = row['skew']
                    temp['curr_percentile'] = z_percentile
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = row['skew']
                    temp['prev_percentile'] = z_percentile

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            # root.update()
            for i, row in table_df2_1.head(28).iterrows():

                ivp = int(row['ivp'])

                if row['current_iv'] == 0:
                    continue

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='short')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(ivp - objects_exist.curr_percentile) >= 10 or abs(row['current_iv'] - objects_exist.curr_val)/row['current_iv'] >= 0.05:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'short'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['current_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = round(row['current_iv'],1)
                        temp['curr_percentile'] = ivp
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'short'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['current_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = round(row['current_iv'],1)
                    temp['curr_percentile'] = ivp
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = round(row['current_iv'],1)
                    temp['prev_percentile'] = ivp

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            for i, row in table_df2_2.head(28).iterrows():

                ivp = int(row['ivp'])

                if row['current_iv'] == 0:
                    continue

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='long')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(ivp - objects_exist.curr_percentile) >= 10 or abs(row['current_iv'] - objects_exist.curr_val)/row['current_iv'] >= 0.05:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'long'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['current_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = round(row['current_iv'],1)
                        temp['curr_percentile'] = ivp
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'long'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['current_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = round(row['current_iv'],1)
                    temp['curr_percentile'] = ivp
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = round(row['current_iv'],1)
                    temp['prev_percentile'] = ivp

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            for i, row in table_df3.head(28).iterrows():

                z_percentile = round(row['z_score'],1)

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol_1'], symbol2=row['symbol_2'], type='pair')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(z_percentile - objects_exist.curr_percentile) >= 0.5:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'pair'
                        temp['symbol1'] = row['symbol_1']
                        temp['symbol2'] = row['symbol_2']
                        temp['iv1'] = round(row['iv_1'],1)
                        temp['iv2'] = round(row['iv_2'],1)
                        temp['curr_val'] = round(row['current_diff'],1)
                        temp['curr_percentile'] = z_percentile
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'pair'
                    temp['symbol1'] = row['symbol_1']
                    temp['symbol2'] = row['symbol_2']
                    temp['iv1'] = round(row['iv_1'],1)
                    temp['iv2'] = round(row['iv_2'],1)
                    temp['curr_val'] = round(row['current_diff'],1)
                    temp['curr_percentile'] = z_percentile
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = round(row['current_diff'],1)
                    temp['prev_percentile'] = z_percentile

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            for i, row in table_df4_1.head(14).iterrows():

                ivp = int(row['ivp'])

                if row['current_iv'] == 0:
                    continue

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='abv_fwd')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(ivp - objects_exist.curr_percentile) >= 10 or abs(row['current_iv'] - objects_exist.curr_val)/row['current_iv'] >= 0.05:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'abv_fwd'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['current_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = round(row['current_iv'],1)
                        temp['curr_percentile'] = ivp
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'abv_fwd'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['current_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = round(row['current_iv'],1)
                    temp['curr_percentile'] = ivp
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = round(row['current_iv'],1)
                    temp['prev_percentile'] = ivp

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            for i, row in table_df4_2.head(14).iterrows():
                ivp = int(row['ivp'])

                if row['current_iv'] == 0:
                    continue

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='blw_fwd')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(ivp - objects_exist.curr_percentile) >= 10 or abs(row['current_iv'] - objects_exist.curr_val)/row['current_iv'] >= 0.05:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'blw_fwd'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['current_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = round(row['current_iv'],1)
                        temp['curr_percentile'] = ivp
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'blw_fwd'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['current_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = round(row['current_iv'],1)
                    temp['curr_percentile'] = ivp
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = round(row['current_iv'],1)
                    temp['prev_percentile'] = ivp

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            for i, row in table_df5.head(28).iterrows():

                ivp = int(row['IVP'])

                if row['current_iv'] == 0:
                    continue

                objects_exist = overwatch_db.objects.filter(symbol1=row['symbol'], type='theta_cover')
                if objects_exist.exists():
                    objects_exist = objects_exist.last()
                    if abs(ivp - objects_exist.curr_percentile) >= 10 or abs(row['current_iv'] - objects_exist.curr_val)/row['current_iv'] >= 0.05:
                        inserts = []
                        temp = dict()
                        #
                        temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                        temp['type'] = 'theta_cover'
                        temp['symbol1'] = row['symbol']
                        temp['symbol2'] = 'NA'
                        temp['iv1'] = round(row['current_iv'],1)
                        temp['iv2'] = 0
                        temp['curr_val'] = round(row['current_iv'],1)
                        temp['curr_percentile'] = ivp
                        temp['prev_time'] = objects_exist.curr_time
                        temp['prev_val'] = objects_exist.curr_val
                        temp['prev_percentile'] = objects_exist.curr_percentile

                        inserts.append(overwatch_db(**temp))
                        overwatch_db.objects.bulk_create(inserts, batch_size=500)
                else:
                    inserts = []
                    temp = dict()
                    #
                    temp['curr_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['type'] = 'theta_cover'
                    temp['symbol1'] = row['symbol']
                    temp['symbol2'] = 'NA'
                    temp['iv1'] = round(row['current_iv'],1)
                    temp['iv2'] = 0
                    temp['curr_val'] = round(row['current_iv'],1)
                    temp['curr_percentile'] = ivp
                    temp['prev_time'] = datetime.datetime.now().replace(microsecond=0)
                    temp['prev_val'] = round(row['current_iv'],1)
                    temp['prev_percentile'] = ivp

                    inserts.append(overwatch_db(**temp))
                    overwatch_db.objects.bulk_create(inserts, batch_size=500)

            overwatch_table_df = overwatch_db.objects.all()
            overwatch_table_df = list(overwatch_table_df.values())
            overwatch_table_df = pd.DataFrame(overwatch_table_df)
            if not overwatch_table_df.empty:
                overwatch_table_df = overwatch_table_df.sort_values(by='curr_time', ascending=False)

            for i, row in overwatch_table_df.head(40).iterrows():
                vals0 = row['curr_time']
                if row['symbol2'] == 'NA':
                    vals1 = (row['type'], row['symbol1'], ' ')
                else:
                    vals1 = (row['type'], row['symbol1'], row['symbol2'])
                vals2 = row['iv1']
                if row['iv2'] == 0:
                    vals3 = ' '
                else:
                    vals3 = row['iv2']
                vals4 = row['curr_val']
                vals5 = row['prev_time']
                vals6 = row['prev_val']

                table1.insert('', 'end', values=vals0)
                table2.insert('', 'end', values=vals1)
                table3.insert('', 'end', values=vals2)
                table4.insert('', 'end', values=vals3)
                table5.insert('', 'end', values=vals4)
                table6.insert('', 'end', values=vals5)
                table7.insert('', 'end', values=vals6)

            root.update()