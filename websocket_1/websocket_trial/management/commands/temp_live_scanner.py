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

warnings.filterwarnings("ignore")
import py_vollib.black_scholes.implied_volatility
import py_vollib.black_scholes.greeks.numerical
import time

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
        root.title("Live Scanner")
        root.geometry('800x600')
        root.configure(bg='gray28')

        total_no_of_frames = 83

        frames = []
        for i in range(total_no_of_frames):
            frame = tk.Frame(root, bg='gray28', borderwidth=0, highlightthickness=0, padx=0, pady=0)
            frames.append(frame)

        # Create a frame to hold the data table
        (frame1_1_0, frame1_1_1, frame1_1_2, frame1_1_3, frame1_1_4) = tuple(frames[0:5])
        (frame1_2_0, frame1_2_1, frame1_2_2, frame1_2_3, frame1_2_4) = tuple(frames[5:10])
        (frame1_3_0, frame1_3_1, frame1_3_2, frame1_3_3, frame1_3_4) = tuple(frames[10:15])
        (frame1_4_0, frame1_4_1, frame1_4_2, frame1_4_3, frame1_4_4) = tuple(frames[15:20])

        (frame2_1_0, frame2_1_1, frame2_1_2, frame2_1_3, frame2_1_4, frame2_1_5, frame2_1_6, frame2_1_7) = tuple(frames[20:28])
        (frame2_2_0, frame2_2_1, frame2_2_2, frame2_2_3, frame2_2_4, frame2_2_5, frame2_2_6, frame2_2_7) = tuple(frames[28:36])

        (frame3_1_0, frame3_1_1, frame3_1_2, frame3_1_3, frame3_1_4, frame3_1_5, frame3_1_6) = tuple(frames[36:43])
        (frame3_2_0, frame3_2_1, frame3_2_2, frame3_2_3, frame3_2_4, frame3_2_5, frame3_2_6) = tuple(frames[43:50])
        frame3_3 = frames[50]

        (frame4_1_0, frame4_1_1, frame4_1_2, frame4_1_3, frame4_1_4, frame4_1_5, frame4_1_6) = tuple(frames[51:58])
        (frame4_2_0, frame4_2_1, frame4_2_2, frame4_2_3, frame4_2_4, frame4_2_5, frame4_2_6) = tuple(frames[58:65])

        (frame5_0, frame5_1, frame5_2, frame5_3, frame5_4, frame5_5, frame5_6) = tuple(frames[65:72])

        (frame6_1, frame6_2, frame6_3, frame6_4) = tuple(frames[72:76])

        (frame7_0, frame7_1, frame7_2, frame7_3, frame7_4, frame7_5, frame7_6) = tuple(frames[76:83])

        my_frames = []
        my_frames = my_frames + [frame1_1_0, frame1_1_1, frame1_1_2, frame1_1_3, frame1_1_4]
        my_frames = my_frames + [frame1_2_0, frame1_2_1, frame1_2_2, frame1_2_3, frame1_2_4]
        my_frames = my_frames + [frame1_3_0, frame1_3_1, frame1_3_2, frame1_3_3, frame1_3_4]
        my_frames = my_frames + [frame1_4_0, frame1_4_1, frame1_4_2, frame1_4_3, frame1_4_4]

        my_frames = my_frames + [frame2_1_0, frame2_1_1, frame2_1_2, frame2_1_3, frame2_1_4, frame2_1_5, frame2_1_6, frame2_1_7]
        my_frames = my_frames + [frame2_2_0, frame2_2_1, frame2_2_2, frame2_2_3, frame2_2_4, frame2_2_5, frame2_2_6, frame2_2_7]

        my_frames = my_frames + [frame3_1_0, frame3_1_1, frame3_1_2, frame3_1_3, frame3_1_4, frame3_1_5, frame3_1_6]
        my_frames = my_frames + [frame3_2_0, frame3_2_1, frame3_2_2, frame3_2_3, frame3_2_4, frame3_2_5, frame3_2_6]
        my_frames = my_frames + [frame3_3]

        my_frames = my_frames + [frame4_1_0, frame4_1_1, frame4_1_2, frame4_1_3, frame4_1_4, frame4_1_5, frame4_1_6]
        my_frames = my_frames + [frame4_2_0, frame4_2_1, frame4_2_2, frame4_2_3, frame4_2_4, frame4_2_5, frame4_2_6]

        my_frames = my_frames + [frame5_0, frame5_1, frame5_2, frame5_3, frame5_4, frame5_5, frame5_6]

        my_frames = my_frames + [frame6_1, frame6_2, frame6_3, frame6_4]

        my_frames = my_frames + [frame7_0, frame7_1, frame7_2, frame7_3, frame7_4, frame7_5, frame7_6]

        centre_orient = tk.N + tk.E + tk.S + tk.W
        left_orient = tk.NW

        coord_place = []
        coord_place = coord_place + [[0,1,i,1,centre_orient,0,0] for i in range(5)]
        coord_place = coord_place + [[1,1,i,1,centre_orient,0,0] for i in range(5)]
        coord_place = coord_place + [[2,1,i,1,centre_orient,0,0] for i in range(5)]
        coord_place = coord_place + [[3,1,i,1,centre_orient,0,0] for i in range(5)]

        coord_place = coord_place + [[0,2,5,1,centre_orient,(2,0),0]]
        coord_place = coord_place + [[0,2,6+i,1,centre_orient,0,0] for i in range(7)]
        coord_place = coord_place + [[2,2,5,1,centre_orient,(2,0),(2,0)]]
        coord_place = coord_place + [[2,2,6+i,1,centre_orient,0,(2,0)] for i in range(7)]

        coord_place = coord_place + [[0,2,13,1,centre_orient,(2,0),0]]
        coord_place = coord_place + [[0,2,14+i,1,centre_orient,0,0] for i in range(6)]
        coord_place = coord_place + [[0,2,20,1,centre_orient,(2,0),0]]
        coord_place = coord_place + [[0,2,21+i,1,centre_orient,0,0] for i in range(6)]
        coord_place = coord_place + [[0,2,27+i,1,left_orient,(2,0),0] for i in range(1)]

        coord_place = coord_place + [[2,1,13,1,centre_orient,(2,0),(2,0)]]
        coord_place = coord_place + [[2,1,14+i,1,centre_orient,0,(2,0)] for i in range(6)]
        coord_place = coord_place + [[3,1,13,1,centre_orient,(2,0),0]]
        coord_place = coord_place + [[3,1,14+i,1,centre_orient,0,0] for i in range(6)]

        coord_place = coord_place + [[2,2,20,1,centre_orient,(2,0),(2,0)]]
        coord_place = coord_place + [[2,2,21+i,1,centre_orient,0,(2,0)] for i in range(6)]

        coord_place = coord_place + [[2,1,50,1,centre_orient,(2,0),(2,0)]]
        coord_place = coord_place + [[2,1,51+i,1,centre_orient,0,(2,0)] for i in range(3)]

        coord_place = coord_place + [[0,2,28,1,centre_orient,(2,0),0]]
        coord_place = coord_place + [[0,2,29+i,1,centre_orient,0,0] for i in range(6)]

        for i, frame in enumerate(my_frames):
            frame.grid(row=coord_place[i][0], rowspan=coord_place[i][1], column=coord_place[i][2],
                       columnspan=coord_place[i][3], sticky=coord_place[i][4],
                       padx=coord_place[i][5], pady=coord_place[i][6], ipadx=0, ipady=0)
            frame.configure(borderwidth=0, highlightthickness=0)

        columns1_1_0 = ('%')
        columns1_1_1 = ('4_L', 'iv')
        columns1_1_2 = ('z',)
        columns1_1_3 = ('IVP',)
        columns1_1_4 = ('put', 'call', 'skew', 'target')

        columns1_2_0 = ('%')
        columns1_2_1 = ('PCF', 'iv')
        columns1_2_2 = ('z',)
        columns1_2_3 = ('IVP',)
        columns1_2_4 = ('put', 'call', 'skew', 'target')

        columns1_3_0 = ('%')
        columns1_3_1 = ('PPF', 'iv')
        columns1_3_2 = ('z',)
        columns1_3_3 = ('IVP',)
        columns1_3_4 = ('put1', 'put2', 'skew', 'target')

        columns1_4_0 = ('%')
        columns1_4_1 = ('CCB', 'iv')
        columns1_4_2 = ('z',)
        columns1_4_3 = ('IVP',)
        columns1_4_4 = ('call1', 'call2', 'skew', 'target')

        columns2_1_0 = ('%')
        columns2_1_1 = ('short', 'iv')
        columns2_1_2 = ('fwd_iv',)
        columns2_1_3 = ('hv',)
        columns2_1_4 = ('fair_iv',)
        columns2_1_5 = ('IVP',)
        columns2_1_6 = ('rp_diff',)
        columns2_1_7 = ('days_theta',)

        columns2_2_0 = ('%')
        columns2_2_1 = ('long', 'iv')
        columns2_2_2 = ('fwd_iv',)
        columns2_2_3 = ('hv',)
        columns2_2_4 = ('fair_iv',)
        columns2_2_5 = ('IVP',)
        columns2_2_6 = ('rp_diff',)
        columns2_2_7 = ('days_theta',)

        columns3_1_0 = ('%')
        columns3_1_1 = ('pair_1_short', 'iv_1')
        columns3_1_2 = ('fwd_iv',)
        columns3_1_3 = ('hv',)
        columns3_1_4 = ('fair_iv',)
        columns3_1_5 = ('IVP1',)
        columns3_1_6 = ('days_theta',)

        columns3_2_0 = ('%')
        columns3_2_1 = ('pair_2_long', 'iv_2')
        columns3_2_2 = ('fwd_iv',)
        columns3_2_3 = ('hv',)
        columns3_2_4 = ('fair_iv',)
        columns3_2_5 = ('IVP2',)
        columns3_2_6 = ('days_theta',)

        columns3_3 = ('z', 'diff', 'target')

        columns4_1_0 = ('%')
        columns4_1_1 = ('abv_fwd', 'iv')
        columns4_1_2 = ('fwd_iv',)
        columns4_1_3 = ('hv',)
        columns4_1_4 = ('fair_iv',)
        columns4_1_5 = ('IVP',)
        columns4_1_6 = ('days_theta',)

        columns4_2_0 = ('%')
        columns4_2_1 = ('blw_fwd', 'iv')
        columns4_2_2 = ('fwd_iv',)
        columns4_2_3 = ('hv',)
        columns4_2_4 = ('fair_iv',)
        columns4_2_5 = ('IVP',)
        columns4_2_6 = ('days_theta',)

        columns5_0 = ('%')
        columns5_1 = ('theta_cover', 'iv')
        columns5_2 = ('fwd_iv',)
        columns5_3 = ('hv',)
        columns5_4 = ('fair_iv',)
        columns5_5 = ('IVP',)
        columns5_6 = ('days_theta',)

        columns6_1 = ('sym', 'iv')
        columns6_2 = ('z',)
        columns6_3 = ('spr',)
        columns6_4 = ('str1', 'str2', 'skew', 'target')

        columns7_0 = ('%')
        columns7_1 = ('intra_short', 'iv')
        columns7_2 = ('fwd_iv',)
        columns7_3 = ('hv',)
        columns7_4 = ('fair_iv',)
        columns7_5 = ('IVP',)
        columns7_6 = ('days_theta',)

        # columns6 = ('symbol', 'put_OI', 'call_OI', 'p_c_ratio')

        table_columns = []
        table_columns = table_columns + [columns1_1_0, columns1_1_1, columns1_1_2, columns1_1_3, columns1_1_4]
        table_columns = table_columns + [columns1_2_0, columns1_2_1, columns1_2_2, columns1_2_3, columns1_2_4]
        table_columns = table_columns + [columns1_3_0, columns1_3_1, columns1_3_2, columns1_3_3, columns1_3_4]
        table_columns = table_columns + [columns1_4_0, columns1_4_1, columns1_4_2, columns1_4_3, columns1_4_4]

        table_columns = table_columns + [columns2_1_0, columns2_1_1, columns2_1_2, columns2_1_3, columns2_1_4, columns2_1_5, columns2_1_6, columns2_1_7]
        table_columns = table_columns + [columns2_2_0, columns2_2_1, columns2_2_2, columns2_2_3, columns2_2_4, columns2_2_5, columns2_2_6, columns2_2_7]

        table_columns = table_columns + [columns3_1_0, columns3_1_1, columns3_1_2, columns3_1_3, columns3_1_4, columns3_1_5, columns3_1_6]
        table_columns = table_columns + [columns3_2_0, columns3_2_1, columns3_2_2, columns3_2_3, columns3_2_4, columns3_2_5, columns3_2_6]
        table_columns = table_columns + [columns3_3]

        table_columns = table_columns + [columns4_1_0, columns4_1_1, columns4_1_2, columns4_1_3, columns4_1_4, columns4_1_5, columns4_1_6]
        table_columns = table_columns + [columns4_2_0, columns4_2_1, columns4_2_2, columns4_2_3, columns4_2_4, columns4_2_5, columns4_2_6]

        table_columns = table_columns + [columns5_0, columns5_1, columns5_2, columns5_3, columns5_4, columns5_5, columns5_6]

        table_columns = table_columns + [columns6_1, columns6_2, columns6_3, columns6_4]

        table_columns = table_columns + [columns7_0, columns7_1, columns7_2, columns7_3, columns7_4, columns7_5, columns7_6]

        table_heights = []
        for i in range(20):
            table_heights.append(13)
        for i in range(16):
            table_heights.append(28)
        for i in range(15):
            table_heights.append(28)
        for i in range(14):
            table_heights.append(13)
        for i in range(7):
            table_heights.append(28)
        for i in range(4):
            table_heights.append(13)
        for i in range(7):
            table_heights.append(28)

        tree_tables = []

        for i, height in enumerate(table_heights):
            table = ttk.Treeview(my_frames[i], columns=table_columns[i], show='headings', height=height)
            tree_tables.append(table)

        (table1_1_0, table1_1_1, table1_1_2, table1_1_3, table1_1_4) = tuple(tree_tables[0:5])
        (table1_2_0, table1_2_1, table1_2_2, table1_2_3, table1_2_4) = tuple(tree_tables[5:10])
        (table1_3_0, table1_3_1, table1_3_2, table1_3_3, table1_3_4) = tuple(tree_tables[10:15])
        (table1_4_0, table1_4_1, table1_4_2, table1_4_3, table1_4_4) = tuple(tree_tables[15:20])

        (table2_1_0, table2_1_1, table2_1_2, table2_1_3, table2_1_4, table2_1_5, table2_1_6, table2_1_7) = tuple(tree_tables[20:28])
        (table2_2_0, table2_2_1, table2_2_2, table2_2_3, table2_2_4, table2_2_5, table2_2_6, table2_2_7) = tuple(tree_tables[28:36])

        (table3_1_0, table3_1_1, table3_1_2, table3_1_3, table3_1_4, table3_1_5, table3_1_6) = tuple(tree_tables[36:43])
        (table3_2_0, table3_2_1, table3_2_2, table3_2_3, table3_2_4, table3_2_5, table3_2_6) = tuple(tree_tables[43:50])
        table3_3 = tree_tables[50]

        (table4_1_0, table4_1_1, table4_1_2, table4_1_3, table4_1_4, table4_1_5, table4_1_6) = tuple(tree_tables[51:58])
        (table4_2_0, table4_2_1, table4_2_2, table4_2_3, table4_2_4, table4_2_5, table4_2_6) = tuple(tree_tables[58:65])

        (table5_0, table5_1, table5_2, table5_3, table5_4, table5_5, table5_6) = tuple(tree_tables[65:72])

        (table6_1, table6_2, table6_3, table6_4) = tuple(tree_tables[72:76])

        (table7_0, table7_1, table7_2, table7_3, table7_4, table7_5, table7_6) = tuple(tree_tables[76:83])

        my_tables = []
        my_tables = my_tables + [table1_1_0, table1_1_1, table1_1_2, table1_1_3, table1_1_4]
        my_tables = my_tables + [table1_2_0, table1_2_1, table1_2_2, table1_2_3, table1_2_4]
        my_tables = my_tables + [table1_3_0, table1_3_1, table1_3_2, table1_3_3, table1_3_4]
        my_tables = my_tables + [table1_4_0, table1_4_1, table1_4_2, table1_4_3, table1_4_4]

        my_tables = my_tables + [table2_1_0, table2_1_1, table2_1_2, table2_1_3, table2_1_4, table2_1_5, table2_1_6, table2_1_7]
        my_tables = my_tables + [table2_2_0, table2_2_1, table2_2_2, table2_2_3, table2_2_4, table2_2_5, table2_2_6, table2_2_7]

        my_tables = my_tables + [table3_1_0, table3_1_1, table3_1_2, table3_1_3, table3_1_4, table3_1_5, table3_1_6]
        my_tables = my_tables + [table3_2_0, table3_2_1, table3_2_2, table3_2_3, table3_2_4, table3_2_5, table3_2_6]
        my_tables = my_tables + [table3_3]

        my_tables = my_tables + [table4_1_0, table4_1_1, table4_1_2, table4_1_3, table4_1_4, table4_1_5, table4_1_6]
        my_tables = my_tables + [table4_2_0, table4_2_1, table4_2_2, table4_2_3, table4_2_4, table4_2_5, table4_2_6]
        my_tables = my_tables + [table5_0, table5_1, table5_2, table5_3, table5_4, table5_5, table5_6]

        my_tables = my_tables + [table6_1, table6_2, table6_3, table6_4]

        my_tables = my_tables + [table7_0, table7_1, table7_2, table7_3, table7_4, table7_5, table7_6]

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
        for i in range(4):
            widths = widths + [[30], [70, 30], [30], [30], [30, 30, 30, 30]]
        for i in range(2):
            widths = widths + [[30], [70, 30], [30], [30], [30], [30], [30], [30]]
        for i in range(2):
            widths = widths + [[30], [70, 30], [30], [30], [30], [30], [30]]
        widths = widths + [[30, 30, 30]]
        for i in range(2):
            widths = widths + [[30], [70, 30], [30], [30], [30], [30], [30]]

        widths = widths + [[30], [70, 30], [30], [30], [30], [30], [30]]

        widths = widths + [[70, 30], [30], [30], [30, 30, 30, 30]]

        widths = widths + [[30], [70, 30], [30], [30], [30], [30], [30]]

        # print(my_tables)
        # print(table_columns)

        for i, table in enumerate(my_tables):
            # print(table)
            # print(table_columns[i])
            for j, col in enumerate(table_columns[i]):
                # print(col)
                table.heading(col, text=col)
                if widths[i][j] == 70:
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
                table_df2_1 = table_df2.sort_values(by=['ivp'], ascending=False)
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

            if not table_df7.empty:
                table_df7.sort_values(by=['sort_ratio'], ascending=False, inplace=True)

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

            for i, row in table_df1_1.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['atm_iv'],1))
                vals2 = (round(row['z'],1))
                vals3 = (int(row['ivp']))
                vals4 = (int(row['put_strike']), int(row['call_strike']), row['skew'], row['target'])

                if row['ivp'] < 60:
                    continue
                else:
                    pass

                if row['pct_change'] < 0:
                    table1_1_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                else:
                    table1_1_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                if row.symbol in future_results:
                    table1_1_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table1_1_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table1_1_1.insert('', 'end', values=vals1)

                if row['z'] <= -1:
                    table1_1_2.insert('', 'end', values=vals2, tags=('z_highlight'))
                else:
                    table1_1_2.insert('', 'end', values=vals2)

                if row['ivp'] >= 60:
                    table1_1_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['ivp'] <= 40:
                    table1_1_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table1_1_3.insert('', 'end', values=vals3)

                table1_1_4.insert('', 'end', values=vals4)

            for i, row in table_df1_2.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['atm_iv'],1))
                vals2 = (round(row['z'],1))
                vals3 = (int(row['ivp']))
                vals4 = (int(row['put_strike']), int(row['call_strike']), row['skew'], row['target'])

                if row['pct_change'] < 0:
                    table1_2_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                else:
                    table1_2_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                if row.symbol in future_results:
                    table1_2_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table1_2_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table1_2_1.insert('', 'end', values=vals1)

                if row['z'] <= -1:
                    table1_2_2.insert('', 'end', values=vals2, tags=('z_highlight'))
                else:
                    table1_2_2.insert('', 'end', values=vals2)

                if row['ivp'] >= 60:
                    table1_2_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['ivp'] <= 40:
                    table1_2_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table1_2_3.insert('', 'end', values=vals3)

                table1_2_4.insert('', 'end', values=vals4)

            for i, row in table_df1_3.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['atm_iv'],1))
                vals2 = (round(row['z'],1))
                vals3 = (int(row['ivp']))
                vals4 = (int(row['put_strike']), int(row['call_strike']), row['skew'], row['target'])

                if row['pct_change'] < 0:
                    table1_3_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                else:
                    table1_3_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                if row.symbol in future_results:
                    table1_3_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table1_3_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table1_3_1.insert('', 'end', values=vals1)

                if row['z'] < -1:
                    table1_3_2.insert('', 'end', values=vals2, tags=('z_highlight'))
                else:
                    table1_3_2.insert('', 'end', values=vals2)

                if row['ivp'] >= 60:
                    table1_3_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['ivp'] <= 40:
                    table1_3_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table1_3_3.insert('', 'end', values=vals3)

                table1_3_4.insert('', 'end', values=vals4)

            for i, row in table_df1_4.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['atm_iv'],1))
                vals2 = (round(row['z'],1))
                vals3 = (int(row['ivp']))
                vals4 = (int(row['put_strike']), int(row['call_strike']), row['skew'], row['target'])

                if row['pct_change'] < 0:
                    table1_4_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                else:
                    table1_4_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                if row.symbol in future_results:
                    table1_4_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table1_4_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table1_4_1.insert('', 'end', values=vals1)

                if row['z'] >= 1:
                    table1_4_2.insert('', 'end', values=vals2, tags=('z_highlight'))
                else:
                    table1_4_2.insert('', 'end', values=vals2)

                if row['ivp'] >= 60:
                    table1_4_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['ivp'] <= 40:
                    table1_4_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table1_4_3.insert('', 'end', values=vals3)

                table1_4_4.insert('', 'end', values=vals4)

            # root.update()
            for i, row in table_df2_1.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['current_iv'],1))
                vals2 = (round(row['fwd_iv'],1))
                vals3 = (round(row['hv_current'],1))
                vals4 = (round(row['fair_vol'], 1))
                vals5 = (int(row['ivp']))
                vals6 = (round(row['rp_diff'],1))
                vals7 = (row['days_theta'])

                if row.symbol in future_results_1:
                    continue
                elif row.symbol in future_results:
                    table2_1_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table2_1_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table2_1_1.insert('', 'end', values=vals1)

                if row['days_theta'] >= 0.5 and row['pct_change'] < 0:
                    table2_1_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                elif row['days_theta'] >= 0.5 and row['pct_change'] > 0:
                    table2_1_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                else:
                    table2_1_0.insert('', 'end', values=vals0)
                if row['ivp'] >= 60:
                    table2_1_5.insert('', 'end', values=vals5, tags=('short_highlight'))
                elif row['ivp'] <= 40:
                    table2_1_5.insert('', 'end', values=vals5, tags=('long_highlight'))
                else:
                    table2_1_5.insert('', 'end', values=vals5)

                if row['current_iv'] >= 1.2*row['fair_vol']:
                    table2_1_4.insert('', 'end', values=vals4, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['fair_vol']:
                    table2_1_4.insert('', 'end', values=vals4, tags=('long_highlight'))
                else:
                    table2_1_4.insert('', 'end', values=vals4)

                if row['current_iv'] >= 1.2*row['fwd_iv']:
                    table2_1_2.insert('', 'end', values=vals2, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['fwd_iv']:
                    table2_1_2.insert('', 'end', values=vals2, tags=('long_highlight'))
                else:
                    table2_1_2.insert('', 'end', values=vals2)

                if row['current_iv'] >= 1.2*row['hv_current']:
                    table2_1_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['hv_current']:
                    table2_1_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table2_1_3.insert('', 'end', values=vals3)

                if row['rp_diff'] >= 0:
                    table2_1_6.insert('', 'end', values=vals6, tags=('short_highlight'))
                else:
                    table2_1_6.insert('', 'end', values=vals6, tags=('long_highlight'))

                if row['days_theta'] < 1:
                    table2_1_7.insert('', 'end', values=vals7, tags=('short_highlight'))
                else:
                    table2_1_7.insert('', 'end', values=vals7, tags=('long_highlight'))

            for i, row in table_df2_2.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['current_iv'],1))
                vals2 = (round(row['fwd_iv'],1))
                vals3 = (round(row['hv_current'],1))
                vals4 = (round(row['fair_vol'], 1))
                vals5 = (int(row['ivp']))
                vals6 = (round(row['rp_diff'],1))
                vals7 = (row['days_theta'])

                if row.symbol in future_results_1:
                    continue
                elif row.symbol in future_results:
                    table2_2_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table2_2_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table2_2_1.insert('', 'end', values=vals1)

                if row['days_theta'] >= 0.5 and row['pct_change'] < 0:
                    table2_2_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                elif row['days_theta'] >= 0.5 and row['pct_change'] > 0:
                    table2_2_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                else:
                    table2_2_0.insert('', 'end', values=vals0)
                if row['ivp'] >= 60:
                    table2_2_5.insert('', 'end', values=vals5, tags=('short_highlight'))
                elif row['ivp'] <= 40:
                    table2_2_5.insert('', 'end', values=vals5, tags=('long_highlight'))
                else:
                    table2_2_5.insert('', 'end', values=vals5)

                if row['current_iv'] >= 1.2*row['fair_vol']:
                    table2_2_4.insert('', 'end', values=vals4, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['fair_vol']:
                    table2_2_4.insert('', 'end', values=vals4, tags=('long_highlight'))
                else:
                    table2_2_4.insert('', 'end', values=vals4)

                if row['current_iv'] >= 1.2*row['fwd_iv']:
                    table2_2_2.insert('', 'end', values=vals2, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['fwd_iv']:
                    table2_2_2.insert('', 'end', values=vals2, tags=('long_highlight'))
                else:
                    table2_2_2.insert('', 'end', values=vals2)

                if row['current_iv'] >= 1.2*row['hv_current']:
                    table2_2_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['hv_current']:
                    table2_2_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table2_2_3.insert('', 'end', values=vals3)

                if row['rp_diff'] >= 0:
                    table2_2_6.insert('', 'end', values=vals6, tags=('short_highlight'))
                else:
                    table2_2_6.insert('', 'end', values=vals6, tags=('long_highlight'))

                if row['days_theta'] < 1:
                    table2_2_7.insert('', 'end', values=vals7, tags=('short_highlight'))
                else:
                    table2_2_7.insert('', 'end', values=vals7, tags=('long_highlight'))

            for i, row in table_df3.iterrows():
                vals1_0 = str(round(row['pct_change_1'],1))
                vals1_1 = (row['symbol_1'], round(row['iv_1'],1))
                vals1_2 = (round(row['fwd_iv_1'],1))
                vals1_3 = (round(row['hv_current_1'],1))
                vals1_4 = (round(row['avg_iv_1'],1))
                vals1_5 = (int(row['ivp1']))
                vals1_6 = (row['days_theta_1'])

                vals2_0 = str(round(row['pct_change_2'],1))
                vals2_1 = (row['symbol_2'], round(row['iv_2'],1))
                vals2_2 = (round(row['fwd_iv_2'],1))
                vals2_3 = (round(row['hv_current_2'],1))
                vals2_4 = (round(row['avg_iv_2'],1))
                vals2_5 = (int(row['ivp2']))
                vals2_6 = (row['days_theta_2'])

                vals3 = (round(row['z_score'],1), round(row['current_diff'],1), round(row['target'],1))

                if row['days_theta_1'] >= 0.5 and row['pct_change_1'] < 0:
                    table3_1_0.insert('', 'end', values=vals1_0, tags=('%_neg_highlight'))
                elif row['days_theta_1'] >= 0.5 and row['pct_change_1'] > 0:
                    table3_1_0.insert('', 'end', values=vals1_0, tags=('%_pos_highlight'))
                else:
                    table3_1_0.insert('', 'end', values=vals1_0)
                if row.symbol_1 in future_results:
                    table3_1_1.insert('', 'end', values=vals1_1, tags=('future_result'))
                elif row.symbol_1 in past_results:
                    table3_1_1.insert('', 'end', values=vals1_1, tags=('past_result'))
                else:
                    table3_1_1.insert('', 'end', values=vals1_1)

                if row['ivp1'] >= 60:
                    table3_1_5.insert('', 'end', values=vals1_5, tags=('short_highlight'))
                elif row['ivp1'] <= 40:
                    table3_1_5.insert('', 'end', values=vals1_5, tags=('long_highlight'))
                else:
                    table3_1_5.insert('', 'end', values=vals1_5)

                if row['iv_1'] >= 1.2*row['avg_iv_1']:
                    table3_1_4.insert('', 'end', values=vals1_4, tags=('short_highlight'))
                elif row['iv_1'] <= 0.8*row['avg_iv_1']:
                    table3_1_4.insert('', 'end', values=vals1_4, tags=('long_highlight'))
                else:
                    table3_1_4.insert('', 'end', values=vals1_4)

                if row['iv_1'] >= 1.2*row['fwd_iv_1']:
                    table3_1_2.insert('', 'end', values=vals1_2, tags=('short_highlight'))
                elif row['iv_1'] <= 0.8*row['fwd_iv_1']:
                    table3_1_2.insert('', 'end', values=vals1_2, tags=('long_highlight'))
                else:
                    table3_1_2.insert('', 'end', values=vals1_2)

                if row['iv_1'] >= 1.2*row['hv_current_1']:
                    table3_1_3.insert('', 'end', values=vals1_3, tags=('short_highlight'))
                elif row['iv_1'] <= 0.8*row['hv_current_1']:
                    table3_1_3.insert('', 'end', values=vals1_3, tags=('long_highlight'))
                else:
                    table3_1_3.insert('', 'end', values=vals1_3)

                if row['days_theta_1'] < 1:
                    table3_1_6.insert('', 'end', values=vals1_6, tags=('short_highlight'))
                else:
                    table3_1_6.insert('', 'end', values=vals1_6, tags=('long_highlight'))
                if row['days_theta_2'] >= 0.5 and row['pct_change_2'] < 0:
                    table3_2_0.insert('', 'end', values=vals2_0, tags=('%_neg_highlight'))
                elif row['days_theta_2'] >= 0.5 and row['pct_change_2'] > 0:
                    table3_2_0.insert('', 'end', values=vals2_0, tags=('%_pos_highlight'))
                else:
                    table3_2_0.insert('', 'end', values=vals2_0)
                if row.symbol_2 in future_results:
                    table3_2_1.insert('', 'end', values=vals2_1, tags=('future_result'))
                elif row.symbol_2 in past_results:
                    table3_2_1.insert('', 'end', values=vals2_1, tags=('past_result'))
                else:
                    table3_2_1.insert('', 'end', values=vals2_1)

                if row['ivp2'] >= 60:
                    table3_2_5.insert('', 'end', values=vals2_5, tags=('short_highlight'))
                elif row['ivp2'] <= 40:
                    table3_2_5.insert('', 'end', values=vals2_5, tags=('long_highlight'))
                else:
                    table3_2_5.insert('', 'end', values=vals2_5)

                if row['iv_2'] >= 1.2*row['avg_iv_2']:
                    table3_2_4.insert('', 'end', values=vals2_4, tags=('short_highlight'))
                elif row['iv_2'] <= 0.8*row['avg_iv_2']:
                    table3_2_4.insert('', 'end', values=vals2_4, tags=('long_highlight'))
                else:
                    table3_2_4.insert('', 'end', values=vals2_4)

                if row['iv_2'] >= 1.2*row['fwd_iv_2']:
                    table3_2_2.insert('', 'end', values=vals2_2, tags=('short_highlight'))
                elif row['iv_2'] <= 0.8*row['fwd_iv_2']:
                    table3_2_2.insert('', 'end', values=vals2_2, tags=('long_highlight'))
                else:
                    table3_2_2.insert('', 'end', values=vals2_2)

                if row['iv_2'] >= 1.2*row['hv_current_2']:
                    table3_2_3.insert('', 'end', values=vals2_3, tags=('short_highlight'))
                elif row['iv_2'] <= 0.8*row['hv_current_2']:
                    table3_2_3.insert('', 'end', values=vals2_3, tags=('long_highlight'))
                else:
                    table3_2_3.insert('', 'end', values=vals2_3)

                if row['days_theta_2'] < 1:
                    table3_2_6.insert('', 'end', values=vals2_6, tags=('short_highlight'))
                else:
                    table3_2_6.insert('', 'end', values=vals2_6, tags=('long_highlight'))

                if row['z_score'] <= -1:
                    table3_3.insert('', 'end', values=vals3, tags=('z_highlight'))
                else:
                    table3_3.insert('', 'end', values=vals3)

            for i, row in table_df4_1.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['current_iv'],1))
                vals2 = (round(row['fwd_vol'],1))
                vals3 = (round(row['hv_current'],1))
                vals4 = (round(row['fair_vol'], 1))
                vals5 = (int(row['ivp']))
                vals6 = (row['days_theta'])

                if row['days_theta'] >= 0.5 and row['pct_change'] < 0:
                    table4_1_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                elif row['days_theta'] >= 0.5 and row['pct_change'] > 0:
                    table4_1_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                else:
                    table4_1_0.insert('', 'end', values=vals0)
                if row.symbol in future_results:
                    table4_1_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table4_1_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table4_1_1.insert('', 'end', values=vals1)

                if row['ivp'] >= 60:
                    table4_1_5.insert('', 'end', values=vals5, tags=('short_highlight'))
                elif row['ivp'] <= 40:
                    table4_1_5.insert('', 'end', values=vals5, tags=('long_highlight'))
                else:
                    table4_1_5.insert('', 'end', values=vals5)

                if row['current_iv'] >= 1.2*row['fair_vol']:
                    table4_1_4.insert('', 'end', values=vals4, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['fair_vol']:
                    table4_1_4.insert('', 'end', values=vals4, tags=('long_highlight'))
                else:
                    table4_1_4.insert('', 'end', values=vals4)

                if row['current_iv'] >= 1.2*row['fwd_vol']:
                    table4_1_2.insert('', 'end', values=vals2, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['fwd_vol']:
                    table4_1_2.insert('', 'end', values=vals2, tags=('long_highlight'))
                else:
                    table4_1_2.insert('', 'end', values=vals2)

                if row['current_iv'] >= 1.2*row['hv_current']:
                    table4_1_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['hv_current']:
                    table4_1_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table4_1_3.insert('', 'end', values=vals3)

                if row['days_theta'] < 1:
                    table4_1_6.insert('', 'end', values=vals6, tags=('short_highlight'))
                else:
                    table4_1_6.insert('', 'end', values=vals6, tags=('long_highlight'))

            for i, row in table_df4_2.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['current_iv'],1))
                vals2 = (round(row['fwd_vol'],1))
                vals3 = (round(row['hv_current'],1))
                vals4 = (round(row['fair_vol'], 1))
                vals5 = (int(row['ivp']))
                vals6 = (row['days_theta'])

                if row['days_theta'] >= 0.5 and row['pct_change'] < 0:
                    table4_2_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                elif row['days_theta'] >= 0.5 and row['pct_change'] > 0:
                    table4_2_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                else:
                    table4_2_0.insert('', 'end', values=vals0)
                if row.symbol in future_results:
                    table4_2_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table4_2_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table4_2_1.insert('', 'end', values=vals1)

                if row['ivp'] >= 60:
                    table4_2_5.insert('', 'end', values=vals5, tags=('short_highlight'))
                elif row['ivp'] <= 40:
                    table4_2_5.insert('', 'end', values=vals5, tags=('long_highlight'))
                else:
                    table4_2_5.insert('', 'end', values=vals5)

                if row['current_iv'] >= 1.2*row['fair_vol']:
                    table4_2_4.insert('', 'end', values=vals4, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['fair_vol']:
                    table4_2_4.insert('', 'end', values=vals4, tags=('long_highlight'))
                else:
                    table4_2_4.insert('', 'end', values=vals4)

                if row['current_iv'] >= 1.2*row['fwd_vol']:
                    table4_2_2.insert('', 'end', values=vals2, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['fwd_vol']:
                    table4_2_2.insert('', 'end', values=vals2, tags=('long_highlight'))
                else:
                    table4_2_2.insert('', 'end', values=vals2)

                if row['current_iv'] >= 1.2*row['hv_current']:
                    table4_2_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['hv_current']:
                    table4_2_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table4_2_3.insert('', 'end', values=vals3)

                if row['days_theta'] < 1:
                    table4_2_6.insert('', 'end', values=vals6, tags=('short_highlight'))
                else:
                    table4_2_6.insert('', 'end', values=vals6, tags=('long_highlight'))

            for i, row in table_df5.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['current_iv'],1))
                vals2 = (round(row['forward_vol'],1))
                vals3 = (round(row['move_iv'],1))
                vals4 = (round(row['avg_iv'], 1))
                vals5 = (int(row['IVP']))
                vals6 = (row['long_moves'] / 2)

                if row['long_moves'] / 2 >= 0.5 and row['pct_change'] < 0:
                    table5_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                elif row['long_moves'] / 2 >= 0.5 and row['pct_change'] > 0:
                    table5_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                else:
                    table5_0.insert('', 'end', values=vals0)
                if row.symbol in future_results:
                    table5_1.insert('', 'end', values=vals1, tags=('future_result'))
                elif row.symbol in past_results:
                    table5_1.insert('', 'end', values=vals1, tags=('past_result'))
                else:
                    table5_1.insert('', 'end', values=vals1)

                if row['IVP'] >= 60:
                    table5_5.insert('', 'end', values=vals5, tags=('short_highlight'))
                elif row['IVP'] <= 40:
                    table5_5.insert('', 'end', values=vals5, tags=('long_highlight'))
                else:
                    table5_5.insert('', 'end', values=vals5)

                if row['current_iv'] >= 1.2*row['avg_iv']:
                    table5_4.insert('', 'end', values=vals4, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['avg_iv']:
                    table5_4.insert('', 'end', values=vals4, tags=('long_highlight'))
                else:
                    table5_4.insert('', 'end', values=vals4)

                if row['current_iv'] >= 1.2*row['forward_vol']:
                    table5_2.insert('', 'end', values=vals2, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['forward_vol']:
                    table5_2.insert('', 'end', values=vals2, tags=('long_highlight'))
                else:
                    table5_2.insert('', 'end', values=vals2)

                if row['current_iv'] >= 1.2*row['move_iv']:
                    table5_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                elif row['current_iv'] <= 0.8*row['move_iv']:
                    table5_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                else:
                    table5_3.insert('', 'end', values=vals3)

                if row['long_moves'] / 2 < 1:
                    table5_6.insert('', 'end', values=vals6, tags=('short_highlight'))
                else:
                    table5_6.insert('', 'end', values=vals6, tags=('long_highlight'))

            for i, row in table_df6.iterrows():
                vals1 = (row['symbol'], round(row['atm_iv'],1))
                vals2 = (round(row['z'],1))
                vals3 = (row['spread'])
                vals4 = (int(row['put_strike']), int(row['call_strike']), row['skew'], row['target'])

                table6_1.insert('', 'end', values=vals1)

                if row['z'] <= -1:
                    table6_2.insert('', 'end', values=vals2, tags=('z_highlight'))
                elif row['z'] >= 1:
                    table6_2.insert('', 'end', values=vals2, tags=('z_highlight'))
                else:
                    table6_2.insert('', 'end', values=vals2)

                table6_3.insert('', 'end', values=vals3)

                table6_4.insert('', 'end', values=vals4)

            for i, row in table_df7.iterrows():
                vals0 = str(round(row['pct_change'],1))
                vals1 = (row['symbol'], round(row['current_iv'],1))
                vals2 = (round(row['fwd_iv'],1))
                vals3 = (round(row['hv_current'],1))
                vals4 = (round(row['fair_vol'], 1))
                vals5 = (int(row['ivp']))
                vals6 = (row['days_theta'])


                condition1_1 = (30 <= row['ivp'] <= 60)
                condition1_2 = (row['current_iv'] > row['fwd_iv'])
                condition1_3 = row['days_theta'] <= 0.5
                condition1_4 = row['hv_current'] <= 0.9*row['current_iv']
                condition1_5 = row['long_moves_yest'] <= 2

                condition2_1 = (60 < row['ivp'] <= 90)
                if datetime.datetime.now() < today_date_10_30:
                    condition2_2 = (row['current_iv'] > row['fwd_iv'])
                else:
                    condition2_2 = (row['current_iv'] > (row['fwd_iv'] + row['bench_mark_iv'])/2)
                condition2_3 = row['days_theta'] <= 0.5
                condition2_4 = row['hv_current'] <= 0.9*row['current_iv']
                condition2_5 = row['long_moves_yest'] <= 2

                condition3_1 = (90 < row['ivp'])
                condition3_2 = (row['current_iv'] > row['bench_mark_iv'])
                condition3_3 = row['days_theta'] <= 0.5
                condition3_4 = row['hv_current'] <= 0.9*row['current_iv']
                condition3_5 = row['long_moves_yest'] <= 2

                if (condition1_1 and condition1_2 and condition1_3 and condition1_4 and condition1_5)\
                        or (condition2_1 and condition2_2 and condition2_3 and condition2_4 and condition2_5)\
                        or (condition3_1 and condition3_2 and condition3_3 and condition3_4 and condition3_5):

                    if row['fwd_iv'] == 0:
                        continue
                    if row['days_theta'] >= 0.5 and row['pct_change'] < 0:
                        table7_0.insert('', 'end', values=vals0, tags=('%_neg_highlight'))
                    elif row['days_theta'] >= 0.5 and row['pct_change'] > 0:
                        table7_0.insert('', 'end', values=vals0, tags=('%_pos_highlight'))
                    else:
                        table7_0.insert('', 'end', values=vals0)
                    if row.symbol in future_results:
                        table7_1.insert('', 'end', values=vals1, tags=('future_result'))
                    elif row.symbol in past_results:
                        table7_1.insert('', 'end', values=vals1, tags=('past_result'))
                    else:
                        table7_1.insert('', 'end', values=vals1)

                    if row['ivp'] >= 60:
                        table7_5.insert('', 'end', values=vals5, tags=('short_highlight'))
                    elif row['ivp'] <= 40:
                        table7_5.insert('', 'end', values=vals5, tags=('long_highlight'))
                    else:
                        table7_5.insert('', 'end', values=vals5)

                    if row['current_iv'] >= 1.2*row['fair_vol']:
                        table7_4.insert('', 'end', values=vals4, tags=('short_highlight'))
                    elif row['current_iv'] <= 0.8*row['fair_vol']:
                        table7_4.insert('', 'end', values=vals4, tags=('long_highlight'))
                    else:
                        table7_4.insert('', 'end', values=vals4)

                    if row['current_iv'] >= 1.2*row['fwd_iv']:
                        table7_2.insert('', 'end', values=vals2, tags=('short_highlight'))
                    elif row['current_iv'] <= 0.8*row['fwd_iv']:
                        table7_2.insert('', 'end', values=vals2, tags=('long_highlight'))
                    else:
                        table7_2.insert('', 'end', values=vals2)

                    if row['current_iv'] >= 1.2*row['hv_current']:
                        table7_3.insert('', 'end', values=vals3, tags=('short_highlight'))
                    elif row['current_iv'] <= 0.8*row['hv_current']:
                        table7_3.insert('', 'end', values=vals3, tags=('long_highlight'))
                    else:
                        table7_3.insert('', 'end', values=vals3)

                    if row['days_theta'] < 1:
                        table7_6.insert('', 'end', values=vals6, tags=('short_highlight'))
                    else:
                        table7_6.insert('', 'end', values=vals6, tags=('long_highlight'))

            root.update()
            # print(3)
            time.sleep(0.1)