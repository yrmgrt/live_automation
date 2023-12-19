import time

from .new_skew_cal import calc_ab
from .next_exp_vol_calc import next_exp_vol_calc
from .atm_vol_calc import atm_vol_calc
# from .pair_cal import pair_cal
from celery import shared_task
from celery.result import AsyncResult
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.management.commands.symbol_update import symbols
import pandas as pd
from .weekly_skew_cal import calc_ab_weekly
all_symbols = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")
all_symbols = all_symbols.symbol.unique()

all_symbols_2 = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols_2.csv")
all_symbols_2 = all_symbols_2.symbol.unique()
# corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\correlation.csv")
# pairs = [[corr_df['stock_1'][i], corr_df['stock_2'][i]] for i in range(len(corr_df))]
scripts = list(set(all_symbols) - set(all_symbols_2))
def on_raw_message(body):
    print(body)

def new_async():

    results = []

    # for symbol in symbols:
    #     pass

    # print('[new_async ]: Called')
    # while True:
    for symbol in all_symbols_2:

        task = calc_ab.apply_async(
            args=([symbol])
            )
        task2 = atm_vol_calc.apply_async(
            args=([symbol])
            )
    for script in scripts:
        task3 = atm_vol_calc.apply_async(
            args=([script])
        )
    for sym in ['NIFTY', 'BANKNIFTY']:
        task4 = calc_ab_weekly.apply_async(
            args=([sym])
        )

    #for symbol in symbols:

    # task = calc_b.delay('NIFTY')
    # print('[new_async ]: End')
    # print('[task]: ', task)
    # # result = task.get(on_message=on_raw_message, propagate=False)
    # # print(result)
    # print('new async task result')
    return 'task'

def new_async_2():

    results = []

    # for symbol in symbols:
    #     pass

    # print('[new_async ]: Called')
    # while True:
    for symbol in all_symbols:

        task = next_exp_vol_calc.apply_async(
            args=([symbol])
            )


    #for symbol in symbols:

    # task = calc_b.delay('NIFTY')
    # print('[new_async ]: End')
    # print('[task]: ', task)
    # # result = task.get(on_message=on_raw_message, propagate=False)
    # # print(result)
    # print('new async task result')
    return 'task'



