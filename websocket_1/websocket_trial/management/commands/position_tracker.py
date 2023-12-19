import datetime

import pandas as pd
df = pd.read_csv(r"C:\Users\Administrator\Downloads\iv_stats.csv")
from websocket_trial.models.atm_vol_table import vol_table
from websocket_trial.models.idv_cal_tb import idv_cal
import http.client, urllib
import py_vollib.black_scholes.implied_volatility
import py_vollib.black_scholes.greeks.numerical
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models.position_fut_table import position_fut_tb
from websocket_trial.models.position_tracker_signals import position_tracker_tb


daily_morn_df = pd.read_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")
dte = daily_morn_df['dte_monthly'].item()
def ping_ios(symbol, Title):
    conn = http.client.HTTPSConnection("api.pushover.net:443")
    conn.request("POST", "/1/messages.json",
      urllib.parse.urlencode({
        "token": "abybcozibie6ae11hjhmvsen1h3d7t",
        "user": "u29fptbwczie7vy72y9s3wnsc5xp4e",
        "title": f"{Title}",
        "message": f"{symbol}",
        "url": "",
        "priority": "0"
      }), { "Content-type": "application/x-www-form-urlencoded" })

    # get response
    conn.getresponse()

def calc_vol(close, fut_close, strike, dte, opt_type):
    if opt_type == 'CE':
        opt_type = 'c'
    if opt_type == 'PE':
        opt_type = 'p'
    try:
        return py_vollib.black_scholes.implied_volatility.implied_volatility(
                close,
                fut_close,
                strike,
                dte/365, 0,
                opt_type)
    except:
        return 0


symbols = df.symbol.unique()
all_symbols = df.symbol.unique()
# print(all_symbols)

benchmark_fut = dict()
symbols = list(set(all_symbols) and set(symbols))

position_types = ['front_spread', 'back_spread', 'four_leg', 'correlation', 'long_short']


prev_symbol_list = []
try:
    while True:
        SHEET_ID = '1eG9XBunwK5fl7-hrkaDgqgdyymRIbuXuO5QaSibbfb8'






        front_spread_df = pd.read_csv('https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}'
                                      .format(SHEET_ID, 'front_spread'))

        front_spread_df = front_spread_df[['symbol', 'init_skew', 'target_skew', 'strike_1', 'instrument_type_1',
                                           'strike_2', 'instrument_type_2', 'position', 'entry']]

        front_spread_df = front_spread_df.dropna()
        front_spread_df = front_spread_df[front_spread_df['position'] == 'Open']



        back_spread_df = pd.read_csv('https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}'
                                     .format(SHEET_ID, 'back_spread'))

        back_spread_df = back_spread_df[['symbol', 'init_skew', 'target_skew', 'strike_1', 'instrument_type_1',
                                           'strike_2', 'instrument_type_2', 'position', 'entry']]


        back_spread_df = back_spread_df.dropna()
        back_spread_df = back_spread_df[back_spread_df['position'] == 'Open']

        four_leg_df = pd.read_csv('https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}'
                                  .format(SHEET_ID, 'four_leg'))

        four_leg_df = four_leg_df[['symbol', 'skew', 'target_skew', 'strike_1', 'instrument_type_1',
           'strike_2', 'instrument_type_2', 'strike_3', 'instrument_type_3',
           'strike_4', 'instrument_type_4', 'position', 'entry', 'Init_atm_iv',
           'target_iv']]


        four_leg_df = four_leg_df.dropna()


        four_leg_df = four_leg_df[four_leg_df['position'] == 'Open']

        correlation_df = pd.read_csv('https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}'
                                     .format(SHEET_ID, 'correlation'))


        correlation_df = correlation_df[['symbol_1', 'symbol_2', 'iv_1', 'iv_2', 'init_skew', 'target_skew',
           'position', 'entry']]

        correlation_df = correlation_df.dropna()
        correlation_df = correlation_df[correlation_df['position'] == 'Open']
        long_short_df = pd.read_csv('https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}'
                                    .format(SHEET_ID, 'long_short_df'))

        long_short_df = long_short_df.dropna()

        long_short_df = long_short_df[long_short_df['position'] == 'Open']

        #
        # concatenated_list = front_spread_df.symbol.unique() + back_spread_df.symbol.unique() \
        #                     + long_short_df.symbol.unique() + four_leg_df.symbol.unique() + \
        #                     correlation_df.symbol_1.unique() + correlation_df.symbol_2.unique()
        #
        #
        # # Remove duplicates
        # # Using the set() function
        # delta_list = list(set(concatenated_list))

        for i, row in front_spread_df.iterrows():
            sym = row.symbol
            try:


                strike_1 = row.strike_1
                instrument_type_1 = row.instrument_type_1
                strike_2 = row.strike_2
                instrument_type_2 = row.instrument_type_2

                k = live_before_df.objects.filter(symbol=sym, instrument_type='FUT').last()
                fut_close = k.close



                price_1_row = live_before_df.objects.filter(symbol=sym, instrument_type=instrument_type_1, strike_price=strike_1).last()
                price_1 = price_1_row.close

                price_2_row = live_before_df.objects.filter(symbol=sym, instrument_type=instrument_type_2,
                                                            strike_price=strike_2).last()
                price_2 = price_2_row.close




                iv1 = calc_vol(price_1, fut_close, strike_1, dte, instrument_type_1

                )
                iv2 = calc_vol(price_2, fut_close, strike_2, dte, instrument_type_2

                         )

                current_skew = iv2 - iv1
                if current_skew < row.target_skew:
                    signals = position_tracker_tb.objects.filter(position_type='Front_spread', instrument=sym)
                    if not signals:
                        ping_ios(sym, 'Front Target Hit')
                        inserts = []
                        temp = dict()
                        temp['position_type'] = 'Front_spread'
                        temp['instrument'] = sym
                        inserts.append(position_tracker_tb(**temp))
                        position_tracker_tb.objects.bulk_create(inserts, batch_size=500)
            except:
                print("error in front spread", sym)









        for i, row in back_spread_df.iterrows():
            sym = row.symbol
            try:


                strike_1 = row.strike_1
                instrument_type_1 = row.instrument_type_1
                strike_2 = row.strike_2
                instrument_type_2 = row.instrument_type_2

                k = live_before_df.objects.filter(symbol=sym, instrument_type='FUT').last()
                fut_close = k.close

                price_1_row = live_before_df.objects.filter(symbol=sym, instrument_type=instrument_type_1,
                                                            strike_price=strike_1).last()
                price_1 = price_1_row.close

                price_2_row = live_before_df.objects.filter(symbol=sym, instrument_type=instrument_type_2,
                                                            strike_price=strike_2).last()
                price_2 = price_2_row.close

                iv1 = calc_vol(price_1, fut_close, strike_1, dte, instrument_type_1

                               )
                iv2 = calc_vol(price_2, fut_close, strike_2, dte, instrument_type_2

                               )

                current_skew = iv2 - iv1
                if current_skew > row.target_skew:
                    signals = position_tracker_tb.objects.filter(position_type='Back_spread', instrument=sym)
                    if not signals:
                        ping_ios(sym, 'Back Target Hit')
                        inserts = []
                        temp = dict()
                        temp['position_type'] = 'Back_spread'
                        temp['instrument'] = sym
                        inserts.append(position_tracker_tb(**temp))
                        position_tracker_tb.objects.bulk_create(inserts, batch_size=500)
            except:
                print("Error in Back Spread", sym)









        for i, row in four_leg_df.iterrows():
            sym = row.symbol
            try:


                current_iv = vol_table.objects.filter(symbol=sym).first().current_iv
                strike_1 = row.strike_1
                instrument_type_1 = row.instrument_type_1
                strike_2 = row.strike_2
                instrument_type_2 = row.instrument_type_2
                strike_3 = row.strike_3
                instrument_type_3 = row.instrument_type_3
                strike_4 = row.strike_4
                instrument_type_4 = row.instrument_type_4





                k = live_before_df.objects.filter(symbol=sym, instrument_type='FUT').last()
                fut_close = k.close

                price_1_row = live_before_df.objects.filter(symbol=sym, instrument_type=instrument_type_1,
                                                            strike_price=strike_1).last()
                price_1 = price_1_row.close

                price_2_row = live_before_df.objects.filter(symbol=sym, instrument_type=instrument_type_2,
                                                            strike_price=strike_2).last()
                price_2 = price_2_row.close

                price_3_row = live_before_df.objects.filter(symbol=sym, instrument_type=instrument_type_3,
                                                            strike_price=strike_3).last()
                price_3 = price_3_row.close

                price_4_row = live_before_df.objects.filter(symbol=sym, instrument_type=instrument_type_4,
                                                            strike_price=strike_4).last()
                price_4 = price_4_row.close






                iv1 = calc_vol(price_1, fut_close, strike_1, dte, instrument_type_1

                               )
                iv2 = calc_vol(price_2, fut_close, strike_2, dte, instrument_type_2

                               )

                iv3 = calc_vol(price_3, fut_close, strike_3, dte, instrument_type_3

                               )
                iv4 = calc_vol(price_4, fut_close, strike_4, dte, instrument_type_4

                               )





                current_skew = iv2 + iv3 - iv1 - iv4
                if current_skew < row.target_skew:
                    signals = position_tracker_tb.objects.filter(position_type='Four_leg_skew', instrument=sym)
                    if not signals:
                        ping_ios(sym, 'Four leg Target Hit')
                        inserts = []
                        temp = dict()
                        temp['position_type'] = 'Four_leg_skew'
                        temp['instrument'] = sym
                        inserts.append(position_tracker_tb(**temp))
                        position_tracker_tb.objects.bulk_create(inserts, batch_size=500)


                if current_iv < row.target_iv:

                    signals = position_tracker_tb.objects.filter(position_type='Four_leg_IV', instrument=sym)
                    if not signals:
                        ping_ios(sym, 'Four leg IV Target Hit')
                        inserts = []
                        temp = dict()
                        temp['position_type'] = 'Four_leg_IV'
                        temp['instrument'] = sym
                        inserts.append(position_tracker_tb(**temp))
                        position_tracker_tb.objects.bulk_create(inserts, batch_size=500)

            except:
                print("error in four leg", sym)









        for i, row in correlation_df.iterrows():
            sym_1 = row.symbol_1
            sym_2 = row.symbol_2
            try:
                iv1 = vol_table.objects.filter(symbol=sym_1).first().current_iv
                iv2 = vol_table.objects.filter(symbol=sym_2).first().current_iv
                current_skew = iv2 - iv1
                if current_skew > row.target_skew:
                    sym_list = sym_1 + ' ' + sym_2



                    signals = position_tracker_tb.objects.filter(position_type='Correlation', instrument=sym_list)
                    if not signals:
                        ping_ios(sym_list, 'Correlation Target Hit')
                        inserts = []
                        temp = dict()
                        temp['position_type'] = 'Correlation'
                        temp['instrument'] = sym_list
                        inserts.append(position_tracker_tb(**temp))
                        position_tracker_tb.objects.bulk_create(inserts, batch_size=500)
            except:
                print("Error in correlation", sym_1, sym_2)






        long_short_df = pd.read_csv('https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}'
                                      .format(SHEET_ID, 'long_short'))

        long_short_df = long_short_df[['symbol', 'trade', 'init_iv', 'target_iv', 'position', 'entry']]

        long_short_df = long_short_df.dropna()

        for i, row in long_short_df.iterrows():

            sym = row.symbol
            # print(sym)
            try:
                current_fut = vol_table.objects.filter(symbol=sym).first().fut_close
                current_iv = vol_table.objects.filter(symbol=sym).first().current_iv
                if row.trade == 'SHORT':
                    if current_iv < row.target_iv:

                        signals = position_tracker_tb.objects.filter(position_type='SHORT', instrument=sym)
                        if not signals:
                            ping_ios(sym, 'Short IV Target hit')
                            inserts = []
                            temp = dict()
                            temp['position_type'] = 'SHORT'
                            temp['instrument'] = sym
                            inserts.append(position_tracker_tb(**temp))
                            position_tracker_tb.objects.bulk_create(inserts, batch_size=500)


                if row.trade == 'LONG':
                    if current_iv > row.target_iv:

                        signals = position_tracker_tb.objects.filter(position_type='LONG', instrument=sym)
                        if not signals:
                            ping_ios(sym, 'LONG IV TARGET HIT')
                            inserts = []
                            temp = dict()
                            temp['position_type'] = 'LONG'
                            temp['instrument'] = sym
                            inserts.append(position_tracker_tb(**temp))
                            position_tracker_tb.objects.bulk_create(inserts, batch_size=500)
            except:
                print("Error in long short ", sym)


        corr_1_fut_df = pd.DataFrame()
        corr_2_fut_df = pd.DataFrame()
        front_fut_df = front_spread_df[['symbol']]
        back_fut_df = back_spread_df[['symbol']]
        four_leg_fut_df = four_leg_df[['symbol']]
        long_short_fut_df = long_short_df[['symbol']]
        corr_1_fut_df['symbol'] = correlation_df[['symbol_1']]
        corr_2_fut_df['symbol'] = correlation_df[['symbol_2']]
        position_fut_df = pd.concat([front_fut_df, back_fut_df, four_leg_fut_df, long_short_fut_df, corr_1_fut_df, corr_2_fut_df])
        # print(position_fut_df)

        for i, row in position_fut_df.iterrows():
            symbol = row.symbol
            try:
                fut_row = live_before_df.objects.filter(symbol=symbol, instrument_type='FUT').last()
                current_fut = fut_row.close
                current_iv = vol_table.objects.filter(symbol=symbol).first().current_iv
                if not current_iv == 0:
                    long_move_val = current_iv/1900 * 0.5 * current_fut
                    objs = position_fut_tb.objects.filter(symbol=symbol)
                    # print(symbol, current_fut, current_iv)

                    if not objs:
                        inserts = []
                        temp = dict()
                        temp['symbol'] = symbol
                        benchmark_fut_val = current_fut
                        temp['benchmark_fut_value'] = current_fut
                        temp['init_fut_value'] = current_fut
                        temp['init_atm_iv'] = current_iv
                        inserts.append(position_fut_tb(**temp))
                        position_fut_tb.objects.bulk_create(inserts, batch_size=500)
                    else:

                        benchmark_fut_val = objs.first().benchmark_fut_value

                        ##### add in position fut_tb




                    # print(benchmark_fut_val)
                    if (abs(current_fut - benchmark_fut_val) > long_move_val):
                        benchmark_fut_val = current_fut
                        # print(benchmark_fut_val, current_fut)
                        for obj in objs:

                            obj.benchmark_fut_value = benchmark_fut_val
                            obj.save()
                        try:
                            print('DELTA', symbol)
                            ping_ios(symbol, "Delta")
                        except:
                            print(symbol, '-------------------------------------------------------------------')


            except:
                print("Error i Delta Cal", symbol)


except Exception as e:
    print('Code Stopped Working')
    time = datetime.datetime.now()
    message = 'Code stopped Working' + e
    ping_ios(time,  message)
















# back_spread_df =

