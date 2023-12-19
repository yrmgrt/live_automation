# from .new_task import calc_fut
# from celery import shared_task
# from celery.result import

# from websocket_trial.models.temp_table_live import live_before_df
# from websocket_trial.models.kite import KiteInstrument
# from websocket_trial.management.commands.websocket import symbols
# import datetime
#
# @shared_task
# def store_data(token, temp):
#     inserts = []
#     # temp = dict()
#     temp['time'] = datetime.datetime.now()
#
#     k = KiteInstrument.get_fo_provider().filter(exchange_token=token).first()
#     temp['symbol'] = k.name
#     temp['ticker'] = k.tradingsymbol
#
#     temp['strike_price'] = k.strike_price
#     temp['instrument_type'] = k.instrument_type
#     temp['expiry'] = k.expiry
#
#     inserts.append(live_before_df(**temp))
#     live_before_df.objects.bulk_create(inserts, batch_size=500)
