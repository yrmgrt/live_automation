import datetime
import socket
import struct
import time,re
import sqlite3

import pandas as pd

from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models import InstrumentProvider, Security, KiteInstrument
# strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Desktop\strike_diff.csv")
df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")
symbol_df = df[df['bucket'] == 3]
symbols = symbol_df.symbol.unique()

daily_morn_df = pd.read_csv(r"C:\Users\Administrator\Desktop\daily_morn.csv")
expiry_date = daily_morn_df['monthly_expiry'].item()


from django.core.management.base import BaseCommand
# from websocket_trial.core.celery.websocket import store_data
new_df = pd.read_csv(r"C:\Users\Administrator\latest_instrument.csv")
new_df = new_df[(new_df['exchange'] == 'NFO')]
new_df = new_df[(new_df['name'].isin(symbols))]
new_df = new_df[(new_df['expiry'] == expiry_date)]
lst = new_df.exchange_token.unique()


class Command(BaseCommand):

    def handle(self, *args, **options):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # create handler for each connection
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', 15300))
        mreq = struct.pack("=4sl", socket.inet_aton("231.101.103.240"), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        k = KiteInstrument.get_fo_provider()
        k = list(k.values())
        k = pd.DataFrame(k)
        # k = k[k['expiry']==expiry_date]

        while True:

            # time_1 = datetime.datetime.now()
            data, addr = sock.recvfrom(65534)
            signature=struct.unpack('<HBHHI49s49sIBdIIdIIdIIdIIdIIdIIdIIdIIdIIdIIIdIIddddddIddd',data)
            instrument=int(re.sub('\D', '', ((((signature[6]).decode('UTF-8').replace('\x00', '').strip()).replace('IT','').strip()).replace('T','').strip())))
            # time_2 = datetime.datetime.now()
            if instrument in lst:
                # time_3 = datetime.datetime.now()
                if signature[9] == 0:
                    continue
                if signature[24] == 0:
                    continue
                if signature[9] < 1:
                    if abs(signature[24]-signature[9]) > 0.05:
                        continue
                if signature[9] > 1:
                    if abs(signature[24]-signature[9])/signature[9] > 0.05:
                        continue
                # time_4 = datetime.datetime.now()
                temp = dict()
                temp['time'] = datetime.datetime.now()


                # temp['close'] = signature[40]
                temp['close'] = (signature[24] + signature[9]) / 2
                # temp['volume'] = signature[39]
                temp['open_interest'] = signature[49]

                # print(temp)
                # temp = dict()
                # time_5 = datetime.datetime.now()

                k_temp = k[k['exchange_token'] == instrument].iloc[0]

                temp['symbol'] = k_temp['name']

                temp['ticker'] = k_temp.tradingsymbol
                temp['strike_price'] = k_temp.strike_price
                temp['instrument_type'] = k_temp.instrument_type
                # temp['expiry'] = k.expiry
                # time_6 = datetime.datetime.now()
                inserts = []
                inserts.append(live_before_df(**temp))
                live_before_df.objects.bulk_create(inserts, batch_size=500)
                # time_7 = datetime.datetime.now()

                # print((time_2-time_1).total_seconds(), (time_3-time_2).total_seconds(), (time_4-time_3).total_seconds(), (time_5-time_4).total_seconds(), (time_6-time_5).total_seconds(), (time_7-time_6).total_seconds())
