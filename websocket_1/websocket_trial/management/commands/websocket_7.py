import pandas as pd
import datetime
import socket
import struct
import time,re
from django.core.management.base import BaseCommand
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models import InstrumentProvider, Security, KiteInstrument
strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Desktop\strike_diff.csv")
symbols = strike_diff_df.symbol.unique()
from websocket_trial.models.next_expiry_live import next_expiry_live
df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")
symbol_df = df[df['bucket'] == 3]
symbols = symbol_df.symbol.unique()

new_df = pd.read_csv(r"C:\Users\Administrator\latest_instrument.csv")
new_df = new_df[(new_df['exchange'] == 'NFO')]
new_df = new_df[(new_df['name'].isin(symbols))]
new_df = new_df[(new_df['expiry'] == '2023-10-26')]
lst = new_df.exchange_token.unique()
print(symbols)
class Command(BaseCommand):

    def handle(self, *args, **options):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # create handler for each connection
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', 15300))
        mreq = struct.pack("=4sl", socket.inet_aton("231.101.103.240"), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        while True:
            inserts = []
            temp = dict()
            data, addr = sock.recvfrom(65534)
            signature=struct.unpack('<HBHHI49s49sIBdIIdIIdIIdIIdIIdIIdIIdIIdIIdIIIdIIddddddIddd',data)
            instrument=int(re.sub('\D', '', ((((signature[6]).decode('UTF-8').replace('\x00', '').strip()).replace('IT','').strip()).replace('T','').strip())))
            # print(instrument)
            if instrument in lst:
                temp['time'] = datetime.datetime.now()


                temp['close'] = signature[40]
                temp['volume'] = signature[39]
                temp['open_interest'] = signature[49]
                inserts = []
                # temp = dict()


                k = KiteInstrument.get_fo_provider().filter(exchange_token=instrument).first()

                temp['symbol'] = k.name
                temp['ticker'] = k.tradingsymbol
                temp['strike_price'] = k.strike_price
                temp['instrument_type'] = k.instrument_type
                temp['expiry'] = k.expiry

                inserts.append(next_expiry_live(**temp))
                next_expiry_live.objects.bulk_create(inserts, batch_size=500)