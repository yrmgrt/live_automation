import datetime
import socket
import struct
import time,re
import sqlite3
import requests
import pandas as pd
from io import StringIO
from django.core.management.base import BaseCommand
from django.db.models import Q
from websocket_trial.models.temp_table_live import live_before_df
from websocket_trial.models import InstrumentProvider, Security
from websocket_trial.models import KiteInstrument
symbols = ['NIFTY', 'BANKNIFTY']
url = 'https://www.nseindia.com/api/equity-stockIndices?csv=true&index=SECURITIES%20IN%20F%26O'
column_rename = {

}
column_rename['SYMBOL \n'] = 'symbol'
column_rename['LTP \n'] = 'close'
class Command(BaseCommand):

    def handle(self, *args, **options):
        symbols = ['POLYCAB', 'ACC']
        df = pd.read_csv(r"C:\Users\Administrator\Downloads\MW-SECURITIES-IN-F&O-23-May-2023 (3).csv")

        for symbol in symbols:
            df.rename(columns=column_rename, inplace=True)
            df = df[['symbol', 'close']]
            sym_df = df[df['symbol'] == symbol]
            if not sym_df.empty:
                upper_limit_strike = 1.2 * int((float(sym_df['close'].item().replace(',', ''))))
                lower_limit_strike = 0.8 * int((float(sym_df['close'].item().replace(',', ''))))
                k = KiteInstrument.get_fo_provider().filter(Q(name=symbol) &
                    Q(strike_price__gt=lower_limit_strike) & Q(strike_price__lt=upper_limit_strike))









