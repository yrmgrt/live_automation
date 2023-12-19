import socket
import struct
import time,re
import sqlite3
#import DatabaseDoQuery as db
from websocket_trial.models.temp_table_live import live_before_df
from django.core.management.base import BaseCommand

i = 1

class Command(BaseCommand):

    def handle(self, *args, **options):
        i = 1
        while i < 1000:

            print(i)
            inserts = []
            #data, addr = sock.recvfrom(65534)
            #if instrument in lst:
            temp = dict()
            temp['symbol'] = 'ADD from token reference'
            temp['ticker'] = 'ADD from token referance'
            temp['close'] = i
            inserts.append(live_before_df(**temp))
            live_before_df.objects.bulk_create(inserts, batch_size=1)
            i = i + 1




