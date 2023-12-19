from django.core.management.base import BaseCommand
import requests
import pandas as pd
from io import StringIO
from websocket_trial.models import Security


STOCKS_URL = 'https://archives.nseindia.com/content/equities/EQUITY_L.csv'
STOCK_CSV_MAPPING = {}
STOCK_CSV_MAPPING['SYMBOL'] = 'code'
STOCK_CSV_MAPPING[' SERIES'] = 'series'
STOCK_CSV_MAPPING['NAME OF COMPANY'] = 'name'
STOCK_CSV_MAPPING[' ISIN NUMBER'] = 'isin'


# https://www1.nseindia.com/content/indices/ind_close_all_20082021.csv
# There is no consistency in codes for index. Need to handle mapping manually
INDEX_DATA = [
    dict(name="NIFTY 50", code="NIFTY"),
    dict(name="NIFTY BANK", code="BANKNIFTY")
]


class Command(BaseCommand):

    def handle(self, *args, **options):
        resp = requests.get(url=STOCKS_URL)
        io = StringIO(resp.text)
        df = pd.read_csv(io)

        existing = Security.objects.values_list('isin', flat=True)
        existing = list(existing)

        df = df.rename(columns=STOCK_CSV_MAPPING)
        df = df[STOCK_CSV_MAPPING.values()]
        df = df[df['series'] == 'EQ']
        df = df[~df['isin'].isin(existing)]

        inserts = []
        for i, item in df.iterrows():
            temp = item.to_dict()
            temp.pop('series')
            temp['type'] = Security.SecurityTypeChoice.STOCK
            inserts.append(Security(**temp))

        Security.objects.bulk_create(inserts, batch_size=500)
        existing = Security.objects.filter(
            type=Security.SecurityTypeChoice.INDEX).values_list('code', flat=True)

        existing = list(existing)
        inserts = []

        for item in INDEX_DATA:
            if item['code'] in existing:
                continue
            temp = item
            temp['type'] = Security.SecurityTypeChoice.INDEX
            inserts.append(Security(**temp))


        Security.objects.bulk_create(inserts, batch_size=500)





