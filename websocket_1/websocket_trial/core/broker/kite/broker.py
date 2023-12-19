import datetime
import json

import dateutil.parser
from kiteconnect import KiteConnect, KiteTicker
from django.conf import settings

import pandas as pd
from ..base import BaseBroker
from websocket_trial.models import KiteInstrument, InstrumentProvider#, Provider, BrokerPosition, BrokerOrder, Instrument
from websocket_trial.core.choices import ProviderChoice, KiteIntervalChoice, BrokerChoice, InstrumentTypeChoice
import requests
from django.utils import timezone
from django.core.cache import cache


class KiteBroker(BaseBroker):

    login_url = 'https://kite.trade/connect/login'
    historical_url = 'https://kite.zerodha.com/oms/instruments/historical/{instrument_token}/{interval}'
    instance = None

    def __init__(self):
        self.kite = KiteConnect(settings.KITE_API_KEY)
        # self.access_token = self.get_access_token()
        # self.kite.set_access_token(self.access_token)
        # self.ticker = KiteTicker(settings.KITE_API_KEY, self.access_token)

    @staticmethod
    def get_instance() -> 'KiteBroker':
        if KiteBroker.instance is None:
            k = KiteBroker()
            KiteBroker.instance = k
        return KiteBroker.instance



    def get_historical(self, instrument, interval, start, end, continuous=False):

        provider = InstrumentProvider.get_kite_provider().filter(
            instrument=instrument
        ).order_by('instrument_id', '-pk').get()
        date_string_format = "%Y-%m-%d"
        from_date_string = start.strftime(date_string_format) if type(start) == datetime.datetime else start
        to_date_string = end.strftime(date_string_format) if type(end) == datetime.datetime else end

        kite_interval = getattr(KiteIntervalChoice, interval)
        url = self.historical_url.format(instrument_token=provider.token, interval=kite_interval)

        data = requests.get(url, params={
            "from": from_date_string,
            "to": to_date_string,
            "continuous": 1 if continuous else 0
        }, headers=dict(authorization=f"enctoken {settings.KITE_TEMP_ENC_TOKEN}"))

        data = json.loads(data.content.decode("utf8"))['data']
        data = self.kite._format_historical(data)
        df = pd.DataFrame(data=data)

        if len(df) > 0:
            cols = list(df.columns)
            cols[0] = 'datetime'
            df.columns = cols
            df = df.set_index(['datetime'])
        return df

    def get_kite_instrument_item(self, instrument) -> InstrumentProvider:
        model = InstrumentProvider.objects.filter(provider=ProviderChoice.KITE, instrument=instrument).order_by('-pk').first()
        return model




