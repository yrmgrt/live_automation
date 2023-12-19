from django.db import models
from ..core.choices import InstrumentTypeChoice, OptionTypeChoice, ProviderChoice
from django.conf import settings
import os
import pandas as pd


# Its assumed that everything is for NSE exchange.
# If working with multiple exchanges then exchange field can be added in Instrument class


class Instrument(models.Model):

    security = models.ForeignKey('websocket_trial.Security', on_delete=models.PROTECT)
    code = models.CharField(max_length=100)
    type = models.CharField(choices=InstrumentTypeChoice.choices, max_length=50)
    expiry = models.DateField(null=True)
    option_type = models.CharField(choices=OptionTypeChoice.choices, max_length=5, null=True)
    strike_price = models.FloatField(null=True)
    lot_size = models.IntegerField(null=True)

    def __str__(self):
        return f"{self.id} - {self.code} - {self.type}"

    @classmethod
    def get_equity_query(cls):
        return Instrument.objects.filter(type=InstrumentTypeChoice.EQUITY)

    def get_data(self, interval):
        path = os.path.join(settings.RUNTIME_DIR, 'data', interval, f"{self.id}.csv")
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return df


# This table holds broker specific instrument id to make the trades/get data

class InstrumentProvider(models.Model):

    instrument = models.ForeignKey('websocket_trial.Instrument', on_delete=models.PROTECT)
    provider = models.CharField(choices=ProviderChoice.choices, default=ProviderChoice.KITE, max_length=10)
    tradingsymbol = models.CharField(max_length=50)
    token = models.BigIntegerField(null=True)
    exchange_token = models.BigIntegerField(null=True)
    exchange = models.CharField(max_length=50, null=True)
    data = models.JSONField(default=dict)

    @classmethod
    def get_kite_provider(cls):
        return InstrumentProvider.objects.filter(provider=ProviderChoice.KITE)