from django.db import models


class token_table(models.Model):

    instrument_token = models.BigIntegerField()
    exchange_token = models.BigIntegerField()
    tradingsymbol = models.CharField(max_length=100)
    name = models.CharField(max_length=100)
    last_price = models.FloatField(null=True)
    expiry = models.DateField(null=True)
    strike_price = models.FloatField(null=True)
    tick_size = models.FloatField(null=True)
    lot_size = models.IntegerField(null=True)
    instrument_type = models.CharField(null=True, max_length=100)
    segment = models.CharField(null=True, max_length=100)
    exchange = models.CharField(null=True, max_length=100)

