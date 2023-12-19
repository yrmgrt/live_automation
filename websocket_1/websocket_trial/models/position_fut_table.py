from django.db import models


class position_fut_tb(models.Model):

    symbol = models.CharField(max_length=200)
    benchmark_fut_value = models.FloatField(null=True)
    init_fut_value = models.FloatField(null=True)
    init_atm_iv = models.FloatField(null=True)