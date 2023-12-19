from django.db import models

class idv_cal(models.Model):
    symbol = models.CharField(max_length=200)

    long_move = models.FloatField(default=0)
    full_move = models.FloatField(default=0)
    current_fut = models.FloatField(default=0)
    current_vol = models.FloatField(null=True)
    bench_vol = models.FloatField(null=True)
    day_theta = models.FloatField(null=True)
