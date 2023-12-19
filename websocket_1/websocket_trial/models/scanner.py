from django.db import models

class scanner_table_skew(models.Model):
    num = models.FloatField(null=True)
    symbol = models.CharField(max_length=200)
    spread = models.CharField(max_length=200, null=True)
    z_score = models.FloatField(null=True)
    straddle_skew = models.FloatField(null=True)

    strike_1 = models.FloatField(null=True)
    strike_2 = models.FloatField(null=True)

    exit = models.FloatField(null=True)
    entry = models.FloatField(null=True)

    current_iv = models.FloatField(null=True)
    correlation_last_day = models.FloatField(null=True)
    correlation_last_week = models.FloatField(null=True)
    r2 = models.FloatField(null=True)

