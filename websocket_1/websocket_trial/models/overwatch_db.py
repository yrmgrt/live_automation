from django.db import models


class overwatch_db(models.Model):

    curr_time = models.TimeField(max_length=200, null=True)
    type = models.CharField(max_length=200)
    symbol1 = models.CharField(max_length=200)
    symbol2 = models.CharField(max_length=200)
    iv1 = models.FloatField(null=True)
    iv2 = models.FloatField(null=True)
    curr_val = models.FloatField(null=True)
    curr_percentile = models.FloatField(null=True)
    prev_time = models.TimeField(max_length=200, null=True)
    prev_val = models.FloatField(null=True)
    prev_percentile = models.FloatField(null=True)