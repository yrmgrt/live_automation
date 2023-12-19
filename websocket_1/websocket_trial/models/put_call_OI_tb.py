from django.db import models

class put_call_OI_tb(models.Model):

    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)

    put_OI_val = models.FloatField(null=True)
    call_OI_val = models.FloatField(null=True)

    put_call_ratio = models.FloatField(null=True)
