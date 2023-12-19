from django.db import models

class skew_scanner(models.Model):

    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)
    spread = models.CharField(max_length=200, null=True)
    z = models.FloatField(null=True)
    ivp = models.FloatField(null=True)
    b_value = models.FloatField(null=True)
    a_value = models.FloatField(null=True)

    put_strike = models.FloatField(null=True)
    call_strike = models.FloatField(null=True)

    atm_strike = models.FloatField(null=True)

    put_iv = models.FloatField(null=True)
    call_iv = models.FloatField(null=True)
    atm_iv = models.FloatField(null=True)

    up_rem = models.FloatField(null=True)
    down_rem = models.FloatField(null=True)

    skew = models.FloatField(null=True)
    target = models.FloatField(null=True)