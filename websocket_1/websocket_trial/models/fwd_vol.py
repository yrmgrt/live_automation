from django.db import models


class forward_vol_tb(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.test_tb', on_delete=models.PROTECT, null=True)

    # SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)
    current_iv = models.FloatField(null=True)
    fwd_vol = models.FloatField(null=True)
    fut_close = models.FloatField(null=True)
    current_atm = models.FloatField(null=True)
    current_call_iv = models.FloatField(null=True)
    current_put_iv = models.FloatField(null=True)
    fair_vol = models.FloatField(null=True)
    ivp = models.FloatField(null=True)
    hv_current = models.FloatField(null=True)