from django.db import models


class move_iv_tb(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.test_tb', on_delete=models.PROTECT, null=True)

    # SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)
    current_iv = models.FloatField(null=True)
    move_based_iv = models.FloatField(null=True)
    high_iv = models.FloatField(null=True)
    low_iv = models.FloatField(null=True)
    iv_diff = models.FloatField(null=True)
    current_call_iv = models.FloatField(null=True)
    current_put_iv = models.FloatField(null=True)
    call_std = models.FloatField(null=True)
    put_std = models.FloatField(null=True)
    four_leg_std = models.FloatField(null=True)