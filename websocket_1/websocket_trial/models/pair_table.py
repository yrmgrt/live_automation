from django.db import models


class pair_table(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.test_tb', on_delete=models.PROTECT, null=True)

    # SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol_1 = models.CharField(max_length=200)
    iv_1 = models.FloatField(null=True)
    ivp1 = models.FloatField(null=True)
    fwd_iv_1 = models.FloatField(null=True)
    symbol_2 = models.CharField(max_length=200)
    iv_2 = models.FloatField(null=True)
    ivp2 = models.FloatField(null=True)
    fwd_iv_2 = models.FloatField(null=True)
    avg_iv_1 = models.FloatField(null=True)
    avg_iv_2 = models.FloatField(null=True)
    short_movement = models.FloatField(null=True)
    long_movement = models.FloatField(null=True)
    signal = models.CharField(max_length=200, null=True)
    current_diff = models.FloatField(null=True)
    target = models.FloatField(null=True)
    hv_current_1 = models.FloatField(null=True)
    hv_current_2 = models.FloatField(null=True)
    z_score = models.FloatField(null=True)