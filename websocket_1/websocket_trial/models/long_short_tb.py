from django.db import models


class long_short_tb(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.test_tb', on_delete=models.PROTECT, null=True)

    # SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)
    current_iv = models.FloatField(null=True)
    fwd_iv = models.FloatField(null=True)
    risk_prem = models.FloatField(null=True)
    risk_prem_historical = models.FloatField(null=True)
    hv_current = models.FloatField(null=True)
    rp_diff = models.FloatField(null=True)
    fair_vol = models.FloatField(null=True)
    skew = models.FloatField(null=True)
    vol_ratio = models.FloatField(null=True)
    ivp = models.FloatField(null=True)