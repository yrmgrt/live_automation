from django.db import models


class long_short_tb(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.test_tb', on_delete=models.PROTECT, null=True)

    # SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)
    current_iv = models.FloatField(null=True)
    current_put_skew = models.FloatField(null=True)
    current_call_skew = models.FloatField(null=True)

