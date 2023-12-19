from django.db import models


class next_exp_vol_table(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.test_tb', on_delete=models.PROTECT, null=True)

    # SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)
    strike_price = models.FloatField(null=True)
    instrument_type = models.CharField(null=True)
    bid_iv = models.FloatField(null=True)
    ask_iv = models.FloatField(null=True)
    fut_close = models.FloatField(null=True)