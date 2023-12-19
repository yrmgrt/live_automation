from django.db import models


#
class weekly_expiry_live(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.live_before_df', on_delete=models.PROTECT, null=True)


    #SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)
    ticker = models.CharField(null=True)
    close = models.FloatField(null=True)
    # volume = models.IntegerField(null=True)
    # open_interest = models.IntegerField(null=True)
    instrument_type = models.CharField(null=True)
    strike_price = models.FloatField(null=True)
    # expiry = models.CharField(null=True)

    #new = models.ForeignKey('websocket_trial.live_before_df', on_delete=models.PROTECT)
    #type = models.CharField(max_length=50, choices=SecurityTypeChoice.choices)
    #is_active = models.BooleanField(default=False)