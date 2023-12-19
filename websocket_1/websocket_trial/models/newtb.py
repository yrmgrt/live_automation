from django.db import models


#
class newtb(models.Model):
    #instrument = models.ForeignKey('automation.Instrument', on_delete=models.PROTECT)


    #SecurityTypeChoice = SecurityTypeChoice

    symbol = models.CharField(max_length=200)
    ticker = models.CharField(null=True)
    close = models.IntegerField(null=True)
    #new = models.ForeignKey('websocket_trial.live_before_df', on_delete=models.PROTECT)
    #type = models.CharField(max_length=50, choices=SecurityTypeChoice.choices)
    #is_active = models.BooleanField(default=False)