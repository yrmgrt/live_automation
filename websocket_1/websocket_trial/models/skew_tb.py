from django.db import models


#
class skew_table(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.test_tb', on_delete=models.PROTECT, null=True)

    # SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)
    put_b_value = models.FloatField(null=True)
    call_b_value = models.FloatField(null=True)
    put_call_b_value = models.FloatField(null=True)
    put_call_a_value = models.FloatField(null=True)
    put_straddle_skew = models.FloatField(null=True)
    call_straddle_skew = models.FloatField(null=True)
    put_call_straddle_skew = models.FloatField(null=True)
    put_call_straddle_kurt = models.FloatField(null=True)
    R2_put_call = models.FloatField(null=True)
    R2_call = models.FloatField(null=True)
    strike_1_put_call = models.FloatField(null=True)
    strike_2_put_call = models.FloatField(null=True)
    strike_1_call = models.FloatField(null=True)
    strike_2_call = models.FloatField(null=True)
    strike_1_put = models.FloatField(null=True)
    strike_2_put = models.FloatField(null=True)
    put_call_strikes_ratio = models.FloatField(null=True)
    call_strikes_ratio = models.FloatField(null=True)