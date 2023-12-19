from django.db import models


#
class new_skew_table(models.Model):
    # live_update_df = models.ForeignKey('websocket_trial.test_tb', on_delete=models.PROTECT, null=True)

    # SecurityTypeChoice = SecurityTypeChoice
    time = models.TimeField(max_length=200, null=True)
    symbol = models.CharField(max_length=200)

    b_value = models.FloatField(null=True)
    a_value = models.FloatField(null=True)

    put_b_value = models.FloatField(null=True)
    put_a_value = models.FloatField(null=True)

    call_b_value = models.FloatField(null=True)
    call_a_value = models.FloatField(null=True)

    put_skew_strike = models.FloatField(null=True)
    call_skew_strike = models.FloatField(null=True)

    put_skew_strike1 = models.FloatField(null=True)
    put_skew_strike2 = models.FloatField(null=True)

    call_skew_strike1 = models.FloatField(null=True)
    call_skew_strike2 = models.FloatField(null=True)

    put_kurt_strike = models.FloatField(null=True)
    call_kurt_strike = models.FloatField(null=True)

    atm_strike = models.FloatField(null=True)

    put_skew_iv = models.FloatField(null=True)
    call_skew_iv = models.FloatField(null=True)

    put_skew_iv1 = models.FloatField(null=True)
    put_skew_iv2 = models.FloatField(null=True)

    call_skew_iv1 = models.FloatField(null=True)
    call_skew_iv2 = models.FloatField(null=True)

    put_kurt_iv = models.FloatField(null=True)
    call_kurt_iv = models.FloatField(null=True)

    atm_iv = models.FloatField(null=True)

    p_c_skew = models.FloatField(null=True)

    skew_atm_diff = models.FloatField(null=True)
    kurt_atm_diff = models.FloatField(null=True)

    put_skew_atm_diff1 = models.FloatField(null=True)
    put_skew_atm_diff2 = models.FloatField(null=True)

    call_skew_atm_diff1 = models.FloatField(null=True)
    call_skew_atm_diff2 = models.FloatField(null=True)


