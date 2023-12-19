from django.db import models


class position_tracker_tb(models.Model):


    position_type = models.CharField(max_length=200)
    instrument = models.CharField(max_length=200)