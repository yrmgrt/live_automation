from django.db import models
from ..core.choices import SecurityTypeChoice


class Security(models.Model):

    SecurityTypeChoice = SecurityTypeChoice

    name = models.CharField(max_length=200)
    isin = models.CharField(max_length=20, null=True)
    code = models.CharField(max_length=10)
    type = models.CharField(max_length=50, choices=SecurityTypeChoice.choices)
    is_active = models.BooleanField(default=False)


