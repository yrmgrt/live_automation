# Generated by Django 4.2 on 2023-12-12 04:43

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('websocket_trial', '0042_weekly_skew_scanner'),
    ]

    operations = [
        migrations.AddField(
            model_name='position_fut_tb',
            name='init_atm_iv',
            field=models.FloatField(null=True),
        ),
        migrations.AddField(
            model_name='position_fut_tb',
            name='init_fut_value',
            field=models.FloatField(null=True),
        ),
    ]
