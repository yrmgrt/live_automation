# Generated by Django 4.2 on 2023-11-01 05:21

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('websocket_trial', '0030_idv_cal_bench_vol'),
    ]

    operations = [
        migrations.AddField(
            model_name='forward_vol_tb',
            name='fair_vol',
            field=models.FloatField(null=True),
        ),
    ]