# Generated by Django 4.2 on 2023-11-29 08:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('websocket_trial', '0036_next_exp_scanner_tb'),
    ]

    operations = [
        migrations.AddField(
            model_name='long_short_tb',
            name='fwd_iv',
            field=models.FloatField(null=True),
        ),
    ]