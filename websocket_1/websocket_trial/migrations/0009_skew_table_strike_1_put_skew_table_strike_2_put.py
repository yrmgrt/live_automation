# Generated by Django 4.2 on 2023-07-12 08:59

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('websocket_trial', '0008_vol_table_fut_close'),
    ]

    operations = [
        migrations.AddField(
            model_name='skew_table',
            name='strike_1_put',
            field=models.FloatField(null=True),
        ),
        migrations.AddField(
            model_name='skew_table',
            name='strike_2_put',
            field=models.FloatField(null=True),
        ),
    ]
