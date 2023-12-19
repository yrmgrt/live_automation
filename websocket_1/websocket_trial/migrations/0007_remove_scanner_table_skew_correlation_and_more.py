# Generated by Django 4.2 on 2023-07-10 03:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('websocket_trial', '0006_scanner_table_skew_current_iv'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='scanner_table_skew',
            name='correlation',
        ),
        migrations.AddField(
            model_name='scanner_table_skew',
            name='correlation_last_day',
            field=models.FloatField(null=True),
        ),
        migrations.AddField(
            model_name='scanner_table_skew',
            name='correlation_last_week',
            field=models.FloatField(null=True),
        ),
    ]