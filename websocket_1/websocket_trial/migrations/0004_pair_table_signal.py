# Generated by Django 4.2 on 2023-06-21 09:21

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('websocket_trial', '0003_long_short_tb_benchmark_iv'),
    ]

    operations = [
        migrations.AddField(
            model_name='pair_table',
            name='signal',
            field=models.CharField(max_length=200, null=True),
        ),
    ]