# Generated by Django 4.2 on 2023-09-11 08:26

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('websocket_trial', '0016_move_iv_tb_call_std_move_iv_tb_four_leg_std_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='forward_vol_tb',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time', models.TimeField(max_length=200, null=True)),
                ('symbol', models.CharField(max_length=200)),
                ('current_iv', models.FloatField(null=True)),
                ('fwd_vol', models.FloatField(null=True)),
                ('fut_close', models.FloatField(null=True)),
                ('current_atm', models.FloatField(null=True)),
                ('current_call_iv', models.FloatField(null=True)),
                ('current_put_iv', models.FloatField(null=True)),
            ],
        ),
    ]
