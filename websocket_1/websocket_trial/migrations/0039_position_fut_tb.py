# Generated by Django 4.2 on 2023-11-30 08:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('websocket_trial', '0038_pair_table_fwd_iv_1_pair_table_fwd_iv_2'),
    ]

    operations = [
        migrations.CreateModel(
            name='position_fut_tb',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(max_length=200)),
                ('benchmark_fut_value', models.FloatField(null=True)),
            ],
        ),
    ]