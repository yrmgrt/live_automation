import os
from celery import Celery
from celery.schedules import crontab
from kombu import Queue

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'websocket_trial.settings')

app = Celery('websocket_trial')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

ENVIRONMENT = os.environ.get('ENVIRONMENT')

# app.conf.task_queues = (
#     Queue('default', routing_key='default.#'),
# )
#
# app.conf.task_default_queue = 'default'
