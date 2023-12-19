from django.core.management.base import BaseCommand


class Command(BaseCommand):

    def handle(self, *args, **options):
        from websocket_trial.core.celery.computations import new_async
        from websocket_trial.core.celery.computations import new_async_2
        # print('[start]')
        while True:
            k = new_async()
            k2 = new_async_2()
            # print('[end]: ', k)

        return 'hello'