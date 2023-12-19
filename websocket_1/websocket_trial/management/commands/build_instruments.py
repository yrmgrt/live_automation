from django.core.management.base import BaseCommand
from websocket_trial.core.broker.kite.instrument import KiteInstrumentManager
from io import StringIO

class Command(BaseCommand):

    def handle(self, *args, **options):
        manager = KiteInstrumentManager()
        manager.rebuild_all_instruments()
        manager.map_equity_instruments()
        manager.map_derivative_instruments()
