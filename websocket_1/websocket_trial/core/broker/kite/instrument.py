from kiteconnect import KiteConnect, KiteTicker
from .broker import KiteBroker
import logging
from websocket_trial.models import KiteInstrument, Security, InstrumentProvider, Instrument
from django.db import connection
from websocket_trial.core.choices import ExchangeChoice, SegmentChoice, ProviderChoice, InstrumentTypeChoice
from django.db import transaction

class KiteInstrumentManager:

    def __init__(self):
        self.logger = logging.getLogger('manager')


    def rebuild_all_instruments(self):
        broker = KiteBroker.get_instance()
        data = broker.kite.instruments()
        # print(data[1798])
        # exit()
        self.logger.debug("Adding %d entries" % len(data))

        if len(data) == 0:
            return
        result = []
        for item in data:
            item['strike_price'] = item.pop('strike', None)
            if not item.get('expiry'):
                item.pop('expiry', None)
            result.append(KiteInstrument(**item))
        cursor = connection.cursor()
        self.logger.debug("Truncating table")
        cursor.execute("TRUNCATE TABLE {} RESTART IDENTITY RESTRICT".format(KiteInstrument._meta.db_table))
        self.logger.debug("Table truncated successfully")
        KiteInstrument.objects.bulk_create(result, batch_size=2000)
        self.logger.debug("Kite Instrument Done")

    def map_equity_instruments(self):
        securities = list(Security.objects.filter().values_list('id', 'code').order_by('code'))

        # This is custom code for handling difference in code for index
        for i, item in enumerate(securities):
            item = list(item)
            if item[1] == 'NIFTY':
                item[1] = 'NIFTY 50'
            elif item[1] == 'BANKNIFTY':
                item[1] = 'NIFTY BANK'
            securities[i] = item

        self.logger.debug("Total Equity Instruments %d" % len(securities))
        dictionary = dict(securities)
        swapped_securities = dict(zip(dictionary.values(), dictionary.keys()))

        kite_base = KiteInstrument.objects.filter(
            tradingsymbol__in=dictionary.values(),
            exchange=ExchangeChoice.NSE,
        )
        kite_tradingsymbol = list(kite_base.values_list('tradingsymbol', flat=True))
        base_query = Instrument.objects.filter(
            type=InstrumentTypeChoice.EQUITY
        )
        existing_symbols = list(base_query.filter(security__code__in=kite_tradingsymbol).values_list('security__code', flat=True))
        new_entries = set(kite_tradingsymbol) - set(existing_symbols)
        kite_instruments = kite_base.filter(
            tradingsymbol__in=new_entries
        )
        self.logger.debug("Total Kite Equity Instruments %d" % len(kite_instruments))
        inserts = []
        for instrument in kite_instruments:
            model = Instrument()
            model.security_id = swapped_securities.get(instrument.tradingsymbol)
            model.code = instrument.tradingsymbol
            model.type = InstrumentTypeChoice.EQUITY
            inserts.append(model)

        Instrument.objects.bulk_create(inserts, batch_size=1000)
        self.logger.debug("Equity instruments mapping done")

        ### Build InstrumentProvider
        base_query = InstrumentProvider.objects.filter(
            instrument__type=InstrumentTypeChoice.EQUITY,
            provider=ProviderChoice.KITE
        )
        instruments = Instrument.objects.filter(type=InstrumentTypeChoice.EQUITY).values_list('code', 'pk')
        instruments = dict(instruments)

        existing_symbols = list(base_query.filter(tradingsymbol__in=kite_tradingsymbol).values_list('tradingsymbol', flat=True))
        new_entries = set(kite_tradingsymbol) - set(existing_symbols)
        kite_instruments = kite_base.filter(
            tradingsymbol__in=new_entries
        )
        inserts = []
        for instrument in kite_instruments:
            model = InstrumentProvider()
            model.instrument_id = instruments.get(instrument.tradingsymbol)
            model.provider = ProviderChoice.KITE
            model.tradingsymbol = instrument.tradingsymbol
            model.token = instrument.instrument_token
            model.exchange_token = instrument.exchange_token
            model.exchange = instrument.exchange
            inserts.append(model)
        InstrumentProvider.objects.bulk_create(inserts, batch_size=1000)
        self.logger.debug("Equity instruments provider mapping done")

    def map_derivative_instruments(self):
        securities = Security.objects.filter().values_list('id', 'code').order_by('code')
        dictionary = dict(securities)
        swapped_securities = dict(zip(dictionary.values(), dictionary.keys()))

        all_instruments = KiteInstrument.objects.filter(
            exchange=ExchangeChoice.NFO,
            name__in=dictionary.values()
        ).order_by('expiry', 'tradingsymbol')

        self.logger.debug('Total Instruments: {}'.format(all_instruments.count()))
        symbols = {s.tradingsymbol: s for s in all_instruments}
        symbols_copy = symbols.copy()

        base_query = Instrument.objects.filter(
            code__in=symbols.keys(),
            type__in=[InstrumentTypeChoice.FUTURE, InstrumentTypeChoice.OPTION]
        )

        all_instruments_tradingsymbol = all_instruments.values_list('tradingsymbol', flat=True)
        existing = base_query.values_list('code', flat=True)

        new_entries = set(all_instruments_tradingsymbol) - set(existing)

        entries = KiteInstrument.objects.filter(
            exchange=ExchangeChoice.NFO
        ).filter(tradingsymbol__in=new_entries)

        inserts = []
        for item in entries:
            d = dict(
                code=item.tradingsymbol,
                security_id=swapped_securities.get(item.name),
                expiry=item.expiry,
                lot_size=item.lot_size,
            )
            if item.segment in ['NFO-OPT']:
                d['type'] = InstrumentTypeChoice.OPTION
                d['strike_price'] = item.strike_price
                d['option_type'] = item.instrument_type
            else:
                d['type'] = InstrumentTypeChoice.FUTURE
            inserts.append(Instrument(**d))

        Instrument.objects.bulk_create(inserts, batch_size=1000)

        ### Build InstrumentProvider
        base_query = InstrumentProvider.objects.filter(
            instrument__type__in=[InstrumentTypeChoice.FUTURE, InstrumentTypeChoice.OPTION],
            provider=ProviderChoice.KITE
        )
        instruments = Instrument.objects.filter(type__in=[InstrumentTypeChoice.FUTURE, InstrumentTypeChoice.OPTION],).values_list('code', 'pk')
        instruments = dict(instruments)

        existing_symbols = list(
            base_query.filter(tradingsymbol__in=all_instruments_tradingsymbol).values_list('tradingsymbol', flat=True))
        new_entries = set(all_instruments_tradingsymbol) - set(existing_symbols)
        kite_instruments = all_instruments.filter(
            tradingsymbol__in=new_entries
        )
        inserts = []
        for instrument in kite_instruments:
            model = InstrumentProvider()
            model.instrument_id = instruments.get(instrument.tradingsymbol)
            model.provider = ProviderChoice.KITE
            model.tradingsymbol = instrument.tradingsymbol
            model.token = instrument.instrument_token
            model.exchange_token = instrument.exchange_token
            model.exchange = instrument.exchange
            inserts.append(model)
        InstrumentProvider.objects.bulk_create(inserts, batch_size=1000)
        self.logger.debug("Equity instruments provider mapping done")


#################
##Whenever you do InstrumentProvider.objects.bulk_create L172 it automatically updates the automation security model