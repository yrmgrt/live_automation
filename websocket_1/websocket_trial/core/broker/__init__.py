from django.db import models


class InstrumentTypeChoice(models.TextChoices):

    EQUITY = 'EQUITY'
    FUTURE = 'FUTURE'
    OPTION = 'OPTION'


class OptionTypeChoice(models.TextChoices):
    CE = 'CE'
    PE = 'PE'


class ProviderChoice(models.TextChoices):
    KITE = 'KITE'
    IIFL = 'IIFL'


class SecurityTypeChoice(models.TextChoices):

    STOCK = 'STOCK'
    INDEX = 'INDEX'


class IntervalChoice(models.TextChoices):

    MINUTE_5 = 'MINUTE_5'
    MINUTE_10 = 'MINUTE_10'
    MINUTE_60 = 'MINUTE_60'
    DAY = 'DAY'


class KiteIntervalChoice:
    DAY = 'day'
    MINUTE_5 = '5minute'
    MINUTE_60 = '60minute'


class OrderStatusChoice(models.TextChoices):

    CREATED = 'CREATED'
    CANCELLED = 'CANCELLED'
    PROCESSED = 'PROCESSED'
    CONFIRMATION_REQUEST_SENT = 'CONFIRMATION_REQUEST_SENT'
    BROKER_ORDER_SUBMITTED = 'BROKER_ORDER_SUBMITTED'
    CANCELLED_AMO = 'CANCELLED AMO'

    COMPLETE = 'COMPLETE'
    REJECTED = 'REJECTED'
    OPEN = 'OPEN'


class OrderTypeChoice(models.TextChoices):
    BUY = 'BUY'
    SELL = 'SELL'


class OrderExecutionTypeChoice(models.TextChoices):
    MARKET = 'MARKET'
    LIMIT = 'LIMIT'
    STOPLOSS = 'STOPLOSS'
    STOPLOSS_MARKET = 'STOPLOSS_MARKET'


class ProductTypeChoice(models.TextChoices):
    MIS = 'MIS', 'MIS'
    CASH_AND_CARRY = 'CASH_AND_CARRY'


class BrokerChoice(models.TextChoices):
    KITE = 'KITE'


class PositionTypeChoice(models.TextChoices):
    DAY = 'DAY'
    NET = 'NET'


class ExchangeChoice(models.TextChoices):
    NSE = 'NSE'
    NFO = 'NFO'


class SegmentChoice(models.TextChoices):
    NSE = 'NSE'
    INDICES = 'INDICES'
