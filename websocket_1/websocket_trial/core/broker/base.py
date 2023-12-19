

class BaseBroker:

    def __init__(self):
        pass

    def get_historical(self):
        pass

    def get_position(self):
        pass

    def buy(self, instrument, quantity=1, order_type=None, price=None, trigger_price=None):
        pass

    def sell(self, instrument, quantity=1, order_type=None, price=None, trigger_price=None):
        pass

    def get_margin(self):
        pass

