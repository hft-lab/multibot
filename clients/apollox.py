from clients.binance import BinanceClient


class ApolloxClient(BinanceClient):
    BASE_WS = 'wss://fstream.apollox.finance/ws/'
    BASE_URL = 'https://fapi.apollox.finance'
    EXCHANGE_NAME = 'APOLLOX'

    def __init__(self, keys, leverage):
        super().__init__(keys, leverage)
