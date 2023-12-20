from clients.binance import BinanceClient
from clients.dydx import DydxClient
from clients.kraken import KrakenClient
from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular, try_exc_async

import configparser
import sys

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")


class Clients_markets_data:
    def __init__(self, clients_list, instance_num, instance_markets_amount):
        self.instance_num = int(instance_num)
        self.clients_list = clients_list
        self.instance_markets_amount = instance_markets_amount
        self.all_markets = self.get_all_markets() # {coins: {exchanges:symbol
        self.instance_markets = self.get_instance_markets()  # {coins: {exchanges:symbol
        self.clients_data = self.get_clients_data()

    # {'CELO': {'DYDX': 'CELO-USD', 'BINANCE': 'CELOUSDT'},
    #  'LINK': {'DYDX': 'LINK-USD', 'BINANCE': 'LINKUSDT', 'KRAKEN': 'PF_LINKUSD'},

    @try_exc_regular
    def get_clients_data(self) -> dict:
        clients_data = dict()
        for client in self.clients_list:
            clients_data[client.EXCHANGE_NAME] = {'all_markets_amt': 0,'instance_markets_amt': 0}
                                                  # 'rate_per_minute': client.requestLimit,
                                                  # 'delay': round(60 / client.requestLimit, 3)}
        for coin, exchanges_symbol in self.all_markets.items():
            for exchange in exchanges_symbol:
                clients_data[exchange]['all_markets_amt'] += 1
                instance_exchanges_symbol = self.instance_markets.get(coin)
                if instance_exchanges_symbol is not None and instance_exchanges_symbol.get(exchange) is not None:
                    clients_data[exchange]['instance_markets_amt'] += 1
        return clients_data

    @try_exc_regular
    def get_all_markets(self) -> dict:
        exchange_coin_symbol = dict()
        # Собираем справочник: {exchange:{coin:symbol, ...},...}
        for client in self.clients_list:
            exchange_coin_symbol[client.EXCHANGE_NAME] = client.get_markets()
        # Меняем порядок ключей в справочнике, получаем {coin:{exchange:symbol, ...},...}
        coins_exchanges_symbol = dict()
        for exchange, coins_symbol in exchange_coin_symbol.items():
            for coin, symbol in coins_symbol.items():
                if coin in coins_exchanges_symbol.keys():
                    coins_exchanges_symbol[coin].update({exchange: symbol})
                else:
                    coins_exchanges_symbol[coin] = {exchange: symbol}
        # Удаляем монеты с единственным маркетом
        for coin, exchanges_symbol in coins_exchanges_symbol.copy().items():
            if len(exchanges_symbol) == 1:
                del coins_exchanges_symbol[coin]
        return coins_exchanges_symbol

    def get_instance_markets(self):
        coins_exchanges_symbol = self.all_markets
        total_len = len(list(coins_exchanges_symbol.keys()))
        list_end = self.instance_num * self.instance_markets_amount if self.instance_num * self.instance_markets_amount < total_len else total_len
        list_start = (self.instance_num - 1) * self.instance_markets_amount
        instance_markets = dict()
        for key in list(coins_exchanges_symbol.keys())[list_start:list_end]:
            instance_markets.update({key: coins_exchanges_symbol[key]})
        return instance_markets


def main():
    setts = config['SETTINGS']
    leverage = float(config['SETTINGS']['LEVERAGE'])
    max_position_part = float(setts['PERCENT_PER_MARKET'])

    ALL_CLIENTS = {
        'DYDX': DydxClient,
        'BINANCE': BinanceClient,
        # 'APOLLOX': ApolloxClient,
        # 'OKX': [OkxClient, config['OKX']],
        'KRAKEN': KrakenClient
    }

    clients = []
    for exchange, client in ALL_CLIENTS.items():
        print(config[exchange], leverage, max_position_part)
        new = client(config[exchange], leverage, max_position_part)
        clients.append(new)

    for client in clients:
        print(client.__class__.__name__, end=" ")

    clients_markets_data = Clients_markets_data(clients)
    print(clients_markets_data.all_markets)
    print(clients_markets_data.clients_data)


if __name__ == '__main__':
    main()
