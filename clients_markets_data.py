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
    def __init__(self, clients_list, instance_num):
        self.instance_num = int(instance_num)
        self.clients_list = clients_list
        self.instance_markets_amount = int(config['SETTINGS']['INSTANCE_MARKETS_AMOUNT'])
        self.coins_clients_symbols = self.get_coins_clients_symbol()
        self.clients_data = self.get_clients_data()

    #
    # {'CELO': {'DYDX': 'CELO-USD', 'BINANCE': 'CELOUSDT'},
    #  'LINK': {'DYDX': 'LINK-USD', 'BINANCE': 'LINKUSDT', 'KRAKEN': 'PF_LINKUSD'},

    @try_exc_regular
    def get_clients_data(self):
        clients_data = dict()
        for client in self.clients_list:
            clients_data[client.EXCHANGE_NAME] = {'markets_amt': 0,
                                                  'rate_per_minute': client.requestLimit,
                                                  'delay': round(60 / client.requestLimit, 3)}
        for coin, exchange_symbol in self.coins_clients_symbols.items():
            for exchange, symbol in exchange_symbol.items():
                clients_data[exchange]['markets_amt'] += 1
        return clients_data

    @try_exc_regular
    def get_coins_clients_symbol(self):
        client_coin_symbol = dict()
        # Собираем справочник: {client1:{coin1:symbol1, ...},...}
        for client in self.clients_list:
            client_coin_symbol[client] = client.get_markets()
        # Меняем порядок ключей в справочнике
        coins_symbols_client = dict()
        for client, coins_symbol in client_coin_symbol.items():
            for coin, symbol in coins_symbol.items():
                if coin in coins_symbols_client.keys():
                    coins_symbols_client[coin].update({client.EXCHANGE_NAME: symbol})
                else:
                    coins_symbols_client[coin] = {client.EXCHANGE_NAME: symbol}
        #Удаляем монеты с единственным маркетом
        for coin, symbols_client in coins_symbols_client.copy().items():
            if len(symbols_client) == 1:
                del coins_symbols_client[coin]
        coins_symbols_client = self.get_instance_markets(coins_symbols_client)
        return coins_symbols_client

    def get_instance_markets(self, coins_symbols_client):
        total_len = len(list(coins_symbols_client.keys()))
        list_end = self.instance_num * self.instance_markets_amount if self.instance_num * self.instance_markets_amount < total_len else total_len
        list_start = (self.instance_num - 1) * self.instance_markets_amount
        new_dict = dict()
        for key in list(coins_symbols_client.keys())[list_start:list_end]:
            new_dict.update({key: coins_symbols_client[key]})
        return new_dict


def main():

    setts = config['SETTINGS']
    leverage = float(config['SETTINGS']['LEVERAGE'])
    alert_id = config['TELEGRAM']['ALERT_CHAT_ID']
    alert_token = config['TELEGRAM']['ALERT_BOT_TOKEN']
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
        print(config[exchange], leverage, alert_id, alert_token, max_position_part)
        new = client(config[exchange], leverage, alert_id, alert_token, max_position_part)
        clients.append(new)


    for client in clients:
        print(client.__class__.__name__, end=" ")

    clients_markets_data = Clients_markets_data(clients)
    print(clients_markets_data.coins_clients_symbols)
    print(clients_markets_data.clients_data)



if __name__ == '__main__':
    main()
