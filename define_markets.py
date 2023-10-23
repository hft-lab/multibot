from clients.binance import BinanceClient
from clients.dydx import DydxClient
from clients.kraken import KrakenClient


class Define_markets:

    def __init__(self):
        pass

    def coins_symbols_client(self, clients_list, instance_num):
        client_coin_symbol_available = dict()

        # Собираем справочник: {client1:{coin1:symbol1, ...},...}
        for client in clients_list:
            try:
                client_coin_symbol_available[client] = client.get_markets()
            except Exception as error:
                print(f'Ошибка в модуле Define_markets, client: {client.__class__.__name__}, error: {error}')

        # Меняем порядок ключей в справочнике
        coins_symbols_client = dict()
        for client, coins_symbol in client_coin_symbol_available.items():
            try:
                for coin, symbol in coins_symbol.items():
                    if coin in coins_symbols_client.keys():
                        coins_symbols_client[coin].update({client.EXCHANGE_NAME: symbol})
                    else:
                        coins_symbols_client[coin] = {client.EXCHANGE_NAME: symbol}
            except Exception as error:
                input(f"Случилась ошибка 0 в модуле Define_markets: {coin},{symbol},{client}. Error: {error}")
        #Удаляем монеты с единственным маркетом
        for coin, symbols_client in coins_symbols_client.copy().items():
            if len(symbols_client) == 1:
                del coins_symbols_client[coin]
        return self.short_markets_for_instance(coins_symbols_client, instance_num)

    @staticmethod
    def short_markets_for_instance(coins_symbols_client, instance_num):
        coins_list = list(coins_symbols_client.keys())
        last_coin = instance_num * 10 if instance_num * 10 < len(coins_list) else len(coins_list)
        first_coin = (instance_num - 1) * 10
        coins_symbols_client = {x: coins_symbols_client[x] for x in coins_list[first_coin:last_coin]}
        return coins_symbols_client

def main():
    clients_list = [BinanceClient(), DydxClient(), KrakenClient()]
    ALL_CLIENTS = {
        # 'BITMEX': [BitmexClient, config['BITMEX'], config['SETTINGS']['LEVERAGE']],
        'DYDX': DydxClient,
        'BINANCE': BinanceClient,
        # 'APOLLOX': ApolloxClient,
        # 'OKX': [OkxClient, config['OKX']],
        'KRAKEN': KrakenClient
    }
    for client in clients_list:
        print(client.__class__.__name__, end=" ")

    # print(coins_symbols_client(clients_list))


if __name__ == '__main__':
    main()