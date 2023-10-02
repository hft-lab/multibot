from clients.binance import BinanceClient
from clients.dydx import DydxClient
from clients.kraken import KrakenClient


def coins_symbols_client(clients_list):
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
                    coins_symbols_client[coin].update({symbol: client})
                else:
                    coins_symbols_client[coin] = {symbol: client}
        except Exception as error:
            input(f"Случилась ошибка 0 в модуле Define_markets: {coin},{symbol},{client}. Error: {error}")
    #Удаляем монеты с единственным маркетом
    for coin, symbols_client in coins_symbols_client.copy().items():
        if len(symbols_client) == 1:
            del coins_symbols_client[coin]
    return coins_symbols_client

def main():
    clients_list = [BinanceClient(), DydxClient(), KrakenClient()]

    for client in clients_list:
        print(client.__class__.__name__, end=" ")

    # print(coins_symbols_client(clients_list))


if __name__ == '__main__':
    main()
