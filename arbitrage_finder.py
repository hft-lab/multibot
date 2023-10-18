import time
from datetime import datetime
import telebot
import csv


class ArbitrageFinder:

    def __init__(self, markets, clients_list, profit_taker, profit_close):
        self.chat_id = -807300930
        self.telegram_bot = telebot.TeleBot('6037890725:AAHSKzK9aazvOYU2AiBSDO8ZLE5bJaBNrBw')
        self.profit_taker = profit_taker
        self.profit_close = profit_close
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_list = clients_list
        self.fees = {x: y.taker_fee for x, y in self.clients_list.items()}

    def arbitrage(self, data, time_parse):
        possibilities = []
        positions = {x: y.get_position() for x, y in self.clients_list.items()}
        for coin in self.coins:
            for exchange_1, client_1 in self.clients_list.items():
                for exchange_2, client_2 in self.clients_list.items():
                    if exchange_1 == exchange_2:
                        continue
                    if compare_1 := data.get(exchange_1 + '__' + coin):
                        if compare_2 := data.get(exchange_2 + '__' + coin):
                            if not float(compare_2['top_bid']) or not float(compare_1['top_ask']):
                                continue
                            buy_market = self.markets[coin][exchange_1]
                            sell_market = self.markets[coin][exchange_2]
                            buy_close = True if positions[exchange_1].get(buy_market, 0) < 0 else False
                            sell_close = True if positions[exchange_2].get(buy_market, 0) > 0 else False
                            target_profit = self.profit_taker
                            deal_direction = 'open'
                            if buy_close and sell_close:
                                target_profit = self.profit_close
                                deal_direction = 'close'
                            elif buy_close or sell_close:
                                target_profit = (self.profit_close + self.profit_taker) / 2
                                deal_direction = 'half_close'
                            profit = (float(compare_2['top_bid']) - float(compare_1['top_ask'])) / float(compare_1['top_ask'])
                            profit = profit - self.fees[exchange_1] - self.fees[exchange_2]
                            if profit > target_profit:
                                # print(f"AP! {coin}: S.E: {exchange_2} | B.E: {exchange_1} | Profit: {profit}")
                                deal_size = min(float(compare_1['bid_vol']), float(compare_2['ask_vol']))
                                deal_size_usd = deal_size * float(compare_2['top_bid'])
                                expect_profit_abs = profit * deal_size_usd
                                possibility = {
                                    'coin': coin,
                                    'buy_exchange': exchange_1,
                                    'sell_exchange': exchange_2,
                                    'buy_market': buy_market,
                                    'sell_market': sell_market,
                                    'buy_fee': self.fees[exchange_1],
                                    'sell_fee': self.fees[exchange_2],
                                    'sell_price': float(compare_2['top_bid']),
                                    'buy_price': float(compare_1['top_ask']),
                                    'sell_size': float(compare_1['bid_vol']),
                                    'buy_size': float(compare_2['ask_vol']),
                                    'deal_size_coin': deal_size,
                                    'deal_size_usd': deal_size_usd,
                                    'expect_profit_rel': round(profit, 5),
                                    'expect_profit_abs_usd': round(expect_profit_abs, 3),
                                    'datetime': datetime.utcnow(),
                                    'timestamp': round(datetime.utcnow().timestamp(), 3),
                                    'time_parser': time_parse,
                                    'deal_direction': deal_direction}
                                # message = '\n'.join([x + ': ' + str(y) for x, y in possibility.items()])
                                with open('arbi.csv', 'a', newline='') as file:
                                    writer = csv.writer(file)
                                    writer.writerow([str(y) for y in possibility.values()])
                                # print(message)
                                # try:
                                #     self.telegram_bot.send_message(self.chat_id,
                                #                                    '<pre>' + message + '</pre>',
                                #                                    parse_mode='HTML')
                                # except:
                                    time.sleep(3)
                                possibilities.append(possibility)
        return possibilities
        # print(possibilities)


if __name__ == '__main__':
    from datetime import datetime
    from define_markets import coins_symbols_client
    from clients.kraken import KrakenClient
    from clients.binance import BinanceClient
    from clients.dydx import DydxClient

    clients_list = [DydxClient(), KrakenClient(), BinanceClient()]  # , Bitfinex()]  # , Bitspay(), Ascendex()]
    markets = coins_symbols_client(clients_list)  # {coin: {symbol:client(),...},...}
    finder = ArbitrageFinder([x for x in markets.keys()], clients_list)
    data = {}
    finder.arbitrage(data)

