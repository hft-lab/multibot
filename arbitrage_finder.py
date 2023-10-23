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

    def check_direction(self, positions, exchange_buy, exchange_sell, buy_market, sell_market):
        buy_close = False
        sell_close = False
        if pos_buy := positions[exchange_buy].get(buy_market):
            buy_close = True if pos_buy['amount_usd'] < 0 else False
        if pos_sell := positions[exchange_sell].get(sell_market):
            sell_close = True if pos_sell['amount_usd'] > 0 else False
        target_profit = self.profit_taker
        deal_direction = 'open'
        if buy_close and sell_close:
            target_profit = self.profit_close
            deal_direction = 'close'
        elif buy_close or sell_close:
            target_profit = (self.profit_close + self.profit_taker) / 2
            deal_direction = 'half_close'
        return target_profit, deal_direction

    def arbitrage(self, data, time_parse):
        possibilities = []
        poses = {x: y.get_positions() for x, y in self.clients_list.items()}
        for coin in self.coins:
            for ex_1, client_1 in self.clients_list.items():
                for ex_2, client_2 in self.clients_list.items():
                    if ex_1 == ex_2:
                        continue
                    if ob_1 := data.get(ex_1 + '__' + coin):
                        if ob_2 := data.get(ex_2 + '__' + coin):
                            if not float(ob_2['top_bid']) or not float(ob_1['top_ask']):
                                continue
                            buy_mrkt = self.markets[coin][ex_1]
                            sell_mrkt = self.markets[coin][ex_2]
                            target_profit, deal_direction = self.check_direction(poses, ex_1, ex_2, buy_mrkt, sell_mrkt)
                            profit = (float(ob_2['top_bid']) - float(ob_1['top_ask'])) / float(ob_1['top_ask'])
                            profit = profit - self.fees[ex_1] - self.fees[ex_2]
                            if profit > target_profit:
                                # print(f"AP! {coin}: S.E: {ex_2} | B.E: {ex_1} | Profit: {profit}")
                                deal_size = min(float(ob_1['bid_vol']), float(ob_2['ask_vol']))
                                deal_size_usd = deal_size * float(ob_2['top_bid'])
                                expect_profit_abs = profit * deal_size_usd
                                possibility = {
                                    'coin': coin,
                                    'buy_exchange': ex_1,
                                    'sell_exchange': ex_2,
                                    'buy_market': buy_mrkt,
                                    'sell_market': sell_mrkt,
                                    'buy_fee': self.fees[ex_1],
                                    'sell_fee': self.fees[ex_2],
                                    'sell_price': float(ob_2['top_bid']),
                                    'buy_price': float(ob_1['top_ask']),
                                    'sell_size': float(ob_1['bid_vol']),
                                    'buy_size': float(ob_2['ask_vol']),
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
    # from datetime import datetime
    # from clients_markets_data import coins_symbols_client
    # # from clients.kraken import KrakenClient
    # # from clients.binance import BinanceClient
    # # from clients.dydx import DydxClient
    # # from clients.apollox import ApolloxClient
    #
    # # clients_list = [DydxClient(), KrakenClient(), BinanceClient(), ApolloxClient()]  # , Bitfinex()]  # , Bitspay(), Ascendex()]
    # markets = coins_symbols_client(clients_list)  # {coin: {symbol:client(),...},...}
    # finder = ArbitrageFinder([x for x in markets.keys()], clients_list)
    # data = {}
    # finder.arbitrage(data)
