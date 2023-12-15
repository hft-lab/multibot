import uuid
from datetime import datetime
from core.wrappers import try_exc_regular
from typing import List


class AP:
    def __init__(self, ap_id, coin, target_profit, client_buy, client_sell, buy_exchange, sell_exchange, buy_market,
                 sell_market, buy_fee, sell_fee, sell_price, buy_price, parser_max_sell_amount,
                 parser_max_buy_amount, deal_size_amount, deal_size_usd, expect_profit_rel, expect_profit_abs_usd,
                 datetime, timestamp, deal_direction):
        # general section # Изначально из парсера, потом частично перезатирается целевыми значениями
        self.ap_id = ap_id
        self.coin = coin
        self.deal_direction = deal_direction  # Не перезатирается
        self.target_profit = target_profit # Не перезатирается
        self.deal_size_amount = deal_size_amount # потом перетирается целевым значением ордеров
        self.deal_size_usd = deal_size_usd # Потом перетирается
        self.expect_profit_rel = expect_profit_rel # Перетирается после обновления ордербука
        self.expect_profit_abs_usd = expect_profit_abs_usd # Потом перезаписывается

        # time section
        self.datetime = datetime
        self.timestamp = timestamp
        self.time_end_choose = None
        self.time_end_define_potential_deals = None
        self.time_parser = None
        self.time_define_potential_deals = None
        self.time_choose = None
        self.time_check = None
        self.time_sent = None  # Момент отправки ордеров

        # buy section
        self.client_buy = client_buy
        self.buy_exchange = buy_exchange
        self.buy_market = buy_market
        self.buy_fee = buy_fee
        self.buy_price = buy_price # Из парсера
        self.parser_max_buy_amount = parser_max_buy_amount # Из парсера
        #
        self.ob_buy = None
        self.max_buy_vol = None  # Максимально возможный размер ордера в крипте. Заполняется после обновления ордербука
        self.limit_buy_px = None # Итоговое целевое значение цены ордера
        self.buy_size = None # Итоговый размер ордера в крипте.

        self.buy_exchange_order_id = None
        self.order_id_buy = None
        self.buy_order_place_time = None

        # sell section
        self.client_sell = client_sell
        self.sell_exchange = sell_exchange
        self.sell_market = sell_market
        self.sell_fee = sell_fee
        self.sell_price = sell_price # Из парсера
        self.parser_max_sell_amount = parser_max_sell_amount

        self.ob_sell = None
        self.max_sell_vol = None  # Заполняется после обновления ордербука
        self.limit_sell_px = None # Итоговое целевое значение цены ордера
        self.sell_size = None  # Изначально из парсера, потом перезатирается целевым значением

        self.sell_exchange_order_id = None
        self.order_id_sell = None
        self.sell_order_place_time = None


class ArbitrageFinder:

    def __init__(self, markets, clients_list, profit_taker, profit_close):
        self.profit_taker = profit_taker
        self.profit_close = profit_close
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_list = clients_list
        self.fees = {x: y.taker_fee for x, y in self.clients_list.items()}


    @try_exc_regular
    def get_target_profit(self, deal_direction):
        if deal_direction == 'open':
            target_profit = self.profit_taker
        elif deal_direction == 'close':
            target_profit = self.profit_close
        else:
            target_profit = (self.profit_taker + self.profit_close) / 2
        return target_profit
    @try_exc_regular
    def get_deal_direction(self, positions, exchange_buy, exchange_sell, buy_market, sell_market):
        buy_close = False
        sell_close = False
        if pos_buy := positions[exchange_buy].get(buy_market):
            buy_close = True if pos_buy['amount_usd'] < 0 else False
        if pos_sell := positions[exchange_sell].get(sell_market):
            sell_close = True if pos_sell['amount_usd'] > 0 else False

        if buy_close and sell_close:
            deal_direction = 'close'
        elif buy_close or sell_close:
            deal_direction = 'half_close'
        else:
            deal_direction = 'open'
        return deal_direction

    @try_exc_regular
    def arbitrage_possibilities(self, data) -> List[AP]:
        # data format:
        # {self.EXCHANGE_NAME + '__' + coin: {'top_bid':, 'top_ask': , 'bid_vol':, 'ask_vol': ,'ts_exchange': }}
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
                            buy_ticksize = client_1.instruments[buy_mrkt]['tick_size']
                            sell_ticksize = client_2.instruments[sell_mrkt]['tick_size']

                            deal_direction = self.get_deal_direction(poses, ex_1, ex_2, buy_mrkt, sell_mrkt)
                            target_profit = self.get_target_profit(deal_direction)
                            if (buy_ticksize / ob_1['top_bid'] > self.profit_taker) or (
                                    sell_ticksize / ob_2['top_ask'] > self.profit_taker):
                                target_profit = 1.5 * max(
                                    buy_ticksize / ob_1['top_bid'], sell_ticksize / ob_2['top_ask'])

                            profit = (float(ob_2['top_bid']) - float(ob_1['top_ask'])) / float(ob_1['top_ask'])
                            profit = profit - self.fees[ex_1] - self.fees[ex_2]
                            if profit > target_profit:
                                # print(f"AP! {coin}: S.E: {ex_2} | B.E: {ex_1} | Profit: {profit}")
                                deal_size = min(float(ob_1['bid_vol']), float(ob_2['ask_vol']))
                                deal_size_usd = deal_size * float(ob_2['top_bid'])
                                expect_profit_abs = profit * deal_size_usd
                                possibility = AP(
                                    ap_id=uuid.uuid4(),
                                    coin=coin,
                                    target_profit=target_profit,
                                    client_buy=client_1,
                                    client_sell=client_2,
                                    buy_exchange=ex_1,
                                    sell_exchange=ex_2,
                                    buy_market=buy_mrkt,
                                    sell_market=sell_mrkt,
                                    buy_fee=self.fees[ex_1],
                                    sell_fee=self.fees[ex_2],
                                    sell_price=float(ob_2['top_bid']),
                                    buy_price=float(ob_1['top_ask']),
                                    parser_max_sell_amount=float(ob_1['bid_vol']),
                                    parser_max_buy_amount=float(ob_2['ask_vol']),
                                    deal_size_amount=deal_size,
                                    deal_size_usd=deal_size_usd,
                                    expect_profit_rel=round(profit, 5),
                                    expect_profit_abs_usd=round(expect_profit_abs, 3),
                                    datetime=datetime.utcnow(),
                                    timestamp=int(round(datetime.utcnow().timestamp() * 1000)),
                                    deal_direction=deal_direction)
                                # message = '\n'.join([x + ': ' + str(y) for x, y in possibility.items()])
                                # with open('arbi.csv', 'a', newline='') as file:
                                #     writer = csv.writer(file)
                                #     writer.writerow([str(y) for y in possibility.values()])
                                possibilities.append(possibility)
        return possibilities


if __name__ == '__main__':
    pass
    # from clients_markets_data import coins_symbols_client
    # # from clients.kraken import KrakenClient
    # # from clients.binance import BinanceClient
    # # from clients.dydx import DydxClient
    # # from clients.apollox import ApolloxClient
    #
    # clients_list = [DydxClient(), KrakenClient(), BinanceClient(), ApolloxClient()]  # , Bitfinex()]  # , Bitspay(), Ascendex()]
    # markets = coins_symbols_client(clients_list)  # {coin: {symbol:client(),...},...}
    # finder = ArbitrageFinder([x for x in markets.keys()], clients_list)
    # data = {}
    # finder.arbitrage(data)
