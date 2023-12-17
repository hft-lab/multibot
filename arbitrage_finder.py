import uuid
from datetime import datetime
from core.wrappers import try_exc_regular
from typing import List
from core.ap_class import AP


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
                                deal_size_amount = min(float(ob_1['bid_vol']), float(ob_2['ask_vol']))
                                deal_size_usd = deal_size_amount * float(ob_2['top_bid'])
                                profit_usd = profit * deal_size_usd
                                possibility = AP(ap_id=uuid.uuid4())
                                possibility.set_data_from_parser(
                                    coin=coin,
                                    target_profit=target_profit,
                                    deal_max_amount_parser=deal_size_amount,
                                    deal_max_usd_parser=deal_size_usd,
                                    expect_profit_rel=round(profit, 5),
                                    expect_profit_usd=round(profit_usd, 3),
                                    datetime=datetime.utcnow(),
                                    timestamp=int(round(datetime.utcnow().timestamp() * 1000)),
                                    deal_direction=deal_direction)

                                possibility.set_side_data_from_parser(
                                    side='buy',
                                    client=client_1,
                                    exchange=ex_1,
                                    market=buy_mrkt,
                                    fee=self.fees[ex_1],
                                    price=float(ob_1['top_ask']),
                                    max_amount=float(ob_2['ask_vol'])
                                )
                                possibility.set_side_data_from_parser(
                                    side='sell',
                                    client = client_2,
                                    exchange = ex_2,
                                    market = sell_mrkt,
                                    fee = self.fees[ex_2],
                                    max_amount = float(ob_1['bid_vol']),
                                    price = float(ob_2['top_bid'])
                                )
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
