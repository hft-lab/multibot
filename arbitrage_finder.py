import uuid
from datetime import datetime
from core.wrappers import try_exc_regular
from typing import List
from core.ap_class import AP
import time
import json


class ArbitrageFinder:

    def __init__(self, markets, clients_with_names, profit_taker, profit_close):
        self.profit_taker = profit_taker
        self.profit_close = profit_close
        self.profit_precise = 4
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_with_names = clients_with_names
        self.fees = {x: y.taker_fee for x, y in self.clients_with_names.items()}
        self.last_record = time.time()
        self.profit_ranges = self.unpack_ranges()
        if not self.profit_ranges.get('timestamp_start'):
            self.profit_ranges.update({'timestamp_start': time.time()})
        # print(self.profit_ranges)
        self.target_profits = self.get_target_profits()


    @staticmethod
    def unpack_ranges() -> dict:
        try:
            with open('ranges.json', 'r') as file:
                return json.load(file)
        except:
            with open('ranges.json', 'w') as file:
                new = {'timestamp': time.time(), 'timestamp_start': time.time()}
                json.dump(new, file)
            return new

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
    def find_arbitrage_possibilities(self, data, ribs = None) -> List[AP]:
        # data format:
        # {self.EXCHANGE_NAME + '__' + coin: {'top_bid':, 'top_ask': , 'bid_vol':, 'ask_vol': ,'ts_exchange': }}
        possibilities = []
        poses = {}
        if not ribs:
            poses = {x: y.get_positions() for x, y in self.clients_with_names.items()}
        for coin in self.coins:
            for ex_1, client_1 in self.clients_with_names.items():
                for ex_2, client_2 in self.clients_with_names.items():
                    if ex_1 == ex_2:
                        continue
                    if ribs:
                        if [ex_1, ex_2] not in ribs:
                            continue
                    if ob_1 := data.get(ex_1 + '__' + coin):
                        if ob_2 := data.get(ex_2 + '__' + coin):
                            if not float(ob_2['top_bid']) or not float(ob_1['top_ask']):
                                continue
                            buy_mrkt = self.markets[coin][ex_1]
                            sell_mrkt = self.markets[coin][ex_2]

                            deal_direction = 'open'
                            target_profit = self.get_target_profit(deal_direction)
                            if not ribs:
                                deal_direction = self.get_deal_direction(poses, ex_1, ex_2, buy_mrkt, sell_mrkt)
                                target_profit = self.get_target_profit(deal_direction)
                                buy_ticksize = client_1.instruments[buy_mrkt]['tick_size']
                                sell_ticksize = client_2.instruments[sell_mrkt]['tick_size']
                                if (buy_ticksize / ob_1['top_bid'] > self.profit_taker) or (
                                        sell_ticksize / ob_2['top_ask'] > self.profit_taker):
                                    target_profit = 1.5 * max(
                                        buy_ticksize / ob_1['top_bid'], sell_ticksize / ob_2['top_ask'])

                            profit = (float(ob_2['top_bid']) - float(ob_1['top_ask'])) / float(ob_1['top_ask'])
                            profit = profit - self.fees[ex_1] - self.fees[ex_2]
                            name = f"B:{ex_1}|S:{ex_2}|C:{coin}"
                            self.append_profit(profit=profit, name=name)
                            target = self.target_profits.get(name)
                            if not target:
                                continue
                            if profit >= self.target_profits[name]:
                                # print(f"AP! {coin}: S.E: {ex_2} | B.E: {ex_1} | Profit: {profit}")
                                deal_size_amount = min(float(ob_1['bid_vol']), float(ob_2['ask_vol']))
                                deal_size_usd_max = deal_size_amount * float(ob_2['top_bid'])
                                profit_usd_max = profit * deal_size_usd_max
                                possibility = AP(ap_id=uuid.uuid4())
                                possibility.set_data_from_parser(
                                    coin=coin,
                                    target_profit=target,
                                    deal_max_amount_parser=deal_size_amount,
                                    deal_max_usd_parser=deal_size_usd_max,
                                    expect_profit_rel=round(profit, 5),
                                    profit_usd_max=round(profit_usd_max, 3),
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
                                    max_amount=float(ob_1['ask_vol']),
                                    ts_ob=ob_1['ts_exchange']
                                )
                                possibility.set_side_data_from_parser(
                                    side='sell',
                                    client=client_2,
                                    exchange=ex_2,
                                    market=sell_mrkt,
                                    fee=self.fees[ex_2],
                                    max_amount=float(ob_2['bid_vol']),
                                    price=float(ob_2['top_bid']),
                                    ts_ob=ob_2['ts_exchange']
                                )
                                # message = '\n'.join([x + ': ' + str(y) for x, y in possibility.items()])
                                # with open('arbi.csv', 'a', newline='') as file:
                                #     writer = csv.writer(file)
                                #     writer.writerow([str(y) for y in possibility.values()])
                                possibilities.append(possibility)
        return possibilities

    @try_exc_regular
    def get_coins_profit_ranges(self):
        coins = {}
        for direction in self.profit_ranges.keys():
            if 'timestamp' in direction:
                continue
            coin = direction.split('C:')[1]
            range = sorted([[float(x), y] for x, y in self.profit_ranges[direction].items()], reverse=True)
            if coins.get(coin):
                coin = coin + '_reversed'
            coins.update({coin: {'range': range,
                                 'direction': direction}})
        return coins

    @try_exc_regular
    def get_target_profits(self):
        coins = self.get_coins_profit_ranges()
        target_profits = {}
        for coin in coins.keys():
            if 'reversed' in coin:
                continue
            direction_one = coins[coin]
            direction_two = coins[coin + '_reversed']
            sum_freq_1 = 0
            sum_freq_2 = 0
            for profit_1, freq_1 in direction_one['range']:
                if sum_freq_1 > 3000:
                    break
                sum_freq_1 += freq_1
            for profit_2, freq_2 in direction_two['range']:
                if sum_freq_2 > 3000:
                    break
                sum_freq_2 += freq_2
            if profit_1 + profit_2 > self.profit_taker and profit_1 > 0 and profit_2 > 0:
                target_1 = [profit_1, sum_freq_1]
                target_2 = [profit_2, sum_freq_2]
                print(F"TARGET PROFIT {direction_one['direction']}:", target_1)
                print(F"TARGET PROFIT REVERSED {direction_two['direction']}:", target_2)
                print()
                target_profits.update({direction_one['direction']: target_1[0] if target_1 else target_1,
                                       direction_two['direction']: target_2[0] if target_2 else target_2})
        return target_profits

    @try_exc_regular
    def append_profit(self, profit: float, name: str):
        profit = round(profit, self.profit_precise)
        if self.profit_ranges.get(name):
            if self.profit_ranges[name].get(profit):
                self.profit_ranges[name][profit] += 1
            else:
                self.profit_ranges[name].update({profit: 1})
        else:
            self.profit_ranges.update({name: {profit: 1}})
        now = time.time()
        if now - self.last_record > 3600:
            with open('ranges.json', 'w') as file:
                json.dump(self.profit_ranges, file)
            self.last_record = now
        if now - self.profit_ranges['timestamp_start'] > 3600 * 24:
            with open(f'ranges{str(datetime.utcnow()).split(" ")[0]}.json', 'w') as file:
                json.dump(self.profit_ranges, file)
            self.profit_ranges = {'timestamp': now, 'timestamp_start': now}


if __name__ == '__main__':
    pass
    # from clients_markets_data import coins_symbols_client
    # # from clients-http.kraken import KrakenClient
    # # from clients-http.binance import BinanceClient
    # # from clients-http.dydx import DydxClient
    # # from clients-http.apollox import ApolloxClient
    #
    # clients_list = [DydxClient(), KrakenClient(), BinanceClient(), ApolloxClient()]  # , Bitfinex()]  # , Bitspay(), Ascendex()]
    # markets = coins_symbols_client(clients_list)  # {coin: {symbol:client(),...},...}
    # finder = ArbitrageFinder([x for x in markets.keys()], clients_list)
    # data = {}
    # finder.arbitrage(data)
