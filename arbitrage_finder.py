import asyncio
import traceback
import uuid
from datetime import datetime
from core.wrappers import try_exc_regular, try_exc_async
from core.ap_class import AP
import time
import json
import threading
from core.telegram import TG_Groups


class ArbitrageFinder:

    def __init__(self, markets, clients_with_names, profit_taker, profit_close,state='Bot'):
        # self.multibot = multibot
        self.state = state
        self.profit_taker = profit_taker
        self.profit_close = profit_close
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_with_names = clients_with_names
        self.fees = {x: y.taker_fee for x, y in self.clients_with_names.items()}
        self.last_record = time.time()
        self.excepts = dict()
        self.loop = asyncio.new_event_loop()
        self._wst = threading.Thread(target=self._run_finder_forever)
        self.update = False
        self.coins_to_check = []
        self._wst.daemon = True
        self._wst.start()
        # PROFIT RANGES FE
        # self.tradable_profits = {x: {} for x in self.coins}  # {coin: {exchange+side: profit_gap}}
        self.profit_precise = 4
        self.profit_ranges = self.unpack_ranges()
        self.potential_deals = []
        # if not self.profit_ranges.get('timestamp_start'):
        #     self.profit_ranges.update({'timestamp_start': time.time()})
        # print(self.profit_ranges)
        # self.target_profits = self.get_all_target_profits()
        # print(f"TARGET PROFIT RANGES FOR {(time.time() - self.profit_ranges['timestamp_start']) / 3600} HOURS")
        # print(self.target_profits)

    @try_exc_regular
    def _run_finder_forever(self):
        while True:
            self.loop.run_until_complete(self.check_coins())

    @try_exc_async
    async def check_coins(self):
        while True:
            clients = self.clients_with_names.items()
            lines = [{x: y.message_queue.qsize()} for x, y in clients if y.message_queue.qsize() > 50]
            if len(lines):
                # self.multibot.telegram.send_message(f"ALERT! WEBSOCKET LINES ARE HUGE: {lines}")
                await asyncio.sleep(1)
                self.coins_to_check = []
                self.update = False
            if self.update:
                if self.potential_deals:
                    await asyncio.sleep(0.5)
                    continue
                self.update = False
                # print(f"COUNTING STARTED, COINS: {self.coins_to_check}")
                for coin in self.coins_to_check:
                    # await self.loop.create_task(self.count_one_coin(coin))
                    asyncio.run_coroutine_threadsafe(self.count_one_coin(coin), self.loop)
                self.coins_to_check = []
            await asyncio.sleep(0.0001)

    @staticmethod
    @try_exc_regular
    def unpack_ranges() -> dict:
        try:
            with open('ranges.json', 'r') as file:
                ranges = json.load(file)
                # print(json.load(file))
            if time.time() - ranges['timestamp_start'] < 3600 * 12:
                try:
                    with open(f'ranges{str(datetime.now()).split(" ")[0]}.json', 'r') as file:
                        return json.load(file)
                except:
                    try:
                        last_date = str(datetime.fromtimestamp(time.time() - (3600 * 24))).split(' ')[0]
                        with open(f'ranges{last_date}.json', 'r') as file:
                            return json.load(file)
                    except:
                        pass
            else:
                return ranges
        except Exception:
            traceback.print_exc()
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
            return 'close'
        elif not buy_close and not sell_close:
            return 'open'
        else:
            return 'half_close'
        # if deal_direction == 'half_close':
        #     print(f"ALERT. WRONG DEAL DIRECTION: {positions[exchange_buy]=}\n{positions[exchange_sell]=}")

    def target_profit_exceptions(self, data):
        targets = dict()
        for coin in self.coins:
            for ex_1, client_1 in self.clients_with_names.items():
                for ex_2, client_2 in self.clients_with_names.items():
                    if ex_1 == ex_2:
                        continue
                    if ob_1 := data.get(ex_1 + '__' + coin):
                        if ob_2 := data.get(ex_2 + '__' + coin):
                            if not ob_2['top_bid'] or not ob_1['top_ask']:
                                continue
                            buy_mrkt = self.markets[coin][ex_1]
                            sell_mrkt = self.markets[coin][ex_2]
                            buy_ticksize_rel = client_1.instruments[buy_mrkt]['tick_size'] / ob_1['top_bid']
                            sell_ticksize_rel = client_2.instruments[sell_mrkt]['tick_size'] / ob_2['top_ask']
                            if buy_ticksize_rel > self.profit_taker or sell_ticksize_rel > self.profit_taker:
                                target_profit = 1.5 * max(buy_ticksize_rel, sell_ticksize_rel)
                                targets.update({sell_mrkt + buy_mrkt: target_profit,
                                                buy_mrkt + sell_mrkt: target_profit})
        self.excepts = targets

    @try_exc_async
    async def count_one_coin(self, coin):
        for ex_1, client_1 in self.clients_with_names.items():
            for ex_2, client_2 in self.clients_with_names.items():
                if ex_1 == ex_2:
                    continue
                if buy_mrkt := client_1.markets.get(coin):
                    if sell_mrkt := client_2.markets.get(coin):
                        ob_1 = client_1.get_orderbook(buy_mrkt)
                        ob_2 = client_2.get_orderbook(sell_mrkt)
                        now_ts = time.time()
                        if not ob_1 or not ob_2:
                            continue
                        if not ob_1.get('bids') or not ob_1.get('asks'):
                            # print(f"OB IS BROKEN {client_1.EXCHANGE_NAME}: {ob_1}")
                            continue
                        if not ob_2.get('bids') or not ob_2.get('asks'):
                            # print(f"OB IS BROKEN {client_2.EXCHANGE_NAME}: {ob_2}")
                            continue
                        buy_own_ts_ping = now_ts - ob_1['ts_ms']
                        sell_own_ts_ping = now_ts - ob_2['ts_ms']

                        if isinstance(ob_1['timestamp'], float):
                            ts_buy = now_ts - ob_1['timestamp']
                            # ts_buy_top = now_ts - ob_1['top_ask_timestamp']
                        else:
                            ts_buy = now_ts - ob_1['timestamp'] / 1000
                            # ts_buy_top = now_ts - ob_1['top_ask_timestamp'] / 1000
                        if isinstance(ob_2['timestamp'], float):
                            ts_sell = now_ts - ob_2['timestamp']
                            # ts_sell_top = now_ts - ob_2['top_bid_timestamp']
                        else:
                            ts_sell = now_ts - ob_2['timestamp'] / 1000
                            # ts_sell_top = now_ts - ob_2['top_bid_timestamp'] / 1000
                        if buy_own_ts_ping > 0.060 or sell_own_ts_ping > 0.060 or ts_sell > 0.3 or ts_buy > 0.3:
                            continue

                        # if ts_sell > 100 or ts_buy > 100:
                        #     message = f"ORDERBOOK IS OLDER THAN 100s! TS NOW: {now_ts}\n"
                        #     message += f"{client_1.EXCHANGE_NAME} OB: {ob_1}\n"
                        #     message += f"{client_2.EXCHANGE_NAME} OB: {ob_2}\n"
                        #     self.multibot.telegram.send_message(message, TG_Groups.Alerts)
                        #     return
                        # if coin == 'BTC':
                        #     if buy_own_ts_ping > 0.010 or sell_own_ts_ping > 0.010:
                        #         continue
                        # else:

                        # if client_1.ob_push_limit and buy_own_ts_ping > client_1.ob_push_limit:
                        #     continue
                        # elif client_2.ob_push_limit and sell_own_ts_ping > client_2.ob_push_limit:
                        #     continue

                            # print(f"BUY OB AGE (OB TS):\n{ts_buy}")
                            # print(f"SELL OBs AGE (OB TS):\n{ts_sell}")
                        is_buy_ping_faster = ts_sell - sell_own_ts_ping > ts_buy - buy_own_ts_ping
                        is_buy_last_ob_update = sell_own_ts_ping > buy_own_ts_ping
                        if is_buy_ping_faster == is_buy_last_ob_update:
                            buy_px = ob_1['asks'][0][0]
                            sell_px = ob_2['bids'][0][0]
                            raw_profit = (sell_px - buy_px) / buy_px
                            name = f"B:{ex_1}|S:{ex_2}|C:{coin}"
                            self.append_profit(profit=raw_profit, name=name)
                            if raw_profit - self.fees[ex_1] - self.fees[ex_2] > 0:
                                print(f"{name}|Profit:{raw_profit - self.fees[ex_1] - self.fees[ex_2]}")
                            if self.state == 'Bot':
                                poses = {x: y.get_positions() for x, y in self.clients_with_names.items()}
                                direction = self.get_deal_direction(poses, ex_1, ex_2, buy_mrkt, sell_mrkt)
                            else:
                                direction = 'open'
                            # target_profit = self.excepts.get(buy_mrkt + sell_mrkt, self.get_target_profit(direction))
                            profit = raw_profit - self.fees[ex_1] - self.fees[ex_2]
                            # self.tradable_profits[coin].update({ex_1+'__'+ex_2: target_profit - profit,
                            #                                     ex_2+'__'+ex_1: target_profit - profit})
                            # name = f"B:{ex_1}|S:{ex_2}|C:{coin}"
                            # self.append_profit(profit=profit, name=name)
                            # target_profit = self.target_profits.get(name)
                            # if not target_profit:
                            target_profit = self.get_target_profit(direction)
                            #     print(f"{coin}: S.E: {ex_2} | B.E: {ex_1} | Profit: {profit}")
                            if profit >= target_profit:
                                buy_sz = ob_1['asks'][0][1]
                                sell_sz = ob_2['bids'][0][1]
                                # self.target_profits[name]:
                                deal_size_amount = min(buy_sz, sell_sz)
                                deal_size_usd_max = deal_size_amount * sell_px
                                profit_usd_max = profit * deal_size_usd_max
                                possibility = AP(ap_id=uuid.uuid4())
                                possibility.start_processing = now_ts
                                possibility.ob_buy = ob_1
                                possibility.ob_sell = ob_2
                                possibility.buy_max_amount_ob = buy_sz
                                possibility.sell_max_amount_ob = sell_sz
                                possibility.buy_price_target = buy_px
                                possibility.sell_price_target = sell_px
                                possibility.deal_max_amount_ob = deal_size_amount
                                possibility.deal_max_usd_ob = deal_size_usd_max
                                possibility.profit_rel_target = profit
                                possibility.set_data_from_parser(
                                    coin=coin,
                                    target_profit=target_profit,
                                    deal_max_amount_parser=deal_size_amount,
                                    deal_max_usd_parser=deal_size_usd_max,
                                    expect_profit_rel=round(profit, 5),
                                    profit_usd_max=round(profit_usd_max, 3),
                                    datetime=datetime.utcnow(),
                                    timestamp=int(round(datetime.utcnow().timestamp() * 1000)),
                                    deal_direction=direction)
                                possibility.set_side_data_from_parser(
                                    side='buy',
                                    client=client_1,
                                    exchange=ex_1,
                                    market=buy_mrkt,
                                    fee=self.fees[ex_1],
                                    price=buy_px,
                                    max_amount=buy_sz,
                                    ts_ob=ob_1['timestamp'])
                                possibility.set_side_data_from_parser(
                                    side='sell',
                                    client=client_2,
                                    exchange=ex_2,
                                    market=sell_mrkt,
                                    fee=self.fees[ex_2],
                                    max_amount=sell_sz,
                                    price=sell_px,
                                    ts_ob=ob_2['timestamp'])
                                # message = '\n'.join([x + ': ' + str(y) for x, y in possibility.items()])
                                # with open('arbi.csv', 'a', newline='') as file:
                                #     writer = csv.writer(file)
                                #     writer.writerow([str(y) for y in possibility.values()])
                                # print(f"AP filling time: {time.time() - time_start} sec")
                                self.potential_deals.append(possibility)
                        # else:
                        #     self.tradable_profits[coin].pop(ex_1 + '__' + ex_2, None)
                        #     self.tradable_profits[coin].pop(ex_2 + '__' + ex_1, None)

    @try_exc_regular
    def get_coins_profit_ranges(self):
        coins = {}
        for direction in self.profit_ranges.keys():
            if 'timestamp' in direction:
                # Passing the timestamp key in profit_ranges dict
                continue
            coin = direction.split('C:')[1]
            range = sorted([[float(x), y] for x, y in self.profit_ranges[direction].items()], reverse=True)
            range_len = sum([x[1] for x in range])
            if coins.get(coin):
                # Filling reversed direction of trades if one direction for this coin already filled
                coin = coin + '_reversed'
            upd_data = {coin: {'range': range,  # profits dictionary in format key = profit, value = frequency
                               'range_len': range_len,  # sample total size of all records
                               'direction': direction}}  # direction in format B:{exch_buy}|S:{exch_sell}|C:{coin} (str)
            coins.update(upd_data)
            # print(upd_data)
            # print()

        return coins

    @try_exc_regular
    def get_all_target_profits(self):
        coins = self.get_coins_profit_ranges()
        target_profits = {}
        for coin in coins.keys():
            if 'reversed' in coin:
                continue
            direction_one = coins[coin]
            direction_two = coins[coin + '_reversed']
            exchange_1 = direction_one['direction'].split(':')[1].split('|')[0]
            exchange_2 = direction_two['direction'].split(':')[1].split('|')[0]
            if exchange_1 not in (self.clients_with_names.keys()) or exchange_2 not in (self.clients_with_names.keys()):
                continue
            # fees = 0.00021 + 0.000375
            # print(fees)
            sum_freq_1 = 0
            sum_freq_2 = 0
            fees = self.fees[exchange_1] + self.fees[exchange_2]
            # print(fees)
            ### Choosing target profit as particular rate of frequency appearing in whole range of profits
            i = 0
            profit_1 = None
            profit_2 = None
            # sum_profit = direction_one['range'][i][0] + direction_two['range'][i][0]
            # print(direction_one['direction'], direction_two['direction'])
            # print(sum_profit - fees)
            # print(sum_profit - fees_1)
            while (direction_one['range'][i][0] + direction_two['range'][i][0]) - 2 * fees >= 0:
                profit_1 = direction_one['range'][i][0]
                profit_2 = direction_two['range'][i][0]
                sum_freq_1 += direction_one['range'][i][1]
                sum_freq_2 += direction_two['range'][i][1]
                i += 1
            if profit_2 != None and profit_1 != None:
                equalizer = 1
                while sum_freq_1 > 100 and sum_freq_1 > 2 * sum_freq_2:
                    profit_1 = direction_one['range'][i - equalizer][0]
                    sum_freq_1 -= direction_one['range'][i - equalizer + 1][1]
                    equalizer += 1
                equalizer = 1
                while sum_freq_2 > 100 and sum_freq_2 > 2 * sum_freq_1:
                    profit_2 = direction_two['range'][i - equalizer][0]
                    sum_freq_2 -= direction_two['range'][i - equalizer + 1][1]
                    equalizer += 1
                freq_relative_1 = sum_freq_1 / direction_one['range_len'] * 100
                freq_relative_2 = sum_freq_2 / direction_two['range_len'] * 100
                print(F"TARGET PROFIT {direction_one['direction']}:", profit_1, sum_freq_1, f"{freq_relative_1} %")
                print(F"TARGET PROFIT REVERSED {direction_two['direction']}:", profit_2, sum_freq_2, f"{freq_relative_2} %")
                print()
                ### Defining of target profit including exchange fees
                target_profits.update({direction_one['direction']: profit_1 - fees,
                                       direction_two['direction']: profit_2 - fees})
        return target_profits

                # for profit_1, freq_1 in direction_one['range']:
                #     if sum_freq_1 > direction_one['range_len'] * 0.07:
                #         target_raw_profit_1 = sum_profit_1 / sum_freq_1
                #         break
                #     sum_freq_1 += freq_1
                #     sum_profit_1 += profit_1 * freq_1
                # for profit_2, freq_2 in direction_two['range']:
                #     if sum_freq_2 > direction_two['range_len'] * 0.07:
                #         target_raw_profit_2 = sum_profit_2 / sum_freq_2
                #         break
                #     sum_freq_2 += freq_2
                #     sum_profit_2 += profit_2 * freq_2
                # print(F"TARGET PROFIT {direction_one['direction']}:", [target_raw_profit_1, profit_1, sum_freq_1])
                # print(F"TARGET PROFIT REVERSED {direction_two['direction']}:", [target_raw_profit_2, profit_2, sum_freq_2])
                # print()

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
            # self.target_profits = self.get_all_target_profits()
            with open(f'ranges{str(datetime.now()).split(" ")[0]}.json', 'w') as file:
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
    # clients_list = [DydxClient(), KrakenClient(), BinanceClient(), ApolloxClient()]  # , Bitfinex()]  # ,
    # Bitspay(), Ascendex()]
    # markets = coins_symbols_client(clients_list)  # {coin: {symbol:client(),...},...}
    # finder = ArbitrageFinder([x for x in markets.keys()], clients_list)
    # data = {}
    # finder.arbitrage(data)
