import asyncio
import uuid
from datetime import datetime
from core.wrappers import try_exc_regular, try_exc_async
from core.ap_class import AP
import time
import threading
import json
import traceback


class ArbitrageFinder:

    def __init__(self, markets, clients_with_names, profit_taker, profit_close, state='Bot'):
        self.state = state
        self.profit_taker = profit_taker
        self.profit_close = profit_close
        self.markets = markets
        self.coins = [x for x in markets.keys()]
        self.clients_with_names = clients_with_names
        self.fees = {x: y.taker_fee for x, y in self.clients_with_names.items()}
        self.last_record = time.time()
        # self.excepts = dict()
        # self.loop = asyncio.new_event_loop()
        # self._wst = threading.Thread(target=self._run_finder_forever)
        # self.loop.create_task(self.check_coins())
        self.coins_to_check = set()
        # self._wst.daemon = True
        # self._wst.start()
        # PROFIT RANGES FE
        # self.tradable_profits = {x: {} for x in self.coins}  # {coin: {exchange+side: profit_gap}}
        self.profit_precise = 4
        self.potential_deals = []
        # if not self.profit_ranges.get('timestamp_start'):
        #     self.profit_ranges.update({'timestamp_start': time.time()})
        # print(self.profit_ranges)
        self.profit_precise = 4
        self.profit_ranges = self.unpack_ranges()
        self.target_profits = self.get_all_target_profits()
        print(f"TARGET PROFIT RANGES FOR {(time.time() - self.profit_ranges['timestamp_start']) / 3600} HOURS")
        print(self.target_profits)

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
            for ex_buy, client_1 in self.clients_with_names.items():
                for ex_sell, client_2 in self.clients_with_names.items():
                    if ex_buy == ex_sell:
                        continue
                    if ob_1 := data.get(ex_buy + '__' + coin):
                        if ob_2 := data.get(ex_sell + '__' + coin):
                            if not ob_2['top_bid'] or not ob_1['top_ask']:
                                continue
                            buy_mrkt = self.markets[coin][ex_buy]
                            sell_mrkt = self.markets[coin][ex_sell]
                            buy_ticksize_rel = client_1.instruments[buy_mrkt]['tick_size'] / ob_1['top_bid']
                            sell_ticksize_rel = client_2.instruments[sell_mrkt]['tick_size'] / ob_2['top_ask']
                            if buy_ticksize_rel > self.profit_taker or sell_ticksize_rel > self.profit_taker:
                                target_profit = 1.5 * max(buy_ticksize_rel, sell_ticksize_rel)
                                targets.update({sell_mrkt + buy_mrkt: target_profit,
                                                buy_mrkt + sell_mrkt: target_profit})
        self.excepts = targets

    @try_exc_async
    async def count_one_coin(self, coin, trigger_exchange, trigger_side, run_arbitrage):
        for exchange, client in self.clients_with_names.items():
            if trigger_exchange == exchange:
                continue
            if trigger_side == 'buy':
                client_buy = self.clients_with_names[trigger_exchange]
                client_sell = client
                ex_buy = trigger_exchange
                ex_sell = exchange
            else:
                client_buy = client
                client_sell = self.clients_with_names[trigger_exchange]
                ex_buy = exchange
                ex_sell = trigger_exchange
            if buy_mrkt := client_buy.markets.get(coin):
                if sell_mrkt := client_sell.markets.get(coin):
                    ob_buy = client_buy.get_orderbook(buy_mrkt)
                    ob_sell = client_sell.get_orderbook(sell_mrkt)
                    now_ts = time.time()
                    if not ob_buy or not ob_sell:
                        continue
                    if not ob_buy.get('bids') or not ob_buy.get('asks'):
                        # print(f"OB IS BROKEN {client_buy.EXCHANGE_NAME}: {ob_buy}")
                        continue
                    if not ob_sell.get('bids') or not ob_sell.get('asks'):
                        # print(f"OB IS BROKEN {client_sell.EXCHANGE_NAME}: {ob_sell}")
                        continue
                    buy_own_ts_ping = now_ts - ob_buy['ts_ms']
                    sell_own_ts_ping = now_ts - ob_sell['ts_ms']

                    if isinstance(ob_buy['timestamp'], float):
                        ts_buy = now_ts - ob_buy['timestamp']
                        # ts_buy_top = now_ts - ob_buy['top_ask_timestamp']
                    else:
                        ts_buy = now_ts - ob_buy['timestamp'] / 1000
                        # ts_buy_top = now_ts - ob_buy['top_ask_timestamp'] / 1000
                    if isinstance(ob_sell['timestamp'], float):
                        ts_sell = now_ts - ob_sell['timestamp']
                        # ts_sell_top = now_ts - ob_sell['top_bid_timestamp']
                    else:
                        ts_sell = now_ts - ob_sell['timestamp'] / 1000
                        # ts_sell_top = now_ts - ob_sell['top_bid_timestamp'] / 1000
                    if buy_own_ts_ping > 0.060 or sell_own_ts_ping > 0.060 or ts_sell > 0.3 or ts_buy > 0.3:
                        continue

                    # if ts_sell > 100 or ts_buy > 100:
                    #     message = f"ORDERBOOK IS OLDER THAN 100s! TS NOW: {now_ts}\n"
                    #     message += f"{client_buy.EXCHANGE_NAME} OB: {ob_buy}\n"
                    #     message += f"{client_sell.EXCHANGE_NAME} OB: {ob_sell}\n"
                    #     self.multibot.telegram.send_message(message, TG_Groups.Alerts)
                    #     return
                    # if coin == 'BTC':
                    #     if buy_own_ts_ping > 0.010 or sell_own_ts_ping > 0.010:
                    #         continue
                    # else:

                    # if client_buy.ob_push_limit and buy_own_ts_ping > client_buy.ob_push_limit:
                    #     continue
                    # elif client_sell.ob_push_limit and sell_own_ts_ping > client_sell.ob_push_limit:
                    #     continue
                    is_buy_ping_faster = ts_sell - sell_own_ts_ping > ts_buy - buy_own_ts_ping
                    is_buy_last_ob_update = sell_own_ts_ping > buy_own_ts_ping
                    if is_buy_ping_faster == is_buy_last_ob_update:
                        buy_px = ob_buy['asks'][0][0]
                        sell_px = ob_sell['bids'][0][0]
                        raw_profit = (sell_px - buy_px) / buy_px
                        # name = f"B:{ex_buy}|S:{ex_sell}|C:{coin}"
                        # self.tradable_profits[coin].update({ex_buy+'__'+ex_sell: target_profit - profit,
                        #                                     ex_sell+'__'+ex_buy: target_profit - profit})
                        name = f"B:{ex_buy}|S:{ex_sell}|C:{coin}"
                        self.append_profit(profit=raw_profit, name=name)
                        # if raw_profit > 0:
                        #     print(f"{name}|RAW profit: {raw_profit}")
                        if self.state == 'Bot':
                            poses = {x: y.get_positions() for x, y in self.clients_with_names.items()}
                            direction = self.get_deal_direction(poses, ex_buy, ex_sell, buy_mrkt, sell_mrkt)
                        else:
                            direction = 'open'
                        # target_profit = self.excepts.get(buy_mrkt + sell_mrkt, self.get_target_profit(direction))
                        profit = raw_profit - self.fees[ex_buy] - self.fees[ex_sell]

                        target_profit = self.target_profits.get(name, 'Not found')
                        if target_profit != 'Not found' and target_profit < 0 and direction == 'open':
                            continue
                        if target_profit == 'Not found':
                            target_profit = self.get_target_profit(direction)
                        if profit >= target_profit:
                            print(f"AP! {coin}: S.E: {ex_sell} | B.E: {ex_buy} | Profit: {profit}")
                            buy_sz = ob_buy['asks'][0][1]
                            sell_sz = ob_sell['bids'][0][1]
                            # self.target_profits[name]:
                            deal_size_amount = min(buy_sz, sell_sz)
                            deal_size_usd_max = deal_size_amount * sell_px
                            profit_usd_max = profit * deal_size_usd_max
                            possibility = AP(ap_id=uuid.uuid4())
                            possibility.start_processing = now_ts
                            possibility.ob_buy = ob_buy
                            possibility.ob_sell = ob_sell
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
                                client=client_buy,
                                exchange=ex_buy,
                                market=buy_mrkt,
                                fee=self.fees[ex_buy],
                                price=buy_px,
                                max_amount=buy_sz,
                                ts_ob=ob_buy['timestamp'])
                            possibility.set_side_data_from_parser(
                                side='sell',
                                client=client_sell,
                                exchange=ex_sell,
                                market=sell_mrkt,
                                fee=self.fees[ex_sell],
                                max_amount=sell_sz,
                                price=sell_px,
                                ts_ob=ob_sell['timestamp'])
                            # message = '\n'.join([x + ': ' + str(y) for x, y in possibility.items()])
                            # with open('arbi.csv', 'a', newline='') as file:
                            #     writer = csv.writer(file)
                            #     writer.writerow([str(y) for y in possibility.values()])
                            # print(f"AP filling time: {time.time() - time_start} sec")
                            await run_arbitrage(possibility)
                        # else:
                        #     self.tradable_profits[coin].pop(ex_buy + '__' + ex_sell, None)
                        #     self.tradable_profits[coin].pop(ex_sell + '__' + ex_buy, None)

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
    def get_all_target_profits(self):
        coins = self.get_coins_profit_ranges()
        if not coins:
            return dict()
        target_profits = dict()
        for coin in coins.keys():
            if 'reversed' in coin:
                continue
            if not coins.get(coin + '_reversed'):
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
                print(F"TARGET PROFIT REVERSED {direction_two['direction']}:", profit_2, sum_freq_2,
                      f"{freq_relative_2} %")
                print()
                ### Defining of target profit including exchange fees
                target_profits.update({direction_one['direction']: profit_1 - fees,
                                       direction_two['direction']: profit_2 - fees})
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
        self.profit_ranges.update({'timestamp': now})
        if now - self.last_record > 3600:
            with open('ranges.json', 'w') as file:
                json.dump(self.profit_ranges, file)
            self.last_record = now
        if now - self.profit_ranges['timestamp_start'] > 3600 * 24:
            self.target_profits = self.get_all_target_profits()
            with open(f'ranges{str(datetime.now()).split(" ")[0]}.json', 'w') as file:
                json.dump(self.profit_ranges, file)
            self.profit_ranges = {'timestamp': now, 'timestamp_start': now}

    @try_exc_regular
    def get_coins_profit_ranges(self):
        coins = dict()
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
