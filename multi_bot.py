import asyncio
from datetime import datetime
import logging
import threading
import time
import traceback
import uuid
from logging.config import dictConfig
from logger import Logging
import aiohttp

from clients.enums import BotState
from clients_markets_data import Clients_markets_data
from arbitrage_finder import ArbitrageFinder
from core.database import DB
from core.telegram import Telegram, TG_Groups
from core.rabbit import Rabbit
from clients.core.all_clients import ALL_CLIENTS
import sys
import configparser

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

dictConfig({'version': 1, 'disable_existing_loggers': False, 'formatters': {
    'simple': {'format': '[%(asctime)s][%(threadName)s] %(funcName)s: %(message)s'}},
            'handlers': {'console': {'class': 'logging.StreamHandler', 'level': 'DEBUG', 'formatter': 'simple',
                                     'stream': 'ext://sys.stdout'}},
            'loggers': {'': {'handlers': ['console'], 'level': 'DEBUG', 'propagate': False}}})
logger = logging.getLogger(__name__)

leverage = float(config['SETTINGS']['LEVERAGE'])


init_time = time.time()
ob_zero = {'top_bid': 0, 'top_ask': 0, 'bid_vol': 0, 'ask_vol': 0, 'ts_exchange': 0, 'ts_start': 0, 'ts_end': 0}


def timeit(func):
    async def wrapper(*args, **kwargs):
        ts_start = int(time.time() * 1000)
        result = await func(*args, **kwargs)
        result['ts_start'] = ts_start
        result['ts_end'] = int(time.time() * 1000)
        return result

    return wrapper


class MultiBot:
    __slots__ = ['deal_pause', 'max_order_size', 'profit_taker', 'shifts', 'rabbit', 'telegram',
                 'state', 'loop', 'start_time', 'last_message', 'trade_exceptions',
                 'last_max_deal_size', 'potential_deals', 'deals_counter', 'deals_executed', 'available_balances',
                 'session', 'clients', 'exchanges', 'ribs', 'env', 'exchanges_len', 'db', 'tasks',
                 'start', 'finish', 's_time', 'f_time', 'run_1', 'run_2', 'run_3', 'run_4', 'loop_1', 'loop_2',
                 'loop_3', 'loop_4', 'need_check_shift', 'last_orderbooks', 'time_start', 'time_parser',
                 'bot_launch_id', 'base_launch_config', 'launch_fields', 'setts', 'rates_file_name', 'time_lock',
                 'markets', 'clients_markets_data', 'finder', 'clients_with_names',
                 'max_position_part', 'profit_close']

    def __init__(self):
        self.bot_launch_id = None
        self.start = None
        self.finish = None
        self.db = None
        self.setts = config['SETTINGS']
        self.env = self.setts['ENV']
        self.trade_exceptions = []
        self.launch_fields = ['env', 'target_profit', 'fee_exchange_1', 'fee_exchange_2', 'shift', 'orders_delay',
                              'max_order_usd', 'max_leverage', 'shift_use_flag']

        with open(f'rates.txt', 'a') as file:
            file.write('')
        self.s_time = ''
        self.f_time = ''
        # self.create_csv('extra_countings.csv')
        self.last_orderbooks = {}
        self.time_lock = 0

        # ORDER CONFIGS
        self.deal_pause = int(self.setts['DEALS_PAUSE'])
        self.max_order_size = int(self.setts['ORDER_SIZE'])
        self.profit_taker = float(self.setts['TARGET_PROFIT'])
        self.profit_close = float(self.setts['CLOSE_PROFIT'])
        self.max_position_part = float(self.setts['PERCENT_PER_MARKET'])
        # self.shifts = {}

        # CLIENTS
        self.state = self.setts['STATE']
        self.exchanges = self.setts['EXCHANGES'].split(',')
        self.clients = []
        for exchange, client in ALL_CLIENTS.items():
            if exchange in self.exchanges:
                new = client(keys=config[exchange], leverage=leverage, max_pos_part=self.max_position_part)
                self.clients.append(new)
        self.exchanges_len = len(self.clients)
        self.clients_with_names = {}
        for client in self.clients:
            self.clients_with_names.update({client.EXCHANGE_NAME: client})

        self.start_time = datetime.utcnow().timestamp()
        self.last_message = None
        self.last_max_deal_size = 0
        self.potential_deals = []
        self.deals_counter = []
        self.deals_executed = []
        self.available_balances = {}
        self.update_all_av_balances()
        self.session = None

        # all_ribs = set([x.EXCHANGE_NAME + ' ' + y.EXCHANGE_NAME for x, y in self.ribs])
        # while not all_ribs <= set(self.shifts):
        #     print('Wait shifts for', all_ribs - set(self.shifts))
        #     self.__prepare_shifts()

        # NEW REAL MULTI BOT
        self.clients_markets_data = Clients_markets_data(self.clients, self.setts['INSTANCE_NUM'])
        self.markets = self.clients_markets_data.coins_clients_symbols
        self.clients_markets_data = self.clients_markets_data.clients_data
        self.finder = ArbitrageFinder(self.markets, self.clients_with_names, self.profit_taker, self.profit_close)
        # close_markets = ['ETH', 'RUNE', 'SNX', 'ENJ', 'DOT', 'LINK', 'ETC', 'DASH', 'XLM', 'WAVES']
        for client in self.clients:
            client.markets_list = list(self.markets.keys())
            # client.markets_list = close_markets
            client.run_updater()
        # time.sleep(5)
        # for client in self.clients:
        #     print(client.EXCHANGE_NAME)
        #     print(client.get_all_tops())
        # quit()

        self.base_launch_config = {
            "env": self.setts['ENV'],
            "shift_use_flag": 0,
            "target_profit": 0.01,
            "orders_delay": 300,
            "max_order_usd": 50,
            "max_leverage": 2,
            'fee_exchange_1': self.clients[0].taker_fee,
            'fee_exchange_2': self.clients[1].taker_fee,
            'exchange_1': self.clients[0].EXCHANGE_NAME,
            'exchange_2': self.clients[1].EXCHANGE_NAME,
            'updated_flag': 1,
            'datetime_update': str(datetime.utcnow()),
            'ts_update': int(time.time() * 1000)
        }
        self.telegram = Telegram()
        self.loop_1 = asyncio.new_event_loop()
        self.loop_2 = asyncio.new_event_loop()
        self.loop_3 = asyncio.new_event_loop()
        self.loop_4 = asyncio.new_event_loop()
        self.rabbit = Rabbit(self.loop_4)

        t1 = threading.Thread(target=self.run_await_in_thread, args=[self.__launch_and_run, self.loop_1])
        t2 = threading.Thread(target=self.run_await_in_thread, args=[self.__check_order_status, self.loop_2])
        t3 = threading.Thread(target=self.run_await_in_thread, args=[self.websocket_cycle_parser, self.loop_3])
        t4 = threading.Thread(target=self.run_await_in_thread, args=[self.rabbit.send_messages, self.loop_4])

        t1.start()
        t2.start()
        t3.start()
        t4.start()

        t1.join()
        t2.join()
        t3.join()
        t4.join()

    @staticmethod
    def run_await_in_thread(func, loop):
        try:
            loop.run_until_complete(func())
        except:
            traceback.print_exc()
        finally:
            loop.close()

    @staticmethod
    async def gather_dict(tasks: dict):
        async def mark(key, coro):
            try:
                return key, await coro
            except:
                return key, dict()

        return {
            key: result
            for key, result in await asyncio.gather(*(mark(key, coro) for key, coro in tasks.items()))
        }

    @timeit
    async def get_ob_top(self, client, symbol):
        try:
            return await client.get_multi_orderbook(symbol)
        except Exception as error:
            print(f'Exception from ob_top, exchange:{client.EXCHANGE_NAME}, market: {symbol}, error: {error}')
            # if 'list' not in str(error):
            #     self.flag = True
            return ob_zero

    async def create_and_await_ob_requests_tasks(self):
        tasks_dict = {}
        iter_start = datetime.utcnow()
        total_delay = 0
        for coin, symbols_client in self.markets.items():
            # coin_start = datetime.utcnow()
            local_delay = 0
            for exchange, symbol in symbols_client.items():
                tasks_dict[exchange + '__' + coin] = asyncio.create_task(
                    self.get_ob_top(self.clients_with_names[exchange],
                                    symbol))
            delays = [self.clients_markets_data[exchange]['delay'] for exchange in symbols_client.keys()]
            local_delay += max(delays)
            total_delay += max(delays)
            time.sleep(max(delays))
            # coin_end = datetime.utcnow()
            # Лог для отладки:
            # print(coin, '# clients:', len(symbols_client.values()), 'coin. delay: ', max(delays),
            #       'Real Delay:', (coin_end - coin_start).total_seconds(), 'Sum of delays: ', local_delay)
        iter_end = datetime.utcnow()
        print('#Coins: ', len(self.markets), '# Clients - Markets: ', len(tasks_dict), 'Total real dur.:',
              (iter_end - iter_start).total_seconds(),
              'Total sum of delay: ', total_delay)

        return await self.gather_dict(tasks_dict)

    @staticmethod
    def add_status(results):
        for exchange_coin_key, value in results.items():
            if results[exchange_coin_key] != {}:
                results[exchange_coin_key]['Status'] = 'Ok'
                # counter_success += 1
            else:
                results[exchange_coin_key] = ob_zero
                results[exchange_coin_key]['Status'] = 'Timeout'
        return results

    def find_ribs(self):
        ribs = self.setts['RIBS'].split(',')
        for rib in ribs:
            for client_1 in self.clients:
                for client_2 in self.clients:
                    if client_1 == client_2:
                        continue
                    if client_1.EXCHANGE_NAME in rib.split('|') and client_2.EXCHANGE_NAME in rib.split('|'):
                        if [client_1, client_2] not in self.ribs:
                            self.ribs.append([client_1, client_2])
                        if [client_2, client_1] not in self.ribs:
                            self.ribs.append([client_2, client_1])

    def find_position_gap(self):
        position_gap = 0
        for client in self.clients:
            if res := client.get_positions().get(client.symbol):
                position_gap += res['amount']
        return position_gap

    def find_balancing_elements(self):
        position_gap = self.find_position_gap()
        amount_to_balancing = abs(position_gap) / len(self.clients)
        return position_gap, amount_to_balancing

    def check_last_ob(self, client_buy, client_sell, ob_sell, ob_buy):
        exchanges = client_buy.EXCHANGE_NAME + ' ' + client_sell.EXCHANGE_NAME
        last_obs = self.last_orderbooks.get(exchanges, None)
        self.last_orderbooks.update({exchanges: {'ob_buy': ob_buy['asks'][0][0], 'ob_sell': ob_sell['bids'][0][0]}})
        if last_obs:
            if ob_buy['asks'][0][0] == last_obs['ob_buy'] and ob_sell['bids'][0][0] == last_obs['ob_sell']:
                return False
            else:
                return True
        else:
            return True

    def get_data_for_parser(self):
        data = dict()
        for client in self.clients:
            data.update(client.get_all_tops())
        return data

    async def websocket_cycle_parser(self):
        while not init_time + 90 > time.time():
            await asyncio.sleep(0.1)
        logger_custom = Logging()
        logger_custom.log_launch_params(self.clients)

        # Принтим показатели клиентов - справочно
        print('CLIENTS MARKET DATA:')
        print(self.clients_markets_data)

        while True:
            await asyncio.sleep(0.01)
            if not round(datetime.utcnow().timestamp() - self.start_time) % 90:
                self.start_time -= 1
                self.telegram.send_message(f"PARSER IS WORKING")
                self.update_all_av_balances()
            time_start_cycle = time.time()
            results = self.get_data_for_parser()
            # results = self.add_status(results)
            # logger_custom.log_rates(iteration, results)

            self.potential_deals = self.finder.arbitrage(results, time.time() - time_start_cycle)
            # print(f"AP FINDER CYCLE TIME: {time.time() - time_start} sec")
            # print(self.potential_deals)

    # async def __websocket_cycle_parser(self):
    #     while not init_time + 90 > time.time():
    #         await asyncio.sleep(0.1)
    #     while True:
    #         # timer = str(round(time.time(), 2))[-1]
    #         # if timer == '2':
    #         if True not in [client.count_flag for client in self.clients]:
    #             await asyncio.sleep(0.01)
    #             continue
    #         time_start = time.time()  # noqa
    #         for client_buy, client_sell in self.ribs:
    #             ob_sell, ob_buy = self.get_orderbooks(client_sell, client_buy)
    #
    #             # print(f"S.E ({client_sell.EXCHANGE_NAME}) price: {ob_sell['bids'][0][0]}")
    #             # print(f"B.E ({client_buy.EXCHANGE_NAME}) price: {ob_buy['asks'][0][0]}")
    #             # print()
    #             # shift = self.shifts[client_buy.EXCHANGE_NAME + ' ' + client_sell.EXCHANGE_NAME] / 2
    #             sell_price = ob_sell['bids'][0][0]
    #             buy_price = ob_buy['asks'][0][0]
    #             # if sell_price > buy_price:
    #             self.taker_order_profit(client_sell, client_buy, sell_price, buy_price, ob_buy, ob_sell, time_start)
    #             for client in self.clients:
    #                 client.count_flag = False
    # print(f"Full cycle time: {time.time() - time_start}")

    def choose_deal(self):
        max_profit = self.profit_close
        chosen_deal = None
        deal_direction = 'open'
        for deal in self.potential_deals:
            buy_exch = deal['buy_exchange']
            sell_exch = deal['sell_exchange']
            try:
                deal_size = self.avail_balance_define(buy_exch, sell_exch, deal['buy_market'], deal['sell_market'])
            except Exception:
                traceback.print_exc()
                print(f"LINE 422 {self.available_balances=}")
                continue
            # print(f"\n\nBUY {buy_exch} {deal['buy_price']}")
            # print(f"SELL {sell_exch} {deal['sell_price']}")
            # print(f"COIN: {deal['coin']}")
            # print(f"DEAL SIZE: {deal_size}")
            # print(f"DEAL PROFIT: {deal['expect_profit_rel']}")
            # print(f"PLANK PROFIT: {max_profit}")
            # print(f"DEAL DIRECTION: {deal['deal_direction']}")
            # print(f"MAX DEAL SIZE(vice versa): {self.available_balances[f'+{sell_exch}-{buy_exch}']}")
            # print(f"{deal['profit']=}\n\n")
            if deal_size >= self.max_order_size:
                if deal_direction != deal['deal_direction']:
                    if deal_direction == 'close':
                        continue
                    elif deal_direction == 'open':
                        chosen_deal = deal
                        continue
                    elif deal['deal_direction'] == 'close':
                        chosen_deal = deal
                        continue
                    elif deal['deal_direction'] == 'open':
                        continue
                if self.check_active_positions(deal['coin'], buy_exch, sell_exch):
                    if deal['expect_profit_rel'] > max_profit:
                        max_profit = deal['expect_profit_rel']
                        chosen_deal = deal
        self.potential_deals = []
        if chosen_deal:
            print(f"{chosen_deal=}")
        return chosen_deal

    def add_trade_exception(self, coin, exchange, direction, reason) -> None:
        self.trade_exceptions.append(
            {'coin': coin, 'exchange': exchange, 'direction': direction,
             'reason': reason, 'ts_added': str(datetime.utcnow())})

    def in_trade_exceptions(self, coin, exchange, direction):
        filtered = [item for item in self.trade_exceptions if item['coin'] == coin and
                    item['exchange'] == exchange and item['direction'] == direction]
        return len(filtered) > 0

    def check_active_positions(self, coin, buy_exchange, sell_exchange):
        result = {}
        for direction, exchange in {'buy': buy_exchange, 'sell': sell_exchange}.items():
            client = self.clients_with_names[exchange]
            market = self.markets[coin][exchange]
            available = client.get_balance() * leverage * self.max_position_part / 100
            position = 0
            if client.get_positions().get(market):
                position = abs(client.get_positions()[market]['amount_usd'])
            if position < available:
                result[direction] = True
            else:
                if self.in_trade_exceptions(coin, exchange, direction):
                    pass
                else:
                    self.add_trade_exception(coin, exchange, direction, 'Превышен порог % на монету')
                    message = self.telegram.coin_threshold_message(coin, exchange, direction, position, available,
                                                                   self.max_position_part)
                    self.telegram.send_message(message, TG_Groups.Alerts)
                result[direction] = False
        return (result['buy'] and result['sell'])

    # def check_active_positions(self, coin, buy_exchange, sell_exchange):
    #     client_buy = self.clients_with_names[buy_exchange]
    #     client_sell = self.clients_with_names[sell_exchange]
    #     market_buy = self.markets[coin][buy_exchange]
    #     market_sell = self.markets[coin][sell_exchange]
    #     available_buy = client_buy.get_balance() * leverage * self.max_position_part / 100
    #     available_sell = client_sell.get_balance() * leverage * self.max_position_part / 100
    #     position_sell = 0
    #     position_buy = 0
    #     if client_buy.get_positions().get(market_buy):
    #         position_buy = abs(client_buy.get_positions()[market_buy]['amount_usd'])
    #     if client_sell.get_positions().get(market_sell):
    #         position_sell = abs(client_sell.get_positions()[market_sell]['amount_usd'])
    #     if position_buy < available_buy and position_sell < available_sell:
    #         return True
    #     elif position_buy >= available_buy:
    #         if self.in_trade_exceptions(coin,buy_exchange,'buy'):
    #             pass
    #         else:
    #             self.add_trade_exception(coin, buy_exchange, 'buy', 'Превышен порог допустимого лимита на 1 монету')
    #             message = self.telegram.coin_threshold_message(coin,self.max_position_part,buy_exchange,
    #                                                            sell_exchange,position_buy,position_sell,
    #                                                            available_buy,available_sell)
    #             self.telegram.send_message(message,TG_Groups.Alerts)
    #         return False
    #     else:
    #         if self.in_trade_exceptions(coin, sell_exchange, 'sell'):
    #             pass
    #         else:
    #             self.add_trade_exception(coin, sell_exchange, 'sell', 'Превышен порог допустимого лимита на 1 монету')
    #             message = self.telegram.coin_threshold_message(coin, self.max_position_part, buy_exchange,
    #                                                            sell_exchange, position_buy, position_sell,
    #                                                            available_buy, available_sell)
    #             self.telegram.send_message(message, TG_Groups.Alerts)
    #         return False

    # def taker_order_profit(self, client_sell, client_buy, sell_price, buy_price, ob_buy, ob_sell, time_start):
    #     profit = ((sell_price - buy_price) / buy_price) - (client_sell.taker_fee + client_buy.taker_fee)
    #     # print(f"S: {client_sell.EXCHANGE_NAME} | B: {client_buy.EXCHANGE_NAME} | PROFIT: {round(profit, 5)}")
    #     if profit > self.profit_taker:
    #         self.potential_deals.append({'buy_exch': client_buy,
    #                                      "sell_exch": client_sell,
    #                                      "sell_px": sell_price,
    #                                      "buy_px": buy_price,
    #                                      'expect_buy_px': ob_buy['asks'][0][0],
    #                                      'expect_sell_px': ob_sell['bids'][0][0],
    #                                      "ob_buy": ob_buy,
    #                                      "ob_sell": ob_sell,
    #                                      'max_deal_size': self.available_balances[
    #                                          f"+{client_buy.EXCHANGE_NAME}-{client_sell.EXCHANGE_NAME}"],
    #                                      "profit": profit,
    #                                      'time_start': time_start,
    #                                      'time_parser': time.time() - time_start})

    @staticmethod
    def _fit_sizes(max_deal_size, client_buy, client_sell, buy_market, sell_market, buy_price, sell_price):
        # print(f"Started _fit_sizes: AMOUNT: {amount}")
        client_buy.fit_sizes(max_deal_size, buy_price, buy_market)
        client_sell.fit_sizes(max_deal_size, sell_price, sell_market)
        # print(f"{client.EXCHANGE_NAME}|AMOUNT: {amount}|FIT AMOUNT: {client.expect_amount_coin}")
        max_amount = min([client_buy.amount, client_sell.amount])
        if client_buy.EXCHANGE_NAME == 'OKX' or client_sell.EXCHANGE_NAME == 'OKX':
            if 0 not in [client_buy.amount, client_sell.amount]:
                max_amount = max([client_buy.amount, client_sell.amount])
            else:
                max_amount = 0
        print('BUY EXCH', client_buy.amount)
        print('SELL EXCH', client_sell.amount)
        client_buy.amount = max_amount
        client_sell.amount = max_amount
        print(f"\n\n\nSIZES FIT\nBUY MARKET: {buy_market}\nSELL MARKET: {sell_market}\nFIT AMOUNT: {max_amount}\n\n\n")
        return max_amount

    def get_target_profit(self, deal_direction):
        if deal_direction == 'open':
            target_profit = self.profit_taker
        elif deal_direction == 'close':
            target_profit = self.profit_close
        else:
            target_profit = (self.profit_taker + self.profit_close) / 2
        return target_profit

    def if_still_good(self, target_profit, ob_buy, ob_sell, exchange_buy, exchange_sell):
        profit = (ob_sell['bids'][0][0] - ob_buy['asks'][0][0]) / ob_buy['asks'][0][0]
        profit = profit - self.clients_with_names[exchange_buy].taker_fee - self.clients_with_names[
            exchange_sell].taker_fee
        if profit >= target_profit:
            return True
        else:
            return False

    def get_limit_prices_for_order(self, ob_buy, ob_sell):
        buy_point_price = (ob_buy['asks'][1][0] - ob_buy['asks'][0][0]) / ob_buy['asks'][0][0]
        sell_point_price = (ob_sell['bids'][0][0] - ob_sell['bids'][1][0]) / ob_sell['bids'][0][0]
        if buy_point_price > 0.2 * self.profit_taker:
            shifted_buy_px = ob_buy['asks'][0][0]
        else:
            shifted_buy_px = ob_buy['asks'][4][0]
        if sell_point_price > 0.2 * self.profit_taker:
            shifted_sell_px = ob_sell['bids'][0][0]
        else:
            shifted_sell_px = ob_sell['bids'][4][0]
        return shifted_buy_px, shifted_sell_px

    async def execute_deal(self, chosen_deal: dict, time_choose) -> None:
        # print(f"B:{chosen_deal['buy_exch'].EXCHANGE_NAME}|S:{chosen_deal['sell_exch'].EXCHANGE_NAME}")
        # print(f"BP:{chosen_deal['expect_buy_px']}|SP:{chosen_deal['expect_sell_px']}")
        # await asyncio.sleep(5)
        # return
        # example = {'coin': 'AGLD', 'buy_exchange': 'BINANCE', 'sell_exchange': 'KRAKEN', 'buy_fee': 0.00036,
        #            'sell_fee': 0.0005, 'sell_price': 0.6167, 'buy_price': 0.6158, 'sell_size': 1207.0,
        #            'buy_size': 1639.0, 'deal_size_coin': 1207.0, 'deal_size_usd': 744.3569,
        #            'expect_profit_rel': 0.0006, 'expect_profit_abs_usd': 0.448, 'buy_market': 'AGLDUSDT',
        #            'sell_market': 'pf_agldusd',
        #            'datetime': datetime(2023, 10, 2, 11, 33, 17, 855077), 'timestamp': 1696246397.855,
        #            'deal_value': 'open|close|half-close'}
        buy_exchange = chosen_deal['buy_exchange']
        sell_exchange = chosen_deal['sell_exchange']
        client_buy = self.clients_with_names[buy_exchange]
        client_sell = self.clients_with_names[sell_exchange]
        coin = chosen_deal['coin']

        buy_market = chosen_deal['buy_market']
        sell_market = chosen_deal['sell_market']
        # tasks = [asyncio.create_task(client_buy.get_orderbook_by_symbol(buy_market)),
        #          asyncio.create_task(client_sell.get_orderbook_by_symbol(sell_market))]
        # orderbooks = await asyncio.gather(*tasks, return_exceptions=True)
        # ob_buy = orderbooks[0]
        # ob_sell = orderbooks[1]
        target_profit = self.get_target_profit(chosen_deal['deal_direction'])
        ob_buy = self.clients_with_names[buy_exchange].get_orderbook(buy_market)
        ob_sell = self.clients_with_names[sell_exchange].get_orderbook(sell_market)
        if not self.if_still_good(target_profit, ob_buy, ob_sell, buy_exchange, sell_exchange):
            # with open('ap_still_active_status.csv', 'a', newline='') as file:
            #     writer = csv.writer(file)
            #     row_data = [str(y) for y in chosen_deal.values()] + ['Inactive']
            #     writer.writerow(row_data)
            print(f'\n\n\nDEAL {chosen_deal} ALREADY EXPIRED\nNEW PRICES:\nBUY: {ob_buy["asks"][0][0]}')
            print(f"SELL: {ob_sell['bids'][0][0]}")
            print(f"TIMESTAMP: {int(round(datetime.utcnow().timestamp() * 1000))}\n")
            return
        # with open('ap_still_active_status.csv', 'a', newline='') as file:
        #     writer = csv.writer(file)
        #     row_data = [str(y) for y in chosen_deal.values()] + ['Active']
        #     writer.writerow(row_data)
        max_deal_size = self.avail_balance_define(buy_exchange, sell_exchange, buy_market, sell_market)
        max_deal_size = max_deal_size / ob_buy['asks'][0][0]
        expect_buy_px = chosen_deal['buy_price']
        expect_sell_px = chosen_deal['sell_price']
        # shift = self.shifts[client_sell.EXCHANGE_NAME + ' ' + client_buy.EXCHANGE_NAME] / 2
        shifted_buy_px, shifted_sell_px = self.get_limit_prices_for_order(ob_buy, ob_sell)
        # shifted_buy_px = price_buy * self.shifts['TAKER']
        # shifted_sell_px = price_sell / self.shifts['TAKER']
        max_buy_vol = ob_buy['asks'][0][1]
        max_sell_vol = ob_sell['bids'][0][1]
        # timer = time.time()
        ap_id = uuid.uuid4()
        self._fit_sizes(max_deal_size, client_buy, client_sell, buy_market, sell_market, shifted_buy_px,
                        shifted_sell_px)
        if not client_buy.amount:
            print(f"DEAL IS BELOW MIN SIZE: SIZE: {client_buy.amount}")
            return
        cl_id_buy = f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}"
        cl_id_sell = f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}"
        time_sent = int(datetime.utcnow().timestamp() * 1000)
        orders = []
        orders.append(self.loop_1.create_task(
            client_buy.create_order(buy_market, 'buy', self.session, client_id=cl_id_buy)))
        orders.append(self.loop_1.create_task(
            client_sell.create_order(sell_market, 'sell', self.session, client_id=cl_id_sell)))
        responses = await asyncio.gather(*orders, return_exceptions=True)
        # добавить сюда анализ response после того как добавить в return create_order
        print(f"[Buy: {buy_exchange}, Sell: {sell_exchange}]\n{responses=}")
        try:
            self.telegram.send_message(f"{self.env}. Orders were created: [{buy_exchange}, {sell_exchange}]\n{responses=}",
                                       TG_Groups.DebugDima)
        except:
            print('Label1: error in sending TG message')
        # print(f"FULL POOL ADDING AND CALLING TIME: {time.time() - timer}")
        # await asyncio.sleep(0.5)
        # !!! ALL TIMERS !!!
        # time_start_parsing = chosen_deal['time_start']
        # self.time_parser = chosen_deal['time_parser']
        buy_order_place_time = self._check_order_place_time(client_buy, time_sent, responses)
        sell_order_place_time = self._check_order_place_time(client_sell, time_sent, responses)

        # Как разберусь с response нужно будет извлечь из него exchange_order_id и bдобавить в save_orders, уйти от Last_ORDER_ID
        order_id_buy = self.db.save_orders(client_buy, 'buy', ap_id, buy_order_place_time, shifted_buy_px, buy_market,
                                           self.env)
        order_id_sell = self.db.save_orders(client_sell, 'sell', ap_id, sell_order_place_time, shifted_sell_px,
                                            sell_market, self.env)

        if client_buy.LAST_ORDER_ID == 'default':
            self.telegram.send_message(
                self.telegram.order_error_message(self.env, buy_market, client_buy, order_id_buy),
                TG_Groups.Alerts)
        if client_sell.LAST_ORDER_ID == 'default':
            self.telegram.send_message(
                self.telegram.order_error_message(self.env, sell_market, client_sell, order_id_sell),
                TG_Groups.Alerts)

        self.db.save_arbitrage_possibilities(ap_id, client_buy, client_sell, max_buy_vol, max_sell_vol,
                                             expect_buy_px, expect_sell_px, time_choose, shift=None,
                                             time_parser=chosen_deal['time_parser'], symbol=coin,
                                             chat_id=config['TELEGRAM']['CHAT_ID'], token=config['TELEGRAM']['TOKEN'])
        self.telegram.send_message(
            self.telegram.ap_executed_message(self, client_buy, client_sell, expect_buy_px, expect_sell_px, buy_market))
        for client in [client_buy, client_sell]:
            client.error_info = None
            client.LAST_ORDER_ID = 'default'
        self.db.update_balance_trigger('post-deal', ap_id, self.env)
        self.update_all_av_balances()
        await asyncio.sleep(self.deal_pause)

    def update_all_av_balances(self):
        for client in self.clients_with_names.values():
            self.available_balances.update({client.EXCHANGE_NAME: client.get_available_balance()})

    @staticmethod
    def _check_order_place_time(client, time_sent, responses) -> int:
        for response in responses:
            try:
                if response['exchange_name'] == client.EXCHANGE_NAME:
                    if response['timestamp']:
                        return (response['timestamp'] - time_sent) / 1000
                    else:
                        return 0
            except:
                return 0

    def avail_balance_define(self, buy_exchange, sell_exchange, buy_market, sell_market):
        if self.available_balances[buy_exchange].get(buy_market):
            buy_size = self.available_balances[buy_exchange][buy_market]['buy']
        else:
            buy_size = self.available_balances[buy_exchange]['buy']
        if self.available_balances[sell_exchange].get(sell_market):
            sell_size = self.available_balances[sell_exchange][sell_market]['sell']
        else:
            sell_size = self.available_balances[sell_exchange]['sell']
        return min(buy_size, sell_size, self.max_order_size)

    def __rates_update(self):
        message = ''
        with open(f'rates.txt', 'a') as file:
            for client in self.clients:
                message += f"{client.EXCHANGE_NAME} | {client.get_orderbook(client.symbol)['asks'][0][0]} | {datetime.utcnow()} | {time.time()}\n"
            file.write(message + '\n')
        self.update_all_av_balances()

    async def potential_real_deals(self, sell_client, buy_client, orderbook_buy, orderbook_sell):
        if datetime.utcnow() - datetime.timedelta(seconds=15) > self.start_time:
            self.start_time = datetime.utcnow()

            # deals_potential = {'SELL': {x: 0 for x in self.exchanges}, 'BUY': {x: 0 for x in self.exchanges}}
            # deals_executed = {'SELL': {x: 0 for x in self.exchanges}, 'BUY': {x: 0 for x in self.exchanges}}
            #
            # deals_potential['SELL'][sell_client.EXCHANGE_NAME] += len(self.deals_counter)
            # deals_potential['BUY'][buy_client.EXCHANGE_NAME] += len(self.deals_counter)
            #
            # deals_executed['SELL'][sell_client.EXCHANGE_NAME] += len(self.deals_executed)
            # deals_executed['BUY'][buy_client.EXCHANGE_NAME] += len(self.deals_executed)
            #
            # self.deals_counter = []
            # self.deals_executed = []

            self.__rates_update()

    # def get_sizes(self):
    #     tick_size = max([x.tick_size for x in self.clients if x.tick_size], default=0.01)
    #     step_size = max([x.step_size for x in self.clients if x.step_size], default=0.01)
    #     quantity_precision = max([x.quantity_precision for x in self.clients if x.quantity_precision])
    #
    #     self.client_1.quantity_precision = quantity_precision
    #     self.client_2.quantity_precision = quantity_precision
    #
    #     self.client_1.tick_size = tick_size
    #     self.client_2.tick_size = tick_size
    #
    #     self.client_1.step_size = step_size
    #     self.client_2.step_size = step_size

    @staticmethod
    def get_positions_data(client):
        coins = []
        total_pos = 0
        abs_pos = 0
        for market, position in client.get_positions().items():
            coins.append(market)
            total_pos += position['amount_usd']
            abs_pos += abs(position['amount_usd'])
        return coins, int(round(total_pos)), int(round(abs_pos))

    # async def close_all_positions(self):
    #     async with aiohttp.ClientSession() as session:
    #         print('START')
    #         while abs(self.client_1.get_positions().get(self.client_1.symbol, {}).get('amount_usd', 0)) > 50 \
    #                 or abs(self.client_2.get_positions().get(self.client_2.symbol, {}).get('amount_usd', 0)) > 50:
    #             print('START WHILE')
    #             for client in self.clients:
    #                 print(f'START CLIENT {client.EXCHANGE_NAME}')
    #                 client.cancel_all_orders()
    #                 if res := client.get_positions().get(client.symbol, {}).get('amount'):
    #                     orderbook = client.get_orderbook()[client.symbol]
    #                     side = 'buy' if res < 0 else 'sell'
    #                     price = orderbook['bids'][0][0] if side == 'buy' else orderbook['asks'][0][0]
    #                     await client.create_order(abs(res), price, side, session)
    #                     time.sleep(7)

    async def __check_order_status(self):
        # Эта функция инициирует обновление данных по ордеру в базе, когда обновление приходит от биржи в клиента после создания
        while True:
            for client in self.clients:
                orders = client.orders.copy()

                for order_id, message in orders.items():
                    self.rabbit.add_task_to_queue(message, "UPDATE_ORDERS")
                    client.orders.pop(order_id)

            await asyncio.sleep(3)
    async def check_active_markets_status(self):
        # Здесь должна быть проверка, что рынки, в которых у нас есть позиции активны. Если нет, то алерт
        pass
    async def __launch_and_run(self):
        self.db = DB(self.rabbit)
        await self.db.setup_postgres()

        start = datetime.utcnow()
        # await self.db.log_launch_config()
        # start_shifts = self.shifts.copy()

        # try:
        #
        #     await self.db.update_launch_config(self)
        # except Exception:
        #     print(f"LINE 723:")
        #     traceback.print_exc()
        self.telegram.send_message(self.telegram.start_message(self))
        self.telegram.send_message(self.telegram.start_balance_message(self))

        # Проверить, что сработает
        # self.db.update_balance_trigger('bot-launch', int(datetime.utcnow()), self.env)
        async with aiohttp.ClientSession() as session:
            self.session = session
            time.sleep(3)

            # self.db.update_config(self)
            # self.update_balances_trigger()
            while True:
                # if (datetime.utcnow()-start).total_seconds() >= 30:
                #     try:
                #         await self.db.update_launch_config()
                #     except Exception:
                #         traceback.print_exc()
                #     start = datetime.utcnow()

                if not round(datetime.utcnow().timestamp() - self.start_time) % 92:
                    self.start_time -= 1
                    self.telegram.send_message(f"CHECK DEALS IS WORKING")

                time_start = time.time()
                deal = None
                if len(self.potential_deals):
                    deal = self.choose_deal()
                if deal & (self.state == BotState.BOT):
                    time_choose = time.time() - time_start
                    await self.execute_deal(deal, time_choose)


if __name__ == '__main__':
    MultiBot()
