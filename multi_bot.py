import asyncio
import datetime
import logging
import threading
import time
import traceback
import uuid
from logging.config import dictConfig
import queue
import csv
from logger import Logging

import aiohttp
import asyncpg
import orjson
import requests
from aio_pika import Message, ExchangeType, connect_robust

from clients.apollox import ApolloxClient
from clients.binance import BinanceClient
from clients.bitmex import BitmexClient
from clients.dydx import DydxClient
from clients.kraken import KrakenClient
from clients.okx import OkxClient
from clients.enums import BotState, RabbitMqQueues
from core.queries import get_last_balance_jumps, get_total_balance, get_last_launch, get_last_deals
from tools.shifts import Shifts
from clients_markets_data import Clients_markets_data
from arbitrage_finder import ArbitrageFinder

import configparser
import sys
config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

dictConfig({'version': 1, 'disable_existing_loggers': False, 'formatters': {
                'simple': {'format': '[%(asctime)s][%(threadName)s] %(funcName)s: %(message)s'}},
            'handlers': {'console': {'class': 'logging.StreamHandler', 'level': 'DEBUG', 'formatter': 'simple',
                'stream': 'ext://sys.stdout'}},
            'loggers': {'': {'handlers': ['console'], 'level': 'DEBUG', 'propagate': False}}})
logger = logging.getLogger(__name__)

leverage = float(config['SETTINGS']['LEVERAGE'])
ALL_CLIENTS = {
    # 'BITMEX': [BitmexClient, config['BITMEX'], config['SETTINGS']['LEVERAGE']],
    'DYDX': DydxClient,
    # 'BINANCE': BinanceClient,
    # 'APOLLOX': ApolloxClient,
    # 'OKX': [OkxClient, config['OKX']],
    'KRAKEN': KrakenClient
}

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
    __slots__ = ['rabbit_url', 'deal_pause', 'max_order_size', 'profit_taker', 'shifts', 'telegram_bot', 'chat_id',
                 'daily_chat_id', 'inv_chat_id', 'state', 'loop', 'start_time', 'last_message',
                 'last_max_deal_size', 'potential_deals', 'deals_counter', 'deals_executed', 'available_balances',
                 'session', 'clients', 'exchanges', 'mq', 'ribs', 'env', 'exchanges_len', 'db', 'tasks',
                 'start', 'finish', 's_time', 'f_time', 'run_1', 'run_2', 'run_3', 'run_4', 'loop_1', 'loop_2',
                 'loop_3', 'loop_4', 'need_check_shift', 'last_orderbooks', 'time_start', 'time_parser',
                 'bot_launch_id', 'base_launch_config', 'launch_fields', 'setts', 'rates_file_name', 'time_lock',
                 'markets', 'clients_markets_data', 'finder', 'clients_with_names', 'alert_token', 'alert_id',
                 'max_position_part', 'profit_close']

    def __init__(self):
        self.bot_launch_id = None
        self.start = None
        self.finish = None
        self.db = None
        self.mq = None
        self.setts = config['SETTINGS']
        rabbit = config['RABBIT']
        self.rabbit_url = f"amqp://{rabbit['USERNAME']}:{rabbit['PASSWORD']}@{rabbit['HOST']}:{rabbit['PORT']}/"

        self.env = self.setts['ENV']
        self.launch_fields = ['env', 'target_profit', 'fee_exchange_1', 'fee_exchange_2', 'shift', 'orders_delay',
                              'max_order_usd', 'max_leverage', 'shift_use_flag']

        with open(f'rates.txt', 'a') as file:
            file.write('')
        self.s_time = ''
        self.f_time = ''
        self.tasks = queue.Queue()
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

        # TELEGRAM
        self.telegram_bot = config['TELEGRAM']['TOKEN']
        self.chat_id = int(config['TELEGRAM']['CHAT_ID'])
        self.daily_chat_id = int(config['TELEGRAM']['DAILY_CHAT_ID'])
        self.inv_chat_id = int(config['TELEGRAM']['INV_CHAT_ID'])
        self.alert_id = config['TELEGRAM']['ALERT_CHAT_ID']
        self.alert_token = config['TELEGRAM']['ALERT_BOT_TOKEN']

        # CLIENTS
        self.state = self.setts['STATE']
        self.exchanges = self.setts['EXCHANGES'].split(',')
        self.clients = []
        for exchange, client in ALL_CLIENTS.items():
            new = client(config[exchange], leverage, self.alert_id, self.alert_token, self.max_position_part)
            self.clients.append(new)
        self.exchanges_len = len(self.clients)
        self.clients_with_names = {}
        for client in self.clients:
            self.clients_with_names.update({client.EXCHANGE_NAME: client})
        self.ribs = []
        self.find_ribs()

        self.start_time = datetime.datetime.utcnow()
        self.last_message = None
        self.last_max_deal_size = 0
        self.potential_deals = []
        self.deals_counter = []
        self.deals_executed = []
        self.available_balances = {}
        self.session = None

        # all_ribs = set([x.EXCHANGE_NAME + ' ' + y.EXCHANGE_NAME for x, y in self.ribs])
        # while not all_ribs <= set(self.shifts):
        #     print('Wait shifts for', all_ribs - set(self.shifts))
        #     self.__prepare_shifts()

        #NEW REAL MULTI BOT
        self.clients_markets_data = Clients_markets_data(self.clients)
        self.markets = self.clients_markets_data.coins_clients_symbols
        self.clients_markets_data = self.clients_markets_data.clients_data
        self.finder = ArbitrageFinder(self.markets, self.clients_with_names, self.profit_taker, self.profit_close)
        for client in self.clients:
            client.markets_list = list(self.markets.keys())
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
            'datetime_update': datetime.datetime.utcnow(),
            'ts_update': int(time.time() * 1000)
        }

        self.loop_1 = asyncio.new_event_loop()
        self.loop_2 = asyncio.new_event_loop()
        self.loop_3 = asyncio.new_event_loop()
        self.loop_4 = asyncio.new_event_loop()

        t1 = threading.Thread(target=self.run_await_in_thread, args=[self.__launch_and_run, self.loop_1])
        t2 = threading.Thread(target=self.run_await_in_thread, args=[self.__check_order_status, self.loop_2])
        t3 = threading.Thread(target=self.run_await_in_thread, args=[self.websocket_cycle_parser, self.loop_3])
        t4 = threading.Thread(target=self.run_await_in_thread, args=[self.__send_messages, self.loop_4])

        t1.start()
        t2.start()
        t3.start()
        t4.start()

        t1.join()
        t2.join()
        t3.join()
        t4.join()

    @staticmethod
    def create_csv(filename):
        # Open the CSV file in write mode
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            # Write header row
            writer.writerow(['TimestampUTC', 'Time Stamp', 'Exchange', 'Coin', 'Flag'])

    @staticmethod
    def append_to_csv(filename, record):
        # Open the CSV file in append mode
        with open(filename, 'a', newline='') as file:
            writer = csv.writer(file)
            # Append new record
            writer.writerow(record)

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
        iter_start = datetime.datetime.utcnow()
        total_delay = 0
        for coin, symbols_client in self.markets.items():
            # coin_start = datetime.datetime.utcnow()
            local_delay = 0
            for exchange, symbol in symbols_client.items():
                tasks_dict[exchange + '__' + coin] = asyncio.create_task(self.get_ob_top(self.clients_with_names[exchange],
                                                                                         symbol))
            delays = [self.clients_markets_data[exchange]['delay'] for exchange in symbols_client.keys()]
            local_delay += max(delays)
            total_delay += max(delays)
            time.sleep(max(delays))
            # coin_end = datetime.datetime.utcnow()
            # Лог для отладки:
            # print(coin, '# clients:', len(symbols_client.values()), 'coin. delay: ', max(delays),
            #       'Real Delay:', (coin_end - coin_start).total_seconds(), 'Sum of delays: ', local_delay)
        iter_end = datetime.datetime.utcnow()
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

    def __prepare_shifts(self):
        time.sleep(10)
        self.__rates_update()
        # !!!!SHIFTS ARE HARDCODED TO A ZERO!!!!
        for x, y in Shifts().get_shifts().items():
            self.shifts.update({x: 0})
        print(self.shifts)

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

    async def __send_messages(self):
        await self.setup_mq(self.loop_4)
        while True:
            processing_tasks = self.tasks.get()
            try:
                processing_tasks.update({'connect': self.mq})
                await self.publish_message(**processing_tasks)
            except:
                await self.setup_mq(self.loop_4)
                await asyncio.sleep(1)
                processing_tasks.update({'connect': self.mq})
                print(f"\n\nERROR WITH SENDING TO MQ:\n{processing_tasks}\n\n")
                await self.publish_message(**processing_tasks)
            finally:
                self.tasks.task_done()
                await asyncio.sleep(0.1)

    @staticmethod
    def run_await_in_thread(func, loop):
        try:
            loop.run_until_complete(func())
        except:
            traceback.print_exc()
        finally:
            loop.close()

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
        print('Print1:')
        print(self.clients_markets_data)

        iteration = 0

        while True:
            if True not in [client.count_flag for client in self.clients]:
                await asyncio.sleep(0.01)
                continue
            self.update_all_av_balances()
            time_start_cycle = time.time()
            print(f"Iteration {iteration} start. ", end=" ")
            # results = await self.create_and_await_ob_requests_tasks()
            results = self.get_data_for_parser()
            # results = self.add_status(results)
            logger_custom.log_rates(iteration, results)
            # parsing_time = self.calculate_parse_time_and_sort(results)
            self.potential_deals = self.finder.arbitrage(results, time.time() - time_start_cycle)
            # print(self.potential_deals)
            # print(f"Iteration  end. Duration.: {(datetime.datetime.utcnow() - time_start_cycle).total_seconds()}")
            iteration += 1

    async def __websocket_cycle_parser(self):
        while not init_time + 90 > time.time():
            await asyncio.sleep(0.1)
        while True:
            # timer = str(round(time.time(), 2))[-1]
            # if timer == '2':
            if True not in [client.count_flag for client in self.clients]:
                await asyncio.sleep(0.01)
                continue
            time_start = time.time()  # noqa
            for client_buy, client_sell in self.ribs:
                ob_sell, ob_buy = self.get_orderbooks(client_sell, client_buy)

                # print(f"S.E ({client_sell.EXCHANGE_NAME}) price: {ob_sell['bids'][0][0]}")
                # print(f"B.E ({client_buy.EXCHANGE_NAME}) price: {ob_buy['asks'][0][0]}")
                # print()
                # shift = self.shifts[client_buy.EXCHANGE_NAME + ' ' + client_sell.EXCHANGE_NAME] / 2
                sell_price = ob_sell['bids'][0][0]
                buy_price = ob_buy['asks'][0][0]
                # if sell_price > buy_price:
                self.taker_order_profit(client_sell, client_buy, sell_price, buy_price, ob_buy, ob_sell, time_start)
                for client in self.clients:
                    client.count_flag = False
            # print(f"Full cycle time: {time.time() - time_start}")

    def choose_deal(self):
        max_profit = self.profit_close
        chosen_deal = None
        deal_direction = 'open'
        for deal in self.potential_deals:
            buy_exch = deal['buy_exchange']
            sell_exch = deal['sell_exchange']
            deal_size = self.avail_balance_define(buy_exch, sell_exch, deal['buy_market'], deal['sell_market'])
            print(f"\n\nBUY {buy_exch} {deal['buy_price']}")
            print(f"SELL {sell_exch} {deal['sell_price']}")
            print(f"COIN: {deal['coin']}")
            print(f"DEAL SIZE: {deal_size}")
            print(f"DEAL PROFIT: {deal['expect_profit_rel']}")
            print(f"PLANK PROFIT: {max_profit}")
            print(f"DEAL DIRECTION: {deal['deal_direction']}")
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
        print(chosen_deal)
        return chosen_deal

    def check_active_positions(self, coin, buy_exchange, sell_exchange):
        client_buy = self.clients_with_names[buy_exchange]
        client_sell = self.clients_with_names[sell_exchange]
        market_buy = self.markets[coin][buy_exchange]
        market_sell = self.markets[coin][sell_exchange]
        available_buy = client_buy.get_balance() * leverage * self.max_position_part / 100
        available_sell = client_sell.get_balance() * leverage * self.max_position_part / 100
        position_sell = 0
        position_buy = 0
        if client_buy.get_positions().get(market_buy):
            position_buy = abs(client_buy.get_positions()[market_buy]['amount_usd'])
        if client_sell.get_positions().get(market_sell):
            position_sell = abs(client_sell.get_positions()[market_sell]['amount_usd'])
        if position_buy < available_buy and position_sell < available_sell:
            return True
        else:
            print(f"ACTIVE POS {coin} > {self.max_position_part} %")
            print(f"B: {buy_exchange}| S: {sell_exchange}")
            print(f"POS: {position_buy} | {position_sell}")
            print(f"AVAIL: {available_buy} | {available_sell}")
            return False

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
        print(client_buy.amount)
        print(client_sell.amount)
        client_buy.amount = max_amount
        client_sell.amount = max_amount
        print(f"\n\n\nSIZES FIT\nBUY MARKET: {buy_market}\nSELL MARKET: {sell_market}\nFIT AMOUNT: {max_amount}\n\n\n")

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
        profit = profit - self.clients_with_names[exchange_buy].taker_fee - self.clients_with_names[exchange_sell].taker_fee
        if profit > target_profit:
            return True
        else:
            return False

    async def execute_deal(self, chosen_deal: dict, time_choose) -> None:
        # print(f"B:{chosen_deal['buy_exch'].EXCHANGE_NAME}|S:{chosen_deal['sell_exch'].EXCHANGE_NAME}")
        # print(f"BP:{chosen_deal['expect_buy_px']}|SP:{chosen_deal['expect_sell_px']}")
        # await asyncio.sleep(5)
        # return
        example = {'coin': 'AGLD', 'buy_exchange': 'BINANCE', 'sell_exchange': 'KRAKEN', 'buy_fee': 0.00036,
                   'sell_fee': 0.0005, 'sell_price': 0.6167, 'buy_price': 0.6158, 'sell_size': 1207.0,
                   'buy_size': 1639.0, 'deal_size_coin': 1207.0, 'deal_size_usd': 744.3569,
                   'expect_profit_rel': 0.0006, 'expect_profit_abs_usd': 0.448, 'buy_market': 'AGLDUSDT',
                   'sell_market': 'pf_agldusd',
                   'datetime': datetime.datetime(2023, 10, 2, 11, 33, 17, 855077), 'timestamp': 1696246397.855,
                   'deal_value': 'open|close|half-close'}
        client_buy = self.clients_with_names[chosen_deal['buy_exchange']]
        client_sell = self.clients_with_names[chosen_deal['sell_exchange']]
        coin = chosen_deal['coin']
        buy_exchange = chosen_deal['buy_exchange']
        sell_exchange = chosen_deal['sell_exchange']
        buy_market = chosen_deal['buy_market']
        sell_market = chosen_deal['sell_market']
        tasks = [asyncio.create_task(client_buy.get_orderbook_by_symbol(buy_market)),
                 asyncio.create_task(client_sell.get_orderbook_by_symbol(sell_market))]
        orderbooks = await asyncio.gather(*tasks, return_exceptions=True)
        ob_buy = orderbooks[0]
        ob_sell = orderbooks[1]
        target_profit = self.get_target_profit(chosen_deal['deal_direction'])
        if not self.if_still_good(target_profit, ob_buy, ob_sell, buy_exchange, sell_exchange):
            with open('ap_still_active_status.csv', 'a', newline='') as file:
                writer = csv.writer(file)
                row_data = [str(y) for y in chosen_deal.values()] + ['Inactive']
                writer.writerow(row_data)
            print(f'\n\n\nDEAL {chosen_deal} ALREADY EXPIRED\n\n\n')
            return
        with open('ap_still_active_status.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            row_data = [str(y) for y in chosen_deal.values()] + ['Active']
            writer.writerow(row_data)
        max_deal_size = self.avail_balance_define(buy_exchange, sell_exchange, buy_market, sell_market)
        max_deal_size = max_deal_size / ob_buy['asks'][0][0]
        expect_buy_px = chosen_deal['buy_price']
        expect_sell_px = chosen_deal['sell_price']
        # shift = self.shifts[client_sell.EXCHANGE_NAME + ' ' + client_buy.EXCHANGE_NAME] / 2
        shifted_buy_px = ob_buy['asks'][4][0]
        shifted_sell_px = ob_sell['bids'][4][0]
        # shifted_buy_px = price_buy * self.shifts['TAKER']
        # shifted_sell_px = price_sell / self.shifts['TAKER']
        max_buy_vol = ob_buy['asks'][0][1]
        max_sell_vol = ob_sell['bids'][0][1]
        # timer = time.time()
        ap_id = uuid.uuid4()
        self._fit_sizes(max_deal_size, client_buy, client_sell, buy_market, sell_market, shifted_buy_px, shifted_sell_px)
        cl_id_buy = f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}"
        cl_id_sell = f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}"
        time_sent = int(datetime.datetime.utcnow().timestamp() * 1000)
        orders = []
        orders.append(self.loop_1.create_task(
            client_buy.create_order(buy_market, 'buy', self.session, client_id=cl_id_buy)))
        orders.append(self.loop_1.create_task(
            client_sell.create_order(sell_market, 'sell', self.session, client_id=cl_id_sell)))
        responses = await asyncio.gather(*orders, return_exceptions=True)
        print(f"[{buy_exchange}, {sell_exchange}]\n{responses=}")
        # print(f"FULL POOL ADDING AND CALLING TIME: {time.time() - timer}")
        # await asyncio.sleep(0.5)
        # !!! ALL TIMERS !!!
        # time_start_parsing = chosen_deal['time_start']
        # self.time_parser = chosen_deal['time_parser']
        buy_order_place_time = self._check_order_place_time(client_buy, time_sent, responses)
        sell_order_place_time = self._check_order_place_time(client_sell, time_sent, responses)
        self.save_orders(client_buy, 'buy', ap_id, buy_order_place_time, shifted_buy_px, buy_market)
        self.save_orders(client_sell, 'sell', ap_id, sell_order_place_time, shifted_sell_px, sell_market)
        self.save_arbitrage_possibilities(ap_id, client_buy, client_sell, max_buy_vol,
                                          max_sell_vol, expect_buy_px, expect_sell_px, time_choose, shift=None,
                                          time_parser=chosen_deal['time_parser'], symbol=coin)
        self.save_balance(ap_id)
        self.update_all_av_balances()
        await asyncio.sleep(self.deal_pause)

    async def check_opposite_deal_side(self, exch_1, exch_2, ap_id):
        await asyncio.sleep(2)
        async with self.db.acquire() as cursor:
            for ap in await get_last_deals(cursor):
                if {exch_1, exch_2} == {ap['buy_exchange'], ap['sell_exchange']}:
                    if ap['id'] != ap_id:
                        if int(time.time()) - 10 < ap['ts'] < int(time.time()) + 10:
                            # print(f"\n\n\nFOUND SECOND DEAL!\nDEAL: {ap}")
                            return True
        return False

    def update_all_av_balances(self):
        for client in self.clients_with_names.values():
            self.available_balances.update({client.EXCHANGE_NAME: client.get_available_balance()})

    def save_balance(self, parent_id) -> None:
        message = {
            'parent_id': parent_id,
            'context': 'post-deal',
            'env': self.env,
            'chat_id': self.chat_id,
            'telegram_bot': self.telegram_bot,
        }

        self.tasks.put({
            'message': message,
            'routing_key': RabbitMqQueues.CHECK_BALANCE,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.CHECK_BALANCE),
            'queue_name': RabbitMqQueues.CHECK_BALANCE
        })

    @staticmethod
    def _check_order_place_time(client, time_sent, responses) -> int:
        for response in responses:
            if response['exchange_name'] == client.EXCHANGE_NAME:
                if response['timestamp']:
                    return (response['timestamp'] - time_sent) / 1000
                else:
                    return 0

    def save_arbitrage_possibilities(self, _id, client_buy, client_sell, max_buy_vol, max_sell_vol, expect_buy_px,
                                     expect_sell_px, time_choose, shift, time_parser, symbol):
        expect_profit_usd = ((expect_sell_px - expect_buy_px) / expect_buy_px - (
                client_buy.taker_fee + client_sell.taker_fee)) * client_buy.amount
        expect_amount_usd = client_buy.amount * (expect_sell_px + expect_buy_px) / 2
        message = {
            'id': _id,
            'datetime': datetime.datetime.utcnow(),
            'ts': int(time.time()),
            'buy_exchange': client_buy.EXCHANGE_NAME,
            'sell_exchange': client_sell.EXCHANGE_NAME,
            'symbol': symbol,
            'buy_order_id': client_buy.LAST_ORDER_ID,
            'sell_order_id': client_sell.LAST_ORDER_ID,
            'max_buy_vol_usd': round(max_buy_vol * expect_buy_px),
            'max_sell_vol_usd': round(max_sell_vol * expect_sell_px),
            'expect_buy_price': expect_buy_px,
            'expect_sell_price': expect_sell_px,
            'expect_amount_usd': expect_amount_usd,
            'expect_amount_coin': client_buy.amount,
            'expect_profit_usd': expect_profit_usd,
            'expect_profit_relative': expect_profit_usd / expect_amount_usd,
            'expect_fee_buy': client_buy.taker_fee,
            'expect_fee_sell': client_sell.taker_fee,
            'time_parser': time_parser,
            'time_choose': time_choose,
            'chat_id': self.chat_id,
            'bot_token': self.telegram_bot,
            'status': 'Processing',
            'bot_launch_id': self.bot_launch_id
        }
        print(f"\n\n\nAP output sending: {message}\n\n\n")

        message_for_tg = {
            "chat_id": config['TELEGRAM']['CHAT_ID'],
            "msg": f"AP EXECUTED | ENV: {self.env}\n"
                   f"ENV ACTIVE EXCHANGES: {self.setts['EXCHANGES']}\n"
                   f"DT: {datetime.datetime.utcnow()}\n"
                   f"B.E.: {client_buy.EXCHANGE_NAME} | S.E.: {client_sell.EXCHANGE_NAME}\n"
                   f"B.P.: {expect_buy_px} | S.P.: {expect_sell_px}\n",
            'bot_token': config['TELEGRAM']['TOKEN']
        }
        self.tasks.put({
            'message': message_for_tg,
            'routing_key': RabbitMqQueues.TELEGRAM,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
            'queue_name': RabbitMqQueues.TELEGRAM
        })
        self.tasks.put({
            'message': message,
            'routing_key': RabbitMqQueues.ARBITRAGE_POSSIBILITIES,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.ARBITRAGE_POSSIBILITIES),
            'queue_name': RabbitMqQueues.ARBITRAGE_POSSIBILITIES
        })

        client_buy.error_info = None
        client_buy.LAST_ORDER_ID = 'default'

        client_sell.error_info = None
        client_sell.LAST_ORDER_ID = 'default'

    def save_orders(self, client, side, parent_id, order_place_time, expect_price, symbol) -> None:
        order_id = uuid.uuid4()
        message = {
            'id': order_id,
            'datetime': datetime.datetime.utcnow(),
            'ts': int(time.time()),
            'context': 'bot',
            'parent_id': parent_id,
            'exchange_order_id': client.LAST_ORDER_ID,
            'type': 'GTT' if client.EXCHANGE_NAME == 'DYDX' else 'GTC',
            'status': 'Processing',
            'exchange': client.EXCHANGE_NAME,
            'side': side,
            'symbol': symbol.upper(),
            'expect_price': expect_price,
            'expect_amount_coin': client.amount,
            'expect_amount_usd': client.amount * client.price,
            'expect_fee': client.taker_fee,
            'factual_price': 0,
            'factual_amount_coin': 0,
            'factual_amount_usd': 0,
            'factual_fee': client.taker_fee,
            'order_place_time': order_place_time,
            'env': self.env,
        }
        self.tasks.put({
            'message': message,
            'routing_key': RabbitMqQueues.ORDERS,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.ORDERS),
            'queue_name': RabbitMqQueues.ORDERS
        })
        if client.LAST_ORDER_ID == 'default':
            error_message = {
                "chat_id": self.alert_id,
                "msg": f"ALERT NAME: Order Mistake\nCOIN: {symbol}\nCONTEXT: BOT\nENV: {self.env}\n"
                       f"EXCHANGE: {client.EXCHANGE_NAME}\nOrder Id:{order_id}\nError:{client.error_info}",
                'bot_token': self.alert_token
            }
            self.tasks.put({
                'message': error_message,
                'routing_key': RabbitMqQueues.TELEGRAM,
                'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
                'queue_name': RabbitMqQueues.TELEGRAM
            })

    @staticmethod
    async def publish_message(connect, message, routing_key, exchange_name, queue_name):
        channel = await connect.channel()
        exchange = await channel.declare_exchange(exchange_name, type=ExchangeType.DIRECT, durable=True)
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key=routing_key)
        message_body = orjson.dumps(message)
        message = Message(message_body)
        await exchange.publish(message, routing_key=routing_key)
        await channel.close()
        return True

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
                message += f"{client.EXCHANGE_NAME} | {client.get_orderbook(client.symbol)['asks'][0][0]} | {datetime.datetime.utcnow()} | {time.time()}\n"
            file.write(message + '\n')
        self.update_all_av_balances()

    def ob_alert_send(self, client_slippage, client_2, ts, client_for_unstuck=None):
        if self.state == BotState.SLIPPAGE:
            msg = "🔴ALERT NAME: Exchange Slippage Suspicion\n"
            msg += f"ENV: {self.env}\nEXCHANGE: {client_slippage.EXCHANGE_NAME}\n"
            msg += f"EXCHANGES: {client_slippage.EXCHANGE_NAME}|{client_2.EXCHANGE_NAME}\n"
            msg += f"Current DT: {datetime.datetime.utcnow()}\n"
            msg += f"Last Order Book Update DT: {datetime.datetime.utcfromtimestamp(ts / 1000)}"
        else:
            msg = "🟢ALERT NAME: Exchange Slippage Suspicion\n"
            msg += f"ENV: {self.env}\nEXCHANGE: {client_for_unstuck.EXCHANGE_NAME}\n"
            msg += f"EXCHANGES: {client_slippage.EXCHANGE_NAME}|{client_2.EXCHANGE_NAME}\n"
            msg += f"Current DT: {datetime.datetime.utcnow()}\n"
            msg += f"EXCHANGES PAIR CAME BACK TO WORK, SLIPPAGE SUSPICION SUSPENDED"
        message = {
            "chat_id": self.alert_id,
            "msg": msg,
            'bot_token': self.alert_token
        }
        self.tasks.put({
            'message': message,
            'routing_key': RabbitMqQueues.TELEGRAM,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
            'queue_name': RabbitMqQueues.TELEGRAM
        })

    def get_orderbooks(self, client_sell, client_buy):
        client_slippage = None
        while True:
            ob_sell = client_sell.get_orderbook(client_sell.symbol)
            ob_buy = client_buy.get_orderbook(client_buy.symbol)
            if client_sell.EXCHANGE_NAME == 'APOLLOX':
                ob_sell_time_shift = 5 * self.deal_pause * 1000
            else:
                ob_sell_time_shift = self.deal_pause * 1000
            if client_buy.EXCHANGE_NAME == 'APOLLOX':
                ob_buy_time_shift = 5 * self.deal_pause * 1000
            else:
                ob_buy_time_shift = self.deal_pause * 1000
            current_timestamp = int(time.time() * 1000)
            if current_timestamp - ob_sell['timestamp'] > ob_sell_time_shift:
                if self.state == BotState.BOT:
                    self.state = BotState.SLIPPAGE
                    self.ob_alert_send(client_sell, client_buy, ob_sell['timestamp'])
                    client_slippage = client_sell
                time.sleep(5)
                continue
            elif current_timestamp - ob_buy['timestamp'] > ob_buy_time_shift:
                if self.state == BotState.BOT:
                    self.state = BotState.SLIPPAGE
                    self.ob_alert_send(client_buy, client_sell, ob_buy['timestamp'])
                    client_slippage = client_buy
                time.sleep(5)
                continue
            elif ob_sell['asks'] and ob_sell['bids'] and ob_buy['asks'] and ob_buy['bids']:
                if self.state == BotState.SLIPPAGE:
                    self.state = BotState.BOT
                    self.ob_alert_send(client_sell, client_buy, ob_sell['timestamp'], client_slippage)
                    client_slippage = None
                return ob_sell, ob_buy

    async def start_message(self):
        coin = self.clients[0].symbol.split('USD')[0].replace('-', '').replace('/', '')
        message = f'MULTIBOT {coin} STARTED\n'
        message += f'{" | ".join(self.exchanges)}\n'
        message += f"RIBS: {self.setts['RIBS']}\n"
        message += f"ENV: {self.env}\n"
        message += f"STATE: {self.setts['STATE']}\n"
        message += f"LEVERAGE: {self.setts['LEVERAGE']}\n"
        # message += f"EXCHANGES: {self.client_1.EXCHANGE_NAME} {self.client_2.EXCHANGE_NAME}\n"
        message += f"DEALS PAUSE: {self.setts['DEALS_PAUSE']}\n"
        message += f"ORDER SIZE: {self.setts['ORDER_SIZE']}\n"
        message += f"TARGET PROFIT: {self.setts['TARGET_PROFIT']}\n"
        message += f"CLOSE PROFIT: {self.setts['CLOSE_PROFIT']}\n"
        message += f"START BALANCE: {self.start}\n"
        message += f"CURRENT BALANCE: {self.finish}\n"

        # for exchange, shift in self.shifts.items():
        #     message += f"{exchange}: {round(shift, 6)}\n"
        # print(80 * '*' + f"START MESSAGE SENT")
        await self.send_message(message, int(config['TELEGRAM']['CHAT_ID']), config['TELEGRAM']['TOKEN'])

    # def create_result_message(self, deals_potential: dict, deals_executed: dict, time: int) -> str:
    #     message = f"For last 3 min\n"
    #     message += f"ENV: {self.setts['ENV']}\n"
    #
    #     if self.__check_env():
    #         message += f'SYMBOL: {self.client_1.symbol}'
    #
    #     message += f"\n\nPotential deals:"
    #     for side, values in deals_potential.items():
    #         message += f"\n   {side}:"
    #         for exchange, deals in values.items():
    #             message += f"\n{exchange}: {deals}"
    #     message += f"\n\nExecuted deals:"
    #     for side, values in deals_executed.items():
    #         message += f"\n   {side}:"
    #         for exchange, deals in values.items():
    #             message += f"\n{exchange}: {deals}"
    #     return message

    async def potential_real_deals(self, sell_client, buy_client, orderbook_buy, orderbook_sell):
        if datetime.datetime.utcnow() - datetime.timedelta(seconds=15) > self.start_time:
            self.start_time = datetime.datetime.utcnow()

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

    async def send_message(self, message: str, chat_id: int, bot_token: str) -> None:
        self.tasks.put({
            'message': {"chat_id": chat_id, "msg": message, 'bot_token': bot_token},
            'routing_key': RabbitMqQueues.TELEGRAM,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
            'queue_name': RabbitMqQueues.TELEGRAM
        })

    async def setup_mq(self, loop) -> None:
        print(f"SETUP MQ START")
        self.mq = await connect_robust(self.rabbit_url, loop=loop)
        print(f"SETUP MQ ENDED")

    async def setup_postgres(self) -> None:
        # print(config.POSTGRES)
        postgres = config['POSTGRES']
        self.db = await asyncpg.create_pool(database=postgres['NAME'],
                                            user=postgres['USER'],
                                            password=postgres['PASSWORD'],
                                            host=postgres['HOST'],
                                            port=postgres['PORT'])

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

    async def save_new_balance_jump(self):
        if self.start and self.finish:
            self.tasks.put({
                'message': {
                    'timestamp': int(time.time()),
                    'total_balance': self.finish,
                    'env': self.env
                },
                'routing_key': RabbitMqQueues.BALANCE_JUMP,
                'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.BALANCE_JUMP),
                'queue_name': RabbitMqQueues.BALANCE_JUMP
            })

    async def get_total_balance_calc(self, cursor, asc_desc):
        result = 0
        exchanges = []
        time_ = 0
        for row in await get_total_balance(cursor, asc_desc):
            if not row['exchange_name'] in exchanges:
                result += row['total_balance']
                exchanges.append(row['exchange_name'])
                time_ = max(time_, row['ts'])

            if len(exchanges) >= self.exchanges_len:
                break

        return result, str(datetime.datetime.fromtimestamp(time_ / 1000).strftime('%Y-%m-%d %H:%M:%S'))

    async def get_balance_percent(self) -> float:
        async with self.db.acquire() as cursor:
            self.finish, self.f_time = await self.get_total_balance_calc(cursor, 'desc')  # todo

            if res := await get_last_balance_jumps(cursor):
                self.start, self.s_time = res[0], res[1]
            else:
                self.start, self.s_time = await self.get_total_balance_calc(cursor, 'asc')
                await self.save_new_balance_jump()

            if self.start and self.finish:
                return abs(100 - self.finish * 100 / self.start)

            return 0

    async def start_balance_message(self):
        message = f'START BALANCES AND POSITION\n'
        message += f"ENV: {self.setts['ENV']}\n"
        total_balance = 0
        total_position = 0
        abs_total_position = 0

        for client in self.clients:
            coins, total_pos, abs_pos = self.get_positions_data(client)
            try:
                message += f"   EXCHANGE: {client.EXCHANGE_NAME}\n"
                message += f"TOT BAL: {int(round(client.get_balance(), 0))} USD\n"
                message += f"ACTIVE POSITIONS: {'|'.join(coins)}\n"
                message += f"TOT POS, USD: {total_pos}\n"
                message += f"ABS POS, USD: {abs_pos}\n"
                message += f"AVL BUY:  {round(client.get_available_balance()['buy'])}\n"
                message += f"AVL SELL: {round(client.get_available_balance()['sell'])}\n"
                total_position += total_pos
                abs_total_position += abs_pos
                total_balance += client.get_balance()
            except:
                traceback.print_exc()
        try:
            message += f"   TOTAL:\n"
            message += f"START BALANCE: {int(round(total_balance))} USD\n"
            message += f"TOT POS: {int(round(total_position))} USD\n"
            message += f"ABS TOT POS: {int(round(abs_total_position))} USD\n"
        except:
            traceback.print_exc()
        # print(80 * '*' + f"START BALANCE MESSAGE SENT")
        await self.send_message(message, int(config['TELEGRAM']['CHAT_ID']), config['TELEGRAM']['TOKEN'])

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

    def __check_env(self) -> bool:
        return 'DEV_' in self.env.upper()

    async def prepare_alert(self):
        percent_change = round(100 - self.finish * 100 / self.start, 2)
        usd_change = self.finish - self.start

        message = f"ALERT NAME: BALANCE JUMP {'🔴' if usd_change < 0 else '🟢'}\n"
        message += f"MULTIBOT {config['SETTINGS']['COIN']}\n"
        message += f'{" | ".join(self.exchanges)}\n'
        message += f"ENV: {self.env}\n"

        if not self.__check_env():
            message += "CHANGE STATE TO PARSER\n"

        message += f"BALANCE CHANGE %: {percent_change}\n"
        message += f"BALANCE CHANGE USD: {usd_change}\n"
        message += f"PREVIOUS BAL, USD: {self.start}\n"
        message += f"CURRENT BAL, USD: {self.finish}\n"
        message += f"PREVIOUS DT: {self.s_time}\n"
        message += f"CURRENT DT: {self.f_time}"

        await self.send_message(message, int(self.alert_id), self.alert_token)

    async def __check_order_status(self):
        while True:
            for client in self.clients:
                orders = client.orders.copy()

                for order_id, message in orders.items():
                    self.tasks.put({
                        'message': message,
                        'routing_key': RabbitMqQueues.UPDATE_ORDERS,
                        'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.UPDATE_ORDERS),
                        'queue_name': RabbitMqQueues.UPDATE_ORDERS
                    })

                    client.orders.pop(order_id)

            await asyncio.sleep(3)

    async def __check_start_launch_config(self):
        async with self.db.acquire() as cursor:
            if not await get_last_launch(cursor,
                                         self.clients[0].EXCHANGE_NAME,
                                         self.clients[1].EXCHANGE_NAME,
                                         self.setts['COIN']):
                if launch := await get_last_launch(cursor,
                                                   self.clients[0].EXCHANGE_NAME,
                                                   self.clients[1].EXCHANGE_NAME,
                                                   self.setts['COIN'], 1):
                    launch = launch[0]
                    data = {
                        "env": self.setts['ENV'],
                        "shift_use_flag": launch['shift_use_flag'],
                        "target_profit": launch['target_profit'],
                        "orders_delay": launch['orders_delay'],
                        "max_order_usd": launch['max_order_usd'],
                        "max_leverage": launch['max_leverage'],
                        'exchange_1': self.clients[0].EXCHANGE_NAME,
                        'exchange_2': self.clients[1].EXCHANGE_NAME,
                    }
                    headers = {
                        'token': 'jnfXhfuherfihvijnfjigt',
                        'context': 'bot-start'
                    }
                else:
                    data = self.base_launch_config
                url = f"http://{self.setts['CONFIG_API_HOST']}:{self.setts['CONFIG_API_PORT']}/api/v1/configs"

                requests.post(url=url, headers=headers, json=data)

    async def start_db_update(self):
        async with self.db.acquire() as cursor:
            if launches := await get_last_launch(cursor,
                                                 self.clients[0].EXCHANGE_NAME,
                                                 self.clients[1].EXCHANGE_NAME,
                                                 self.setts['COIN']):
                launch = launches.pop(0)
                self.bot_launch_id = str(launch['id'])

                for field in launch:
                    if not launch.get('field') and field not in ['id', 'datetime', 'ts', 'bot_config_id',
                                                                 'coin', 'shift']:
                        launch[field] = self.base_launch_config[field]

                launch['launch_id'] = str(launch.pop('id'))
                launch['bot_config_id'] = str(launch['bot_config_id'])

                # if not launch.get('shift_use_flag'):
                #     for client_1, client_2 in self.ribs:
                #         self.shifts.update({f'{client_1.EXCHANGE_NAME} {client_2.EXCHANGE_NAME}': 0})
                # else:
                #     self.shifts = start_shifts

                self.tasks.put({
                    'message': launch,
                    'routing_key': RabbitMqQueues.UPDATE_LAUNCH,
                    'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.UPDATE_LAUNCH),
                    'queue_name': RabbitMqQueues.UPDATE_LAUNCH
                })

                for launch in launches:
                    launch['datetime_update'] = self.base_launch_config['datetime_update']
                    launch['ts_update'] = self.base_launch_config['ts_update']
                    launch['updated_flag'] = -1
                    launch['launch_id'] = str(launch.pop('id'))
                    launch['bot_config_id'] = str(launch['bot_config_id'])
                    self.tasks.put({
                        'message': launch,
                        'routing_key': RabbitMqQueues.UPDATE_LAUNCH,
                        'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.UPDATE_LAUNCH),
                        'queue_name': RabbitMqQueues.UPDATE_LAUNCH
                    })
                self.update_config()

    def update_config(self):
        # UPDATE BALANCES
        message = {
            'parent_id': self.bot_launch_id,
            'context': 'bot-config-update',
            'env': self.env,
            'chat_id': self.chat_id,
            'telegram_bot': self.telegram_bot,
        }

        self.tasks.put({
            'message': message,
            'routing_key': RabbitMqQueues.CHECK_BALANCE,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.CHECK_BALANCE),
            'queue_name': RabbitMqQueues.CHECK_BALANCE
        })

    def update_balances(self):
        message = {
            'parent_id': self.bot_launch_id,
            'context': 'bot-launch',
            'env': self.env,
            'chat_id': self.chat_id,
            'telegram_bot': self.telegram_bot,
        }

        self.tasks.put({
            'message': message,
            'routing_key': RabbitMqQueues.CHECK_BALANCE,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.CHECK_BALANCE),
            'queue_name': RabbitMqQueues.CHECK_BALANCE
        })

    async def __launch_and_run(self):
        await self.setup_postgres()
        print(f"POSTGRES STARTED SUCCESSFULLY")
        start = datetime.datetime.utcnow()
        first_launch = True

        await self.__check_start_launch_config()
        # start_shifts = self.shifts.copy()

        async with aiohttp.ClientSession() as session:
            self.session = session
            time.sleep(3)
            start_message = False

            while True:
                if (start - datetime.datetime.utcnow()).seconds >= 30 or first_launch:
                    first_launch = False
                    start = datetime.datetime.utcnow()
                    await self.start_db_update()

                if not start_message:
                    await self.start_message()
                    await self.start_balance_message()
                    self.update_balances()
                    start_message = True

                time_start = time.time()
                deal = None
                if len(self.potential_deals):
                    print(f"\nPOTENTIAL DEALS: {len(self.potential_deals)}")
                    deal = self.choose_deal()
                if self.state == BotState.BOT:
                    if deal:
                        time_choose = time.time() - time_start
                        await self.execute_deal(deal, time_choose)

if __name__ == '__main__':
    MultiBot()
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-c1', nargs='?', const=True, default='apollox', dest='client_1')
    # parser.add_argument('-c2', nargs='?', const=True, default='binance', dest='client_2')
    # args = parser.parse_args()

    # import cProfile
    #
    #
    # def your_function():
    #
    #
    # # Your code here
    #
    # # Start the profiler
    # profiler = cProfile.Profile()
    # profiler.enable()
    #
    # # Run your code
    # your_function()
    #
    # # Stop the profiler
    # profiler.disable()
    #
    # # Print the profiling results
    # profiler.print_stats(sort='time')
