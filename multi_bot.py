import argparse
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
# from config import Config
from clients.enums import BotState, RabbitMqQueues
from core.queries import get_last_balance_jumps, get_total_balance, get_last_launch
from tools.shifts import Shifts

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

CLIENTS_WITH_CONFIGS = {
    'BITMEX': [BitmexClient, config['BITMEX'], config['SETTINGS']['LEVERAGE']],
    'DYDX': [DydxClient, config['DYDX'], config['SETTINGS']['LEVERAGE']],
    'BINANCE': [BinanceClient, config['BINANCE'], config['SETTINGS']['LEVERAGE']],
    'APOLLOX': [ApolloxClient, config['APOLLOX'], config['SETTINGS']['LEVERAGE']],
    'OKX': [OkxClient, config['OKX'], config['SETTINGS']['LEVERAGE']],
    'KRAKEN': [KrakenClient, config['KRAKEN'], config['SETTINGS']['LEVERAGE']]
}


class MultiBot:
    __slots__ = ['rabbit_url', 'deal_pause', 'max_order_size', 'profit_taker', 'shifts', 'telegram_bot', 'chat_id',
                 'daily_chat_id', 'inv_chat_id', 'state', 'loop', 'client_1', 'client_2', 'start_time', 'last_message',
                 'last_max_deal_size', 'potential_deals', 'deals_counter', 'deals_executed', 'available_balances',
                 'session', 'clients', 'exchanges', 'mq', 'ribs', 'env', 'exchanges_len', 'db', 'tasks',
                 'start', 'finish', 's_time', 'f_time', 'run_1', 'run_2', 'run_3', 'run_4', 'loop_1', 'loop_2',
                 'loop_3', 'loop_4', 'need_check_shift', 'last_orderbooks', 'time_start', 'time_parser',
                 'bot_launch_id', 'base_launch_config', 'launch_fields', 'setts']

    def __init__(self, client_1: str, client_2: str):
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

        self.s_time = ''
        self.f_time = ''
        self.tasks = queue.Queue()
        # self.create_csv('extra_countings.csv')
        self.last_orderbooks = {}

        # ORDER CONFIGS
        self.deal_pause = int(self.setts['DEALS_PAUSE'])
        self.max_order_size = int(self.setts['ORDER_SIZE'])
        self.profit_taker = float(self.setts['TARGET_PROFIT'])
        self.shifts = {'TAKER': float(self.setts['LIMIT_SHIFTS'])}

        # TELEGRAM
        self.telegram_bot = config['TELEGRAM']['TOKEN']
        self.chat_id = int(config['TELEGRAM']['CHAT_ID'])
        self.daily_chat_id = int(config['TELEGRAM']['DAILY_CHAT_ID'])
        self.inv_chat_id = int(config['TELEGRAM']['INV_CHAT_ID'])

        self.state = self.setts['STATE']
        self.exchanges_len = len(self.setts['EXCHANGES'].split(', '))

        # CLIENTS
        client_1 = CLIENTS_WITH_CONFIGS[client_1.upper()]
        client_2 = CLIENTS_WITH_CONFIGS[client_2.upper()]

        self.client_1 = client_1[0](client_1[1], client_1[2])
        self.client_2 = client_2[0](client_2[1], client_2[2])
        self.clients = [self.client_1, self.client_2]

        self.exchanges = [x.EXCHANGE_NAME for x in self.clients]
        self.ribs = [self.clients, list(reversed(self.clients))]

        self.start_time = datetime.datetime.utcnow()
        self.last_message = None
        self.last_max_deal_size = 0
        self.potential_deals = []
        self.deals_counter = []
        self.deals_executed = []
        self.available_balances = {'+DYDX-OKEX': 0}
        self.session = None

        for client in self.clients:
            client.run_updater()

        time.sleep(10)

        self.base_launch_config = {
            "env": self.setts['ENV'],
            "shift_use_flag": 0,
            "target_profit": 0.01,
            "orders_delay": 300,
            "max_order_usd": 50,
            "max_leverage": 2,
            'fee_exchange_1': self.client_1.taker_fee,
            'fee_exchange_2': self.client_2.taker_fee,
            'exchange_1': self.client_1.EXCHANGE_NAME,
            'exchange_2': self.client_2.EXCHANGE_NAME,
            'updated_flag': 1,
            'datetime_update': datetime.datetime.utcnow(),
            'ts_update': int(time.time() * 1000)
        }

        self.loop_1 = asyncio.new_event_loop()
        self.loop_2 = asyncio.new_event_loop()
        self.loop_3 = asyncio.new_event_loop()
        self.loop_4 = asyncio.new_event_loop()

        t1 = threading.Thread(target=self.run_await_in_thread, args=[self.__start, self.loop_1])
        t2 = threading.Thread(target=self.run_await_in_thread, args=[self.__check_order_status, self.loop_2])
        t3 = threading.Thread(target=self.run_await_in_thread, args=[self.__cycle_parser, self.loop_3])
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

    def __prepare_shifts(self):
        time.sleep(10)
        self.__rates_update()
        # !!!!SHIFTS ARE HARDCODED TO A ZERO!!!!
        for x, y in Shifts().get_shifts().items():
            self.shifts.update({x: y})

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
            task = self.tasks.get()
            try:
                task.update({'connect': self.mq})
                await self.publish_message(**task)

            except:
                await self.setup_mq(self.loop_4)
                await asyncio.sleep(1)
                task.update({'connect': self.mq})
                print(task)
                await self.publish_message(**task)

            finally:
                self.tasks.task_done()
                await asyncio.sleep(0.1)

    def available_balance_update(self, client_buy, client_sell):
        max_deal_size = self.avail_balance_define(client_buy, client_sell)
        self.available_balances.update({f"+{client_buy.EXCHANGE_NAME}-{client_sell.EXCHANGE_NAME}": max_deal_size})

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

    async def __cycle_parser(self):
        time.sleep(12)
        while True:
            # print(f"{self.client_1.count_flag} {self.client_2.count_flag}")
            if self.client_1.count_flag or self.client_2.count_flag:
                try_list = []
                time_start = time.time()  # noqa
                for client_buy, client_sell in self.ribs:
                    if client_buy.EXCHANGE_NAME + client_sell.EXCHANGE_NAME not in try_list:
                        self.available_balance_update(client_buy, client_sell)
                        try_list.append(client_buy.EXCHANGE_NAME + client_sell.EXCHANGE_NAME)
                    ob_sell, ob_buy = self.get_orderbooks(client_sell, client_buy)
                    # shift = self.shifts[client_buy.EXCHANGE_NAME + ' ' + client_sell.EXCHANGE_NAME] / 2
                    sell_price = ob_sell['bids'][0][0]  # * (1 + shift)
                    buy_price = ob_buy['asks'][0][0]  # * (1 - shift)
                    # if sell_price > buy_price:
                    self.taker_order_profit(client_sell, client_buy, sell_price, buy_price, ob_buy, ob_sell, time_start)
                    await self.potential_real_deals(client_sell, client_buy, ob_buy, ob_sell)
                self.client_1.count_flag = False
                self.client_2.count_flag = False
            await asyncio.sleep(0.02)

    async def find_price_diffs(self):
        time_start = time.time()
        chosen_deal = None
        if len(self.potential_deals):
            chosen_deal = self.choose_deal()
        if self.state == BotState.BOT:
            if chosen_deal:
                time_choose = time.time() - time_start
                await self.execute_deal(chosen_deal,
                                        time_choose)

    def choose_deal(self):
        max_profit = self.profit_taker
        chosen_deal = None
        for deal in self.potential_deals:
            if deal['profit'] > max_profit:
                buy_exch = deal['buy_exch'].EXCHANGE_NAME
                sell_exch = deal['sell_exch'].EXCHANGE_NAME
                print(f"\n\n\nBUY {buy_exch} {deal['ob_buy']['asks'][0][0]} SIZE: {deal['ob_buy']['asks'][0][1]}")
                print(f"SELL {sell_exch} {deal['ob_sell']['bids'][0][0]} SIZE: {deal['ob_sell']['bids'][0][1]}")
                print(f"MAX DEAL SIZE: {self.available_balances[f'+{buy_exch}-{sell_exch}']}")
                print(f"MAX DEAL SIZE(vice versa): {self.available_balances[f'+{sell_exch}-{buy_exch}']}")
                print(f"{deal['profit']=}\n\n\n")
                if self.available_balances[f"+{buy_exch}-{sell_exch}"] >= self.max_order_size:  # noqa
                    max_profit = deal['profit']
                    chosen_deal = deal

        self.potential_deals = []
        return chosen_deal

    def taker_order_profit(self, client_sell, client_buy, sell_price, buy_price, ob_buy, ob_sell, time_start):
        profit = ((sell_price - buy_price) / buy_price) - (client_sell.taker_fee + client_buy.taker_fee)
        if profit > self.profit_taker:
            self.potential_deals.append({'buy_exch': client_buy,
                                         "sell_exch": client_sell,
                                         "sell_px": sell_price,
                                         "buy_px": buy_price,
                                         'expect_buy_px': ob_buy['asks'][0][0],
                                         'expect_sell_px': ob_sell['bids'][0][0],
                                         "ob_buy": ob_buy,
                                         "ob_sell": ob_sell,
                                         'max_deal_size': self.available_balances[
                                             f"+{client_buy.EXCHANGE_NAME}-{client_sell.EXCHANGE_NAME}"],
                                         "profit": profit,
                                         'time_start': time_start,
                                         'time_parser': time.time() - time_start})

    def __get_amount_for_all_clients(self, amount):
        print(f"Started __get_amount_for_all_clients: AMOUNT: {amount}")
        for client in self.clients:
            client.fit_amount(amount)
            print(f"{client.EXCHANGE_NAME}|AMOUNT: {amount}|FIT AMOUNT: {client.expect_amount_coin}")
        max_amount = max([client.expect_amount_coin for client in self.clients])
        for client in self.clients:
            client.expect_amount_coin = max_amount

    async def execute_deal(self, chosen_deal: dict, time_choose) -> None:
        client_buy = chosen_deal['buy_exch']
        client_sell = chosen_deal['sell_exch']
        ob_buy = chosen_deal['ob_buy']
        ob_sell = chosen_deal['ob_sell']
        max_deal_size = self.available_balances[f"+{client_buy.EXCHANGE_NAME}-{client_sell.EXCHANGE_NAME}"]
        max_deal_size = max_deal_size / ob_buy['asks'][0][0]
        expect_buy_px = chosen_deal['expect_buy_px']
        expect_sell_px = chosen_deal['expect_sell_px']
        if expect_buy_px <= chosen_deal["buy_px"] and expect_sell_px >= chosen_deal["sell_px"]:
            # shift = self.shifts[client_sell.EXCHANGE_NAME + ' ' + client_buy.EXCHANGE_NAME] / 2
            shifted_buy_px = ob_buy['asks'][4][0]
            shifted_sell_px = ob_sell['bids'][4][0]
            # shifted_buy_px = price_buy * self.shifts['TAKER']
            # shifted_sell_px = price_sell / self.shifts['TAKER']
            max_buy_vol = ob_buy['asks'][0][1]
            max_sell_vol = ob_sell['bids'][0][1]
            # timer = time.time()
            arbitrage_possibilities_id = uuid.uuid4()
            self.__get_amount_for_all_clients(max_deal_size)
            cl_id_buy = f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}"
            cl_id_sell = f"api_deal_{str(uuid.uuid4()).replace('-', '')[:20]}"
            time_sent = int(datetime.datetime.utcnow().timestamp() * 1000)
            responses = await asyncio.gather(*[
                self.loop_1.create_task(
                    client_buy.create_order(shifted_buy_px, 'buy', self.session, client_id=cl_id_buy)),
                self.loop_1.create_task(
                    client_sell.create_order(shifted_sell_px, 'sell', self.session, client_id=cl_id_sell))
            ], return_exceptions=True)
            print(responses)
            # print(f"FULL POOL ADDING AND CALLING TIME: {time.time() - timer}")
            await asyncio.sleep(0.5)
            # !!! ALL TIMERS !!!
            # time_start_parsing = chosen_deal['time_start']
            # self.time_parser = chosen_deal['time_parser']
            buy_order_place_time = self._check_order_place_time(client_buy, time_sent, responses)
            sell_order_place_time = self._check_order_place_time(client_sell, time_sent, responses)
            self.save_orders(client_buy, 'buy', arbitrage_possibilities_id, buy_order_place_time, shifted_buy_px)
            self.save_orders(client_sell, 'sell', arbitrage_possibilities_id, sell_order_place_time, shifted_sell_px)
            self.save_arbitrage_possibilities(arbitrage_possibilities_id, client_buy, client_sell, max_buy_vol,
                                              max_sell_vol, expect_buy_px, expect_sell_px, time_choose, shift=None,
                                              time_parser=chosen_deal['time_parser'])
            self.save_balance(arbitrage_possibilities_id)

            await asyncio.sleep(self.deal_pause)

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
                                     expect_sell_px, time_choose, shift, time_parser):
        expect_profit_usd = ((expect_sell_px - expect_buy_px) / expect_buy_px - (
                client_buy.taker_fee + client_sell.taker_fee)) * client_buy.expect_amount_coin
        expect_amount_usd = client_buy.expect_amount_coin * (expect_sell_px + expect_buy_px) / 2
        message = {
            'id': _id,
            'datetime': datetime.datetime.utcnow(),
            'ts': int(time.time()),
            'buy_exchange': client_buy.EXCHANGE_NAME,
            'sell_exchange': client_sell.EXCHANGE_NAME,
            'symbol': client_buy.symbol,
            'buy_order_id': client_buy.LAST_ORDER_ID,
            'sell_order_id': client_sell.LAST_ORDER_ID,
            'max_buy_vol_usd': round(max_buy_vol * expect_buy_px),
            'max_sell_vol_usd': round(max_sell_vol * expect_sell_px),
            'expect_buy_price': expect_buy_px,
            'expect_sell_price': expect_sell_px,
            'expect_amount_usd': expect_amount_usd,
            'expect_amount_coin': client_buy.expect_amount_coin,
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

    def save_orders(self, client, side, parent_id, order_place_time, expect_price) -> None:
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
            'symbol': client.symbol.upper(),
            'expect_price': expect_price,
            'expect_amount_coin': client.expect_amount_coin,
            'expect_amount_usd': client.expect_amount_coin * client.expect_price,
            'expect_fee': client.taker_fee,
            'factual_price': 0,
            'factual_amount_coin': 0,
            'factual_amount_usd': 0,
            'factual_fee': client.taker_fee,
            'order_place_time': order_place_time,
            'env': self.env,
        }

        if client.LAST_ORDER_ID == 'default':
            error_message = {
                "chat_id": config['TELEGRAM']['ALERT_CHAT_ID'],
                "msg": f"ALERT NAME: Order Mistake\nCOIN: {self.setts['COIN']}\nCONTEXT: BOT\nENV: {self.env}\n"
                       f"EXCHANGE: {client.EXCHANGE_NAME}\nOrder Id:{order_id}\nError:{client.error_info}",
                'bot_token': config['TELEGRAM']['ALERT_BOT_TOKEN']
            }
            self.tasks.put({
                'message': error_message,
                'routing_key': RabbitMqQueues.TELEGRAM,
                'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
                'queue_name': RabbitMqQueues.TELEGRAM
            })

        self.tasks.put({
            'message': message,
            'routing_key': RabbitMqQueues.ORDERS,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.ORDERS),
            'queue_name': RabbitMqQueues.ORDERS
        })

    async def publish_message(self, connect, message, routing_key, exchange_name, queue_name):
        channel = await connect.channel()
        exchange = await channel.declare_exchange(exchange_name, type=ExchangeType.DIRECT, durable=True)
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key=routing_key)
        message_body = orjson.dumps(message)
        message = Message(message_body)
        await exchange.publish(message, routing_key=routing_key)
        await channel.close()
        return True

    def avail_balance_define(self, client_buy, client_sell):
        return min(client_buy.get_available_balance('buy'), client_sell.get_available_balance('sell'),
                   self.max_order_size)

    def __rates_update(self):
        message = ''
        with open('rates.txt', 'a') as file:
            for client in self.clients:
                message += f"{client.EXCHANGE_NAME} | {client.get_orderbook()[client.symbol]['asks'][0][0]} | {datetime.datetime.utcnow()} | {time.time()}\n"

            file.write(message + '\n')

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
            "chat_id": config['TELEGRAM']['ALERT_CHAT_ID'],
            "msg": msg,
            'bot_token': config['TELEGRAM']['ALERT_BOT_TOKEN']
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
            ob_sell = client_sell.get_orderbook()[client_sell.symbol]
            ob_buy = client_buy.get_orderbook()[client_buy.symbol]
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
        coin = self.client_1.symbol.split('USD')[0].replace('-', '').replace('/', '')
        message = f'MULTIBOT STARTED\n{self.client_1.EXCHANGE_NAME} | {self.client_2.EXCHANGE_NAME}\n'
        message += f"COIN: {coin}\n"
        message += f"ENV: {self.env}\n"
        message += f"STATE: {self.setts['STATE']}\n"
        message += f"LEVERAGE: {self.setts['LEVERAGE']}\n"
        message += f"EXCHANGES: {self.client_1.EXCHANGE_NAME} {self.client_2.EXCHANGE_NAME}\n"
        message += f"DEALS_PAUSE: {self.setts['DEALS_PAUSE']}\n"
        message += f"ORDER_SIZE: {self.setts['ORDER_SIZE']}\n"
        message += f"TARGET_PROFIT: {self.setts['TARGET_PROFIT']}\n"
        message += f"START BALANCE: {self.start}\n"
        message += f"CURRENT BALANCE: {self.finish}\n"

        for exchange, shift in self.shifts.items():
            message += f"{exchange}: {round(shift, 6)}\n"
        # print(80 * '*' + f"START MESSAGE SENT")
        await self.send_message(message, int(config['TELEGRAM']['CHAT_ID']), config['TELEGRAM']['TOKEN'])

    def create_result_message(self, deals_potential: dict, deals_executed: dict, time: int) -> str:
        message = f"For last 3 min\n"
        message += f"ENV: {self.setts['ENV']}\n"

        if self.__check_env():
            message += f'SYMBOL: {self.client_1.symbol}'

        message += f"\n\nPotential deals:"
        for side, values in deals_potential.items():
            message += f"\n   {side}:"
            for exchange, deals in values.items():
                message += f"\n{exchange}: {deals}"
        message += f"\n\nExecuted deals:"
        for side, values in deals_executed.items():
            message += f"\n   {side}:"
            for exchange, deals in values.items():
                message += f"\n{exchange}: {deals}"
        return message

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

    def get_sizes(self):
        tick_size = max([x.tick_size for x in self.clients if x.tick_size], default=0.01)
        step_size = max([x.step_size for x in self.clients if x.step_size], default=0.01)
        quantity_precision = max([x.quantity_precision for x in self.clients if x.quantity_precision])

        self.client_1.quantity_precision = quantity_precision
        self.client_2.quantity_precision = quantity_precision

        self.client_1.tick_size = tick_size
        self.client_2.tick_size = tick_size

        self.client_1.step_size = step_size
        self.client_2.step_size = step_size

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
        total_balance = 0
        total_position = 0
        index_price = []

        for client in self.clients:
            try:
                coin = client.symbol.split('USD')[0].replace('-', '').replace('/', '')
                message += f"   EXCHANGE: {client.EXCHANGE_NAME}\n"
                message += f"ENV: {self.setts['ENV']}\n"
                message += f"TOT BAL: {client.get_real_balance()} USD\n"
                message += f"POS: {round(client.get_positions()[client.symbol]['amount'], 4)} {coin}\n"
                message += f"AVL BUY:  {round(client.get_available_balance('buy'))}\n"
                message += f"AVL SELL: {round(client.get_available_balance('sell'))}\n"
                index_price.append((client.get_orderbook()[client.symbol]['bids'][0][0] +
                                    client.get_orderbook()[client.symbol]['asks'][0][0]) / 2)
                total_position += client.get_positions()[client.symbol]['amount']
                total_balance += client.get_real_balance()
            except:
                traceback.print_exc()
        try:
            message += f"   TOTAL:\n"
            message += f"START BALANCE: {round(total_balance, 2)} USD\n"
            message += f"POSITION: {round(total_position, 4)} {coin}\n"
            message += f"INDEX PX: {round(sum(index_price) / len(index_price), 2)} USD\n"
        except:
            traceback.print_exc()
        # print(80 * '*' + f"START BALANCE MESSAGE SENT")
        await self.send_message(message, int(config['TELEGRAM']['CHAT_ID']), config['TELEGRAM']['TOKEN'])

    async def close_all_positions(self):
        async with aiohttp.ClientSession() as session:
            print('START')
            while abs(self.client_1.get_positions().get(self.client_1.symbol, {}).get('amount_usd', 0)) > 50 \
                    or abs(self.client_2.get_positions().get(self.client_2.symbol, {}).get('amount_usd', 0)) > 50:
                print('START WHILE')

                for client in self.clients:
                    print(f'START CLIENT {client.EXCHANGE_NAME}')
                    client.cancel_all_orders()
                    if res := client.get_positions().get(client.symbol, {}).get('amount'):
                        orderbook = client.get_orderbook()[client.symbol]
                        side = 'buy' if res < 0 else 'sell'
                        price = orderbook['bids'][0][0] if side == 'buy' else orderbook['asks'][0][0]
                        await client.create_order(abs(res), price, side, session)
                        time.sleep(7)

    def __check_env(self) -> bool:
        return 'DEV_' in self.env.upper()

    async def prepare_alert(self):
        percent_change = round(100 - self.finish * 100 / self.start, 2)
        usd_change = self.finish - self.start

        message = f"ALERT NAME: BALANCE JUMP {'🔴' if usd_change < 0 else '🟢'}\n"
        message += f"MULTIBOT {self.client_1.EXCHANGE_NAME}-{self.client_2.EXCHANGE_NAME}\n"
        message += f"ENV: {self.env}\n"

        if not self.__check_env():
            message += "CHANGE STATE TO PARSER\n"

        message += f"BALANCE CHANGE %: {percent_change}\n"
        message += f"BALANCE CHANGE USD: {usd_change}\n"
        message += f"PREVIOUS BAL, USD: {self.start}\n"
        message += f"CURRENT BAL, USD: {self.finish}\n"
        message += f"PREVIOUS DT: {self.s_time}\n"
        message += f"CURRENT DT: {self.f_time}"

        await self.send_message(message, int(config['TELEGRAM']['ALERT_CHAT_ID']), config['TELEGRAM']['ALERT_BOT_TOKEN'])

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
                                         self.client_1.EXCHANGE_NAME,
                                         self.client_2.EXCHANGE_NAME,
                                         self.setts['COIN']):
                if launch := await get_last_launch(cursor,
                                                   self.client_1.EXCHANGE_NAME,
                                                   self.client_2.EXCHANGE_NAME,
                                                   self.setts['COIN'], 1):

                    launch = launch[0]
                    data = {
                        "env": self.setts['ENV'],
                        "shift_use_flag": launch['shift_use_flag'],
                        "target_profit": launch['target_profit'],
                        "orders_delay": launch['orders_delay'],
                        "max_order_usd": launch['max_order_usd'],
                        "max_leverage": launch['max_leverage'],
                        'exchange_1': self.client_1.EXCHANGE_NAME,
                        'exchange_2': self.client_2.EXCHANGE_NAME,
                    }
                    headers = {
                        'token': 'jnfXhfuherfihvijnfjigt',
                        'context': 'bot-start'
                    }
                else:
                    data = self.base_launch_config
                url = f"http://{self.setts['CONFIG_API_HOST']}:{self.setts['CONFIG_API_PORT']}/api/v1/configs"

                requests.post(url=url, headers=headers, json=data)

    async def __start(self):
        await self.setup_postgres()
        print(f"POSTGRES STARTED SUCCESSFULLY")
        start = datetime.datetime.utcnow()
        first_launch = True
        exchanges = self.client_1.EXCHANGE_NAME + ' ' + self.client_2.EXCHANGE_NAME

        await self.__check_start_launch_config()

        while not exchanges in self.shifts:
            print('Wait shifts for', self.client_1.EXCHANGE_NAME + ' ' + self.client_2.EXCHANGE_NAME)
            self.__prepare_shifts()

        start_shifts = self.shifts.copy()

        async with aiohttp.ClientSession() as session:
            self.session = session
            time.sleep(3)
            start_message = False

            while True:
                if self.state == BotState.PARSER:
                    time.sleep(1)

                if (start - datetime.datetime.utcnow()).seconds >= 30 or first_launch:
                    first_launch = False
                    start = datetime.datetime.utcnow()

                    async with self.db.acquire() as cursor:
                        if launches := await get_last_launch(cursor,
                                                             self.client_1.EXCHANGE_NAME,
                                                             self.client_2.EXCHANGE_NAME,
                                                             self.setts['COIN']):
                            launch = launches.pop(0)
                            self.bot_launch_id = str(launch['id'])

                            for field in launch:
                                if not launch.get('field') and field not in ['id', 'datetime', 'ts', 'bot_config_id',
                                                                             'coin', 'shift']:
                                    launch[field] = self.base_launch_config[field]

                            launch['launch_id'] = str(launch.pop('id'))
                            launch['bot_config_id'] = str(launch['bot_config_id'])

                            if not launch.get('shift_use_flag'):
                                for client_1, client_2 in self.ribs:
                                    self.shifts.update({f'{client_1.EXCHANGE_NAME} {client_2.EXCHANGE_NAME}': 0})
                            else:
                                self.shifts = start_shifts

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

                if not start_message:
                    await self.start_message()
                    await self.start_balance_message()
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
                    start_message = True

                await self.find_price_diffs()


if __name__ == '__main__':
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
    clients = config['SETTINGS']['EXCHANGES'].split(', ')
    MultiBot(clients[0], clients[1])
