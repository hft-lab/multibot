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

import aiohttp
import asyncpg
import orjson
from aio_pika import Message, ExchangeType, connect_robust

from clients.apollox import ApolloxClient
from clients.binance import BinanceClient
from clients.bitmex import BitmexClient
from clients.dydx import DydxClient
from clients.kraken import KrakenClient
from clients.okx import OkxClient
from config import Config
from clients.enums import BotState, RabbitMqQueues, OrderStatus
from core.queries import get_last_balance_jumps, get_total_balance
from tools.shifts import Shifts

dictConfig(Config.LOGGING)
logger = logging.getLogger(__name__)

CLIENTS_WITH_CONFIGS = {
    'BITMEX': [BitmexClient, Config.BITMEX, Config.LEVERAGE],
    'DYDX': [DydxClient, Config.DYDX, Config.LEVERAGE],
    'BINANCE': [BinanceClient, Config.BINANCE, Config.LEVERAGE],
    'APOLLOX': [ApolloxClient, Config.APOLLOX, Config.LEVERAGE],
    'OKX': [OkxClient, Config.OKX, Config.LEVERAGE],
    'KRAKEN': [KrakenClient, Config.KRAKEN, Config.LEVERAGE]
}


class MultiBot:
    __slots__ = ['rabbit_url', 'deal_pause', 'max_order_size', 'profit_taker', 'shifts', 'telegram_bot', 'chat_id',
                 'daily_chat_id', 'inv_chat_id', 'state', 'loop', 'client_1', 'client_2', 'start_time', 'last_message',
                 'last_max_deal_size', 'potential_deals', 'deals_counter', 'deals_executed', 'available_balances',
                 'session', 'clients', 'exchanges', 'mq', 'ribs', 'env', 'exchanges_len', 'db', 'tasks',
                 'start', 'finish', 's_time', 'f_time', 'run_1', 'run_2', 'run_3', 'run_4', 'loop_1', 'loop_2',
                 'loop_3', 'loop_4', 'need_check_shift']

    def __init__(self, client_1: str, client_2: str):
        self.start = None
        self.finish = None
        self.db = None
        self.mq = None
        self.rabbit_url = f"amqp://{Config.RABBIT['username']}:{Config.RABBIT['password']}@{Config.RABBIT['host']}:{Config.RABBIT['port']}/"

        self.env = Config.ENV

        self.s_time = ''
        self.f_time = ''
        self.tasks = queue.Queue()

        # ORDER CONFIGS
        self.deal_pause = Config.DEALS_PAUSE
        self.max_order_size = Config.ORDER_SIZE
        self.profit_taker = Config.TARGET_PROFIT
        self.shifts = {'TAKER': Config.LIMIT_SHIFTS}

        # TELEGRAM
        self.telegram_bot = Config.TELEGRAM_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        self.daily_chat_id = Config.TELEGRAM_DAILY_CHAT_ID
        self.inv_chat_id = Config.TELEGRAM_INV_CHAT_ID

        self.state = Config.STATE
        self.exchanges_len = len(Config.EXCHANGES)

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

        self.get_sizes()

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

    def __prepare_shifts(self):
        time.sleep(10)
        self.__rates_update()

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
            task.update({'connect': self.mq})

            await self.publish_message(**task)
            await asyncio.sleep(0.1)

    def available_balance_update(self, client_buy, client_sell):
        max_deal_size = self.avail_balance_define(client_buy, client_sell)
        self.available_balances.update({f"+{client_buy.EXCHANGE_NAME}-{client_sell.EXCHANGE_NAME}": max_deal_size})

    def run_await_in_thread(self, func, loop):
        try:
            loop.run_until_complete(func())
        except:
            traceback.print_exc()
        finally:
            loop.close()

    async def __cycle_parser(self):

        time.sleep(12)

        while True:
            for client_buy, client_sell in self.ribs:
                self.available_balance_update(client_buy, client_sell)
                orderbook_sell, orderbook_buy = self.get_orderbooks(client_sell, client_buy)
                shift = self.shifts[client_buy.EXCHANGE_NAME + ' ' + client_sell.EXCHANGE_NAME] / 2
                sell_price = orderbook_sell['bids'][0][0] * (1 + shift)
                buy_price = orderbook_buy['asks'][0][0] * (1 - shift)

                if sell_price > buy_price:
                    self.taker_order_profit(client_sell, client_buy, sell_price, buy_price)

                await self.potential_real_deals(client_sell, client_buy, orderbook_buy, orderbook_sell)

            await asyncio.sleep(0.7)

    async def find_price_diffs(self):
        time_start = time.time()
        time_parser = time.time() - time_start
        chosen_deal = None

        if len(self.potential_deals):
            chosen_deal = self.choose_deal()

        if self.state == BotState.BOT:
            position_gap, amount_to_balancing = self.find_balancing_elements()
            if chosen_deal and amount_to_balancing < self.max_order_size:  # todo REFACTOR THIS
                time_choose = time.time() - time_start - time_parser
                await self.execute_deal(chosen_deal['buy_exch'],
                                        chosen_deal['sell_exch'],
                                        chosen_deal['orderbook_buy'],
                                        time_start,
                                        time_parser,
                                        time_choose)

    def choose_deal(self):
        max_profit = 0
        chosen_deal = None

        for deal in self.potential_deals:
            self.deals_counter.append({'buy_exch': deal['buy_exch'],
                                       "sell_exch": deal['sell_exch'],
                                       "profit": deal['profit']})

            if deal['profit'] > max_profit:
                if self.available_balances[
                    f"+{deal['buy_exch'].EXCHANGE_NAME}-{deal['sell_exch'].EXCHANGE_NAME}"] >= self.max_order_size:
                    if deal['buy_exch'].EXCHANGE_NAME in self.exchanges or deal[
                        'sell_exch'].EXCHANGE_NAME in self.exchanges:
                        max_profit = deal['profit']
                        chosen_deal = deal

        self.potential_deals = []
        return chosen_deal

    def taker_order_profit(self, client_sell, client_buy, sell_price, buy_price):
        orderbook_sell, orderbook_buy = self.get_orderbooks(client_sell, client_buy)
        profit = (sell_price - buy_price) / buy_price

        if profit > self.profit_taker + client_sell.taker_fee + client_buy.taker_fee:
            self.potential_deals.append({'buy_exch': client_buy,
                                         "sell_exch": client_sell,
                                         "orderbook_buy": orderbook_buy,
                                         "orderbook_sell": orderbook_sell,
                                         'max_deal_size': self.available_balances[
                                             f"+{client_buy.EXCHANGE_NAME}-{client_sell.EXCHANGE_NAME}"],
                                         "profit": profit})

    async def execute_deal(self, client_buy, client_sell, orderbook_buy, time_start, time_parser, time_choose):
        max_deal_size = self.available_balances[f"+{client_buy.EXCHANGE_NAME}-{client_sell.EXCHANGE_NAME}"]
        self.deals_executed.append([f'+{client_buy.EXCHANGE_NAME}-{client_sell.EXCHANGE_NAME}', max_deal_size])
        max_deal_size = max_deal_size / ((orderbook_buy['asks'][0][0] + orderbook_buy['bids'][0][0]) / 2)
        await self.create_orders(client_buy, client_sell, max_deal_size, time_start, time_parser, time_choose)

    async def create_orders(self, client_buy, client_sell, max_deal_size, time_start, time_parser, time_choose):
        orderbook_sell, orderbook_buy = self.get_orderbooks(client_sell, client_buy)
        expect_buy_px = orderbook_buy['asks'][0][0]
        expect_sell_px = orderbook_sell['bids'][0][0]
        shift = self.shifts[client_sell.EXCHANGE_NAME + ' ' + client_buy.EXCHANGE_NAME] / 2
        price_buy = orderbook_buy['asks'][0][0]
        price_sell = orderbook_sell['bids'][0][0]

        max_buy_vol = orderbook_buy['asks'][0][1]
        max_sell_vol = orderbook_sell['bids'][0][1]

        price_buy_limit_taker = price_buy * self.shifts['TAKER']
        price_sell_limit_taker = price_sell / self.shifts['TAKER']
        timer = time.time() * 1000
        arbitrage_possibilities_id = uuid.uuid4()

        print('CREATE ORDER', f'{max_deal_size=}', f'{price_buy_limit_taker=}')

        responses = await asyncio.gather(*[
            self.loop_1.create_task(
                client_buy.create_order(max_deal_size, price_buy_limit_taker, 'buy', self.session)),
            self.loop_1.create_task(
                client_sell.create_order(max_deal_size, price_sell_limit_taker, 'sell', self.session))
        ], return_exceptions=True)
        print(responses)
        print(f"FULL POOL ADDING AND CALLING TIME: {time.time() * 1000 - timer}")

        deal_time = time.time() - time_start - time_parser - time_choose
        self.save_orders(client_buy, 'buy', arbitrage_possibilities_id, deal_time)
        self.save_orders(client_sell, 'sell', arbitrage_possibilities_id, deal_time)
        self.save_arbitrage_possibilities(arbitrage_possibilities_id, client_buy, client_sell, max_buy_vol,
                                          max_sell_vol, expect_buy_px, expect_sell_px, time_parser,
                                          time_choose, shift)

        await asyncio.sleep(self.deal_pause)

    def save_arbitrage_possibilities(self, _id, client_buy, client_sell, max_buy_vol, max_sell_vol, expect_buy_px,
                                     expect_sell_px, time_parser, time_choose, shift):
        expect_profit_usd = (expect_sell_px - expect_buy_px) * client_buy.expect_amount_coin - (
                client_buy.taker_fee + client_sell.taker_fee)
        expect_amount_usd = client_buy.expect_amount_coin * (expect_sell_px + expect_buy_px) / 2
        message = {
            'id': _id,
            'datetime': datetime.datetime.utcnow(),
            'ts': time.time(),
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
            'shift': shift,
            'status': 'Processing'
        }

        self.tasks.put({
            'message': message,
            'routing_key': RabbitMqQueues.ARBITRAGE_POSSIBILITIES,
            'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.ARBITRAGE_POSSIBILITIES),
            'queue_name': RabbitMqQueues.ARBITRAGE_POSSIBILITIES
        })

    def save_orders(self, client, side, parent_id, order_place_time) -> None:
        message = {
            'id': uuid.uuid4(),
            'datetime': datetime.datetime.utcnow(),
            'ts': time.time(),
            'context': 'bot',
            'parent_id': parent_id,
            'exchange_order_id': client.LAST_ORDER_ID,
            'type': 'GTT' if client.EXCHANGE_NAME == 'DYDX' else 'GTC',
            'status': 'Processing',
            'exchange': client.EXCHANGE_NAME,
            'side': side,
            'symbol': client.symbol,
            'expect_price': client.expect_price,
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

    @staticmethod
    def get_orderbooks(client_sell, client_buy):
        time_start = time.time()
        while True:
            try:
                orderbook_sell = client_sell.get_orderbook()[client_sell.symbol]
                orderbook_buy = client_buy.get_orderbook()[client_buy.symbol]
                if orderbook_sell['timestamp'] > 10 * orderbook_buy['timestamp']:
                    orderbook_sell['timestamp'] = orderbook_sell['timestamp'] / 1000
                elif orderbook_buy['timestamp'] > 10 * orderbook_sell['timestamp']:
                    orderbook_buy['timestamp'] = orderbook_buy['timestamp'] / 1000
                func_time = time.time() - time_start
                # if func_time > 0.001:
                #     print(f"GET ORDERBOOKS FUNC TIME: {func_time} sec")
                return orderbook_sell, orderbook_buy
            except Exception as e:
                print(f"Exception with orderbooks: {e}")

    async def start_message(self):
        coin = self.client_1.symbol.split('USD')[0].replace('-', '').replace('/', '')
        message = f'MULTIBOT STARTED\n{self.client_1.EXCHANGE_NAME} | {self.client_2.EXCHANGE_NAME}\n'
        message += f"COIN: {coin}\n"
        message += f"ENV: {self.env}\n"
        message += f"STATE: {Config.STATE}\n"
        message += f"LEVERAGE: {Config.LEVERAGE}\n"
        message += f"EXCHANGES: {self.client_1.EXCHANGE_NAME} {self.client_2.EXCHANGE_NAME}\n"
        message += f"DEALS_PAUSE: {Config.DEALS_PAUSE}\n"
        message += f"ORDER_SIZE: {Config.ORDER_SIZE}\n"
        message += f"TARGET_PROFIT: {Config.TARGET_PROFIT}\n"
        message += f"START BALANCE: {self.start}\n"
        message += f"CURRENT BALANCE: {self.finish}\n"

        for exchange, shift in self.shifts.items():
            message += f"{exchange}: {round(shift, 6)}\n"

        await self.send_message(message, Config.TELEGRAM_CHAT_ID, Config.TELEGRAM_TOKEN)

    def create_result_message(self, deals_potential: dict, deals_executed: dict, time: int) -> str:
        message = f"For last 3 min\n"
        message += f'ENV: {Config.ENV}\n'

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

            deals_potential = {'SELL': {x: 0 for x in self.exchanges}, 'BUY': {x: 0 for x in self.exchanges}}
            deals_executed = {'SELL': {x: 0 for x in self.exchanges}, 'BUY': {x: 0 for x in self.exchanges}}

            deals_potential['SELL'][sell_client.EXCHANGE_NAME] += len(self.deals_counter)
            deals_potential['BUY'][buy_client.EXCHANGE_NAME] += len(self.deals_counter)

            deals_executed['SELL'][sell_client.EXCHANGE_NAME] += len(self.deals_executed)
            deals_executed['BUY'][buy_client.EXCHANGE_NAME] += len(self.deals_executed)

            self.deals_counter = []
            self.deals_executed = []

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
        self.db = await asyncpg.create_pool(**Config.POSTGRES)

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
                    'timestamp': int(round(time.time() * 1000)),
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
        for r in await get_total_balance(cursor, asc_desc):
            if not r['exchange_name'] in exchanges:
                result += r['total_balance']
                exchanges.append(r['exchange_name'])
                time_ = max(time_, r['ts'])

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
                message += f"ENV: {Config.ENV}\n"
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

        await self.send_message(message, Config.TELEGRAM_CHAT_ID, Config.TELEGRAM_TOKEN)

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

        await self.send_message(message, Config.ALERT_CHAT_ID, Config.ALERT_BOT_TOKEN)

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

    async def __start(self):
        while not self.shifts.get(self.client_1.EXCHANGE_NAME + ' ' + self.client_2.EXCHANGE_NAME):
            print('Wait shifts for', self.client_1.EXCHANGE_NAME + ' ' + self.client_2.EXCHANGE_NAME)
            self.__prepare_shifts()

        await self.setup_postgres()

        async with aiohttp.ClientSession() as session:
            self.session = session
            time.sleep(3)
            start_message = False

            while True:
                if self.state == BotState.PARSER:
                    time.sleep(1)

                if self.state == BotState.BOT and Config.STOP_PERCENT < await self.get_balance_percent():
                    self.state = BotState.PARSER

                    if self.__check_env():
                        self.state = BotState.BOT

                    await self.save_new_balance_jump()
                    await self.prepare_alert()

                if not start_message:
                    await self.start_message()
                    await self.start_balance_message()
                    start_message = True

                await self.find_price_diffs()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c1', nargs='?', const=True, default='dydx', dest='client_1')
    parser.add_argument('-c2', nargs='?', const=True, default='binance', dest='client_2')
    args = parser.parse_args()

    MultiBot(args.client_1, args.client_2)
