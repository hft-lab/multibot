import asyncio
import configparser
import json
import logging
import sys
import threading
import time
import traceback
import uuid
import math
from datetime import datetime
from logging.config import dictConfig
from typing import List
import random

import aiohttp

from arbitrage_finder import ArbitrageFinder, AP
from clients.core.all_clients import ALL_CLIENTS
from clients_markets_data import Clients_markets_data
from core.database import DB
from core.rabbit import Rabbit
from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular, try_exc_async
# from logger import Logging

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

dictConfig({'version': 1, 'disable_existing_loggers': False, 'formatters': {
    'simple': {'format': '[%(asctime)s][%(threadName)s] %(funcName)s: %(message)s'}},
            'handlers': {'console': {'class': 'logging.StreamHandler', 'level': 'DEBUG', 'formatter': 'simple',
                                     'stream': 'ext://sys.stdout'}},
            'loggers': {'': {'handlers': ['console'], 'level': 'INFO', 'propagate': False}}})
logger = logging.getLogger(__name__)

leverage = float(config['SETTINGS']['LEVERAGE'])

init_time = time.time()


class MultiBot:
    __slots__ = ['deal_pause', 'cycle_parser_delay', 'max_order_size_usd', 'chosen_deal', 'profit_taker', 'shifts',
                 'rabbit', 'telegram', 'start_time', 'trade_exceptions', 'close_only_exchanges',
                 'available_balances', 'positions', 'session', 'clients', 'exchanges', 'env', 'db', 'tasks',
                 'loop_1', 'loop_2', 'loop_3', 'last_orderbooks', 'time_start', 'time_parser', 'bot_launch_id',
                 'base_launch_config', 'instance_markets_amount', 'markets_data',
                 'launch_fields', 'setts', 'rates_file_name', 'markets', 'clients_markets_data', 'finder',
                 'clients_with_names', 'max_position_part', 'profit_close', 'potential_deals', 'found']

    def __init__(self):
        self.bot_launch_id = uuid.uuid4()
        self.db = None
        self.found = asyncio.Event()
        self.setts = config['SETTINGS']
        self.cycle_parser_delay = float(self.setts['CYCLE_PARSER_DELAY'])
        self.env = self.setts['ENV']
        self.trade_exceptions = {}
        self.close_only_exchanges = []
        self.instance_markets_amount = int(config['SETTINGS']['INSTANCE_MARKETS_AMOUNT'])

        self.launch_fields = ['env', 'target_profit', 'fee_exchange_1', 'fee_exchange_2', 'shift', 'orders_delay',
                              'max_order_usd', 'max_leverage', 'shift_use_flag']
        with open(f'rates.txt', 'a') as file:
            file.write('')

        # self.create_csv('extra_countings.csv')
        # self.last_orderbooks = {}

        # ORDER CONFIGS
        self.deal_pause = float(self.setts['DEALS_PAUSE'])
        self.max_order_size_usd = int(self.setts['ORDER_SIZE'])
        self.profit_taker = float(self.setts['TARGET_PROFIT'])
        self.profit_close = float(self.setts['CLOSE_PROFIT'])
        self.max_position_part = float(self.setts['PERCENT_PER_MARKET'])
        # self.shifts = {}

        # CLIENTS
        self.exchanges = self.setts['EXCHANGES'].split(',')
        self.clients = []

        for exchange in self.exchanges:
            client = ALL_CLIENTS[exchange](keys=config[exchange], leverage=leverage,
                                           max_pos_part=self.max_position_part)
            self.clients.append(client)
        self.clients_with_names = {}
        for client in self.clients:
            self.clients_with_names.update({client.EXCHANGE_NAME: client})

        self.start_time = datetime.utcnow().timestamp()
        self.available_balances = {}
        self.positions = {}
        self.session = None
        self.potential_deals = []
        # all_ribs = set([x.EXCHANGE_NAME + ' ' + y.EXCHANGE_NAME for x, y in self.ribs])
        # while not all_ribs <= set(self.shifts):
        #     print('Wait shifts for', all_ribs - set(self.shifts))
        #     self.__prepare_shifts()

        # NEW REAL MULTI BOT
        self.clients_markets_data = Clients_markets_data(self.clients, self.setts['INSTANCE_NUM'],self.instance_markets_amount)
        self.markets = self.clients_markets_data.get_instance_markets()
        self.markets_data = self.clients_markets_data.get_clients_data()
        self.finder = ArbitrageFinder(self.markets, self.clients_with_names, self.profit_taker, self.profit_close, self)
        # close_markets = ['ETH', 'RUNE', 'SNX', 'ENJ', 'DOT', 'LINK', 'ETC', 'DASH', 'XLM', 'WAVES']
        self.chosen_deal: AP
        for client in self.clients:
            client.markets_list = list(self.markets.keys())
            client.finder = self.finder
            # client.markets_list = close_markets
            client.run_updater()

        self.finder.target_profit_exceptions(self.get_data_for_parser())
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
        self.rabbit = Rabbit(self.loop_3)

        t1 = threading.Thread(target=self.run_await_in_thread, args=[self.__check_order_status, self.loop_1])
        t2 = threading.Thread(target=self.run_await_in_thread, args=[self.websocket_main_cycle, self.loop_2])
        t3 = threading.Thread(target=self.run_await_in_thread, args=[self.rabbit.send_messages, self.loop_3])

        t1.start()
        t2.start()
        t3.start()

        t1.join()
        t2.join()
        t3.join()

    @staticmethod
    @try_exc_regular
    def run_await_in_thread(func, loop):
        loop.run_until_complete(func())

    @try_exc_async
    async def create_session_keep_alive_orders(self):
        orders = []
        cancels = []
        for client in self.clients:
            market = client.markets[client.markets_list[0]]
            client.amount = client.instruments[market]['min_size']
            client.price = client.get_orderbook(market)['bids'][-1][0]
            orders.append(self.loop_2.create_task(client.create_order(market, 'buy', self.session)))
        responses = await asyncio.gather(*orders, return_exceptions=True)
        for resp in responses:
            client = self.clients_with_names[resp['exchange_name']]
            market = client.markets[client.markets_list[0]]
            cancels.append(self.loop_2.create_task(client.cancel_order(market,
                                                                       resp['exchange_order_id'],
                                                                       self.session)))
        cancel_responses = await asyncio.gather(*cancels, return_exceptions=True)
        print(cancel_responses)
        print()
        print()

    @try_exc_async
    async def session_keep_alive(self):
        time_start = time.time()
        while True:
            await asyncio.sleep(0.001)
            start = time.time()
            if not int(start - time_start) % 25:
                await self.create_session_keep_alive_orders()
                print(f"Session keep-alive time: {time.time() - start} sec")
                await asyncio.sleep(23.5)

    @try_exc_async
    async def websocket_main_cycle(self):
        await self.launch()
        async with aiohttp.ClientSession() as session:
            self.session = session
            self.session.headers.update({'Connection': 'keep-alive'})
            # await self.loop_2.create_task(self.session_keep_alive())
            while True:
                if not self.found:
                    await asyncio.sleep(0.000007)
                    continue
                self.found = False
                # await asyncio.sleep(self.cycle_parser_delay)
                # if not round(datetime.utcnow().timestamp() - self.start_time) % 90:
                #     self.start_time -= 1
                #     self.telegram.send_message(f"PARSER IS WORKING", TG_Groups.MainGroup)
                #     print('PARSER IS WORKING')
                #     self.update_all_av_balances()
                # # Шаг 1 (Сбор данных с бирж по рынкам)
                # time_start_parsing = time.time()
                # results = self.get_data_for_parser()
                # time_end_parsing = time.time()
                #
                # # logger_custom.log_rates(iteration, results)
                #
                # # Шаг 2 (Анализ маркет данных с бирж и поиск потенциальных AP)
                # potential_deals = self.finder.find_arbitrage_possibilities(results)
                #
                # # print('Potential deals:', json.dumps(potential_deals, indent=2))
                time_end_define_potential_deals = time.time()


                # if self.potential_deals:
                    # Шаг 3 (Выбор лучшей AP, если их несколько)
                self.chosen_deal: AP = self.choose_deal(self.potential_deals)
                self.potential_deals = []
                if self.chosen_deal:
                    # time_end_choose = time.time()
                    # self.chosen_deal.ts_define_potential_deals_end = time_end_define_potential_deals
                    self.chosen_deal.ts_choose_end = time.time()
                    # self.chosen_deal.time_parser = time_end_parsing - time_start_parsing
                    # self.chosen_deal.time_define_potential_deals = time_end_define_potential_deals - time_end_parsing
                    self.chosen_deal.time_choose = self.chosen_deal.ts_choose_end - time_end_define_potential_deals
                    # Шаг 4 (Проверка, что выбранная AP все еще действует, здесь заново запрашиваем OB)
                    # if self.check_prices_still_good():
                        # Шаг 5. Расчет размеров сделки и цен для лимитных ордеров
                    if self.fit_sizes_and_prices():
                        # time_end_fit_sizes = time.time()
                        # Шаг 6 (Отправка ордеров на исполнение и получение результатов)
                        await self.execute_deal()
                        # result = await self.execute_deal()
                        # if result == 'DEPRECATED':
                        #     continue
                        # Шаг 7 (Анализ, логирование, нотификация по ордерам
                        await self.notification_and_logging()
                        # Удалить обнуление Last Order ID, когда разберусь с ним
                        for client in [self.chosen_deal.client_buy, self.chosen_deal.client_sell]:
                            client.error_info = None
                            client.LAST_ORDER_ID = 'default'
                        await asyncio.sleep(self.deal_pause)
                        self.update_all_av_balances()
                        self.found = False
                        self.potential_deals = []

                            # with open('ap_still_active_status.csv', 'a', newline='') as file:
                            #     writer = csv.writer(file)
                            #     row_data = [str(y) for y in chosen_deal.values()] + ['inactive clients-http']
                            #     writer.writerow(row_data)
    @try_exc_async
    async def launch(self):
        self.db = DB(self.rabbit)
        await self.db.setup_postgres()
        self.update_all_av_balances()
        self.update_all_positions_aggregates()
        # Принтим показатели клиентов - справочно
        print('CLIENTS MARKET DATA:')
        for exchange, exchange_data in self.markets_data.items():
            print(exchange, exchange_data['instance_markets_amt'])
        print('PARSER STARTED')
        # print(json.dumps(self.available_balances, indent=2))
        # print(json.dumps(self.positions, indent=2))

        # self.add_exceptions_on_bot_launch()
        self.telegram.send_bot_launch_message(self, TG_Groups.MainGroup)
        self.telegram.send_start_balance_message(self, TG_Groups.MainGroup)

        # print('block1: Available_balances')
        # print('Available_balances', json.dumps(self.available_balances, indent=2))
        #
        # print('block2: Positions')
        # print('Positions', json.dumps(self.positions, indent=2))
        #
        self.db.save_launch_balance(self)
        # while not init_time + 90 > time.time():
        #     await asyncio.sleep(0.1)
        # logger_custom = Logging()
        # logger_custom.log_launch_params(self.clients-http)

    @try_exc_regular
    def get_data_for_parser(self):
        data = dict()
        for client in self.clients:
            data.update(client.get_all_tops())
        return data

    @try_exc_regular
    def choose_deal(self, potential_deals: List[AP]) -> AP:
        max_profit = None
        chosen_deal = None
        for deal in potential_deals:
            # if self.trade_exceptions.get(deal.buy_exchange + deal.buy_market + 'buy'):
            #     continue
            # if self.trade_exceptions.get(deal.sell_exchange + deal.sell_market + 'sell'):
            #     continue
            if not max_profit:
                max_profit = deal.profit_rel_parser
                chosen_deal = deal
            if deal.profit_rel_parser > max_profit:
                max_profit = deal.profit_rel_parser
                chosen_deal = deal
        return chosen_deal

    @try_exc_regular
    def check_min_size(self, exchange, market, deal_avail_size_usd,
                                              price, direction, context, send_flag: bool = True):
        min_size_amount = self.clients_with_names[exchange].instruments[market]['min_size']
        min_size_usd = min_size_amount * price
        if deal_avail_size_usd < min_size_usd:
            # message = f"Context: {context}. Доступный баланс: {int(round(deal_avail_size_usd))} недостаточен для совершения сделки. "
            # if deal_avail_size_usd > 0:
            #     message += f"Конкретный рынок. Доступный баланс есть, но меньше минимального размера ордера {min_size_usd}"
            # if deal_avail_size_usd == 0:
            #     message += "Биржа в целом. Режим Close Only"
            # if deal_avail_size_usd < 0:
            #     message += "Конкретный рынок. Close Only"
            # self.add_trade_exception(exchange, market, direction, message, send_flag)
            return False
        else:
            return True

    @try_exc_regular
    def get_close_only_exchanges(self):
        close_only_exchanges = []
        for exchange, available_balances in self.available_balances.items():
            if available_balances['buy'] == 0:
                close_only_exchanges.append(exchange)
        return close_only_exchanges

    @try_exc_regular
    def add_exceptions_on_bot_launch(self):
        for exchange, positions_details in self.positions.items():
            for market in positions_details['markets']:
                price = abs(self.positions[exchange]['position_details'][market]['amount_usd'] / \
                            self.positions[exchange]['position_details'][market]['amount'])

                deal_avail_size_buy_usd = self.available_balances[exchange][market]['buy']
                deal_avail_size_sell_usd = self.available_balances[exchange][market]['sell']
                self.check_min_size(exchange, market, deal_avail_size_buy_usd,
                                                           price, 'buy', context='Bot launch', send_flag=False)
                self.check_min_size(exchange, market, deal_avail_size_sell_usd,
                                                           price, 'sell', context='Bot launch', send_flag=False)
                min_size_amount = self.clients_with_names[exchange].instruments[market]['min_size']
                min_size_usd = min_size_amount * price

                if self.max_order_size_usd < min_size_usd:
                    message = f"Максимальный размер ордера: {self.max_order_size_usd} " \
                              f"меньше минимального размера ордера для рынка: {int(round(min_size_usd))}"
                    self.add_trade_exception(exchange, market, 'buy', message, send_flag=False)
                    self.add_trade_exception(exchange, market, 'sell', message, send_flag=False)
        self.close_only_exchanges = self.get_close_only_exchanges()
        # Костыль под DYDX
        self.close_only_exchanges.append('DYDX')
        for exchange, positions_details in self.positions.items():
            if exchange == 'DYDX':
                for market in positions_details['markets']:
                    side = self.positions[exchange]['position_details'][market]['side']
                    message = f"Костыльно отключаем Open направления торговли"
                    if side == 'LONG':
                        self.add_trade_exception(exchange, market, 'buy', message, send_flag=False)
                    if side == 'SHORT':
                        self.add_trade_exception(exchange, market, 'sell', message, send_flag=False)

        exception_count = {}
        for exception in self.trade_exceptions:
            exchange = self.trade_exceptions[exception]['exchange']
            exception_count[exchange] = exception_count.get(exchange, 0) + 1

        message = f'ALERT: Bot launch. Exception Added\n' \
                  f'Биржи торгующиеся только на закрытие позиций: {self.close_only_exchanges}\n ' \
                  f'Количество добавленных рынков исключений:\n' \
                  f'{json.dumps(exception_count,indent=2)}'
        self.telegram.send_message(message, TG_Groups.Alerts)

    @try_exc_regular
    def add_trade_exception(self, exchange, market, direction, reason, send_flag: bool = True) -> None:
        if self.trade_exceptions.get(exchange+market+direction):
            return
        exception = {exchange+market+direction:
                         {'market': market, 'exchange': exchange, 'direction': direction,
                          'reason': reason, 'Time added': str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))}}
        self.trade_exceptions.update(exception)
        if send_flag:
            print(f'ALERT: Exception Added\n{json.dumps(exception, indent=2, ensure_ascii=False)}')
            self.telegram.send_message((f'ALERT: Bot work. Exception Added\n'
                                        f'{json.dumps(exception, indent=2, ensure_ascii=False)}'), TG_Groups.Alerts)

    @try_exc_regular
    def check_prices_still_good(self):
        buy_market, sell_market = self.chosen_deal.buy_market, self.chosen_deal.sell_market
        ob_buy = self.chosen_deal.client_buy.get_orderbook(buy_market)
        ob_sell = self.chosen_deal.client_sell.get_orderbook(sell_market)

        self.chosen_deal.ob_buy = {key: value[:5] if key in ['asks', 'bids'] else value for key, value in
                                   ob_buy.items()}
        self.chosen_deal.ob_sell = {key: value[:5] if key in ['asks', 'bids'] else value for key, value in
                                    ob_sell.items()}

        buy_price, sell_price = self.chosen_deal.ob_buy['asks'][0][0], self.chosen_deal.ob_sell['bids'][0][0]
        profit_brutto = (sell_price - buy_price) / buy_price
        profit = profit_brutto - self.chosen_deal.buy_fee - self.chosen_deal.sell_fee

        # Для контрактов работает не идеально, жить не мешает, но логируется скорее всего некорректно

        self.chosen_deal.buy_max_amount_ob = self.chosen_deal.ob_buy['asks'][0][1]
        self.chosen_deal.sell_max_amount_ob = self.chosen_deal.ob_sell['bids'][0][1]
        self.chosen_deal.buy_price_target = buy_price
        self.chosen_deal.sell_price_target = sell_price

        self.chosen_deal.deal_max_amount_ob = min(self.chosen_deal.buy_max_amount_ob,
                                                  self.chosen_deal.sell_max_amount_ob)
        self.chosen_deal.deal_max_usd_ob = self.chosen_deal.deal_max_amount_ob * (buy_price + sell_price) / 2

        self.chosen_deal.profit_rel_target = profit
        self.chosen_deal.ts_check_still_good_end = time.time()
        self.chosen_deal.time_check_ob = self.chosen_deal.ts_check_still_good_end - self.chosen_deal.ts_choose_end

        if profit >= self.chosen_deal.target_profit:
            return True
        else:
            self.telegram.send_ap_expired_message(self.chosen_deal, TG_Groups.Alerts)
            return False

    @try_exc_regular
    def _get_available_balance(self, exchange, market, direction):
        if self.available_balances.get(exchange):
            if self.available_balances[exchange].get(market):
                avail_size = self.available_balances[exchange][market][direction]
            else:
                avail_size = self.available_balances[exchange][direction]
            return avail_size
        else:
            return 'updating'

    @try_exc_regular
    def get_shifted_price_for_order(self, ob, direction):
        point_price = abs(ob[direction][1][0] - ob[direction][0][0]) / ob[direction][0][0]
        if point_price > 0.2 * self.profit_taker:
            shifted_price = ob[direction][0][0]
        else:
            shifted_price = ob[direction][4][0]
        return shifted_price

    @try_exc_regular
    def if_tradable(self, buy_ex, sell_ex, buy_mrkt, sell_mrkt, buy_px, sell_px):
        avl_sz_buy_usd = self._get_available_balance(buy_ex, buy_mrkt, 'buy')
        avl_sz_sell_usd = self._get_available_balance(sell_ex, sell_mrkt, 'sell')
        if avl_sz_buy_usd == 'updating' or avl_sz_sell_usd == 'updating':
            return False
        max_deal_size_usd = min(avl_sz_buy_usd, avl_sz_sell_usd, self.max_order_size_usd)
        if not self.check_min_size(buy_ex, buy_mrkt, avl_sz_buy_usd, buy_px, 'buy', 'Bot work'):
            return False
        if not self.check_min_size(sell_ex, sell_mrkt, avl_sz_sell_usd, sell_px, 'sell', 'Bot work'):
            return False
        return max_deal_size_usd

    @try_exc_regular
    def fit_sizes_and_prices(self):
        deal_size_amount = self.chosen_deal.deal_size_usd_target / self.chosen_deal.ob_buy['asks'][0][0]
        step_size = max(self.chosen_deal.client_buy.instruments[self.chosen_deal.buy_market]['step_size'],
                        self.chosen_deal.client_sell.instruments[self.chosen_deal.sell_market]['step_size'])
        rounded_deal_size_amount = math.floor(deal_size_amount / step_size) * step_size
        # Округления до нуля произойти не может, потому, что deal_size_amount заведомо >= step_size
        self.chosen_deal.client_buy.amount = rounded_deal_size_amount
        self.chosen_deal.client_sell.amount = rounded_deal_size_amount
        buy_price_shifted = self.chosen_deal.ob_buy['asks'][1][0]
        sell_price_shifted = self.chosen_deal.ob_sell['bids'][1][0]
        self.chosen_deal.sell_price_shifted = sell_price_shifted
        self.chosen_deal.buy_price_shifted = buy_price_shifted
        # Здесь происходит уточнение и финализации размеров ордеров и их цен на клиентах
        self.chosen_deal.client_buy.fit_sizes(buy_price_shifted, self.chosen_deal.buy_market)
        self.chosen_deal.client_sell.fit_sizes(sell_price_shifted, self.chosen_deal.sell_market)
        # Сохраняем значения на объект AP. Именно по ним будет происходить попытка исполнения ордеров
        if not self.chosen_deal.client_buy.amount or not self.chosen_deal.client_sell.amount:
            self.telegram.send_message(f'STOP2.{rounded_deal_size_amount=}')
            return False
        # По логике округления, amount на клиентах изменяться в fit_sizes не должны, но контрольно проверим
        # if (client_sell.amount != client_buy.amount) or abs(client_buy.amount - rounded_deal_size_amount) > 1e-9:
        #     print(
        #         f'{rounded_deal_size_amount=},{deal_size_amount=},{step_size=},{math.floor(deal_size_amount / step_size)=}')
        #     message = self.telegram.send_different_amounts_alert(deal, rounded_deal_size_amount, TG_Groups.Alerts)
        #     print(message)
        #     return False
        return True

    @try_exc_async
    async def execute_deal(self):
        # with open('ap_still_active_status.csv', 'a', newline='') as file:
        #     writer = csv.writer(file)
        #     row_data = [str(y) for y in chosen_deal.values()] + ['Active']
        #     writer.writerow(row_data)

        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        cl_id_buy, cl_id_sell = (f"api_deal_{id1.replace('-', '')[:20]}",
                                 f"api_deal_{id2.replace('-', '')[:20]}")
        self.chosen_deal.ts_orders_sent = time.time()
        orders = [self.loop_2.create_task(self.chosen_deal.client_buy.create_order(self.chosen_deal.buy_market,
                                                                                   'buy', self.session,
                                                                                   client_id=cl_id_buy)),
                  self.loop_2.create_task(self.chosen_deal.client_sell.create_order(self.chosen_deal.sell_market,
                                                                                    'sell', self.session,
                                                                                    client_id=cl_id_sell))]
        responses = await asyncio.gather(*orders, return_exceptions=True)
        self.chosen_deal.deal_size_amount_target = self.chosen_deal.client_buy.amount
        self.chosen_deal.profit_usd_target = self.chosen_deal.profit_rel_target * self.chosen_deal.deal_size_usd_target
        self.chosen_deal.buy_price_fitted = self.chosen_deal.client_buy.price
        self.chosen_deal.sell_price_fitted = self.chosen_deal.client_sell.price
        self.chosen_deal.buy_amount_target = self.chosen_deal.client_buy.amount
        self.chosen_deal.sell_amount_target = self.chosen_deal.client_sell.amount
        self.chosen_deal.buy_order_id, self.chosen_deal.sell_order_id = id1, id2
        self.chosen_deal.ts_orders_responses_received = time.time()
        self.chosen_deal.buy_order_place_time = (responses[0]['timestamp'] - self.chosen_deal.ts_orders_sent) / 1000
        self.chosen_deal.sell_order_place_time = (responses[1]['timestamp'] - self.chosen_deal.ts_orders_sent) / 1000
        self.chosen_deal.buy_order_id_exchange = responses[0]['exchange_order_id']
        self.chosen_deal.sell_order_id_exchange = responses[1]['exchange_order_id']
        self.chosen_deal.buy_order_status = responses[0]['status']
        self.chosen_deal.sell_order_status = responses[1]['status']
        message = f"Results of create_order requests:\n" \
                  f"{self.chosen_deal.buy_exchange=}\n" \
                  f"{self.chosen_deal.buy_market=}\n" \
                  f"{self.chosen_deal.sell_exchange=}\n" \
                  f"{self.chosen_deal.sell_market=}\n" \
                  f"Responses:\n{json.dumps(responses, indent=2)}"
        print(message)
        self.send_timings()
        self.telegram.send_message(message, TG_Groups.DebugDima)

    @staticmethod
    @try_exc_regular
    def count_pings(deal):
        if '.' in str(deal.ob_buy['timestamp']):
            ts_buy = deal.ts_orders_sent - deal.ob_buy['timestamp']
        else:
            ts_buy = deal.ts_orders_sent - deal.ob_buy['timestamp'] / 1000
        if '.' in str(deal.ob_sell['timestamp']):
            ts_sell = deal.ts_orders_sent - deal.ob_sell['timestamp']
        else:
            ts_sell = deal.ts_orders_sent - deal.ob_sell['timestamp'] / 1000
        # print(f"BUY OB AGE (OB TS):\n{ts_buy}")
        # print(f"SELL OB AGE (OB TS):\n{ts_sell}")
        buy_own_ts = deal.ts_orders_sent - deal.ob_buy['ts_ms']
        sell_own_ts = deal.ts_orders_sent - deal.ob_sell['ts_ms']
        return ts_buy, ts_sell, buy_own_ts, sell_own_ts

    @try_exc_regular
    def send_timings(self):
        # print()
        # print()
        # print(f"BUY EXCH: {self.chosen_deal.buy_exchange}")
        # print(f"SELL EXCH: {self.chosen_deal.sell_exchange}")
        # print(f"BUY PRICE: {self.chosen_deal.buy_price_target}")
        # print(f"SELL PRICE: {self.chosen_deal.sell_price_target}")
        # print(f"BUY OB:\n{self.chosen_deal.ob_buy}")
        # print(f"SELL OB:\n{self.chosen_deal.ob_sell}")
        # print(f"TIMESTAMP NOW: {time.time()}")
        ts_buy, ts_sell, buy_own_ts, sell_own_ts = self.count_pings(self.chosen_deal)
        # print(f"BUY OB AGE (OWN TS):\n{buy_own_ts}")
        # print(f"SELL OB AGE (OWN TS):\n{sell_own_ts}")
        # print()
        # print()
        message = f'{self.chosen_deal.coin} DEAL TIMINGS:\n'
        message += f'{self.chosen_deal.sell_exchange} SELL OB:\n'
        message += f"OB AGE BY OB TS: {round(ts_sell, 5)} sec\n"
        message += f"OB AGE BY OWN TS: {round(sell_own_ts, 5)} sec\n"
        message += f'{self.chosen_deal.buy_exchange} BUY OB:\n'
        message += f"OB AGE BY OB TS: {round(ts_buy, 5)} sec\n"
        message += f"OB AGE BY OWN TS: {round(buy_own_ts, 5)} sec\n"
        countings = self.chosen_deal.ts_orders_sent - self.chosen_deal.start_processing
        message += f"TIME FROM START COUNTING: {round(countings, 5)} sec\n"
        orders_sendings = self.chosen_deal.ts_orders_responses_received - self.chosen_deal.ts_orders_sent
        message += f"ORDERS SENDING TIME: {round(orders_sendings, 5)} sec\n"
        self.telegram.send_message(message, TG_Groups.MainGroup)

    @try_exc_async
    async def notification_and_logging(self):

        ap_id = self.chosen_deal.ap_id
        client_buy, client_sell = self.chosen_deal.client_buy, self.chosen_deal.client_sell
        shifted_buy_px, shifted_sell_px = self.chosen_deal.buy_price_fitted, self.chosen_deal.sell_price_fitted
        buy_market, sell_market = self.chosen_deal.buy_market, self.chosen_deal.sell_market
        order_id_buy, order_id_sell = self.chosen_deal.buy_order_id, self.chosen_deal.sell_order_id
        buy_exchange_order_id, sell_exchange_order_id = self.chosen_deal.buy_order_id_exchange, self.chosen_deal.sell_order_id_exchange
        buy_order_place_time = self.chosen_deal.buy_order_place_time
        sell_order_place_time = self.chosen_deal.sell_order_place_time

        self.db.save_arbitrage_possibilities(self.chosen_deal)
        self.db.save_order(order_id_buy, buy_exchange_order_id, client_buy, 'buy', ap_id, buy_order_place_time,
                           shifted_buy_px, buy_market, self.env)
        self.db.save_order(order_id_sell, sell_exchange_order_id, client_sell, 'sell', ap_id, sell_order_place_time,
                           shifted_sell_px, sell_market, self.env)

        if self.chosen_deal.buy_order_status != 'error':
            # message = f'запрос инфы по ордеру, см. логи{self.chosen_deal.client_buy.EXCHANGE_NAME=}{buy_market=}{order_id_buy=}'
            # self.telegram.send_message(message)
            order_result = self.chosen_deal.client_buy.orders.get(buy_exchange_order_id, None)
            if not order_result:
                order_result = self.chosen_deal.client_buy.get_order_by_id(buy_market, buy_exchange_order_id)
            self.chosen_deal.buy_price_real = order_result['factual_price']
            self.chosen_deal.buy_amount_real = order_result['factual_amount_coin']
            print(f'{order_result=}')
            print('STOP1')

        else:
            self.telegram.send_order_error_message(self.env, buy_market, client_buy, order_id_buy, TG_Groups.Alerts)

        if self.chosen_deal.sell_order_status != 'error':
            # message = f'запрос инфы по ордеру, см. логи{self.chosen_deal.client_sell.EXCHANGE_NAME=}{sell_market=}{order_id_buy=}'
            # self.telegram.send_message(message)
            order_result = self.chosen_deal.client_sell.orders.get(sell_exchange_order_id, None)
            if not order_result:
                order_result = self.chosen_deal.client_sell.get_order_by_id(sell_market, sell_exchange_order_id)
            self.chosen_deal.sell_price_real = order_result['factual_price']
            self.chosen_deal.sell_amount_real = order_result['factual_amount_coin']
            print(f'{order_result=}')
            print('STOP2')

        else:
            self.telegram.send_order_error_message(self.env, sell_market, client_sell, order_id_sell, TG_Groups.Alerts)

        self.db.update_balance_trigger('post-deal', ap_id, self.env)
        self.telegram.send_ap_executed_message(self.chosen_deal, TG_Groups.MainGroup)

    @try_exc_regular
    def update_all_av_balances(self):
        for exchange, client in self.clients_with_names.items():
            self.available_balances.update({exchange: client.get_available_balance()})

    @try_exc_regular
    def update_all_positions_aggregates(self):
        for exchange, client in self.clients_with_names.items():
            markets = []
            total_pos = 0
            abs_pos = 0
            details = {}
            for market, position in client.get_positions().items():
                markets.append(market)
                total_pos += position['amount_usd']
                abs_pos += abs(position['amount_usd'])
                details.update({market: position})
            self.positions.update(
                {exchange: {'balance': int(round(client.get_balance())), 'total_pos': int(round(total_pos)),
                            'abs_pos': int(round(abs_pos)), 'markets': markets, 'position_details': details}})

    # @try_exc_regular
    # def __rates_update(self):
    #     message = ''
    #     with open(f'rates.txt', 'a') as file:
    #         for client in self.clients-http:
    #             message += f"{client.EXCHANGE_NAME} | {client.get_orderbook(client.symbol)['asks'][0][0]} | {datetime.utcnow()} | {time.time()}\n"
    #         file.write(message + '\n')
    #     self.update_all_av_balances()

    # def get_sizes(self):
    #     tick_size = max([x.tick_size for x in self.clients-http if x.tick_size], default=0.01)
    #     step_size = max([x.step_size for x in self.clients-http if x.step_size], default=0.01)
    #     quantity_precision = max([x.quantity_precision for x in self.clients-http if x.quantity_precision])
    #
    #     self.client_1.quantity_precision = quantity_precision
    #     self.client_2.quantity_precision = quantity_precision
    #
    #     self.client_1.tick_size = tick_size
    #     self.client_2.tick_size = tick_size
    #
    #     self.client_1.step_size = step_size
    #     self.client_2.step_size = step_size

    @try_exc_async
    async def __check_order_status(self):
        # Эта функция инициирует обновление данных по ордеру в базе,
        # когда от биржи в клиенте появляется обновление после создания ордера
        while True:
            for client in self.clients:
                orders = client.orders.copy()

                for order_id, message in orders.items():
                    self.rabbit.add_task_to_queue(message, "UPDATE_ORDERS")
                    client.orders.pop(order_id)

            await asyncio.sleep(3)


if __name__ == '__main__':
    MultiBot()
