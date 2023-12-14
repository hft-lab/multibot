import asyncio
import configparser
import json
import logging
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime
from logging.config import dictConfig
from typing import List

import aiohttp

from arbitrage_finder import ArbitrageFinder, AP
from clients.core.all_clients import ALL_CLIENTS
from clients_markets_data import Clients_markets_data
from core.database import DB
from core.rabbit import Rabbit
from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular, try_exc_async
from logger import Logging

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
    __slots__ = ['deal_pause', 'cycle_parser_delay', 'max_order_size', 'chosen_deal', 'profit_taker', 'shifts',
                 'rabbit', 'telegram','state', 'start_time', 'trade_exceptions', 'exchange_exceptions',
                 'available_balances', 'session', 'clients', 'exchanges', 'ribs', 'env', 'db', 'tasks',
                 'loop_1', 'loop_2', 'loop_3', 'need_check_shift',
                 'last_orderbooks', 'time_start', 'time_parser', 'bot_launch_id', 'base_launch_config',
                 'launch_fields', 'setts', 'rates_file_name', 'markets', 'clients_markets_data', 'finder',
                 'clients_with_names', 'max_position_part', 'profit_close']

    def __init__(self):
        self.bot_launch_id = uuid.uuid4()
        self.db = None
        self.setts = config['SETTINGS']
        self.state = self.setts['STATE']
        self.cycle_parser_delay = float(self.setts['CYCLE_PARSER_DELAY'])
        self.env = self.setts['ENV']
        self.trade_exceptions = []
        self.exchange_exceptions = []
        self.launch_fields = ['env', 'target_profit', 'fee_exchange_1', 'fee_exchange_2', 'shift', 'orders_delay',
                              'max_order_usd', 'max_leverage', 'shift_use_flag']
        with open(f'rates.txt', 'a') as file:
            file.write('')

        # self.create_csv('extra_countings.csv')
        # self.last_orderbooks = {}

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

        for exchange in self.exchanges:
            client = ALL_CLIENTS[exchange](keys=config[exchange], leverage=leverage,
                                           max_pos_part=self.max_position_part)
            self.clients.append(client)
        self.clients_with_names = {}
        for client in self.clients:
            self.clients_with_names.update({client.EXCHANGE_NAME: client})

        self.start_time = datetime.utcnow().timestamp()
        self.available_balances = {}
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
        self.chosen_deal: AP
        for client in self.clients:
            client.markets_list = list(self.markets.keys())
            # client.markets_list = close_markets
            client.run_updater()

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
        try:
            loop.run_until_complete(func())
        except:
            traceback.print_exc()
        finally:
            loop.close()

    @try_exc_regular
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

    @try_exc_regular
    def find_position_gap(self):
        position_gap = 0
        for client in self.clients:
            if res := client.get_positions().get(client.symbol):
                position_gap += res['amount']
        return position_gap

    @try_exc_regular
    def find_balancing_elements(self):
        position_gap = self.find_position_gap()
        amount_to_balancing = abs(position_gap) / len(self.clients)
        return position_gap, amount_to_balancing

    # @try_exc_regular
    # def check_last_ob(self, client_buy, client_sell, ob_sell, ob_buy):
    #     exchanges = client_buy.EXCHANGE_NAME + ' ' + client_sell.EXCHANGE_NAME
    #     last_obs = self.last_orderbooks.get(exchanges, None)
    #     self.last_orderbooks.update({exchanges: {'ob_buy': ob_buy['asks'][0][0], 'ob_sell': ob_sell['bids'][0][0]}})
    #     if last_obs:
    #         if ob_buy['asks'][0][0] == last_obs['ob_buy'] and ob_sell['bids'][0][0] == last_obs['ob_sell']:
    #             return False
    #         else:
    #             return True
    #     else:
    #         return True

    @try_exc_async
    async def launch(self):
        self.db = DB(self.rabbit)
        await self.db.setup_postgres()
        self.telegram.send_message(self.telegram.start_message(self), TG_Groups.MainGroup)
        self.telegram.send_message(self.telegram.start_balance_message(self), TG_Groups.MainGroup)

        self.update_all_av_balances()
        # print('Available_balances', json.dumps(self.available_balances, indent=2))
        #
        # for client in self.clients:
        #     print(f"{client.EXCHANGE_NAME}:\n "
        #           f"Balance: {client.get_balance()}\n"
        #           f"Positions: {json.dumps(client.get_positions(), indent=2)}\n")

        self.db.save_launch_balance(self)
        # while not init_time + 90 > time.time():
        #     await asyncio.sleep(0.1)
        logger_custom = Logging()
        logger_custom.log_launch_params(self.clients)

        # Принтим показатели клиентов - справочно
        print('CLIENTS MARKET DATA:')
        print(json.dumps(self.clients_markets_data, indent=2))

    @try_exc_async
    async def websocket_main_cycle(self):
        await self.launch()
        async with aiohttp.ClientSession() as session:
            self.session = session
            while True:
                await asyncio.sleep(self.cycle_parser_delay)
                if not round(datetime.utcnow().timestamp() - self.start_time) % 90:
                    self.start_time -= 1
                    self.telegram.send_message(f"PARSER IS WORKING", TG_Groups.MainGroup)
                    print('PARSER IS WORKING')
                    self.update_all_av_balances()
                time_start_parsing = time.time()
                # Шаг 1 (Сбор данных с бирж по рынкам)
                results = self.get_data_for_parser()
                time_end_parsing = time.time()
                # print('Data for parser:',json.dumps(results, indent=2))

                # results = self.add_status(results)
                # logger_custom.log_rates(iteration, results)

                # Шаг 2 (Анализ маркет данных с бирж и поиск потенциальных AP)
                potential_deals = self.finder.arbitrage_possibilities(results)

                # print('Potential deals:', json.dumps(self.potential_deals, indent=2))
                time_end_define_potential_deals = time.time()

                if len(potential_deals):
                    # Шаг 3 (Выбор лучшей AP, если их несколько)
                    self.chosen_deal: AP = self.choose_deal(potential_deals)
                    if self.chosen_deal:
                        time_end_choose = time.time()
                        self.chosen_deal.time_parser = time_end_parsing - time_start_parsing
                        self.chosen_deal.time_define_potential_deals = time_end_define_potential_deals - time_end_parsing
                        self.chosen_deal.time_choose = time_end_choose - time_end_define_potential_deals
                        # Шаг 4 (Проверка, что выбранная AP все еще действует, здесь заново запрашиваем OB)
                        if self.check_ap_still_good():
                            # Шаг 5. Расчет размеров сделки и цен для лимитных ордеров
                            if self.fit_sizes_and_prices():
                                # Шаг 6. Финальная проверка профита
                                if self.final_profit_check():
                                    if self.state == 'BOT':
                                        # Шаг 7 (Отправка ордеров на исполнение и получение результатов)
                                        await self.execute_deal()
                                        # Шаг 8 (Анализ, логирование, нотификация по ордерам
                                        await self.notification_and_logging()
                                        # Удалить обнуление Last Order ID, когда разберусь с ним
                                        for client in [self.chosen_deal.client_buy, self.chosen_deal.client_sell]:
                                            client.error_info = None
                                            client.LAST_ORDER_ID = 'default'
                                        self.update_all_av_balances()
                                        await asyncio.sleep(self.deal_pause)
                                    if self.state == 'PARSER':
                                        pass

                            # with open('ap_still_active_status.csv', 'a', newline='') as file:
                            #     writer = csv.writer(file)
                            #     row_data = [str(y) for y in chosen_deal.values()] + ['inactive clients']
                            #     writer.writerow(row_data)

    @try_exc_regular
    def get_data_for_parser(self):
        data = dict()
        for client in self.clients:
            data.update(client.get_all_tops())
        return data

    @try_exc_regular
    def choose_deal(self, potential_deals: List[AP]) -> AP:
        max_profit = 0
        chosen_deal = None
        for deal in potential_deals:
            if deal.expect_profit_rel > max_profit:
                if self.check_available_balance_and_exceptions(deal.coin, deal.buy_exchange, deal.sell_exchange):
                    max_profit = deal.expect_profit_rel
                    chosen_deal = deal
        return chosen_deal

    @try_exc_regular
    def check_ap_still_good(self):
        buy_market, sell_market = self.chosen_deal.buy_market, self.chosen_deal.sell_market
        self.chosen_deal.ob_buy = self.chosen_deal.client_buy.get_orderbook(buy_market)
        self.chosen_deal.ob_sell = self.chosen_deal.client_sell.get_orderbook(sell_market)

        profit = (self.chosen_deal.ob_sell['bids'][0][0] - self.chosen_deal.ob_buy['asks'][0][0]) / \
                 self.chosen_deal.ob_buy['asks'][0][0]
        profit = profit - self.chosen_deal.client_buy.taker_fee - self.chosen_deal.client_sell.taker_fee
        if profit >= self.chosen_deal.target_profit:
            self.chosen_deal.expect_profit_rel = profit
            return True
        else:
            message = f'\nDEAL ALREADY EXPIRED\n' \
                      f'EXCH_BUY: {self.chosen_deal.buy_exchange}, EXCH_SELL: {self.chosen_deal.sell_exchange}\n' \
                      f'OLD PRICES: BUY: {self.chosen_deal.buy_price}, SELL: {self.chosen_deal.sell_price}\n' \
                      f'NEW PRICES: BUY: {self.chosen_deal.ob_buy["asks"][0][0]}, SELL: {self.chosen_deal.ob_sell["bids"][0][0]}\n' \
                      f'TIMESTAMP: {int(round(datetime.utcnow().timestamp() * 1000))}\n'
            print(message)
            self.telegram.send_message(message, TG_Groups.Alerts)
            return False

    def fit_sizes_and_prices(self):
        buy_exchange, sell_exchange = self.chosen_deal.buy_exchange, self.chosen_deal.sell_exchange
        client_buy, client_sell = self.chosen_deal.client_buy, self.chosen_deal.client_sell
        buy_market, sell_market = self.chosen_deal.buy_market, self.chosen_deal.sell_market

        # Определяем размер ордеров в крипте
        deal_size_result = self.deal_size_define(buy_exchange, sell_exchange, buy_market, sell_market)
        deal_size_usd = deal_size_result['deal_size']
        if deal_size_usd <= 0:
            # print(f"NOT ENOUGH AVAILABLE BALANCE:"
            #       f"\n {deal_size_result['status']}")
            # ДОБАВИТЬ РЫНКИ В ИСКЛЮЧЕНИЯ
            return False

        # НУЖНО ДОБАВИТЬ ПРОВЕРКУ НА МИНИМАЛЬНЫЙ РАЗМЕР ОРДЕРА
        deal_size_amount = deal_size_usd / self.chosen_deal.ob_buy['asks'][0][0]
        step_size = max(client_buy.instruments[buy_market]['step_size'],
                        client_sell.instruments[sell_market]['step_size'])
        # Важно, чтобы step_size на двух биржах отличались в ЦЕЛОЕ количество раз, иначе итоговые размеры ордеров могут оказаться разными
        size_amount = round(deal_size_amount / step_size) * step_size
        client_buy.amount = size_amount
        client_sell.amount = size_amount

        limit_buy_price, limit_sell_price = self.get_limit_prices_for_order(self.chosen_deal.ob_buy,
                                                                            self.chosen_deal.ob_sell)
        client_buy.fit_sizes(limit_buy_price, buy_market)
        client_sell.fit_sizes(limit_sell_price, sell_market)

        # Сохраняем значения, которые пойдут на исполнение на AP
        self.chosen_deal.limit_buy_px = client_buy.price
        self.chosen_deal.limit_sell_px = client_sell.price
        self.chosen_deal.buy_size = client_buy.amount
        self.chosen_deal.sell_size = client_sell.amount
        self.chosen_deal.deal_size_amount = (client_buy.amount + client_sell.amount) / 2
        self.chosen_deal.deal_size_usd = self.chosen_deal.deal_size_amount * (client_buy.price + client_sell.price) / 2

        return True

    @try_exc_regular
    def final_profit_check(self):
        profit = (self.chosen_deal.limit_sell_px - self.chosen_deal.limit_buy_px) / self.chosen_deal.limit_buy_px
        profit = profit - self.chosen_deal.buy_fee - self.chosen_deal.sell_fee
        if profit >= self.chosen_deal.target_profit:
            return True
        else:
            message = f'MULTIBOT. Final profit check failed. \n' \
                      f'Actual profit: {round(profit,5)}\n' \
                      f'Target profit: {self.chosen_deal.target_profit}'
            print(message)
            self.telegram.send_message(message, TG_Groups.Alerts)
            return False

    @try_exc_async
    async def execute_deal(self):

        # example = {'coin': 'AGLD', 'buy_exchange': 'BINANCE', 'sell_exchange': 'KRAKEN', 'buy_fee': 0.00036,
        #            'sell_fee': 0.0005, 'sell_price': 0.6167, 'buy_price': 0.6158, 'sell_size': 1207.0,
        #            'buy_size': 1639.0, 'deal_size_coin': 1207.0, 'deal_size_usd': 744.3569,
        #            'expect_profit_rel': 0.0006, 'expect_profit_abs_usd': 0.448, 'buy_market': 'AGLDUSDT',
        #            'sell_market': 'pf_agldusd',
        #            'datetime': datetime(2023, 10, 2, 11, 33, 17, 855077), 'timestamp': 1696246397.855,
        #            'deal_value': 'open|close|half-close'}

        client_buy = self.chosen_deal.client_buy
        client_sell = self.chosen_deal.client_sell
        buy_market = self.chosen_deal.buy_market
        sell_market = self.chosen_deal.sell_market

        # with open('ap_still_active_status.csv', 'a', newline='') as file:
        #     writer = csv.writer(file)
        #     row_data = [str(y) for y in chosen_deal.values()] + ['Active']
        #     writer.writerow(row_data)

        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        self.chosen_deal.order_id_buy, self.chosen_deal.order_id_sell = id1, id2
        cl_id_buy, cl_id_sell = f"api_deal_{id1.replace('-', '')[:20]}", f"api_deal_{id2.replace('-', '')[:20]}"

        self.chosen_deal.time_sent = int(datetime.utcnow().timestamp() * 1000)

        orders = []
        orders.append(self.loop_2.create_task(
            client_buy.create_order(buy_market, 'buy', self.session, client_id=cl_id_buy)))
        orders.append(self.loop_2.create_task(
            client_sell.create_order(sell_market, 'sell', self.session, client_id=cl_id_sell)))
        responses = await asyncio.gather(*orders, return_exceptions=True)

        time_sent = self.chosen_deal.time_sent

        self.chosen_deal.buy_order_place_time = (responses[0]['timestamp'] - time_sent) / 1000
        self.chosen_deal.sell_order_place_time = (responses[1]['timestamp'] - time_sent) / 1000
        self.chosen_deal.buy_exchange_order_id = responses[0]['exchange_order_id']
        self.chosen_deal.sell_exchange_order_id = responses[1]['exchange_order_id']

        message = f"Results of create_order requests: [{self.chosen_deal.buy_exchange=}, {self.chosen_deal.sell_exchange=}]\n{responses=}"
        print(message)
        self.telegram.send_message(message, TG_Groups.DebugDima)

    @try_exc_async
    async def notification_and_logging(self):

        self.chosen_deal.max_buy_vol = self.chosen_deal.ob_buy['asks'][0][1]
        self.chosen_deal.max_sell_vol = self.chosen_deal.ob_sell['bids'][0][1]

        ap_id = self.chosen_deal.ap_id
        client_buy, client_sell = self.chosen_deal.client_buy, self.chosen_deal.client_sell
        shifted_buy_px, shifted_sell_px = self.chosen_deal.limit_buy_px, self.chosen_deal.limit_sell_px
        buy_market, sell_market = self.chosen_deal.buy_market, self.chosen_deal.sell_market
        order_id_buy, order_id_sell = self.chosen_deal.order_id_buy, self.chosen_deal.order_id_sell
        buy_exchange_order_id, sell_exchange_order_id = self.chosen_deal.buy_exchange_order_id, self.chosen_deal.sell_exchange_order_id
        buy_order_place_time = self.chosen_deal.buy_order_place_time
        sell_order_place_time = self.chosen_deal.sell_order_place_time

        self.telegram.send_ap_executed_message(self.chosen_deal, TG_Groups.MainGroup)
        self.db.save_arbitrage_possibilities(self.chosen_deal)
        self.db.save_order(order_id_buy, buy_exchange_order_id, client_buy, 'buy', ap_id, buy_order_place_time,
                           shifted_buy_px,
                           buy_market, self.env)
        self.db.save_order(order_id_sell, sell_exchange_order_id, client_sell, 'sell', ap_id, sell_order_place_time,
                           shifted_sell_px,
                           sell_market, self.env)

        if buy_exchange_order_id == 'default':
            self.telegram.send_order_error_message(self.env, buy_market, client_buy, order_id_buy, TG_Groups.Alerts)
        if sell_exchange_order_id == 'default':
            self.telegram.send_order_error_message(self.env, sell_market, client_sell, order_id_sell, TG_Groups.Alerts)
        self.db.update_balance_trigger('post-deal', ap_id, self.env)

    @try_exc_regular
    def update_all_av_balances(self):
        for client in self.clients:
            self.available_balances.update({client.EXCHANGE_NAME: client.get_available_balance()})

    @try_exc_regular
    def add_exchange_exception(self, exchange, reason) -> None:
        self.exchange_exceptions.append(
            {'exchange': exchange,'reason': reason, 'ts_added': str(datetime.utcnow())})

    @try_exc_regular
    def in_exchange_exceptions(self, exchange):
        filtered = [item for item in self.exchange_exceptions if item['exchange'] == exchange]
        return len(filtered) > 0
    @try_exc_regular
    def add_trade_exception(self, coin, exchange, direction, reason) -> None:
        self.trade_exceptions.append(
            {'coin': coin, 'exchange': exchange, 'direction': direction,
             'reason': reason, 'ts_added': str(datetime.utcnow())})

    @try_exc_regular
    def in_trade_exceptions(self, coin, exchange, direction):
        filtered = [item for item in self.trade_exceptions if item['coin'] == coin and
                    item['exchange'] == exchange and item['direction'] == direction]
        return len(filtered) > 0


    @try_exc_regular
    def check_available_balance_and_exceptions(self, coin, buy_exchange, sell_exchange):
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
                    self.add_trade_exception(coin, exchange, direction,
                                             f'Превышено максимальная доля баланса на монету')
                    message = self.telegram.coin_threshold_message(coin, exchange, direction, position, available,
                                                                   self.max_position_part)
                    self.telegram.send_message(message, TG_Groups.Alerts)
                result[direction] = False
        return result['buy'] and result['sell']



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

    @try_exc_regular
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

    @try_exc_regular
    def deal_size_define(self, buy_exchange, sell_exchange, buy_market, sell_market):

        if self.available_balances[buy_exchange].get(buy_market):
            buy_size = self.available_balances[buy_exchange][buy_market]['buy']
        else:
            buy_size = self.available_balances[buy_exchange]['buy']
        if self.available_balances[sell_exchange].get(sell_market):
            sell_size = self.available_balances[sell_exchange][sell_market]['sell']
        else:
            sell_size = self.available_balances[sell_exchange]['sell']
        if buy_size == 0:
            status = f'{buy_exchange=}{buy_market=} недоступен для покупок, превышено абсолютное плечо для БИРЖИ'
        elif buy_size < 0:
            status = f'{buy_exchange=}{buy_market=} недоступен для покупок, превышено плечо для РЫНКА'
        elif sell_size == 0:
            status = f'{sell_exchange=}{sell_market=} недоступен для продаж, превышено абсолютное плечо для БИРЖИ'
        elif sell_size < 0:
            status = f'{sell_exchange=}{sell_market=} недоступен для продаж, превышено плечо для РЫНКА'
        else:
            status = 'OK'
        return {'deal_size': min(buy_size, sell_size, self.max_order_size), 'status': status}

    # @try_exc_regular
    # def __rates_update(self):
    #     message = ''
    #     with open(f'rates.txt', 'a') as file:
    #         for client in self.clients:
    #             message += f"{client.EXCHANGE_NAME} | {client.get_orderbook(client.symbol)['asks'][0][0]} | {datetime.utcnow()} | {time.time()}\n"
    #         file.write(message + '\n')
    #     self.update_all_av_balances()

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
    @try_exc_regular
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

    @try_exc_async
    async def __check_order_status(self):
        # Эта функция инициирует обновление данных по ордеру в базе, когда обновление приходит от биржи в клиента после создания
        while True:
            for client in self.clients:
                orders = client.orders.copy()

                for order_id, message in orders.items():
                    self.rabbit.add_task_to_queue(message, "UPDATE_ORDERS")
                    client.orders.pop(order_id)

            await asyncio.sleep(3)

    @try_exc_async
    async def check_active_markets_status(self):
        # Здесь должна быть проверка, что рынки, в которых у нас есть позиции активны. Если нет, то алерт
        pass


if __name__ == '__main__':
    MultiBot()
