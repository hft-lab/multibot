import configparser
import sys
import time
import json
from datetime import datetime
from typing import List

from arbitrage_finder import ArbitrageFinder, AP
from clients.core.all_clients import ALL_CLIENTS
from clients_markets_data import Clients_markets_data

from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular
import logging

logging.basicConfig(filename='ap_logs.txt', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")


class AP_Log:
    def __init__(self, ap: AP):
        self.buy_market = ap.buy_market
        self.sell_market = ap.sell_market
        self.buy_exchange = ap.buy_exchange
        self.sell_exchange = ap.sell_exchange
        self.coin = ap.coin
        self.status = 'Open'
        self.ts_start = time.time()
        self.ts_end = None
        self.duration = None

    def __eq__(self, other):
        if isinstance(other, AP_Log):
            return self.buy_market == other.buy_market and \
                self.sell_market == other.sell_market and \
                self.buy_exchange == other.buy_exchange and \
                self.sell_exchange == other.sell_exchange
        return False


class MultiParser:
    # __slots__ = ['cycle_parser_delay', 'chosen_deal', 'profit_taker', 'markets_data',
    #              'telegram', 'start_time', 'ribs_exceptions', 'clients', 'exchanges', 'ribs', 'env',
    #              'exception_pause', 'loop_2', 'last_orderbooks', 'time_start', 'time_parser',
    #              'setts', 'rates_file_name', 'main_exchange', 'markets', 'clients_markets_data', 'finder', 'instance_markets_amount',
    #              'clients_with_names', 'exchanges_in_ribs']

    def __init__(self):
        print('INIT PROCESS STARTED')
        self.setts = config['SETTINGS']
        self.cycle_parser_delay = float(self.setts['CYCLE_PARSER_DELAY'])
        self.instance_markets_amount = int(config['SETTINGS']['INSTANCE_MARKETS_AMOUNT'])
        self.env = self.setts['ENV']
        self.profit_taker = float(self.setts['TARGET_PROFIT'])
        self.main_exchange = self.setts['MAIN_EXCHANGE']
        self.exchanges = self.setts['EXCHANGES'].split(',')

        self.exception_pause = 60
        self.ap_logs: List[AP_Log] = []
        self.ribs_exceptions = []
        self.ribs = self.get_exchanges_ribs()

        self.clients = []

        for exchange in self.exchanges:
            print(exchange)
            client = ALL_CLIENTS[exchange](keys=config[exchange], leverage=None, max_pos_part=None)
            self.clients.append(client)
        self.clients_with_names = {}

        for client in self.clients:
            self.clients_with_names.update({client.EXCHANGE_NAME: client})

        self.start_time = datetime.utcnow().timestamp()

        self.clients_markets_data = Clients_markets_data(self.clients, self.setts['INSTANCE_NUM'],
                                                         self.instance_markets_amount)
        self.markets = self.clients_markets_data.get_instance_markets()  # coin:exchange:symbol
        self.markets_data = self.clients_markets_data.get_clients_data()

        self.finder = ArbitrageFinder(self.markets, self.clients_with_names, self.profit_taker, self.profit_taker)
        self.chosen_deal: AP

        self.telegram = Telegram()
        self.launch()

    @try_exc_regular
    def get_exchanges_ribs(self):
        ribs = []
        if self.main_exchange:
            for exchange in self.exchanges:
                if self.main_exchange != exchange:
                    ribs.append([self.main_exchange, exchange])
                    ribs.append([exchange, self.main_exchange])
        else:
            ribs_raw = self.setts['RIBS'].split(',')
            for rib in ribs_raw:
                ex1, ex2 = rib.split('|')
                ribs.append([ex1, ex2])
                ribs.append([ex2, ex1])
        return ribs

    # @try_exc_regular
    # def get_exchages_from_ribs(self):
    #     ribs = self.setts['RIBS'].split(',')
    #     exchanges = []
    #     for rib in ribs:
    #         ex1, ex2 = rib.split('|')
    #         if ex1 not in exchanges:
    #             exchanges.append(ex1)
    #         if ex2 not in exchanges:
    #             exchanges.append(ex2)
    #     return exchanges

    @try_exc_regular
    def launch(self):

        for client in self.clients:
            client.markets_list = list(self.markets.keys())
            client.run_updater()
        print('CLIENTS MARKET DATA:')
        print(f'PARSER STARTED\n{self.ribs=}')

        # with open(f'rates.txt', 'a') as file:
        #     file.write('')
        self.telegram.send_parser_launch_message(self, TG_Groups.MainGroup)
        self.websocket_main_cycle()
        # logger_custom = Logging()
        # logger_custom.log_launch_params(self.clients)

    @try_exc_regular
    def websocket_main_cycle(self):

        while True:
            time.sleep(self.cycle_parser_delay)
            if not round(datetime.utcnow().timestamp() - self.start_time) % 90:
                self.start_time -= 1
                self.telegram.send_message(f"MULTI PARSER IS WORKING", TG_Groups.MainGroup)
                print('MULTI PARSER IS WORKING')
            # Шаг 1 (Сбор данных с бирж по рынкам)
            time_start_parsing = time.time()
            results = self.get_data_for_parser()
            time_end_parsing = time.time()

            # Шаг 2 (Анализ маркет данных с бирж и поиск потенциальных AP)
            potential_possibilities = self.finder.find_arbitrage_possibilities(results, self.ribs)
            time_end_define_potential_deals = time.time()

            if len(potential_possibilities):
                # Логирование получившихся AP
                self.update_ap_logs_with_new_possibilities(potential_possibilities)
                # Шаг 3 (Выбор лучшей AP, если их несколько)
                self.chosen_deal: AP = self.choose_deal(potential_possibilities)
                if self.chosen_deal:
                    time_end_choose = time.time()
                    self.chosen_deal.ts_define_potential_deals_end = time_end_define_potential_deals
                    self.chosen_deal.ts_choose_end = time.time()
                    self.chosen_deal.time_parser = time_end_parsing - time_start_parsing
                    self.chosen_deal.time_define_potential_deals = time_end_define_potential_deals - time_end_parsing
                    self.chosen_deal.time_choose = time_end_choose - time_end_define_potential_deals
                    # Шаг 4 (Проверка, что выбранная AP все еще действует, здесь заново запрашиваем OB)
                    if self.check_prices_still_good():
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
    def update_ap_logs_with_new_possibilities(self, ap_list: List[AP]):
        aps_cycle = []
        intersection_cycle = []
        intersection_logs = []
        for ap in ap_list:
            ap_cycle = AP_Log(ap)
            aps_cycle.append(ap_cycle)
        for ap_log in self.ap_logs:
            if ap_log.status == 'Close':
                continue
            for ap_cycle in aps_cycle:
                if ap_log == ap_cycle:
                    intersection_cycle.append(ap_cycle)
                    intersection_logs.append(ap_log)

        only_cycle = [item for item in aps_cycle if
                      item not in intersection_cycle]  # Именно новые AP, которые появились в рамках цикла
        ap_logs_open = []
        for ap_log in self.ap_logs:
            if ap_log.status == 'Open':
                ap_logs_open.append(ap_log)

        only_open_logs = [item for item in ap_logs_open if
                          item not in intersection_logs]  # Открытые AP переставшие быть актуальным
        # Добавляем новые AP, которые обнаружились в рамках цикла
        self.ap_logs += only_cycle
        # Закрываем AP переставшие быть актуальными
        for ap_log in only_open_logs:
            ap_log.status = 'Close'
            ap_log.ts_end = time.time()
            ap_log.duration = round(ap_log.ts_end - ap_log.ts_start, 4)
        for ap in self.ap_logs:
            if ap.status == 'Close':
                message = f'ALERT: Ended AP\n' \
                          f'Duration: {round(ap.duration,2)}\n' \
                          f'Coind:{ap.coin}\n' \
                          f'B.E.:{ap.buy_exchange}\n' \
                          f'S.E.:{ap.sell_exchange}\n'
                self.telegram.send_message(message,TG_Groups.Alerts)
                # logging.info(message)
                self.ap_logs.remove(ap)

    @try_exc_regular
    def choose_deal(self, potential_deals: List[AP]) -> AP:
        max_profit = 0
        chosen_deal = None
        for deal in potential_deals:
            if self.is_in_ribs_exception(deal.buy_exchange, deal.buy_market, deal.sell_exchange, deal.sell_market):
                continue
            if deal.profit_rel_parser > max_profit:
                max_profit = deal.profit_rel_parser
                chosen_deal = deal
        return chosen_deal

    @try_exc_regular
    def check_prices_still_good(self):
        buy_market, sell_market = self.chosen_deal.buy_market, self.chosen_deal.sell_market
        buy_exchange, sell_exchange = self.chosen_deal.buy_exchange, self.chosen_deal.sell_exchange
        ob_buy = self.chosen_deal.client_buy.get_orderbook(buy_market)
        ob_sell = self.chosen_deal.client_sell.get_orderbook(sell_market)

        self.chosen_deal.ob_buy = {key: value[:5] if key in ['asks', 'bids'] else value for key, value in
                                   ob_buy.items()}
        self.chosen_deal.ob_sell = {key: value[:5] if key in ['asks', 'bids'] else value for key, value in
                                    ob_sell.items()}

        buy_price, sell_price = self.chosen_deal.ob_buy['asks'][0][0], self.chosen_deal.ob_sell['bids'][0][0]
        profit_brutto = (sell_price - buy_price) / buy_price
        profit = profit_brutto - self.chosen_deal.buy_fee - self.chosen_deal.sell_fee

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
            self.telegram.send_ap_still_active_parser(self.chosen_deal, TG_Groups.Alerts)
            self.add_ribs_exception(buy_exchange, buy_market, sell_exchange, sell_market)
            return True
        else:
            self.telegram.send_ap_expired_message(self.chosen_deal, TG_Groups.Alerts)
            return False

    @try_exc_regular
    def add_ribs_exception(self, buy_exchange, buy_market, sell_exchange, sell_market):
        self.ribs_exceptions.append({'be': buy_exchange, 'se': sell_exchange,
                                     'bm': buy_market, 'sm': sell_market, 'ts': int(time.time())})

    @try_exc_regular
    def is_in_ribs_exception(self, buy_exchange, buy_market, sell_exchange, sell_market):
        filtered = [item for item in self.ribs_exceptions if item['be'] == buy_exchange and
                    item['se'] == sell_exchange and item['bm'] == buy_market and item['sm'] == sell_market
                    and item['ts'] > int(time.time()) - self.exception_pause]
        return len(filtered) > 0


if __name__ == '__main__':
    MultiParser()
