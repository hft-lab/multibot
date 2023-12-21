import configparser
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

logging.basicConfig(filename='ap_logs.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = configparser.ConfigParser()
config.read('config_parser.ini', "utf-8")


class AP_Log:
    def __init__(self, ap: AP):
        self.buy_market = ap.buy_market
        self.sell_market = ap.sell_market
        self.buy_exchange = ap.buy_exchange
        self.sell_exchange = ap.sell_exchange
        self.profit_rel_parser = ap.profit_rel_parser
        self.coin = ap.coin
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

        self.ap_active_logs: List[AP_Log] = []
        self.ap_log_filled_flag: bool = False
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
        print('INIT PROCESS FINISHED')
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

        print('STARTING RUN CLIENTS')
        for client in self.clients:
            client.markets_list = list(self.markets.keys())
            client.run_updater()
        print(f'CLIENTS HAVE STARTED. MARKET DATA:\n RIBS: {self.ribs}\n{json.dumps(self.markets_data, indent=2)}')

        # with open(f'rates.txt', 'a') as file:
        #     file.write('')
        self.telegram.send_parser_launch_message(self, TG_Groups.MainGroup)
        self.websocket_main_cycle()
        # logger_custom = Logging()
        # logger_custom.log_launch_params(self.clients)

    @try_exc_regular
    def websocket_main_cycle(self):
        print("")
        while True:
            time.sleep(self.cycle_parser_delay)
            if not round(datetime.utcnow().timestamp() - self.start_time) % 90:
                self.start_time -= 1
                self.telegram.send_message(f"MULTI PARSER IS WORKING", TG_Groups.MainGroup)
                print('MULTI PARSER IS WORKING')
            # Шаг 1 (Сбор данных с бирж по рынкам)
            results = self.get_data_for_parser()

            # Шаг 2 (Анализ маркет данных с бирж и поиск потенциальных AP)
            potential_possibilities = self.finder.find_arbitrage_possibilities(results, self.ribs)
            time_end_define_potential_deals = time.time()

            if len(potential_possibilities) == 0 and self.ap_log_filled_flag:
                self.close_all_open_possibilities()
            if len(potential_possibilities):
                self.update_ap_logs_with_new_possibilities(potential_possibilities)

    @try_exc_regular
    def get_data_for_parser(self):
        data = dict()
        for client in self.clients:
            data.update(client.get_all_tops())
        return data

    @try_exc_regular
    def close_all_open_possibilities(self):
        self.ap_log_filled_flag = False
        for ap_log in self.ap_active_logs:
            ap_log.ts_end = time.time()
            ap_log.duration = round(ap_log.ts_end - ap_log.ts_start, 4)
            message = f'ALERT: Ended AP (All AP gone)\n' \
                      f'Duration: {round(ap_log.duration, 2)}\n' \
                      f'Coin:{ap_log.coin}\n' \
                      f'Initial rel. profit: {round(ap_log.profit_rel_parser, 5)}\n' \
                      f'B.E.:{ap_log.buy_exchange}\n' \
                      f'S.E.:{ap_log.sell_exchange}\n'
            print(message)
            self.telegram.send_message(message, TG_Groups.Alerts)
            # logging.info(message)
            self.ap_active_logs.remove(ap_log)

    @try_exc_regular
    def update_ap_logs_with_new_possibilities(self, ap_list: List[AP]):
        self.ap_log_filled_flag = True
        aps_cycle = []
        intersection_cycle = []
        intersection_logs = []
        for ap_log in ap_list:
            ap_cycle = AP_Log(ap_log)
            aps_cycle.append(ap_cycle)
        for ap_log in self.ap_active_logs:
            for ap_cycle in aps_cycle:
                if ap_log == ap_cycle:
                    intersection_cycle.append(ap_cycle)
                    intersection_logs.append(ap_log)

        only_in_cycle = [item for item in aps_cycle if
                         item not in intersection_cycle]  # Именно новые AP, которые появились в рамках цикла

        only_in_logs = [item for item in self.ap_active_logs if
                        item not in intersection_logs]  # Открытые AP переставшие быть актуальным

        # Исключаем AP переставшие быть актуальными
        for ap_log in only_in_logs:
            ap_log.ts_end = time.time()
            ap_log.duration = round(ap_log.ts_end - ap_log.ts_start, 2)
            message = f'ALERT: Ended AP\n' \
                      f'Duration: {round(ap_log.duration, 2)}\n' \
                      f'Coin:{ap_log.coin}\n' \
                      f'Initial rel. profit: {round(ap_log.profit_rel_parser, 5)}\n' \
                      f'B.E.:{ap_log.buy_exchange}\n' \
                      f'S.E.:{ap_log.sell_exchange}\n'
            self.telegram.send_message(message, TG_Groups.Alerts)
            # logging.info(message)
            self.ap_active_logs.remove(ap_log)

        # Добавляем новые AP, которые обнаружились в рамках цикла
        self.ap_active_logs += only_in_cycle


if __name__ == '__main__':
    MultiParser()
