import time
import json
from datetime import datetime
from typing import List

from arbitrage_finder import ArbitrageFinder
from clients.core.all_clients_parser import ALL_CLIENTS
from clients_markets_data import Clients_markets_data

from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular
from core.ap_class import AP
import logging

logging.basicConfig(filename='ap_logs.txt', level=logging.INFO, format='%(asctime)s,%(message)s')
import configparser

config = configparser.ConfigParser()
config.read('config_parser.ini', "utf-8")


class AP_Log:
    def __init__(self, ap: AP, target_profit):
        self.coin = ap.coin
        self.buy_market = ap.buy_market
        self.sell_market = ap.sell_market
        self.buy_exchange = ap.buy_exchange
        self.sell_exchange = ap.sell_exchange
        self.profit_rel_parser = ap.profit_rel_parser
        self.max_profit_rel = ap.profit_rel_parser
        self.min_profit_rel = ap.profit_rel_parser
        self.deal_usd_parser = ap.deal_max_usd_parser
        self.max_deal_usd = ap.deal_max_usd_parser
        self.min_deal_usd = ap.deal_max_usd_parser
        self.more_one_cycle_flag = False
        self.target_profit = target_profit
        self.ts_start = ap.ts_create_ap / 1000 + 25200
        self.ts_end = None
        self.duration = None

    def __eq__(self, other):
        if isinstance(other, AP_Log):
            return self.buy_market == other.buy_market and \
                self.sell_market == other.sell_market and \
                self.buy_exchange == other.buy_exchange and \
                self.sell_exchange == other.sell_exchange and \
                self.target_profit == other.target_profit
        return False


class MultiParser:
    # __slots__ = ['cycle_parser_delay', 'chosen_deal', 'profit_taker', 'markets_data',
    #              'telegram', 'start_time', 'ribs_exceptions', 'clients-http', 'exchanges', 'ribs', 'env',
    #              'exception_pause', 'loop_2', 'last_orderbooks', 'time_start', 'time_parser',
    #              'setts', 'rates_file_name', 'main_exchange', 'markets', 'clients_markets_data', 'finder', 'instance_markets_amount',
    #              'clients_with_names', 'exchanges_in_ribs']

    def __init__(self):
        print('INIT PROCESS STARTED')
        self.setts = config['SETTINGS']
        self.cycle_parser_delay = float(self.setts['CYCLE_PARSER_DELAY'])
        self.instance_markets_amount = int(config['SETTINGS']['INSTANCE_MARKETS_AMOUNT'])
        self.env = self.setts['ENV']

        self.main_exchange = self.setts['MAIN_EXCHANGE']
        self.exchanges = self.setts['EXCHANGES'].split(',')
        self.mode = self.setts['MODE']
        self.profits_list = list(map(float, self.setts['TARGET_PROFITS'].split(',')))
        self.profit_open = self.profits_list[0]
        self.profit_close = self.profits_list[0]
        self.ap_active_logs: List[AP_Log] = []
        self.ap_log_filled_flag: bool = False
        self.ribs_exceptions = []
        self.ribs = self.get_exchanges_ribs()

        self.clients = []

        for exchange in self.exchanges:
            print(exchange)
            try:
                keys = config[exchange]
            except:
                keys = None
            client = ALL_CLIENTS[exchange](keys=keys, leverage=None, state='Parser', max_pos_part=None)
            self.clients.append(client)
        self.clients_with_names = {}

        for client in self.clients:
            self.clients_with_names.update({client.EXCHANGE_NAME: client})

        self.start_time = datetime.utcnow().timestamp()

        self.clients_markets_data = Clients_markets_data(self.clients, self.setts['INSTANCE_NUM'],
                                                         self.instance_markets_amount)
        self.markets = self.clients_markets_data.get_instance_markets()  # coin:exchange:symbol
        self.markets_data = self.clients_markets_data.get_clients_data()

        self.finder = ArbitrageFinder(self.markets, self.clients_with_names, self.profit_open, self.profit_close,
                                      state='Parser')
        self.chosen_deal: AP

        self.telegram = Telegram()
        print('INIT PROCESS FINISHED')

    @try_exc_regular
    def get_exchanges_ribs(self):
        ribs = []
        if self.mode == "MAIN_EXCHANGE":
            for exchange in self.exchanges:
                if self.main_exchange != exchange:
                    ribs.append([self.main_exchange, exchange])
                    ribs.append([exchange, self.main_exchange])
        if self.mode == "ALL_RIBS":
            for exchange1 in self.exchanges:
                for exchange2 in self.exchanges:
                    if exchange1 != exchange2:
                        ribs.append([exchange1, exchange2])
        else:
            ribs_raw = self.setts['RIBS'].split(',')
            for rib in ribs_raw:
                ex1, ex2 = rib.split('|')
                ribs.append([ex1, ex2])
                ribs.append([ex2, ex1])
        return ribs

    @try_exc_regular
    def launch(self):
        self.telegram.send_message(f"MULTI HAS STARTED", TG_Groups.MainGroup)
        print(f'TARGET PROFIT: {self.profit_open}')
        print('STARTING CLIENTS')
        for client in self.clients:
            client.markets_list = list(self.markets.keys())
            print(f'EXCHANGE: {client.EXCHANGE_NAME}, FEE: {client.taker_fee}')
            client.finder = self.finder
            client.run_updater()
        print(f'CLIENTS HAVE STARTED. MARKET DATA:\n RIBS: {self.ribs}\n{json.dumps(self.markets_data, indent=2)}')

        self.telegram.send_parser_launch_message(self, TG_Groups.MainGroup)
        self.websocket_main_cycle()
        # logger_custom = Logging()
        # logger_custom.log_launch_params(self.clients-http)

    @try_exc_regular
    def exchange_ob_analize(self):
        exchange = 'WHITEBIT'
        client = self.clients_with_names[exchange]
        data = client.get_all_tops()
        # {EXCHANGE_NAME__coin: {'top_bid': , 'top_ask': ,'bid_vol': , 'ask_vol': ,'ts_exchange': }}
        # HITBTC  - наше, есть много медленных стаканов
        # GLOBE - биржевое, время со сдвигом
        # BIT - биржевое, время со сдвигом
        # BITMAKE - биржевое, но поломанное, время со сдвигом
        # BIBOX - наше
        # BTSE -
        ts_start_analisys = round(datetime.utcnow().timestamp(), 2)
        for exchange__coin in data:
            data[exchange__coin]['ts_exchange'] -= 7 * 60 * 60 * 1000

        ts_data = []

        for exchange__coin in data:
            coin = exchange__coin.split('__')[1]
            ts_data.append({'coin': coin, 'ts_exchange': data[exchange__coin]['ts_exchange']})

        print(f'Exchange: {exchange}')
        print(f"TS начала анализа: {round(ts_start_analisys, 2)}")
        min_ts = min(ts_data, key=lambda x: x['ts_exchange'])
        max_ts = max(ts_data, key=lambda x: x['ts_exchange'])
        print(
            f"Coin: {min_ts['coin']}, Max Diff (сек.): {int((ts_start_analisys - min_ts['ts_exchange'] / 1000) * 100) / 100}")
        print(
            f"Coin: {max_ts['coin']}, Min Diff (сек.): {int((ts_start_analisys - max_ts['ts_exchange'] / 1000) * 100) / 100}")
        print("\n")
        time.sleep(1)

    @try_exc_regular
    def websocket_main_cycle(self):
        print("PARSING AND LOOKING FOR AP")
        while True:
            time.sleep(self.cycle_parser_delay)
            if not round(datetime.utcnow().timestamp() - self.start_time) % 120:
                self.start_time -= 1
                self.telegram.send_message(f"MULTI PARSER IS WORKING", TG_Groups.MainGroup)
                print('MULTI PARSER IS WORKING')

            # Шаг 0. Тестирование стаканов на новой бирже
            # self.exchange_ob_analize()
            # Шаг 1 (Сбор данных с бирж по рынкам)
            # Шаг 2 (Анализ маркет данных с бирж и поиск потенциальных AP)
            potential_possibilities = self.finder.potential_deals.copy()
            self.finder.potential_deals = [item for item in self.finder.potential_deals if item not in potential_possibilities]
            if potential_possibilities == [] and self.ap_log_filled_flag:
                self.close_all_open_possibilities()
            if len(potential_possibilities):
                self.update_ap_logs_with_new_possibilities(potential_possibilities)

    @try_exc_regular
    def get_data_for_parser(self):
        data = dict()
        for client in self.clients:
            data.update(
                client.get_all_tops())  # {EXCHANGE_NAME__coin: {'top_bid': , 'top_ask': ,'bid_vol': , 'ask_vol': ,'ts_exchange': }}
        return data

    @try_exc_regular
    def close_all_open_possibilities(self):
        self.ap_log_filled_flag = False
        for ap_log in self.ap_active_logs:
            ap_log.ts_end = time.time()
            ap_log.duration = round(ap_log.ts_end - ap_log.ts_start, 4)
            dt_start = datetime.fromtimestamp(ap_log.ts_start).strftime("%H:%M:%S.%f")[:-3]
            dt_end = datetime.fromtimestamp(ap_log.ts_end).strftime("%H:%M:%S.%f")[:-3]
            message = f'ALERT: Ended AP (All AP gone)\n' \
                      f'Duration: {round(ap_log.duration, 2)}\n' \
                      f'Coin:{ap_log.coin}\n' \
                      f'B.E.:{ap_log.buy_exchange}\n' \
                      f'S.E.:{ap_log.sell_exchange}\n' \
                      f'Profit threshold.:{ap_log.target_profit}\n' \
                      f'Initial rel. profit: {round(ap_log.profit_rel_parser, 5)}\n' \
                      f'Min rel. profit: {round(ap_log.min_profit_rel, 5)}\n' \
                      f'Max rel. profit: {round(ap_log.max_profit_rel, 5)}\n' \
                      f'Initial size usd: {round(ap_log.deal_usd_parser, 1)}\n' \
                      f'Min size usd: {round(ap_log.min_deal_usd, 1)}\n' \
                      f'Max size usd: {round(ap_log.max_deal_usd, 1)}\n' \
                      f'Start: {dt_start}\n' \
                      f'End: {dt_end}\n' \
                      f'More than one cycle: {ap_log.more_one_cycle_flag}\n'
            print(message)
            self.telegram.send_message(message, TG_Groups.Alerts)
            self.ap_active_logs.remove(ap_log)
            logging.info(f'All_AP_gone,{ap_log.coin},{ap_log.buy_exchange},{ap_log.sell_exchange},'
                         f'{ap_log.target_profit},{round(ap_log.profit_rel_parser, 5)},'
                         f'{round(ap_log.min_profit_rel, 5)},{round(ap_log.max_profit_rel, 5)},'
                         f'{round(ap_log.deal_usd_parser, 1)},{round(ap_log.min_deal_usd, 1)},'
                         f'{round(ap_log.max_deal_usd, 1)},'
                         f'{dt_start},{dt_end},{round(ap_log.duration, 2)},{ap_log.more_one_cycle_flag}')

    @try_exc_regular
    def update_ap_logs_with_new_possibilities(self, ap_list: List[AP]):
        self.ap_log_filled_flag = True
        aps_cycle = []  # Здесь будут храниться потенциальные AP из цикла преобразованные к формату AP_LOG
        intersection_cycle = []
        intersection_logs = []

        for ap in ap_list:
            profit = ap.profit_rel_parser
            for profit_threshold in self.profits_list:
                if profit >= profit_threshold:
                    aps_cycle.append(AP_Log(ap, profit_threshold))

        for ap_log in self.ap_active_logs:
            for ap_cycle in aps_cycle:
                if ap_log == ap_cycle:
                    ap_log.more_one_cycle_flag = True
                    if ap_cycle.profit_rel_parser > ap_log.max_profit_rel:
                        ap_log.max_profit_rel = ap_cycle.profit_rel_parser
                    if ap_cycle.profit_rel_parser < ap_log.min_profit_rel:
                        ap_log.min_profit_rel = ap_cycle.profit_rel_parser
                    if ap_cycle.deal_usd_parser > ap_log.max_deal_usd:
                        ap_log.max_deal_usd = ap_cycle.deal_usd_parser
                    if ap_cycle.deal_usd_parser < ap_log.min_deal_usd:
                        ap_log.min_deal_usd = ap_cycle.deal_usd_parser
                    intersection_cycle.append(ap_cycle)
                    intersection_logs.append(ap_log)

        # Исключаем AP переставшие быть актуальными
        only_in_logs = [item for item in self.ap_active_logs if item not in intersection_logs]

        for ap_log in only_in_logs:
            ap_log.ts_end = time.time()
            ap_log.duration = round(ap_log.ts_end - ap_log.ts_start, 2)
            dt_start = datetime.fromtimestamp(ap_log.ts_start).strftime("%H:%M:%S.%f")[:-3]
            dt_end = datetime.fromtimestamp(ap_log.ts_end).strftime("%H:%M:%S.%f")[:-3]
            message = f'ALERT: Ended AP (New AP came)\n' \
                      f'Duration: {round(ap_log.duration, 2)}\n' \
                      f'Coin:{ap_log.coin}\n' \
                      f'B.E.:{ap_log.buy_exchange}\n' \
                      f'S.E.:{ap_log.sell_exchange}\n' \
                      f'Profit threshold.:{ap_log.target_profit}\n' \
                      f'Initial rel. profit: {round(ap_log.profit_rel_parser, 5)}\n' \
                      f'Min rel. profit: {round(ap_log.min_profit_rel, 5)}\n' \
                      f'Max rel. profit: {round(ap_log.max_profit_rel, 5)}\n' \
                      f'Initial size usd: {round(ap_log.deal_usd_parser, 1)}\n' \
                      f'Min size usd: {round(ap_log.min_deal_usd, 1)}\n' \
                      f'Max size usd: {round(ap_log.max_deal_usd, 1)}\n' \
                      f'Start: {dt_start}\n' \
                      f'End: {dt_end}\n' \
                      f'More than one cycle: {ap_log.more_one_cycle_flag}\n'
            print(message)
            self.telegram.send_message(message, TG_Groups.Alerts)
            self.ap_active_logs.remove(ap_log)
            logging.info(f'New_AP_came,{ap_log.coin},{ap_log.buy_exchange},{ap_log.sell_exchange},'
                         f'{ap_log.target_profit},{round(ap_log.profit_rel_parser, 5)},'
                         f'{round(ap_log.min_profit_rel, 5)},{round(ap_log.max_profit_rel, 5)},'
                         f'{round(ap_log.deal_usd_parser, 1)},{round(ap_log.min_deal_usd, 1)},'
                         f'{round(ap_log.max_deal_usd, 1)},'
                         f'{dt_start},{dt_end},{round(ap_log.duration, 2)},{ap_log.more_one_cycle_flag}')

        # Добавляем новые AP, которые обнаружились в рамках цикла
        only_in_cycle = [item for item in aps_cycle if item not in intersection_cycle]
        for ap_log in only_in_cycle:
            self.ap_active_logs.append(ap_log)


if __name__ == '__main__':
    mp = MultiParser()
    mp.launch()
