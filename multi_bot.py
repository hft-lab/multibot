import datetime
import logging
import configparser
import sys
import time
import okex_client
import dydx_client
import bitmex_client
import binance_client
import multicall
# import database
import telebot
import shifts
# import database_writer
import orjson
from aio_pika import Message, ExchangeType, connect_robust
import asyncio
import threading
import aiohttp
import traceback

cp = configparser.ConfigParser()
if len(sys.argv) != 2:
    print("Usage %s <config.ini>" % sys.argv[0])
    sys.exit(1)
cp.read(sys.argv[1], "utf-8")

FORMAT = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s (%(funcName)s)'
logging.basicConfig(format=FORMAT)
log = logging.getLogger("sample bot")
log.setLevel(logging.DEBUG)


class MultiBot:

    state = "BOT"
    # state = 'PARSER'
    cp = cp
    telegram_bot = telebot.TeleBot(cp["TELEGRAM"]["token"])
    chat_id = cp["TELEGRAM"]["chat_id"]
    daily_chat_id = cp["TELEGRAM"]["daily_chat_id"]
    inv_chat_id = cp["TELEGRAM"]["inv_chat_id"]
    exchanges = cp['SETTINGS']['exchanges'].split(', ')
    if type(exchanges) == str:
        exchanges = [exchanges]
    min_disbalance = 500 * len(exchanges)
    RABBIT = {
        'host': '13.127.170.236',
        'port': 5672,
        'username': 'supauser',
        'password': 'XClO8wNcv9stGmfCO7'
    }

    def __init__(self):
        self._loop_1 = asyncio.new_event_loop()
        self._loop_2 = asyncio.new_event_loop()
        self.rabbit_url = f"amqp://{self.RABBIT['username']}:{self.RABBIT['password']}@{self.RABBIT['host']}:{self.RABBIT['port']}/"
        self.leverage = float(cp['SETTINGS']['leverage'])
        self.clients = {'BITMEX': bitmex_client.BitmexClient(self.cp['BITMEX'], leverage=self.leverage),
                        'DYDX': dydx_client.DydxClient(self.cp['DYDX'], leverage=self.leverage)}
                        # 'OKEX': okex_client.OkexClient(self.cp['OKEX'], leverage=self.leverage)}
                        # 'BINANCE': binance_client.BinanceClient(self.cp['BINANCE'], leverage=self.leverage)}
        for i in self.clients.values():
            i.run_updater()
        self.pool = multicall.Pool()
        self.max_order_size = float(cp['SETTINGS']['order_size'])
        self.profit_taker = float(cp['SETTINGS']['target_profit'])
        self.shifts = {'TAKER': float(cp['SETTINGS']['limit_shift'])}
                       # 'BITMEX': float(cp['SETTINGS']['bitmex_shift']),
                       # 'DYDX': float(cp['SETTINGS']['dydx_shift']),
                       # 'OKEX': float(cp['SETTINGS']['okex_shift']),
                       # 'BINANCE': float(cp['SETTINGS']['binance_shift']),
        for x, y in shifts.Shifts().get_shifts().items():
            self.shifts.update({x: y})
        self.deal_pause = int(cp['SETTINGS']['deal_pause'])
        self.start_time = int(round(time.time()))
        self.last_message = None
        self.last_max_deal_size = 0
        self.potential_deals = []
        self.deals_counter = []
        self.deals_executed = []
        self.available_balances = {'+DYDX-OKEX': 0}
        self.server_side = self.server_side()
        self.ribs = self.find_ribs()

    def find_ribs(self):
        ribs = []
        for exchange_1 in self.exchanges:
            for exchange_2 in self.exchanges:
                if exchange_1 != exchange_2:
                    ribs.append(exchange_1 + '|' + exchange_2)
        ribs = set(ribs)
        print(ribs)
        return ribs

    def server_side(self):
        server_side = None
        if 'OKEX' in self.exchanges:
            server_side = 'HONG-KONG'
        elif 'DYDX' in self.exchanges:
            server_side = 'TOKYO'
        elif 'BITMEX' in self.exchanges:
            server_side = 'DUBLIN'
        return server_side

    def daily_report(self):
        # base_data = self.database.fetch_data_from_table(f'deals_{self.database.symbol}')
        counted_deals = self.day_deals_count(base_data)
        message = self.create_daily_message(counted_deals)
        self.send_message(self.daily_chat_id, message)
        self.send_message(self.inv_chat_id, message)

    @staticmethod
    def day_deals_count(base_data):
        timestamp = int(round(time.time() - 86400))
        data = {'deal_count': 0,
                'volume': 0,
                'theory_profit': 0,
                }
        for deal in base_data[::-1]:
            if deal[1] < timestamp:
                break
            data['deal_count'] += 1
            data['volume'] += deal[8]
            data['theory_profit'] += deal[10]
            if not data.get(deal[2] + 'SELL'):
                data.update({deal[2] + 'SELL': 1})
            else:
                data[deal[2] + 'SELL'] += 1
            if not data.get(deal[3] + 'BUY'):
                data.update({deal[3] + 'BUY': 1})
            else:
                data[deal[3] + 'BUY'] += 1
        return data

    def create_daily_message(self, deals_data):
        message = f'DAILY REPORT FOR {str(datetime.datetime.now()).split(" ")[0]}'
        message += f"\nSERVER SIDE: {self.server_side}"
        for exchange, client in self.clients.items():
            message += f"\n  {exchange} pair: {client.symbol}"
        message += f"\nBALANCES:"
        tot_balance = 0
        for exchange, client in self.clients.items():
            tot_balance += client.get_real_balance()
            message += f"\n  {exchange}, USD: {round(client.get_real_balance())}"
        message += f"\n  TOTAL: {round(tot_balance)} USD"
        message += f"\nDEALS STATISTICS:"
        message += f"\n  DEALS NUMBER: {deals_data['deal_count']}"
        message += f"\n  VOLUME, USD: {round(deals_data['volume'])}"
        message += f"\n  THEORY PROFIT, USD: {round(deals_data['theory_profit'], 2)}"
        if deals_data['deal_count']:
            message += f"\n  THEORY PER DEAL, USD: {round(deals_data['theory_profit'] / deals_data['deal_count'], 3)}"
        message += f"\n  TARGET PROFIT, %: {round(self.profit_taker * 100, 4)}"
        for exchange in self.clients.keys():
            message += f"\n{exchange} DEALS:"
            for deal_type, number in deals_data.items():
                if exchange in deal_type:
                    if 'BUY' in deal_type:
                        message += f"\n  BUY: {number}"
                    elif 'SELL' in deal_type:
                        message += f"\n  SELL: {number}"
        return message

    def find_position_gap(self):
        position_gap = 0
        # min_fee = self.clients['OKEX'].taker_fee
        # balancing_exchange = 'DYDX'
        for exchange, client in self.clients.items():
            symbol = client.symbol
            position = client.get_positions()[symbol]['amount']
            position_gap += position
            # print(f"POSITION {exchange}: {position}. SYMBOL: {symbol}")
            # if client.taker_fee < min_fee:
            #     balancing_exchange = exchange
            #     min_fee = client.taker_fee
        return position_gap

    def find_balancing_elements(self):
        time_start = time.time()
        client = self.clients[self.exchanges[-1]]
        position_gap = self.find_position_gap()
        orderbook = client.get_orderbook()[client.symbol]
        change = ((orderbook['asks'][0][0] + orderbook['bids'][0][0]) / 2)
        amount_to_balancing = abs(position_gap) * change / 2
        # print(f"Disbalance finding time: {time.time() - time_start} sec")
        return position_gap, amount_to_balancing

    async def position_balancing(self):
        position_gap, amount_to_balancing = self.find_balancing_elements()
        exchanges_num = len(self.clients.items())
        position_gap = position_gap / exchanges_num
        if amount_to_balancing < self.min_disbalance:
            return
        ob_side = 'bids' if position_gap > 0 else 'asks'
        side = 'sell' if position_gap > 0 else 'buy'
        exchanges = ''
        av_price = 0
        av_fee = 0
        for exchange, client in self.clients.items():
            #CREATE ORDER PRICE TO BE SURE IT CLOSES
            orderbook = client.get_orderbook()[client.symbol]
            price = orderbook[ob_side][0][0]
            av_price += price
            av_fee += client.taker_fee
            self.create_balancing_order(client, position_gap, price, side)
            exchanges += exchange + ' '
        price = av_price / len(self.clients.keys())
        taker_fee = av_fee / len(self.clients.keys())
        await self.balancing_bd_update(exchanges, client, position_gap, price, side, taker_fee)
        # self.send_message(self.chat_id, message)
        # return message

    def create_balancing_order(self, client, position_gap, price, side):
        self.pool.add(client.create_order, abs(position_gap), price, side)
        self.pool.call_all()

    async def balancing_bd_update(self, exchanges, client, position_gap, price, side, taker_fee):
        coin = client.symbol.split('USD')[0].replace('-', '').replace('/', '')
        size_usd = abs(round(position_gap * price, 2))
        to_base = {
            'timestamp': int(round(time.time() * 1000)),
            'exchange_name': exchanges,
            'side': side,
            'price': price,
            'taker_fee': taker_fee,
            'position_gap': position_gap,
            'size_usd': size_usd,
            'coin': coin
        }
        # await self.publish_message(self.mq,
        #                            to_base,
        #                            'logger.event.insert_balancing_reports',
        #                            'logger.event',
        #                            'logger.event.insert_balancing_reports')

        # message = f"CREATED BALANCING ORDER\n"
        # message += f"SIZE, {coin}: {position_gap}\n"
        # message += f"SIZE, USD: {size_usd}\n"
        # message += f"PRICE: {round(price, 2)}\n"
        # message += f"SIDE: {side}\n"
        # message += f"TIMESTAMP, SEC: {round(time.time())}"
        # to_base = self.balancing_data_for_base(exchange, side, price, taker_fee, position_gap, size_usd)
        # self.database.balancing_base_update(to_base)
        # return message

    def send_message(self, chat_id, message):
        self.pool.add(self.telegram_bot.send_message, chat_id, '<pre>' + message + '</pre>', parse_mode='HTML')
        self.pool.call_all()

    def available_balance_update(self, buy_exch, sell_exch):
        max_deal_size = self.avail_balance_define(buy_exch, sell_exch)
        self.available_balances.update({f"+{buy_exch}-{sell_exch}": max_deal_size})

    def cycle_parser(self):
        # avr_time = []
        for pair in self.ribs:
            buy_exch = pair.split('|')[0]
            sell_exch = pair.split('|')[1]
            client_buy = self.clients[buy_exch]
            client_sell = self.clients[sell_exch]
            # time_start = time.time()
            self.available_balance_update(buy_exch, sell_exch)
            orderbook_sell, orderbook_buy = self.get_orderbooks(client_sell, client_buy)
            shift = self.shifts[sell_exch + ' ' + buy_exch] / 2
            sell_price = orderbook_sell['bids'][0][0] * (1 + shift)
            buy_price = orderbook_buy['asks'][0][0] * (1 - shift)
            if sell_price > buy_price:
                # print(f"{datetime.datetime.now()}Exchanges: {sell_exch} {sell_price} | {buy_exch} {buy_price}")
                self.taker_order_profit(sell_exch, buy_exch, sell_price, buy_price)
            self.potential_real_deals(sell_exch, buy_exch, orderbook_buy, orderbook_sell)
                # avr_time.append(time.time() - time_start)
        # print(f"Avr cycle time: {sum(avr_time) / len(avr_time)} sec")
        # print(f"Total cycles: {len(avr_time)}")
        # print(f"Total cycles time: {sum(avr_time)}")
        # print()
        # print(f"Max cycle time: {max(avr_time)} sec")
        # print(f"Min cycle time: {min(avr_time)} sec")

    async def find_price_diffs(self):
        time_start = time.time()
        self.cycle_parser()
        time_parser = time.time() - time_start
        chosen_deal = None
        if len(self.potential_deals):
            chosen_deal = self.choose_deal()
        time_choose = time.time() - time_start - time_parser
        if self.state == 'BOT':
            position_gap, amount_to_balancing = self.find_balancing_elements()
            if chosen_deal and amount_to_balancing < 5000:
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
                if self.available_balances[f"+{deal['buy_exch']}-{deal['sell_exch']}"] >= 140:
                    if deal['buy_exch'] in self.exchanges or deal['sell_exch'] in self.exchanges:
                        max_profit = deal['profit']
                        chosen_deal = deal
        self.potential_deals = []
        return chosen_deal

    def taker_order_profit(self, sell_exch, buy_exch, sell_price, buy_price):
        orderbook_sell, orderbook_buy = self.get_orderbooks(self.clients[sell_exch], self.clients[buy_exch])
        profit = (sell_price - buy_price) / buy_price
        if profit > self.profit_taker + self.clients[sell_exch].taker_fee + self.clients[buy_exch].taker_fee:
            # print(f"S.Ex.: {sell_exch} | B.Ex.: {buy_exch} | Profit: {profit}")
            self.potential_deals.append({'buy_exch': buy_exch,
                                         "sell_exch": sell_exch,
                                         "orderbook_buy": orderbook_buy,
                                         "orderbook_sell": orderbook_sell,
                                         'max_deal_size': self.available_balances[f"+{buy_exch}-{sell_exch}"],
                                         "profit": profit})

    async def execute_deal(self, buy_exch, sell_exch, orderbook_buy, time_start, time_parser, time_choose):
        max_deal_size = self.available_balances[f"+{buy_exch}-{sell_exch}"]
        self.deals_executed.append([f'+{buy_exch}-{sell_exch}', max_deal_size])
        max_deal_size = max_deal_size / ((orderbook_buy['asks'][0][0] + orderbook_buy['bids'][0][0]) / 2)
        await self.create_orders(buy_exch, sell_exch, max_deal_size, time_start, time_parser, time_choose)

    async def create_orders(self, buy_exch, sell_exch, max_deal_size, time_start, time_parser, time_choose):
        orderbook_sell, orderbook_buy = self.get_orderbooks(self.clients[sell_exch], self.clients[buy_exch])
        expect_buy_px = orderbook_buy['asks'][0][0]
        expect_sell_px = orderbook_sell['bids'][0][0]
        shift = self.shifts[sell_exch + ' ' + buy_exch] / 2
        price_buy = (orderbook_buy['asks'][0][0] * (1 - shift))
        price_sell = (orderbook_sell['bids'][0][0] * (1 + shift))
        price_buy_limit_taker = price_buy * self.shifts['TAKER']
        price_sell_limit_taker = price_sell / self.shifts['TAKER']
        # time_first_part = time.time() - time_start - time_parser - time_choose
        # print(f"First part of create orders func time: {time_first_part} sec")
        timer = time.time()
        # a = []
        # if buy_exch in self.exchanges:
            # print(buy_exch)
        data = [[sell_exch, 'sell', price_sell_limit_taker, self._loop_1],
                [buy_exch, 'buy', price_buy_limit_taker, self._loop_2]]
        for exch, side, price, _loop in data:
            threading.Thread(target=self.run_create_order, args=[exch, max_deal_size, price, side, _loop]).start()
        print(f"FULL POOL ADDING AND CALLING TIME: {time.time() - timer}")
        # print(f"Pool call time: {time.time() - time_start - time_parser - time_choose - time_first_part - time_adding}")
        deal_time = time.time() - time_start - time_parser - time_choose
        await self.deal_details(buy_exch, sell_exch, expect_buy_px, expect_sell_px, max_deal_size, deal_time, time_parser, time_choose)
        # a.append(self.loop.create_task(self.clients[buy_exch].create_order(amount=max_deal_size,
        #                                                                    price=price_buy_limit_taker,
        #                                                                    side='buy',
        #                                                                    session=self.session)))
        # self.pool.add(self.clients[buy_exch].create_order(amount=max_deal_size,
        #                                                   price=price_buy_limit_taker,
        #                                                   side='buy',
        #                                                   session=self.session))
        # if sell_exch in self.exchanges:
            # print(sell_exch)
        # a.append(self.loop.create_task(self.clients[sell_exch].create_order(amount=max_deal_size,
        #                                                                     price=price_sell_limit_taker,
        #                                                                     side='sell',
        #                                                                     session=self.session)))
        # self.pool.add(self.clients[buy_exch].create_order(amount=max_deal_size,
        #                                                   price=price_buy_limit_taker,
        #                                                   side='buy',
        #                                                   session=self.session))

        # await asyncio.gather(*a)
        # self.pool.add(self.clients[sell_exch].create_order(amount=max_deal_size, price=price_sell_limit_taker, side='sell'))
        # time_adding = time.time() - time_start - time_parser - time_choose - time_first_part
        # print(f"Pool adding time: {time_adding}")
        # self.pool.call_all()
    def run_create_order(self, exchange, amount, price, side, _loop):
        _loop.run_until_complete(self.clients[exchange].create_order(amount=abs(amount),
                                                                     price=abs(price),
                                                                     side=side,
                                                                     session=self.session))

    async def deal_details(self, buy_exch, sell_exch, expect_buy_px, expect_sell_px, deal_size, deal_time, time_parser, time_choose):
        orderbook_sell, orderbook_buy = self.get_orderbooks(self.clients[sell_exch], self.clients[buy_exch])
        time.sleep(self.deal_pause)
        await self.send_data_for_base(buy_exch,
                                      sell_exch,
                                      expect_buy_px,
                                      expect_sell_px,
                                      deal_size,
                                      orderbook_sell['asks'][0][0],
                                      orderbook_buy['bids'][0][0],
                                      deal_time,
                                      time_parser,
                                      time_choose
                                      )
        # self.report_message(to_base,
        #                     orderbook_sell['asks'][0][0],
        #                     orderbook_buy['bids'][0][0],
        #                     deal_time,
        #                     time_parser,
        #                     time_choose)
    async def publish_message(self, connect, message, routing_key, exchange_name, queue_name):
        try:
            channel = await connect.channel()
            exchange = await channel.declare_exchange(exchange_name, type=ExchangeType.DIRECT, durable=True)
            queue = await channel.declare_queue(queue_name, durable=True)
            await queue.bind(exchange, routing_key=routing_key)
            message_body = orjson.dumps(message)
            message = Message(message_body)
            await exchange.publish(message, routing_key=routing_key)
            await channel.close()
            return True
        except Exception as e:
            traceback.print_exc()
            print(e)
            if 'RuntimeError' in str(e):
                print(f"RABBIT MQ RESTART")
                await self.setup_mq(self.loop)

    # def report_message(self, to_base, sell_ob_ask, buy_ob_bid, deal_time, time_parser, time_choose):
    #     message = f"{to_base['deal_type']} ORDER EXECUTED\n{to_base['sell_exch']}- | {to_base['buy_exch']}+\n"
    #     message += f"SERVER SIDE: {self.server_side}\n"
    #     message += f"CREATE ORDERS TIME, SEC: {round(deal_time, 6)}\n"
    #     message += f"PARSE TIME, SEC: {round(time_parser, 6)}\n"
    #     message += f"CHOOSE DEAL TIME, SEC: {round(time_choose, 6)}\n"
    #     message += f"TIME: {datetime.datetime.fromtimestamp(int(float(round(to_base['timestamp']))))} \n"
    #     message += f"SELL PX: {round(to_base['sell_px'], 2)}\n"
    #     message += f"EXPECTED SELL PX: {round(to_base['expect_sell_px'], 2)}\n"
    #     message += f"SELL EXCHANGE TOP ASK: {sell_ob_ask}\n"
    #     message += f"BUY PX: {to_base['buy_px']}\n"
    #     message += f"EXPECTED BUY PX: {to_base['expect_buy_px']}\n"
    #     message += f"BUY EXCHANGE TOP BID: {buy_ob_bid}\n"
    #     message += f"DEAL SIZE: {round(to_base['amount_coin'], 6)}\n"
    #     message += f"DEAL SIZE, USD: {round(to_base['amount_USD'])}\n"
    #     message += f"PROFIT REL, %: {round(to_base['profit_relative'] * 100, 4)}\n"
    #     message += f"PROFIT ABS, USD: {round(to_base['profit_USD'], 2)}\n"
    #     message += f"FEE SELL, %: {round(to_base['fee_sell'] * 100, 6)}\n"
    #     message += f"FEE BUY, %: {round(to_base['fee_buy'] * 100, 6)}\n"
    #     if to_base['buy_px'] == 0:
    #         message += f"WARNING! {to_base['buy_exch']} CLIENT DOESN'T CREATE ORDERS"
    #     elif to_base['sell_px'] == 0:
    #         message += f"WARNING! {to_base['sell_exch']} CLIENT DOESN'T CREATE ORDERS"
    #     self.send_message(self.chat_id, message)

    async def balance_message(self, exchange):
        if self.state == 'BOT':
            print(f"STARTED POSITION BALANCING")
            await self.position_balancing()
        client = self.clients[exchange]
        position = round(client.get_positions()[client.symbol]['amount'], 4)
        balance = round(client.get_real_balance())
        avl_buy = round(client.get_available_balance('buy'))
        avl_sell = round(client.get_available_balance('sell'))
        orderbook = client.get_orderbook()[client.symbol]
        to_base = {
            'timestamp': int(round(time.time() * 1000)),
            'exchange_name': exchange,
            # 'side': 'sell' if position <= 0 else 'long',
            'total_balance': balance,
            'position': position,
            'available_for_buy': avl_buy,
            'available_for_sell': avl_sell,
            'ask': orderbook['asks'][0][0],
            'bid': orderbook['bids'][0][0],
            'symbol': client.symbol
        }
        # print(to_base)
        # await self.publish_message(self.mq,
        #                            to_base,
        #                            'logger.event.insert_balance_check',
        #                            'logger.event',
        #                            'logger.event.insert_balance_check')
        # message = f'BALANCES AND POSITIONS\nSERVER SIDE: {self.server_side}\n'
        # symbol = self.clients['DYDX'].symbol.split('-')
        # for_base = ''
        # for i in symbol:
        #     for_base += i
        # total_position = 0
        # total_balance = 0
        # index_price = []
        # for exchange, client in self.clients.items():
        #     coin = client.symbol.split('USD')[0].replace('-', '').replace('/', '')
        #     message += f"   EXCHANGE: {exchange}\n"
        #     message += f"TOT BAL: {round(client.get_real_balance())} USD\n"
        #     message += f"POS: {round(client.get_positions()[client.symbol]['amount'], 4)} {coin}\n"
        #     message += f"AVL BUY: {round(client.get_available_balance('buy'))}\n"
        #     message += f"AVL SELL: {round(client.get_available_balance('sell'))}\n"
        #     ob = client.get_orderbook()[client.symbol]
        #     index_price.append((ob['bids'][0][0] + ob['asks'][0][0]) / 2)
        #     total_position += client.get_positions()[client.symbol]['amount']
        #     total_balance += client.get_real_balance()
        # index_price = sum(index_price) / len(index_price)
        # message += f"   TOTAL:\n"
        # message += f"BALANCE: {round(total_balance)} USD\n"
        # message += f"POSITION: {round(total_position, 4)} {coin}\n"
        # # last_timestamp = self.database.fetch_data_from_table(f'deals_{for_base}')
        # # last_timestamp = 0 if not len(last_timestamp) else last_timestamp[-1][1]
        # # min_to_last_deal = round((time.time() - last_timestamp) / 60)
        # # message += f"LAST DEAL WAS {min_to_last_deal} MIN BEFORE\n"
        # message += f"INDEX PX: {round(index_price, 2)} USD\n"
        # self.send_message(self.chat_id, message)

    async def send_data_for_base(self,
                            buy_exch,
                            sell_exch,
                            expect_buy_px,
                            expect_sell_px,
                            deal_size,
                            sell_ob_ask,
                            buy_ob_bid,
                            deal_time,
                            time_parser,
                            time_choose):
        price_buy = self.clients[buy_exch].get_last_price('buy')
        price_sell = self.clients[sell_exch].get_last_price('sell')
        orderbook = self.clients[buy_exch].get_orderbook()[self.clients[buy_exch].symbol]
        change = ((orderbook['asks'][0][0] + orderbook['bids'][0][0]) / 2)
        if price_buy != 0 and price_sell != 0:
            real_profit = (price_sell - price_buy) / price_buy
            real_profit = real_profit - (self.clients[sell_exch].taker_fee + self.clients[buy_exch].taker_fee)
            real_profit_usd = real_profit * deal_size * change
        else:
            real_profit = 0
            real_profit_usd = 0
        if self.clients[buy_exch].get_positions()[self.clients[buy_exch].symbol]['side'] == 'LONG':
            long = buy_exch
        else:
            long = sell_exch
        to_base = {
            'timestamp': int(round(time.time() * 1000)),
            'sell_exch': sell_exch,
            'buy_exch': buy_exch,
            'sell_px': price_sell,
            'expect_sell_px': expect_sell_px,
            'buy_px': price_buy,
            'expect_buy_px': expect_buy_px,
            'amount_USD': deal_size * change,
            'amount_coin': deal_size,
            'profit_USD': real_profit_usd,
            'profit_relative': real_profit,
            'fee_sell': self.clients[sell_exch].taker_fee,
            'fee_buy': self.clients[buy_exch].taker_fee,
            'long_side': long,
            'sell_ob_ask': sell_ob_ask,
            'buy_ob_bid': buy_ob_bid,
            'deal_time': deal_time,
            'time_parser': time_parser,
            'time_choose': time_choose}
        # await self.publish_message(self.mq,
        #                            to_base,
        #                            'logger.event.insert_deals_reports',
        #                            'logger.event',
        #                            'logger.event.insert_deals_reports')

    @staticmethod
    def balancing_data_for_base(exchange, side, price, fee, size, size_usd):
        to_base = {
            'timestamp': time.time(),
            'side': side,
            'exchange': exchange,
            'price': price,
            'size': size,
            'size_usd': size_usd,
            'fee': fee
        }
        return to_base

    def avail_balance_define(self, buy_exch, sell_exch):
        avl_bal_buy = self.clients[buy_exch].get_available_balance('buy')
        avl_bal_sell = self.clients[sell_exch].get_available_balance('sell')
        return min(avl_bal_buy, avl_bal_sell, self.max_order_size)

    def __rates_update(self):
        message = ''
        for exchange, client in self.clients.items():
            message += f"{exchange} | {client.get_orderbook()[client.symbol]['asks'][0][0]}\n"
        with open('rates.txt', 'a') as file:
            file.write(message + '\n')

    def _update_log(self, sell_exch, buy_exch, orderbook_buy, orderbook_sell):
        message = f"{buy_exch} BUY: {orderbook_buy['asks'][0]}\n"
        message += f"{sell_exch} SELL: {orderbook_sell['bids'][0]}\n"
        shift = self.shifts[sell_exch + ' ' + buy_exch] / 2
        message += f"Shifts: {sell_exch}={shift}, {buy_exch}={-shift}\n"
        message += f"Max deal size: {self.available_balances[f'+{buy_exch}-{sell_exch}']} USD\n"
        message += f"Datetime: {datetime.datetime.now()}\n\n"
        if message == self.last_message:
            return
        with open('arbi.txt', 'a') as file:
            file.write(message)
            self.last_message = message

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
                if func_time > 0.001:
                    print(f"GET ORDERBOOKS FUNC TIME: {func_time} sec")
                return orderbook_sell, orderbook_buy
            except Exception as e:
                print(f"Exception with orderbooks: {e}")

    def start_message(self):
        exc_str = ''
        exs = [x for x in self.clients.keys()]
        for i in exs:
            if exc_str == '':
                exc_str += i
            else:
                exc_str += '|' + i
        message = f'MULTIBOT STARTED\n{exc_str}\n'
        for i in self.cp['SETTINGS']:
            if 'shift' in i:
                continue
            message += f"{i}: {cp['SETTINGS'][i]}\n"
        for exchange, shift in self.shifts.items():
            message += f"{exchange}: {round(shift, 6)}\n"
        self.send_message(self.chat_id, message)

    async def time_based_messages(self):
        time_from = (int(round(time.time())) - 10 - self.start_time) % 180
        if not time_from:
            for exchange in self.clients.keys():
                await self.balance_message(exchange)
            self.start_time -= 1
        if ' 09:00:00' in str(datetime.datetime.now()):
            self.daily_report()
            time.sleep(1)

    @staticmethod
    def create_result_message(deals_potential: dict, deals_executed: dict, time: int) -> str:
        message = f"For last {time / 60} min:"
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

    def potential_real_deals(self, sell_exch, buy_exch, orderbook_buy, orderbook_sell):
        if not (int(round(time.time())) - self.start_time) % 15:
            deals_potential = {'SELL': {x: 0 for x in self.exchanges}, 'BUY': {x: 0 for x in self.exchanges}}
            for x in self.deals_counter:
                sell_exch = x['sell_exch']
                buy_exch = x['buy_exch']
                deals_potential['SELL'][sell_exch] += 1
                deals_potential['BUY'][buy_exch] += 1
            deals_executed = {'SELL': {x: 0 for x in self.exchanges}, 'BUY': {x: 0 for x in self.exchanges}}
            for x in self.deals_executed:
                sell_exch = x[0].split('-')[1]
                buy_exch = x[0].split('-')[0].split('+')[1]
                deals_executed['SELL'][sell_exch] += 1
                deals_executed['BUY'][buy_exch] += 1
            self.__rates_update()
            self._update_log(sell_exch, buy_exch, orderbook_buy, orderbook_sell)
            if not (int(round(time.time())) - self.start_time) % 600:
                message = self.create_result_message(deals_potential, deals_executed, 600)
                self.send_message(self.chat_id, message)
                self.deals_counter = []
                self.deals_executed = []
            self.start_time -= 1

    async def setup_mq(self, loop):
        print(f"SETUP MQ START")
        self.mq = await connect_robust(self.rabbit_url, loop=loop)
        print(f"SETUP MQ ENDED")

    async def run(self, loop):
        self.loop = loop
        # await self.setup_mq(loop)
        self.start_message()
        print("CYCLE START")
        time.sleep(3)
        self.session = aiohttp.ClientSession()
        while True:
            time.sleep(0.005)
            if self.state == 'PARSER':
                time.sleep(1)
            await self.find_price_diffs()
            await self.time_based_messages()
            if (int(round(time.time())) - self.start_time) == 25:
                await self.position_balancing()
                await self.execute_deal('DYDX',
                                        'BITMEX',
                                         {'asks': [[27800, 0.1]], 'bids': [[27700, 0.1]]},
                                         0.0001,
                                         0.0001,
                                         0.0001)
                # await self.position_balancing()
                # self.execute_deal(buy_exch='DYDX',
                #                   sell_exch='OKEX',
                #                   orderbook_buy=self.clients['DYDX'].get_orderbook()[self.clients['DYDX'].symbol],
                #                   profit=0.0001)

#
# bot = MultiBot()
# bot.run()



if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    worker = MultiBot()
    loop.run_until_complete(worker.run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()

