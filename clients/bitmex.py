import asyncio
from datetime import datetime
import json
import math
import threading
import time
import urllib.parse
from pprint import pprint

import aiohttp
from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient

from core.base_client import BaseClient
from core.enums import ResponseStatus
from tools.APIKeyAuthenticator import APIKeyAuthenticator as auth


# Naive implementation of connecting to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without polling the hell out of the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll really often if it wants.

class BitmexClient(BaseClient):
    BASE_WS = 'wss://ws.bitmex.com/realtime'
    BASE_URL = 'https://www.bitmex.com'
    EXCHANGE_NAME = 'BITMEX'
    MAX_TABLE_LEN = 200

    def __init__(self, keys, leverage=2):
        self._loop = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self.leverage = leverage
        self.symbol = keys['symbol']
        self.api_key = keys['api_key']
        self.api_secret = keys['api_secret']
        self.pos_power = 6 if 'USDT' in self.symbol else 8
        self.currency = 'USDt' if 'USDT' in self.symbol else 'XBt'

        self.auth = auth(self.BASE_URL, self.api_key, self.api_secret)
        self.data = {}
        self.keys = {}
        self.exited = False
        self.swagger_client = self.swagger_client_init()

        commission = self.swagger_client.User.User_getCommission().result()[0]
        self.taker_fee = commission[self.symbol]['takerFee']
        self.maker_fee = commission[self.symbol]['makerFee']

        self.wst = threading.Thread(target=self._run_ws_forever, daemon=True)
        self.tick_size = None
        self.step_size = None
        self.quantity_precision = None
        self.price_precision = 0
        self.time_sent = time.time()

    def run_updater(self):
        self.wst.start()
        self.__wait_for_account()
        self.get_contract_price()

        self.tick_size = self.get_instrument()['tick_size']
        self.step_size = self.get_instrument()['step_size']
        self.quantity_precision = len(str(self.step_size).split('.')[1]) if '.' in str(self.step_size) else 1

    def _run_ws_forever(self):
        while True:
            try:
                self._loop.run_until_complete(self._run_ws_loop())
            finally:
                print("WS loop completed. Restarting")

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not {'instrument', 'margin', 'order', 'position', 'orderBook10'} <= set(self.data):
            time.sleep(0.1)

    async def _run_ws_loop(self):
        async with aiohttp.ClientSession(headers=self.__get_auth('GET', '/realtime')) as s:
            async with s.ws_connect(self.__get_url()) as ws:
                print("Bitmex: connected")
                self._connected.set()
                try:
                    self._ws = ws
                    async for msg in ws:
                        self._process_msg(msg)
                except Exception as e:
                    print("Bitmex ws loop exited: ", e)
                finally:
                    self._connected.clear()

    def _process_msg(self, msg: aiohttp.WSMessage):
        if msg.type == aiohttp.WSMsgType.TEXT:
            message = json.loads(msg.data)
            table = message.get("table")
            action = message.get("action")
            if action:
                if table not in self.data and table == 'orderBook10':
                    self.data[table] = {}
                elif table not in self.data:
                    self.data[table] = []
                if action == 'partial':
                    self.keys[table] = message['keys']
                    if table == 'orderBook10':
                        message['data'][0].update({'timestamp': time.time()})
                        symbol = message['filter']['symbol']
                        self.data[table].update({symbol: message['data'][0]})
                    else:
                        self.data[table] = message['data']
                elif action == 'insert':
                    self.data[table] += message['data']
                    if table == 'order':
                        # print(f"BITMEX PROCESS MSG: {message}")
                        timestamp = self.timestamp_from_date(message['data'][0]['transactTime'])
                        print(f'BITMEX ORDER PLACE TIME: {timestamp - self.time_sent} sec')
                    # Limit the max length of the table to avoid excessive memory usage.
                    # Don't trim orders because we'll lose valuable state if we do.
                    if table not in ['order', 'orderBook10'] and len(self.data[table]) > self.MAX_TABLE_LEN:
                        self.data[table] = self.data[table][self.MAX_TABLE_LEN // 2:]
                elif action == 'update':
                    # Locate the item in the collection and update it.
                    for updateData in message['data']:
                        if table == 'orderBook10':
                            updateData.update({'timestamp': time.time()})
                            symbol = updateData['symbol']
                            self.data[table].update({symbol: updateData})
                        elif table == 'trade':
                            self.data[table].insert(0, updateData)
                        elif table == 'execution':
                            self.data[table].insert(0, updateData)
                        else:
                            item = self.find_by_keys(self.keys[table], self.data[table], updateData)
                            if not item:
                                return  # No item found to update. Could happen before push
                            item.update(updateData)
                            # Remove cancelled / filled orders
                            if table == 'order' and not self.order_leaves_quantity(item):
                                self.data[table].remove(item)
                elif action == 'delete':
                    # Locate the item in the collection and remove it.
                    for deleteData in message['data']:
                        item = self.find_by_keys(self.keys[table], self.data[table], deleteData)
                        self.data[table].remove(item)

    @staticmethod
    def timestamp_from_date(date: str):
        # date = '2023-02-15T02:55:27.640Z'
        ms = int(date.split(".")[1].split('Z')[0]) / 1000
        return time.mktime(datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()) + ms

    @staticmethod
    def order_leaves_quantity(o):
        if o['leavesQty'] is None:
            return True
        return o['leavesQty'] > 0

    @staticmethod
    def find_by_keys(keys, table, matchData):
        for item in table:
            if all(item[k] == matchData[k] for k in keys):
                return item

    def swagger_client_init(self, config=None):
        if config is None:
            # See full config options at http://bravado.readthedocs.io/en/latest/configuration.html
            config = {
                # Don't use models (Python classes) instead of dicts for #/definitions/{models}
                'use_models': False,
                # bravado has some issues with nullable fields
                'validate_responses': False,
                # Returns response in 2-tuple of (body, response); if False, will only return body
                'also_return_response': True,
            }
        spec_uri = self.BASE_URL + '/api/explorer/swagger.json'
        request_client = RequestsClient()
        request_client.authenticator = self.auth
        return SwaggerClient.from_url(spec_uri, config=config, http_client=request_client)

    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self.ws.close()

    def get_instrument(self):
        """Get the raw instrument data for this symbol."""
        # Turn the 'tick_size' into 'tickLog' for use in rounding
        instrument = self.data['instrument'][0]
        print(instrument)
        instrument['tick_size'] = instrument['tickSize']
        instrument['step_size'] = instrument['lotSize']
        return instrument

    def funds(self):
        """Get your margin details."""
        return self.data['margin']

    def open_orders(self):
        """Get all your open orders."""
        return self.data['order']

    def recent_trades(self):
        """Get recent trades."""
        return self.data['trade']

    def fit_price(self, price):
        if not self.price_precision:
            if '.' in str(self.tick_size):
                round_price_len = len(str(self.tick_size).split('.')[1])
            else:
                round_price_len = 0
            price = round(price - (price % self.tick_size), round_price_len)
        else:
            price = float(round(float(round(price / self.tick_size) * self.tick_size), self.price_precision))
        return price

    def fit_amount(self, amount):
        orderbook = self.get_orderbook()[self.symbol]
        change = (orderbook['asks'][0][0] + orderbook['bids'][0][0]) / 2
        amount = amount * change
        if self.symbol == 'XBTUSD':
            amount = int(round(amount - (amount % self.step_size)))
        else:
            amount = int(round(amount / self.contract_price))
            amount = int(round(amount - amount % self.step_size))
        return amount

    async def create_order(self, amount: float, price: float, side: str, session: aiohttp.ClientSession,
                           expire: int = 100, client_id=None):
        self.time_sent = time.time()
        price = self.fit_price(price)
        amount = self.fit_amount(amount)
        body = {
            "symbol": self.symbol,
            "ordType": "Limit",
            "price": price,
            "orderQty": amount,
            "side": side.capitalize()
        }
        if client_id is not None:
            body["clOrdID"] = client_id

        res = await self._post("/api/v1/order", body, session)
        timestamp = 0000000000000
        if res.get('errors'):
            status = ResponseStatus.ERROR
        elif res.get('order') and res['order'].get('status'):
            timestamp = int(
                datetime.timestamp(datetime.strptime(res['order']['createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ')) * 1000)
            status = ResponseStatus.SUCCESS
            self.LAST_ORDER_ID = res['orderID']
        else:
            status = ResponseStatus.NO_CONNECTION

        return {
            'exchange_name': self.EXCHANGE_NAME,
            'timestamp': timestamp,
            'status': status
        }


    async def _post(self, path: str, data: any, session: aiohttp.ClientSession):
        headers_body = f"symbol={data['symbol']}&side={data['side']}&ordType=Limit&orderQty={data['orderQty']}&price={data['price']}"
        headers = self.__get_auth("POST", path, headers_body)
        headers.update(
            {
                "Content-Length": str(len(headers_body.encode('utf-8'))),
                "Content-Type": "application/x-www-form-urlencoded"}
        )
        async with session.post(url=self.BASE_URL + path, headers=headers, data=headers_body) as resp:
            return await resp.json()

    def change_order(self, amount, price, id):
        if amount:
            self.swagger_client.Order.Order_amend(orderID=id, orderQty=amount, price=price).result()
        else:
            self.swagger_client.Order.Order_amend(orderID=id, price=price).result()

    def cancel_all_orders(self, orderID=None):
        self.swagger_client.Order.Order_cancel(orderID=orderID).result()

    def __get_auth(self, method, uri, body=''):
        """
        Return auth headers. Will use API Keys if present in settings.
        """
        # To auth to the WS using an API key, we generate a signature of a nonce and
        # the WS API endpoint.
        expires = str(int(round(time.time()) + 100))
        return {
            "api-expires": expires,
            "api-signature": self.auth.generate_signature(self.api_secret, method, uri, expires, body),
            "api-key": self.api_key,
        }

    def __get_url(self):
        """
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        """
        # Some subscriptions need to xhave the symbol appended.
        subscriptions_full = map(lambda sub: (
            sub if sub in ['account', 'affiliate', 'announcement', 'connected', 'chat', 'insurance', 'margin',
                           'publicNotifications', 'privateNotifications', 'transact', 'wallet']
            else (sub + ':' + self.symbol)
        ), ['execution', 'instrument', 'margin', 'order', 'position', 'trade', 'orderBook10'])
        urlParts = list(urllib.parse.urlparse(self.BASE_WS))
        urlParts[2] += "?subscribe={}".format(','.join(subscriptions_full))
        urlParts[2] += ',orderBook10:XBTUSD'

        return urllib.parse.urlunparse(urlParts)

    def get_pnl(self):
        positions = self.positions()
        pnl = [x for x in positions if x['symbol'] == self.symbol]
        pnl = None if not len(pnl) else pnl[0]
        if not pnl:
            return [0, 0, 0]
        multiplier_power = 6 if pnl['currency'] == 'USDt' else 8
        change = 1 if pnl['currency'] == 'USDt' else self.get_orderbook()['XBTUSD']['bids'][0][0]
        realized_pnl = pnl['realisedPnl'] / 10 ** multiplier_power * change
        unrealized_pnl = pnl['unrealisedPnl'] / 10 ** multiplier_power * change
        return [realized_pnl + unrealized_pnl, pnl, realized_pnl]

    def get_last_price(self, side):
        side = side.capitalize()
        # last_trades = self.recent_trades()
        last_trades = self.data['execution']
        last_price = 0
        for trade in last_trades:
            if trade['side'] == side and trade['symbol'] == self.symbol and trade.get('avgPx'):
                last_price = trade['avgPx']
        return last_price

    def get_real_balance(self):
        currency = 'XBt' if not 'USDT' in self.symbol else 'USDt'
        orderbook_btc = self.get_orderbook()['XBTUSD']
        change = 1 if currency == 'USDt' else (orderbook_btc['asks'][0][0] + orderbook_btc['bids'][0][0]) / 2
        transactions = None
        while not transactions:
            try:
                transactions = self.swagger_client.User.User_getWalletHistory(currency=currency).result()
            except:
                pass
        real = transactions[0][0]['marginBalance'] if transactions[0][0]['marginBalance'] else transactions[0][0][
            'walletBalance']
        real = (real / 10 ** self.pos_power) * change
        return real

    def get_available_balance(self, side):
        if 'USDT' in self.symbol:
            funds = [x for x in self.funds() if x['currency'] == 'USDt'][0]
            change = 1
        else:
            funds = [x for x in self.funds() if x['currency'] == 'XBt'][0]
            change = self.get_orderbook()['XBTUSD']['bids'][0][0]
        positions = self.get_positions()
        wallet_balance = (funds['walletBalance'] / 10 ** self.pos_power) * change
        available_balance = wallet_balance * self.leverage
        wallet_balance = wallet_balance if self.symbol == 'XBTUSD' else 0
        position_value = 0
        for symbol, position in positions.items():
            if symbol == self.symbol:
                if position['foreignNotional']:
                    position_value = position['homeNotional'] * position['markPrice']
                    self.contract_price = abs(position_value / position['currentQty'])
                else:
                    print('ERROR')
                    print(position)
        if side == 'buy':
            max_ask = self.get_orderbook()[self.symbol]['asks'][0][1] * self.contract_price
            return min(available_balance - position_value - wallet_balance, max_ask)
        else:
            max_bid = self.get_orderbook()[self.symbol]['bids'][0][1] * self.contract_price
            return min(available_balance + position_value + wallet_balance, max_bid)

    def get_positions(self):
        '''Get your positions.'''
        pos_bitmex = {x['symbol']: x for x in self.data['position']}
        for symbol, position in pos_bitmex.items():
            if position['homeNotional'] >= 0:
                pos_bitmex[symbol].update({'side': 'LONG', 'amount': position['homeNotional']})
            else:
                pos_bitmex[symbol].update({'side': 'SHORT', 'amount': position['homeNotional']})
        if self.symbol == 'XBTUSD':
            pos_xbt = pos_bitmex.get('XBTUSD')
            if pos_xbt:
                bal_bitmex = [x for x in self.funds() if x['currency'] == self.currency][0]
                xbt_extra_pos = bal_bitmex['walletBalance'] / 10 ** self.pos_power
                pos_bitmex['XBTUSD']['amount'] += xbt_extra_pos
        return pos_bitmex

    def get_orderbook(self):
        return self.data['orderBook10']

    def get_xbt_pos(self):
        bal_bitmex = [x for x in self.funds() if x['currency'] == 'XBt'][0]
        xbt_pos = bal_bitmex['walletBalance'] / 10 ** 8
        return xbt_pos

    def get_contract_price(self):
        # self.__wait_for_account()
        instrument = self.get_instrument()
        self.contract_price = instrument['foreignNotional24h'] / instrument['volume24h']

# EXAMPLES:

# import configparser
# import sys
#
# cp = configparser.ConfigParser()
# if len(sys.argv) != 2:
#     sys.exit(1)
# cp.read(sys.argv[1], "utf-8")
# keys = cp
# keys = {'BITMEX': {'api_key': 'HKli7p7qxcsqQmsYvR-fDDmM',
#                    'api_secret': '28Dt_sLKMaGpbM2g-EI-NI1yT_mm880L56H_PhfAZ1Jug_R1',
#                    'symbol': 'XBTUSD'}}
# #
# # #
# api_key = keys["BITMEX"]["api_key"]
# api_secret = keys["BITMEX"]["api_secret"]
# bitmex_client = BitmexClient(keys['BITMEX'])
# bitmex_client.run_updater()
# #
# time.sleep(1)
#
# print(bitmex_client.get_positions())
# # loop = asyncio.new_event_loop()
# async def func():
#     async with aiohttp.ClientSession() as session:
#         await bitmex_client.create_order(0.01, 30000, 'sell', session)
# asyncio.run(func())

# time.sleep(1)
# open_orders = bitmex_client.swagger_client.Order.Order_getOrders(symbol='XBTUSD', reverse=True).result()[0]
# # print(f"{open_orders=}")
#
# for order in open_orders:
#     bitmex_client.cancel_order(order['orderID'])


# # # #
# # # /position/leverage
# # time.sleep(3)
# # funding = bitmex_client.swagger_client.Position.Position_updateLeverage(symbol='ETHUSD', leverage=10).result()
# # print(funding)
# # time.sleep(3)
# # time.sleep(1)
# # print(bitmex_client.get_positions())
# time.sleep(2)
# # print(f"BUY {bitmex_client.get_last_price('buy')}")
# while True:
#     bitmex_client.create_order(0.01, 22500, 'sell')
# time.sleep(2)
# print(f"SELL {bitmex_client.get_last_price('sell')}")

#     time.sleep(1)
#     orders = bitmex_client.open_orders()
#     for order in orders:
#         print(orders)
#         bitmex_client.cancel_order(order['orderID'])
#     print(bitmex_client.open_orders())
#     # print(f"BALANCE {bitmex_client.get_real_balance()}")
#     # print(f"POSITION {bitmex_client.get_positions()}")
#     print(f"BUY {bitmex_client.get_available_balance('buy')}")
#     print(f"SELL {bitmex_client.get_available_balance('sell')}")
#     print(f'\n\n')

# print(bitmex_client.funds())
# # bitmex_client.get_real_balance()
# while True:
#     print(bitmex_client.get_orderbook())
#     time.sleep(1)

# funding = bitmex_client.swagger_client.Funding.Funding_get(symbol='XBTUSD').result()
# print(funding)


# bal_bitmex = [x for x in self.client_Bitmex.funds() if x['currency'] == currency][0]


# while True:
#     pos_bitmex = [x for x in bitmex_client.positions() if x['symbol'] == 'XBTUSDT'][0]
#     side = 'Buy' if pos_bitmex['currentQty'] < 0 else 'Sell'
#     size = abs(pos_bitmex['currentQty'])
#     open_orders = bitmex_client.open_orders(clOrdIDPrefix='')
#     price = orderbook['asks'][0][0] if side == 'Sell' else orderbook['bids'][0][0]
#     exist = False
#     for order in open_orders:
#         if 'CANCEL' in order['clOrdID']:
#             if price != order['price']:
#                 bitmex_client.change_order(size, price, order['orderID'])
#                 print(f"Changed {size}")
#             exist = True
#             break
#     if exist:
#         continue
#     bitmex_client.create_order(size, price, side, 'Limit', 'CANCEL')

# print(bitmex_client.get_available_balance('Sell'))
# print(bitmex_client.get_available_balance('Buy'))
# # while True:
#     # orders = bitmex_client.swagger_client.Order.Order_getOrders(symbol='XBTUSD', reverse=True).result()[0]
# a = bitmex_client.get_instrument()
# print(a)
# print(f"volume24h: {a['volume24h']}")
# print(f"homeNotional24h: {a['homeNotional24h']}")
# print(f"turnover24h: {a['turnover24h']}")
# print(f"foreignNotional24h: {a['foreignNotional24h']}")
# contract_price = a['foreignNotional24h'] / a['volume24h']
# print(contract_price)
# orders = bitmex_client.swagger_client.User.User_getExecutionHistory(symbol='XBTUSD', timestamp=date).result()
# if len(orders[0]):
#     print(orders[0])
#     break
# for order in orders[0]:
#     print(f"Time: {order['transactTime']}")
#     print(f"Order ID: {order['clOrdID']}")
#     # print(f"Realized PNL: {order['realisedPnl'] / 100000000} USD")
#     print(f"Side: {order['side']}")
#     print(f"Order size: {order['orderQty']} USD")
#     print(f"Price: {order['price']}")
#     print()
# orders = bitmex_client.swagger_client.Settlement.Settlement_get(symbol='XBTUSDT',).result()
# orders = bitmex_client.swagger_client.User.User_getWalletHistory(currency='USDt',).result()
# instruments = bitmex_client.swagger_client.Instrument.Instrument_getActiveAndIndices().result()
# print(instruments)
# for instrument in instruments[0]:
#     print(instrument['symbol'])
# time.sleep(1)
# orderbook = bitmex_client.get_orderbook()()['XBT/USDT']
# print(orderbook)
# print(orders)
# money = bitmex_client.funds()
# print(money)
#     bitmex_client.create_order(size, price, side, 'Limit', 'CANCEL')
# bitmex_client.create_order(1000, 12000, 'Buy', 'Limit', 'CANCEL1')
# time.sleep(1)

#     print(order)


# orders = bitmex_client.open_orders('')
# print(orders)

# TRANZACTION HISTORY
# orders = bitmex_client.swagger_client.User.User_getWalletHistory(currency='USDt',).result()
#
# for tranz in orders[0]:
#     print("TRANZ:" + tranz['transactID'])
#     print("type:" + str(tranz['transactType']))
#     print("status:" + str(tranz['transactStatus']))
#     print("amount:" + str(tranz['amount'] / (10 ** 6)))
#     if tranz['fee']:
#         print("fee:" + str(tranz['fee'] / (10 ** 6)))
#     print("walletBalance:" + str(tranz['walletBalance'] / (10 ** 6)))
#     if tranz['marginBalance']:
#         print("marginBalance:" + str(tranz['marginBalance'] / (10 ** 6)))
#     print('Timestamp:' + str(tranz['timestamp']))
#     print()
# time.sleep(1)


# time.sleep(1)
# open_orders = bitmex_client.open_orders(clOrdIDPrefix='BALANCING')
# print(open_orders)

# open_orders_resp = [{'orderID': '20772f48-24a3-4cff-a470-670d51a1666e', 'clOrdID': 'BALANCING', 'clOrdLinkID': '', 'account': 2133275,
#   'symbol': 'XBTUSDT', 'side': 'Buy', 'simpleOrderQty': None, 'orderQty': 1000, 'price': 15000, 'displayQty': None,
#   'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '', 'currency': 'USDT', 'settlCurrency': 'USDt',
#   'ordType': 'Limit', 'timeInForce': 'GoodTillCancel', 'execInst': '', 'contingencyType': '', 'exDestination': 'XBME',
#   'ordStatus': 'New', 'triggered': '', 'workingIndicator': True, 'ordRejReason': '', 'simpleLeavesQty': None,
#   'leavesQty': 1000, 'simpleCumQty': None, 'cumQty': 0, 'avgPx': None, 'multiLegReportingType': 'SingleSecurity',
#   'text': 'Submitted via API.', 'transactTime': '2022-11-16T17:18:45.740Z', 'timestamp': '2022-11-16T17:18:45.740Z'}]
# bitmex_client.create_order(1000, 17000, 'Buy', 'Limit', 'BALANCING BTC2')
# time.sleep(1)
# print(commission.result())
# print(orders.objRef)
# print(orders.op)
# print(orders.status)
# bitmex_client.cancel_order(id)
# print(bitmex_client.data['execution'])
# print(bitmex_client.recent_trades())
# print(bitmex_client.funds()[1])
# print(bitmex_client.get_orderbook()())

#   [{'orderID': 'baf6fc1e-8f76-4090-a3f3-254314da86b4', 'clOrdID': 'BALANCING BTC', 'clOrdLinkID': '', 'account': 2133275,
#   'symbol': 'XBTUSDT', 'side': 'Buy', 'simpleOrderQty': None, 'orderQty': 1000, 'price': 17000, 'displayQty': None,
#   'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '', 'currency': 'USDT', 'settlCurrency': 'USDt',
#   'ordType': 'Limit', 'timeInForce': 'GoodTillCancel', 'execInst': '', 'contingencyType': '', 'exDestination': 'XBME',
#   'ordStatus': 'New', 'triggered': '', 'workingIndicator': True, 'ordRejReason': '', 'simpleLeavesQty': None,
#   'leavesQty': 1000, 'simpleCumQty': None, 'cumQty': 0, 'avgPx': None, 'multiLegReportingType': 'SingleSecurity',
#   'text': 'Submitted via API.', 'transactTime': '2022-11-16T17:24:10.721Z', 'timestamp': '2022-11-16T17:24:10.721Z',
#   'lastQty': None, 'lastPx': None, 'lastLiquidityInd': '', 'tradePublishIndicator': '',
#   'trdMatchID': '00000000-0000-0000-0000-000000000000', 'execID': 'cee84b5e-3946-afe5-c1c7-f7d99945dfd7',
#   'execType': 'New', 'execCost': None, 'homeNotional': None, 'foreignNotional': None, 'commission': None,
#   'lastMkt': '', 'execComm': None, 'underlyingLastPx': None},
#    {'orderID': 'baf6fc1e-8f76-4090-a3f3-254314da86b4',
#   'clOrdID': 'BALANCING BTC', 'clOrdLinkID': '', 'account': 2133275, 'symbol': 'XBTUSDT', 'side': 'Buy',
#   'simpleOrderQty': None, 'orderQty': 1000, 'price': 17000, 'displayQty': None, 'stopPx': None, 'pegOffsetValue': None,
#   'pegPriceType': '', 'currency': 'USDT', 'settlCurrency': 'USDt', 'ordType': 'Limit', 'timeInForce': 'GoodTillCancel',
#   'execInst': '', 'contingencyType': '', 'exDestination': 'XBME', 'ordStatus': 'Filled', 'triggered': '',
#   'workingIndicator': False, 'ordRejReason': '', 'simpleLeavesQty': None, 'leavesQty': 0, 'simpleCumQty': None,
#   'cumQty': 1000, 'avgPx': 16519, 'multiLegReportingType': 'SingleSecurity', 'text': 'Submitted via API.',
#   'transactTime': '2022-11-16T17:24:10.721Z', 'timestamp': '2022-11-16T17:24:10.721Z', 'lastQty': 1000, 'lastPx': 16519,
#   'lastLiquidityInd': 'RemovedLiquidity', 'tradePublishIndicator': 'PublishTrade',
#   'trdMatchID': '15cd273d-ded8-e339-b3b1-9a9080b5d10f', 'execID': 'adcc6b75-2d57-a9d2-47c4-8db921d8aae1',
#   'execType': 'Trade', 'execCost': 16519000, 'homeNotional': 0.001, 'foreignNotional': -16.519,
#   'commission': 0.00022500045, 'lastMkt': 'XBME', 'execComm': 3716, 'underlyingLastPx': None}]
