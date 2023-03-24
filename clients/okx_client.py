import aiohttp
import asyncio
import time
import hmac
import base64
import json
import threading
import string
import datetime
import requests
import random
import queue

URI_WS_AWS = "wss://wsaws.okx.com:8443/ws/v5/public"
URI_WS_PRIVATE = "wss://ws.okx.com:8443/ws/v5/private"
URI_WS = "wss://ws.okx.com:8443/ws/v5/public"

class OkexClient:

    orderbook = {}
    orders = {}
    last_price = {'buy': 0, 'sell': 0}
    balance = {'free': 0, 'total': 0}
    taker_fee = 0
    start_time = int(time.time())
    time_sent = time.time()

    def __init__(self, keys, leverage=2):
        self.symbol = keys['symbol']
        self.leverage = leverage
        self.public_key = keys['public_key']
        self.secret_key = keys['secret_key']
        self.passphrase = keys['passphrase']
        self.positions = {self.symbol: {'side': 0, 'amount_usd': 0, 'amount': 0, 'entry_price': 0,
                                        'unrealized_pnl_usd': 0, 'realized_pnl_usd': 0, 'lever': self.leverage}}
        self._loop_public = asyncio.new_event_loop()
        self._loop_private = asyncio.new_event_loop()
        # self._loop_orders = asyncio.new_event_loop()
        self.queue = queue.Queue()
        self._connected = asyncio.Event()
        # self._connected_orders = asyncio.Event()
        # self._order = asyncio.Event()
        self.wst_public = threading.Thread(target=self._run_ws_forever, args=['public', self._loop_public])
        self.wst_private = threading.Thread(target=self._run_ws_forever, args=['private', self._loop_private])
        # self.wst_orders = threading.Thread(target=self._run_ws_orders, args=[self._loop_orders])
        self.instrument = self.get_instrument()
        self.ticksize = float(self.instrument['tickSz'])
        # self._get_fees()

    @staticmethod
    def id_generator(size=12, chars=string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def run_updater(self):
        self.wst_public.daemon = True
        self.wst_public.start()
        self.wst_private.daemon = True
        self.wst_private.start()
        # self.wst_orders.daemon = True
        # self.wst_orders.start()

    async def _login(self, ws, event):
        request_path = '/users/self/verify'
        timestamp = str(int(round(time.time())))
        signature = self.signature(timestamp, 'GET', request_path, None)
        msg = {"op": "login",
            "args": [{
                "apiKey": self.public_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": signature
            }]}
        await event.wait()
        await ws.send_json(msg)

    def signature(self, timestamp, method, request_path, body):
        if str(body) == '{}' or str(body) == 'None':
            body = ''
        message = str(timestamp) + str.upper(method) + request_path + str(body)

        mac = hmac.new(bytes(self.secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        signature = mac.digest()

        return base64.b64encode(signature).decode('UTF-8')

    async def _subscribe_orderbook(self):
        msg = {
        "op": "subscribe",
        "args": [{
            "channel": "bbo-tbt",#0-l2-tbt",
            "instId": self.symbol
            }]}
        await self._connected.wait()
        await self._ws_public.send_json(msg)

    async def _subscribe_account(self):
        msg = {
        "op": "subscribe",
        "args": [{
            "channel": "account"
            }]}
        await self._connected.wait()
        await self._ws_private.send_json(msg)

    async def _subscribe_positions(self):
        msg = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "positions",
                    "instType": "SWAP",
                    "instFamily": "BTC-USDT",
                    "instId": "BTC-USDT-SWAP"
                }
            ]
        }
        await self._connected.wait()
        await self._ws_private.send_json(msg)

    async def _subscribe_orders(self):
        msg = {
        "op": "subscribe",
        "args": [
            {
                "channel": "orders",
                "instType": "SWAP",
                "instFamily": "BTC-USDT",
                "instId": "BTC-USDT-SWAP"
            }
            ]
        }
        await self._connected.wait()
        await self._ws_private.send_json(msg)

    def _run_ws_forever(self, type, loop):
        while True:
            try:
                loop.run_until_complete(self._run_ws_loop(type))
            except Exception as e:
                print(f"Line 100. Error: {e}")
            finally:
                print(f"WS loop {type} completed. Restarting")

    # def _run_ws_orders(self, loop):
    #     while True:
    #         try:
    #             loop.run_until_complete(self._creating_orders_loop())
    #         except Exception as e:
    #             print(f"Line 100. Error: {e}")
    #         finally:
    #             print("WS loop orders completed. Restarting")

    async def _run_ws_loop(self, type):
        async with aiohttp.ClientSession() as s:
            if type == 'private':
                endpoint = URI_WS_PRIVATE
            else:
                endpoint = URI_WS_AWS
            async with s.ws_connect(endpoint) as ws:
                print(f"OKEX: connected {type}")
                self._connected.set()
                # try:
                if type == 'private':
                    self._ws_private = ws
                    self._loop_private.create_task(self._login(ws, self._connected))
                    async for msg in ws:
                        if json.loads(msg.data).get('event'):
                            break
                    self._loop_private.create_task(self._subscribe_account())
                    self._loop_private.create_task(self._subscribe_positions())
                    self._loop_private.create_task(self._subscribe_orders())
                else:
                    self._ws_public = ws
                    self._loop_public.create_task(self._login(ws, self._connected))
                    self._loop_public.create_task(self._subscribe_orderbook())
                async for msg in ws:
                    # print(time.time())
                    try:
                        await self._send_order(**self.queue.get_nowait())
                    except queue.Empty:
                        await self._ping(ws)
                    self._process_msg(msg)
                # except Exception as e:
                #     print("OKEX ws loop exited: ", e)
                #     await ws.ping('ping')
                # finally:
                #     self._connected.clear()

    async def _ping(self, ws):
        time_from = int(int(round(time.time())) - self.start_time) % 5
        if not time_from:
            await ws.ping(b'PING')
            self.start_time -= 1

    # async def _creating_orders_loop(self):
    #     async with aiohttp.ClientSession() as s:
    #         endpoint = URI_WS_PRIVATE
    #         async with s.ws_connect(endpoint) as ws:
    #             # try:
    #             self._connected_orders.set()
    #             self._ws_orders = ws
    #             self._loop_orders.create_task(self._login(ws, self._connected_orders))
    #             async for msg in ws:
    #                 if json.loads(msg.data).get('event'):
    #                     break
    #             while not self.get_orderbook().get(self.symbol):
    #                 time.sleep(0.1)
    #             orderbook = self.get_orderbook()[self.symbol]
    #             price = orderbook['asks'][0][0] * 1.01
    #             await self._loop_orders.create_task(self._send_order(0.01, price, 'buy', 100))
    #             while True:
    #                 await self._ping(ws)
    #                 if self._order.is_set():
    #                     self.time_sent = time.time() * 1000
    #                     # print(f"SENT time: {time.time()}")
    #                     self._order.clear()
    #                     await self._loop_orders.create_task(self._send_order(self._new_order['amount'],
    #                                                                     self._new_order['price'],
    #                                                                     self._new_order['side'],
    #                                                                     self._new_order['expire']))
    #                     async for msg in ws:
    #                         msg = json.loads(msg.data)
    #                         # print(msg)
    #                         if not msg['data'][0]['sMsg'] == 'Order successfully placed.':
    #                             print(msg)
    #                         break
                # except Exception as e:
                #     print('line 205. Error:')
                #     print(e)

    def _update_positions(self, obj):
        if not obj['data']:
            return
        if obj['data'][0]['pos'] != '0':
            side = 'LONG' if float(obj['data'][0]['pos']) > 0 else 'SHORT'
            amount_usd = float(obj['data'][0]['notionalUsd'])
            if side == 'SHORT':
                amount = -amount_usd / float(obj['data'][0]['markPx'])
            else:
                amount = amount_usd / float(obj['data'][0]['markPx'])
            self.positions.update({obj['arg']['instId']: {'side': side, 'amount_usd': amount_usd, 'amount': amount,
                    'entry_price': float(obj['data'][0]['avgPx']), 'unrealized_pnl_usd': float(obj['data'][0]['upl']),
                    'realized_pnl_usd': 0}})
        else:
            self.positions.update({obj['arg']['instId']: {'side': 'LONG', 'amount_usd': 0, 'amount': 0,
                                                    'entry_price': 0, 'unrealized_pnl_usd': 0, 'realized_pnl_usd': 0}})
        # print(self.positions)
        # for one in obj['data'][0]:
        #     if obj['data'][0][one]:
        #         self.positions[obj['arg']['instId']].update({one: obj['data'][0][one]})

    def _update_orderbook(self, obj):
        symbol = obj['arg']['instId']
        orderbook = obj['data'][0]
        self.orderbook.update({symbol: {'asks': [[float(x[0]), float(x[1]), int(x[3])] for x in orderbook['asks']],
                                        'bids': [[float(x[0]), float(x[1]), int(x[3])] for x in orderbook['bids']],
                                        'timestamp': int(orderbook['ts'])}})

    def _update_account(self, obj):
       resp = obj['data'][0]['details'][0]
       self.balance = {'free': float(resp['availBal']),
                       'total': float(resp['availBal']) + float(resp['frozenBal'])}

    def _update_orders(self, obj):
        # print(obj)
        if obj.get('data') and obj.get('arg'):
            if obj['data'][0]['state'] == 'live':
                # print(self.time_sent)
                print(f"OKEX ORDER PLACE TIME: {float(obj['data'][0]['uTime']) - self.time_sent * 1000} ms\n")
        # print(obj)
        if not self.taker_fee:
            if obj.get('arg'):
                if obj['data'][0]['fillNotionalUsd'] != '':
                    self.taker_fee = abs(float(obj['data'][0]['fillFee'])) / float(obj['data'][0]['fillNotionalUsd'])
                    self.taker_fee = 0.0003
        if not obj['arg']['instId'] == self.symbol:
            return
        if obj['data'][0]['state'] in ['partially_filled', 'filled', 'canceled']:
            try:
                self.orders.pop(obj['data'][0]['ordId'])
            except:
                pass
            last_order = obj['data'][0]
            if last_order['fillPx'] != '':
                try:
                    self.last_price[last_order['side']] = float(last_order['fillPx'])
                except Exception as e:
                    print(f"last_order info:\nERROR:{e}\nValue:{last_order}")
        elif obj['data'][0]['state'] == 'live':
            self.orders.update({obj['data'][0]['ordId']: obj['data'][0]})

    def _process_msg(self, msg: aiohttp.WSMessage):
        obj = json.loads(msg.data)
        if obj.get('event'):
            print(obj)
            return
        if obj.get('arg'):
            if obj['arg']['channel'] == 'account':
                self._update_account(obj)
            elif obj['arg']['channel'] in ['bbo-tbt', 'books50-l2-tbt', 'books5']:
                self._update_orderbook(obj)
            elif obj['arg']['channel'] == 'positions':
                self._update_positions(obj)
            elif obj['arg']['channel'] == 'orders':
                self._update_orders(obj)

    def create_order(self, amount, price, side, expire=100):
        self.time_sent = time.time()
        self.queue.put_nowait({'amount': amount, 'price': price, 'side': side, 'expire': expire})

    async def _send_order(self, amount, price, side, expire=1000):
        # expire_date = str(round((time.time() + expire) * 1000))
        amount = self.fit_amount(amount)
        price = self.fit_price(price)
        msg = {
            "id": self.id_generator(),
            "op": "order",
            "args": [
                {
                    "side": side,
                    "instId": self.symbol,
                    "tdMode": "cross",
                    "ordType": 'limit',
                    "sz": amount,
                    # "expTime": expire_date,
                    "px": price
                }
            ]
            }
        await self._ws_private.send_json(msg)

    def fit_amount(self, amount):
        contract_price = float(self.instrument['ctVal'])
        lot_size = int(float(self.instrument['lotSz']))
        amount = int((amount - (amount % contract_price)) / contract_price)
        amount = int(amount - (amount % lot_size))
        return str(amount)

    def fit_price(self, price):
        if '.' in self.instrument['tickSz']:
            float_len = len(self.instrument['tickSz'].split('.')[1])
            price = round(price - (price % self.ticksize), float_len)
        else:
            price = int(round(price - (price % self.ticksize)))
        return str(price)

    @staticmethod
    def get_timestamp():
        now = datetime.datetime.utcnow()
        t = now.isoformat("T", "milliseconds")
        return t + "Z"

    # def _get_fees(self):
    #     request_path = f'https://www.okx.com/api/v5/account/trade-fee?instType=SWAP'
    #     timestamp = self.get_timestamp()
    #     signature = self.signature(timestamp, 'GET', request_path, None)
    #     headers = {'accept': 'application/json',
    #                'Content-Type': 'application/json',
    #                "OK-ACCESS-KEY": str(self.public_key),
    #                "OK-ACCESS-SIGN": str(signature),
    #                "OK-ACCESS-TIMESTAMP": str(timestamp),
    #                "OK-ACCESS-PASSPHRASE": self.passphrase,
    #                'instId': self.symbol}
    #     fees = requests.get(url=request_path, headers=headers).json()
    #     print(fees)
    #     self.taker_fee = float(fees['data'][0]['taker'])
    #     print(self.taker_fee)

    def get_instrument(self):
        way = f'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
        headers = {'Content-Type': 'application/json',
                   'instId': self.symbol}
        instrument = requests.get(url=way, headers=headers).json()
        for inst in instrument['data']:
            if inst['instId'] == self.symbol:
                instrument = inst
                break
        return instrument

    def get_available_balance(self, side):
        orderbook = self.get_orderbook()[self.symbol]
        position = self.get_positions()[self.symbol]
        change = (orderbook['asks'][0][0] + orderbook['bids'][0][0]) / 2
        while not self.balance['total']:
            print(f"Balance not updated. Line 380. Func get_avail_bal")
            time.sleep(0.01)
        locked_balance = self.balance['total'] - self.balance['free']
        real_leverage = (locked_balance * self.leverage) / self.balance['total']
        if side == 'buy':
            avail_balance = (self.leverage - real_leverage) * self.balance['total'] - position['amount'] * change
        else:
            avail_balance = (self.leverage - real_leverage) * self.balance['total'] + position['amount'] * change
        order_size = orderbook['bids'][0][1] * change if side == 'sell' else orderbook['asks'][0][1] * change
        return min(avail_balance, order_size)

    def get_positions(self):
        return self.positions

    def get_real_balance(self):
        return self.balance['total']

    def get_orderbook(self):
        while not self.orderbook.get(self.symbol):
            time.sleep(0.001)
        return self.orderbook

    def get_last_price(self, side):
        # if self.last_price[side] == 0:
        #     print(f"OKEX CLIENT. LINE 398")
        #     self._ws_orders.close()
        #     self._ws_private.close()
        #     self._ws_public.close()
        #     quit()
        #     exit()
        return self.last_price[side]

# EXAMPLES:
# import configparser
# import sys
#
# cp = configparser.ConfigParser()
# if len(sys.argv) != 2:
#     sys.exit(1)
# cp.read(sys.argv[1], "utf-8")
# keys = cp['OKEX']
# #
# # import random
# client_okex = OkexClient(keys)
# client_okex.run_updater()
# # #
# while True:
#     time.sleep(2)
#     ob = client_okex.get_orderbook()
# # #     client_okex.get_real_balance()
# # #     client_okex.get_positions()
# # #     client_okex.get_last_price('buy')
# # #     time_start = time.time()
#     client_okex.create_order(0.021007, ob[client_okex.symbol]['bids'][0][0] * 0.96, 'buy')
    # print(f"Order place time: {time.time() - time_start}")



# 1674154165380
# 1597026383085
