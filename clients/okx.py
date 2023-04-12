import traceback

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

from config import Config
from core.base_client import BaseClient
from core.enums import ResponseStatus


class OkxClient(BaseClient):
    URI_WS_AWS = "wss://wsaws.okx.com:8443/ws/v5/public"
    URI_WS_PRIVATE = "wss://ws.okx.com:8443/ws/v5/private"
    EXCHANGE_NAME = 'OKX'

    def __init__(self, keys, leverage=2):
        self.create_order_response = {}
        self.symbol = keys['symbol']
        self.leverage = leverage
        self.public_key = keys['public_key']
        self.secret_key = keys['secret_key']
        self.passphrase = keys['passphrase']
        self.positions = {self.symbol: {'side': 0, 'amount_usd': 0, 'amount': 0, 'entry_price': 0,
                                        'unrealized_pnl_usd': 0, 'realized_pnl_usd': 0, 'lever': self.leverage}}
        self._loop_public = asyncio.new_event_loop()
        self._loop_private = asyncio.new_event_loop()
        self.queue = queue.Queue()
        self._connected = asyncio.Event()
        self.wst_public = threading.Thread(target=self._run_ws_forever, args=['public', self._loop_public])
        self.wst_private = threading.Thread(target=self._run_ws_forever, args=['private', self._loop_private])
        self.instrument = self.get_instrument()
        self.tick_size = float(self.instrument['tickSz'])
        self.step_size = float(self.instrument['lotSz'])
        print('OKX STEP', self.step_size)
        self.quantity_precision = len(str(self.step_size).split('.')[1]) if '.' in str(self.step_size) else 1

        self.orderbook = {}
        self.orders = {}
        self.last_price = {'buy': 0, 'sell': 0}
        self.balance = {'free': 0, 'total': 0}
        self.taker_fee = 0
        self.start_time = int(time.time())
        self.time_sent = time.time()

    @staticmethod
    def id_generator(size=12, chars=string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def run_updater(self):
        self.wst_public.daemon = True
        self.wst_public.start()
        self.wst_private.daemon = True
        self.wst_private.start()

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
                "channel": "bbo-tbt",  # 0-l2-tbt",
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
                traceback.print_exc()
                print(f"Line 100. Error: {e}")
            finally:
                print(f"WS loop {type} completed. Restarting")

    async def _run_ws_loop(self, type):
        async with aiohttp.ClientSession() as s:
            if type == 'private':
                endpoint = self.URI_WS_PRIVATE
            else:
                endpoint = self.URI_WS_AWS
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

                    try:
                        await self._send_order(**self.queue.get_nowait())
                    except queue.Empty:
                        await self._ping(ws)
                    self._process_msg(msg)

    async def _ping(self, ws):
        time_from = int(int(round(time.time())) - self.start_time) % 5
        if not time_from:
            await ws.ping(b'PING')
            self.start_time -= 1

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
                                                          'entry_price': float(obj['data'][0]['avgPx']),
                                                          'unrealized_pnl_usd': float(obj['data'][0]['upl']),
                                                          'realized_pnl_usd': 0}})
        else:
            self.positions.update({obj['arg']['instId']: {'side': 'LONG', 'amount_usd': 0, 'amount': 0,
                                                          'entry_price': 0, 'unrealized_pnl_usd': 0,
                                                          'realized_pnl_usd': 0}})
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
        if obj.get('data') and obj.get('arg'):
            self.create_order_response = obj
            if obj['data'][0]['state'] == 'live':
                print(f"OKEX ORDER PLACE TIME: {float(obj['data'][0]['uTime']) - self.time_sent * 1000} ms\n")
        # print(obj)
        if not self.taker_fee:
            if obj.get('arg'):
                if obj['data'][0]['fillNotionalUsd'] != '':  # TODO ???
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

    async def create_order(self, amount: float, price: float, side: str, session: aiohttp.ClientSession,
                           expire: int = 100, client_ID: str = None) -> dict:
        self.time_sent = time.time() * 1000
        self.queue.put_nowait({'amount': amount, 'price': price, 'side': side, 'expire': expire})

        response = {
            'exchange_name': self.EXCHANGE_NAME,
            'timestamp': (self.time_sent +  time.time() * 1000) / 2,
            'status': ResponseStatus.SUCCESS if self.create_order_response.get('code') == '0' else ResponseStatus.ERROR
        }
        self.create_order_response = {}
        return response

    async def _send_order(self, amount, price, side, expire=1000):
        # expire_date = str(round((time.time() + expire) * 1000))

        amount = self.fit_amount(float(amount))
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
        amount = int((amount - (amount % float(self.instrument['ctVal']))) / float(self.instrument['ctVal']))
        return str(amount - (amount % self.step_size))

    def fit_price(self, price):
        if '.' in str(self.tick_size):
            float_len = len(str(self.tick_size).split('.')[1])
            price = round(price - (price % self.tick_size), float_len)
        else:
            price = int(round(price - (price % self.tick_size)))
        return str(price)

    @staticmethod
    def get_timestamp():
        now = datetime.datetime.utcnow()
        t = now.isoformat("T", "milliseconds")
        return t + "Z"

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
        return self.last_price[side]

    def cancel_all_orders(self, orderID=None):
        pass
