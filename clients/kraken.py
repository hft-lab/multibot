import asyncio
import base64
import hashlib
import hmac

import threading
import time

from datetime import datetime

from urllib.parse import urlencode

import aiohttp
import orjson
import requests

from config import Config
from core.base_client import BaseClient
from core.enums import ConnectMethodEnum, ResponseStatus


class KrakenClient(BaseClient):
    BASE_WS = 'wss://futures.kraken.com/ws/v1'
    BASE_URL = 'https://futures.kraken.com'
    EXCHANGE_NAME = 'KRAKEN'

    def __init__(self, keys, leverage):
        self.taker_fee = 0.00036
        self.leverage = leverage
        self.symbol = keys['symbol']
        self.__api_key = keys['api_key']
        self.__secret_key = keys['secret_key']
        self.__last_challenge = None

        self.step_size = None
        self.tick_size = None
        self.price_precision = 0
        self.quantity_precision = 0

        self.balance = {
            'total': 0.0,
        }
        self.last_price = {
            'sell': 0,
            'buy': 0
        }
        self.positions = {
            self.symbol: {
                'amount': 0,
                'entry_price': 0,
                'unrealized_pnl_usd': 0,
                'side': 'LONG',
                'amount_usd': 0,
                'realized_pnl_usd': 0}
        }
        self.orderbook = {
            self.symbol: {
                'sell': [],
                'buy': [],
                'timestamp': 0
            }
        }
        self._loop_public = asyncio.new_event_loop()
        self._loop_private = asyncio.new_event_loop()
        self.wsd_public = threading.Thread(target=self._run_forever,
                                           args=[ConnectMethodEnum.PUBLIC, self._loop_public])
        self.bal_check = threading.Thread(target=self._run_forever,
                                          args=[ConnectMethodEnum.PRIVATE, self._loop_private])

    def get_available_balance(self, side: str) -> float:
        return self.__get_available_balance(side)

    async def create_order(self, amount, price, side, session: aiohttp.ClientSession,
                           expire: int = 5000, client_ID: str = None) -> dict:
        return await self.__create_order(amount, price, side.upper(), session, expire, client_ID)

    def cancel_all_orders(self, orderID=None) -> dict:
        return self.__cancel_open_orders()

    def get_positions(self) -> dict:
        return self.positions

    def get_real_balance(self) -> float:
        return self.balance['total']

    def get_orderbook(self) -> dict:
        orderbook = {}
        while not self.orderbook.get(self.symbol):
            time.sleep(0.001)

        snap = self.orderbook[self.symbol]
        orderbook[self.symbol] = {'timestamp': self.orderbook[self.symbol]['timestamp']}
        orderbook[self.symbol]['asks'] = [[x, snap['sell'][x]] for x in sorted(snap['sell'])[:5]]
        orderbook[self.symbol]['bids'] = [[x, snap['buy'][x]] for x in sorted(snap['buy'], reverse=True)[:5]]

        return orderbook

    def get_last_price(self, side: str) -> float:
        return self.last_price[side.lower()]

    def run_updater(self) -> None:
        self.wsd_public.start()
        self.bal_check.start()

    def _run_forever(self, type, loop) -> None:
        loop.run_until_complete(self._run_loop(type))

    async def _run_loop(self, type) -> None:
        async with aiohttp.ClientSession() as session:
            if type == ConnectMethodEnum.PUBLIC:
                await self._symbol_data_getter(session)
            elif type == ConnectMethodEnum.PRIVATE:
                await self._user_balance_getter(session)

    # PUBLIC -----------------------------------------------------------------------------------------------------------

    async def _symbol_data_getter(self, session: aiohttp.ClientSession) -> None:

        async with session.ws_connect(self.BASE_WS) as ws:
            await ws.send_str(orjson.dumps({
                "event": "subscribe",
                "feed": "book",
                'snapshot': False,
                "product_ids": [
                    self.symbol.upper(),
                ]
            }).decode('utf-8'))

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    payload = orjson.loads(msg.data)
                    if payload.get('feed') == 'book_snapshot':
                        self.orderbook[self.symbol] = {
                            'sell': {x['price']: x['qty'] for x in payload['asks']},
                            'buy': {x['price']: x['qty'] for x in payload['bids']},
                            'timestamp': payload['timestamp']
                        }

                    elif payload.get('feed') == 'book' and payload.get('side'):
                        res = self.orderbook[self.symbol][payload['side']]
                        if res.get(payload['price']) and payload['qty'] == 0.0:
                            del res[payload['price']]
                        else:
                            self.orderbook[self.symbol][payload['side']][payload['price']] = payload['qty']
                            self.orderbook[self.symbol]['timestamp'] = payload['timestamp']

    # PRIVATE ----------------------------------------------------------------------------------------------------------
    def _sign_message(self, api_path: str, data: dict) -> str:
        plain_data = []
        for key, value in data.items():
            if isinstance(value, (list, tuple)):
                plain_data.extend([(key, str(item)) for item in value])
            else:
                plain_data.append((key, str(value)))

        post_data = urlencode(plain_data)
        encoded = (str(data["nonce"]) + post_data).encode()
        message = api_path.encode() + hashlib.sha256(encoded).digest()
        signature = hmac.new(base64.b64decode(self.__secret_key), message, hashlib.sha512)
        sig_digest = base64.b64encode(signature.digest())

        return sig_digest.decode()

    def __get_available_balance(self, side: str = 'sell') -> float:
        position_value = 0
        orderbook = self.get_orderbook()
        if orderbook[self.symbol].get('asks') and orderbook[self.symbol].get('bids'):
            change = (orderbook[self.symbol]['asks'][0][0] + orderbook[self.symbol]['bids'][0][0]) / 2
            for market, position in self.positions.items():
                if market == self.symbol:
                    position_value = position['amount_usd'] * change
                    break

            available_margin = self.balance['total'] * self.leverage

            if side == 'buy':
                max_ask = orderbook[self.symbol]['asks'][0][1] * change
                return min(available_margin - position_value, max_ask)

            max_bid = orderbook[self.symbol]['bids'][0][1] * change
            return min(available_margin + position_value, max_bid)

    def get_kraken_futures_signature(self, endpoint: str, data: str, nonce: str):
        if endpoint.startswith("/derivatives"):
            endpoint = endpoint[len("/derivatives"):]

        sha256_hash = hashlib.sha256()
        sha256_hash.update((data + nonce + endpoint).encode("utf8"))

        return base64.b64encode(
            hmac.new(
                base64.b64decode(self.__secret_key), sha256_hash.digest(), hashlib.sha512
            ).digest()
        )

    def __cancel_open_orders(self):
        url_path = "/derivatives/api/v3/cancelallorders"
        nonce = str(int(time.time() * 1000))
        params = {'symbol': self.symbol}
        post_string = "&".join([f"{key}={params[key]}" for key in sorted(params)])
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Nonce": nonce,
            "APIKey": self.__api_key,
            "Authent": self.get_kraken_futures_signature(
                url_path, post_string, nonce
            ),
        }
        return requests.post(headers=headers, url=self.BASE_URL + url_path, data=post_string).json()

    def fit_amount(self, amount):
        if not self.quantity_precision:
            if '.' in str(self.step_size):
                round_amount_len = len(str(self.step_size).split('.')[1])
            else:
                round_amount_len = 0
            amount = str(round(amount - (amount % self.step_size), round_amount_len))
        else:
            amount = str(float(round(float(round(amount / self.step_size) * self.step_size), self.quantity_precision)))

        return amount

    async def __create_order(self, amount: float, price: float, side: str, session: aiohttp.ClientSession,
                             expire=5000, client_ID=None) -> dict:
        nonce = str(int(time.time() * 1000))
        url_path = "/derivatives/api/v3/sendorder"
        params = {
            "orderType": "lmt",
            "limitPrice": float(round(float(round(price / self.tick_size) * self.tick_size), self.price_precision)),
            "side": side.lower(),
            "size": amount,
            "symbol": self.symbol
        }
        post_string = "&".join([f"{key}={params[key]}" for key in sorted(params)])

        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Nonce": nonce,
            "APIKey": self.__api_key,
            "Authent": self.get_kraken_futures_signature(
                url_path, post_string, nonce
            ).decode('utf-8'),
        }
        async with session.post(
                url=self.BASE_URL + url_path + '?' + post_string, headers=headers, data=post_string
        ) as resp:
            response = await resp.json()
            try:
                timestamp = response['sendStatus']['orderEvents'][0]['orderPriorExecution']['timestamp']
                if response.get('sendStatus').get('status'):
                    status = ResponseStatus.SUCCESS
            except:
                timestamp = 0000000000000
                status = ResponseStatus.ERROR


            return {
                'exchange_name': self.EXCHANGE_NAME,
                'timestamp': timestamp,
                'status': status
            }


    def _get_sign_challenge(self, challenge: str) -> str:
        sha256_hash = hashlib.sha256()
        sha256_hash.update(challenge.encode("utf-8"))
        return base64.b64encode(
            hmac.new(
                base64.b64decode(self.__secret_key), sha256_hash.digest(), hashlib.sha512
            ).digest()
        ).decode("utf-8")

    async def _user_balance_getter(self, session: aiohttp.ClientSession) -> None:
        async with session.ws_connect(self.BASE_WS) as ws:
            if not self.__last_challenge:
                await ws.send_str(orjson.dumps({"event": "challenge", "api_key": self.__api_key}).decode('utf-8'))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        msg_data = orjson.loads(msg.data)
                        if msg_data.get('event') == 'challenge':
                            self.__last_challenge = msg_data['message']
                            await ws.send_str(orjson.dumps({
                                "event": "subscribe",
                                "feed": "balances",
                                "api_key": self.__api_key,
                                'original_challenge': self.__last_challenge,
                                "signed_challenge": self._get_sign_challenge(self.__last_challenge)
                            }).decode('utf-8'))

                            await ws.send_str(orjson.dumps({
                                "event": "subscribe",
                                "feed": "open_positions",
                                "api_key": self.__api_key,
                                'original_challenge': self.__last_challenge,
                                "signed_challenge": self._get_sign_challenge(self.__last_challenge)
                            }).decode('utf-8'))

                        elif msg_data.get('feed') in ['balances']:
                            if msg_data.get('flex_futures'):
                                self.balance['total'] = msg_data['flex_futures']['balance_value']

                        elif msg_data.get('feed') == 'open_positions':
                            for position in msg_data.get('positions', []):
                                self.positions.update({position['instrument']: {
                                    'side': 'LONG' if position['balance'] >= 0 else 'SHORT',
                                    'amount_usd': position['balance'] * position['mark_price'],
                                    'amount': position['balance'],
                                    'entry_price': position['entry_price'],
                                    'unrealized_pnl_usd': 0,
                                    'realized_pnl_usd': 0,
                                    'lever': self.leverage
                                }})



