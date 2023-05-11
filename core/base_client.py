import aiohttp
from abc import ABC, abstractmethod


class BaseClient(ABC):
    BASE_URL = None
    BASE_WS = None
    EXCHANGE_NAME = None
    LAST_ORDER_ID = 'default'

    @abstractmethod
    def get_available_balance(self, side: str) -> float:
        """
        Amount available to trade in certain direction in USD

        :param side: SELL/BUY side for check balance
        :return: float
        """
        pass

    @abstractmethod
    async def create_order(self, amount: float, price: float, side: str,
                           session: aiohttp.ClientSession, expire: int = 100, client_ID: str = None) -> dict:
        """
        Create order func

        :param amount: amount in coin
        :param price: SELL/BUY price
        :param side: SELL/BUY in lowercase
        :param order_type: LIMIT or MARKET
        :param session: session from aiohttpClientSession
        :param expire: int value for exp of order
        :param client_ID:
        :return:
        """
        pass

    @abstractmethod
    def cancel_all_orders(self, orderID=None) -> dict:
        """
        cancels all orders by symbol or orderID
        :return: response from exchange api
        """
        pass

    @abstractmethod
    def get_positions(self) -> dict:
        pass

    @abstractmethod
    def get_real_balance(self) -> float:
       pass

    @abstractmethod
    def get_orderbook(self) -> dict:
        pass

    @abstractmethod
    def get_last_price(self, side: str) -> float:
        pass
