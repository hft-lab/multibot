import asyncio
import json
import uuid
import time
from datetime import datetime
from configparser import ConfigParser
from core.queries import get_last_balance_jumps, get_total_balance, get_last_launch, get_last_deals
from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular, try_exc_async

# from core.enums import Context
# from queries import get_last_balance_jumps, get_total_balance, get_last_launch, get_last_deals
# from telegram import Telegram, TG_Groups
# from enums import Context
import requests
import asyncpg
from arbitrage_finder import AP

config = ConfigParser()
config.read('config.ini', "utf-8")


class DB:
    def __init__(self, rabbit):
        self.telegram = Telegram()
        self.setts = config['SETTINGS']
        self.db = None

        self.loop = asyncio.new_event_loop()
        self.rabbit = rabbit

    @try_exc_async
    async def setup_postgres(self) -> None:
        postgres = config['POSTGRES']
        conn = await asyncpg.create_pool(database=postgres['NAME'],
                                         user=postgres['USER'],
                                         password=postgres['PASSWORD'],
                                         host=postgres['HOST'],
                                         port=postgres['PORT'])
        self.db = conn
        print(f"SETUP POSTGRES ENDED")

    @try_exc_regular
    def save_launch_balance(self, multibot) -> None:
        for client in multibot.clients:
            balance_id = uuid.uuid4()
            sum_abs_position_usd = sum([abs(x.get('amount_usd', 0)) for _, x in client.get_positions().items()])
            balance = client.get_balance()
            current_margin = round(sum_abs_position_usd / balance, 1) if balance else 0
            message = {
                'id': balance_id,
                'datetime': datetime.utcnow(),
                'ts': int(time.time() * 1000),
                'context': 'bot-launch',
                'parent_id': multibot.bot_launch_id,
                'exchange': client.EXCHANGE_NAME,
                'exchange_balance': round(balance, 1),
                'available_for_buy': round(client.get_available_balance()['buy'], 1),
                'available_for_sell': round(client.get_available_balance()['sell'], 1),
                'env': multibot.env,
                'chat_id': 123,
                'bot_token': 'placeholder',
                'current_margin': current_margin
            }
            self.rabbit.add_task_to_queue(message, "BALANCES")

    # async def save_start_balance_detalization(self, symbol, client, parent_id):
    #     client_position_by_symbol = client.get_positions()[symbol]
    #     mark_price = (client.get_orderbook(symbol)['asks'][0][0] +
    #                   client.get_orderbook(symbol)['bids'][0][0]) / 2
    #     position_usd = round(client_position_by_symbol['amount'] * mark_price, 1)
    #     real_balance = client.get_balance()
    #     message = {
    #         'id': uuid.uuid4(),
    #         'datetime': datetime.utcnow(),
    #         'ts': int(time.time() * 1000),
    #         'context': self.context,
    #         'parent_id': parent_id,
    #         'exchange': client.EXCHANGE_NAME,
    #         'symbol': symbol,
    #         'current_margin': round(abs(client_position_by_symbol['amount_usd'] / real_balance), 1),
    #         'position_coin': client_position_by_symbol['amount'],
    #         'position_usd': position_usd,
    #         'entry_price': client_position_by_symbol['entry_price'],
    #         'mark_price': mark_price,
    #         'grand_parent_id': self.parent_id,
    #         'available_for_buy': round(real_balance * client.leverage - position_usd, 1),
    #         'available_for_sell': round(real_balance * client.leverage + position_usd, 1)
    #     }
    #     await self.publish_message(connect=self.app['mq'],
    #                                message=message,
    #                                routing_key=RabbitMqQueues.BALANCE_DETALIZATION,
    #                                exchange_name=RabbitMqQueues.get_exchange_name(RabbitMqQueues.BALANCE_DETALIZATION),
    #                                queue_name=RabbitMqQueues.BALANCE_DETALIZATION
    #                                )
    @try_exc_regular
    def save_arbitrage_possibilities(self, ap : AP):

        message = {
            'id': ap.ap_id,
            'datetime': datetime.utcnow(),
            'ts': int(round(datetime.utcnow().timestamp())),
            'buy_exchange': ap.buy_exchange,
            'sell_exchange': ap.sell_exchange,
            'symbol': ap.coin,
            'buy_order_id': ap.buy_order_id_exchange,
            'sell_order_id': ap.sell_order_id_exchange,
            'max_buy_vol_usd': round(ap.buy_max_amount_ob * ap.buy_price_target),
            'max_sell_vol_usd': round(ap.sell_max_amount_ob * ap.sell_price_target),
            'expect_buy_price': ap.buy_price_target,
            'expect_sell_price': ap.sell_price_target,
            'expect_amount_usd': ap.deal_size_usd_target,
            'expect_amount_coin': ap.deal_size_amount_target,
            'expect_profit_usd': ap.profit_usd_target,
            'expect_profit_relative': ap.profit_rel_target,
            'expect_fee_buy': ap.buy_fee,
            'expect_fee_sell': ap.sell_fee,
            'time_parser': 0,
            'time_choose': ap.time_choose,
            'chat_id': 12345678,
            'bot_token': 'Placeholder',
            'status': 'Processing',
            'bot_launch_id': 12345678
        }
        print(f"SENDING TO MQ. SAVE AP: {message}")
        self.rabbit.add_task_to_queue(message, "ARBITRAGE_POSSIBILITIES")

    @try_exc_regular
    def save_order(self, order_id, exchange_order_id, client, side, parent_id, order_place_time, expect_price, symbol, env):
        message = {
            'id': order_id,
            'datetime': datetime.utcnow(),
            'ts': int(round((datetime.utcnow().timestamp()) * 1000)),
            'context': 'bot',
            'parent_id': parent_id,
            'exchange_order_id': exchange_order_id,
            'type': 'GTT' if client.EXCHANGE_NAME == 'DYDX' else 'GTC',
            'status': 'Processing',
            'exchange': client.EXCHANGE_NAME,
            'side': side,
            'symbol': symbol.upper(),
            'expect_price': expect_price,
            'expect_amount_coin': client.amount,
            'expect_amount_usd': client.amount * client.price,
            'expect_fee': client.taker_fee,
            'factual_price': 0,
            'factual_amount_coin': 0,
            'factual_amount_usd': 0,
            'factual_fee': client.taker_fee,
            'order_place_time': order_place_time,
            'env': env,
        }
        print(f"SENDING TO MQ. SAVE ORDER: {message}")
        self.rabbit.add_task_to_queue(message, "ORDERS")

    # ex __check_start_launch_config
    # Смотрится есть ли в базе неиспользованные настройки, если есть используются они, если нет,
    # то берутся уже использованные и заносятся через вызов метода config_api.
    @try_exc_async
    async def log_launch_config(self, multibot):
        async with self.db.acquire() as cursor:
            # Поиск, что есть подходящие еще не использованные настройки
            if not await get_last_launch(cursor,
                                         multibot.clients[0].EXCHANGE_NAME,
                                         multibot.clients[1].EXCHANGE_NAME,
                                         'MULTICOIN'):
                # Если таких нет, то поиск последней подходящей использованной настройки
                if launch := await get_last_launch(cursor,
                                                   multibot.clients[0].EXCHANGE_NAME,
                                                   multibot.clients[1].EXCHANGE_NAME,
                                                   'MULTICOIN', 1):

                    launch = launch[0]
                    data = json.dumps({
                        "env": multibot.setts['ENV'],
                        "shift_use_flag": launch['shift_use_flag'],
                        "target_profit": launch['target_profit'],
                        "orders_delay": launch['orders_delay'],
                        "max_order_usd": launch['max_order_usd'],
                        "max_leverage": launch['max_leverage'],
                        'exchange_1': multibot.clients[0].EXCHANGE_NAME,
                        'exchange_2': multibot.clients[1].EXCHANGE_NAME,
                    })
                else:
                    data = multibot.base_launch_config
                headers = {
                    'token': 'jnfXhfuherfihvijnfjigt',
                    'context': 'bot-start'
                }
                url = f"http://{self.setts['CONFIG_API_HOST']}:{self.setts['CONFIG_API_PORT']}/api/v1/configs"

                requests.post(url=url, headers=headers, json=data)

    # раньше называлось start_db_update. В конце добавление в очередь.
    @try_exc_async
    async def update_launch_config(self, multibot):
        async with self.db.acquire() as cursor:
            if launches := await get_last_launch(cursor,
                                                 multibot.clients[0].EXCHANGE_NAME,
                                                 multibot.clients[1].EXCHANGE_NAME,
                                                 'MULTICOIN'):
                launch = launches.pop(0)
                multibot.bot_launch_id = str(launch['id'])

                for field in launch:
                    if not launch.get('field') and field not in ['id', 'datetime', 'ts', 'bot_config_id',
                                                                 'coin', 'shift']:
                        launch[field] = multibot.base_launch_config[field]

                launch['launch_id'] = str(launch.pop('id'))
                launch['bot_config_id'] = str(launch['bot_config_id'])

                # if not launch.get('shift_use_flag'):
                #     for client_1, client_2 in self.ribs:
                #         self.shifts.update({f'{client_1.EXCHANGE_NAME} {client_2.EXCHANGE_NAME}': 0})
                # else:
                #     self.shifts = start_shifts
                message = "launch"
                self.rabbit.add_task_to_queue(message, "UPDATE_LAUNCH")
                for launch in launches:
                    launch['datetime_update'] = multibot.base_launch_config['datetime_update']
                    launch['ts_update'] = multibot.base_launch_config['ts_update']
                    launch['updated_flag'] = -1
                    launch['launch_id'] = str(launch.pop('id'))
                    launch['bot_config_id'] = str(launch['bot_config_id'])
                    message = "launch"
                    self.rabbit.add_task_to_queue(message, "UPDATE_LAUNCH")
                    self.update_balance_trigger('bot-config-update', multibot.bot_launch_id, multibot.env)

    @try_exc_regular
    def update_balance_trigger(self, context: str, parent_id, env: str):
        message = {
            'parent_id': parent_id,
            'context': context,
            'env': env,
            'chat_id': 12345678,
            'telegram_bot': 'placeholder'
        }
        self.rabbit.add_task_to_queue(message, "CHECK_BALANCE")

    # async def save_new_balance_jump(self):
    #     if self.start and self.finish:
    #         message = {
    #             'timestamp': int(round(datetime.utcnow().timestamp())),
    #             'total_balance': self.finish,
    #             'env': self.env
    #         },
    #         self.messaging.add_task_to_queue(message, "BALANCE_JUMP")
    #
