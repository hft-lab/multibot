import asyncio
import queue
import traceback
import json
from datetime import datetime

import asyncpg
from aio_pika import connect_robust, ExchangeType, Message
from orjson import orjson
import requests
from core.queries import get_last_balance_jumps, get_total_balance, get_last_launch, get_last_deals
from clients.enums import RabbitMqQueues

from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini', "utf-8")

from telegram import Telegram, TG_Groups


class DB:
    def __init__(self, rabbit):
        self.telegram = Telegram()
        self.setts = config['SETTINGS']
        self.db = None

        self.loop = asyncio.new_event_loop()
        self.rabbit = rabbit

    async def setup_postgres(self) -> None:
        print(f"SETUP POSTGRES START")
        postgres = config['POSTGRES']
        self.db = await asyncpg.create_pool(database=postgres['NAME'],
                                            user=postgres['USER'],
                                            password=postgres['PASSWORD'],
                                            host=postgres['HOST'],
                                            port=postgres['PORT'])
        print(f"SETUP POSTGRES ENDED")

    def save_arbitrage_possibilities(self, _id, client_buy, client_sell, max_buy_vol, max_sell_vol, expect_buy_px,
                                     expect_sell_px, time_choose, shift, time_parser, symbol):
        expect_profit_usd = ((expect_sell_px - expect_buy_px) / expect_buy_px - (
                client_buy.taker_fee + client_sell.taker_fee)) * client_buy.amount
        expect_amount_usd = client_buy.amount * (expect_sell_px + expect_buy_px) / 2
        message = {
            'id': _id,
            'datetime': datetime.utcnow(),
            'ts': int(round(datetime.utcnow().timestamp())),
            'buy_exchange': client_buy.EXCHANGE_NAME,
            'sell_exchange': client_sell.EXCHANGE_NAME,
            'symbol': symbol,
            'buy_order_id': client_buy.LAST_ORDER_ID,
            'sell_order_id': client_sell.LAST_ORDER_ID,
            'max_buy_vol_usd': round(max_buy_vol * expect_buy_px),
            'max_sell_vol_usd': round(max_sell_vol * expect_sell_px),
            'expect_buy_price': expect_buy_px,
            'expect_sell_price': expect_sell_px,
            'expect_amount_usd': expect_amount_usd,
            'expect_amount_coin': client_buy.amount,
            'expect_profit_usd': expect_profit_usd,
            'expect_profit_relative': expect_profit_usd / expect_amount_usd,
            'expect_fee_buy': client_buy.taker_fee,
            'expect_fee_sell': client_sell.taker_fee,
            'time_parser': time_parser,
            'time_choose': time_choose,
            'chat_id': 12345678,
            'bot_token': 'placeholder',
            'status': 'Processing'#,
            # 'bot_launch_id': self.bot_launch_id
        }
        return message
    # ex __check_start_launch_config. В конце вызов config_api, т.е. как бы эмуляция внесения настроек. Не работает
    async def log_launch_config(self, multibot):
        async with self.db.acquire() as cursor:
            # Поиск, что есть подходящие еще не использованные настройки
            if not await get_last_launch(cursor,
                                         multibot.clients[0].EXCHANGE_NAME,
                                         multibot.clients[1].EXCHANGE_NAME,
                                         multibot.setts['COIN']):
                # Если таких нет, то поиск последней подходящей использованной настройки
                if launch := await get_last_launch(cursor,
                                                   multibot.clients[0].EXCHANGE_NAME,
                                                   multibot.clients[1].EXCHANGE_NAME,
                                                   multibot.setts['COIN'], 1):

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

    # раньше называлось start_db_update. В конце добавление в очередь
    async def update_launch_config(self, multibot):
        async with self.db.acquire() as cursor:
            if launches := await get_last_launch(cursor,
                                                 multibot.clients[0].EXCHANGE_NAME,
                                                 multibot.clients[1].EXCHANGE_NAME,
                                                 multibot.setts['COIN']):
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
                multibot.messaging.add_task_to_queue(message, "UPDATE_LAUNCH")

                for launch in launches:
                    launch['datetime_update'] = multibot.base_launch_config['datetime_update']
                    launch['ts_update'] = multibot.base_launch_config['ts_update']
                    launch['updated_flag'] = -1
                    launch['launch_id'] = str(launch.pop('id'))
                    launch['bot_config_id'] = str(launch['bot_config_id'])
                    message = "launch"
                    multibot.messaging.add_task_to_queue(message, "UPDATE_LAUNCH")
                    self.update_config()

    # Сигнал для обновления баланса
    def update_config(self, multibot):
        # UPDATE BALANCES
        message = {
            'parent_id': multibot.bot_launch_id,
            'context': 'bot-config-update',
            'env': multibot.env,
            'chat_id': multibot.chat_id,
            'telegram_bot': multibot.telegram_bot,
        }
        self.rabbit.add_task_to_queue(message, "CHECK_BALANCE")

    def update_balance_trigger(self,env : str, context: str):
        pass
    def update_balance_trigger_temp(self, multibot, parent_id) -> None:
        message = {
            'parent_id': parent_id,
            'context': 'post-deal',
            'env': multibot.env,
            'chat_id': multibot.chat_id,
            'telegram_bot': multibot.telegram_bot,
        }
        self.rabbit.add_task_to_queue(message, "CHECK_BALANCE")
        self.telegram.send_message('Post update balance trigger message: '+str(message),TG_Groups.DebugDima)

    # async def save_new_balance_jump(self):
    #     if self.start and self.finish:
    #         message = {
    #             'timestamp': int(round(datetime.utcnow().timestamp())),
    #             'total_balance': self.finish,
    #             'env': self.env
    #         },
    #         self.messaging.add_task_to_queue(message, "BALANCE_JUMP")
    #


class Rabbit:
    def __init__(self, loop):
        self.telegram = Telegram()
        rabbit = config['RABBIT']
        self.rabbit_url = f"amqp://{rabbit['USERNAME']}:{rabbit['PASSWORD']}@{rabbit['HOST']}:{rabbit['PORT']}/"
        self.mq = None
        self.tasks = queue.Queue()
        self.loop = loop

    def add_task_to_queue(self, message, queue_name):
        event_name = getattr(RabbitMqQueues, queue_name)
        if hasattr(RabbitMqQueues, queue_name):
            task = {
                'message': message,
                'routing_key': event_name,
                'exchange_name': RabbitMqQueues.get_exchange_name(event_name),
                'queue_name': event_name
            }
            self.tasks.put(task)
        else:
            print(f"Method '{queue_name}' not found in RabbitMqQueues class")

    @staticmethod
    def run_await_in_thread(func, loop):
        try:
            loop.run_until_complete(func())
        except:
            traceback.print_exc()
        finally:
            loop.close()

    async def setup_mq(self, loop) -> None:
        print(f"SETUP MQ START")
        self.mq = await connect_robust(self.rabbit_url, loop=loop)
        print(f"SETUP MQ ENDED")

    async def send_messages(self):
        await self.setup_mq(self.loop)
        while True:
            processing_tasks = self.tasks.get()
            try:
                processing_tasks.update({'connect': self.mq})
                await self.publish_message(**processing_tasks)
            except:
                await self.setup_mq(self.loop)
                await asyncio.sleep(1)
                processing_tasks.update({'connect': self.mq})
                print(f"\n\nERROR WITH SENDING TO MQ:\n{processing_tasks}\n\n")
                await self.publish_message(**processing_tasks)
            finally:
                self.tasks.task_done()
                await asyncio.sleep(0.1)

    @staticmethod
    async def publish_message(connect, message, routing_key, exchange_name, queue_name):
        channel = await connect.channel()
        exchange = await channel.declare_exchange(exchange_name, type=ExchangeType.DIRECT, durable=True)
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key=routing_key)
        message_body = orjson.dumps(message)
        message = Message(message_body)
        await exchange.publish(message, routing_key=routing_key)
        await channel.close()
        return True


if __name__ == '__main__':
    pass
    # await db.setup_postgres()
    # telegram = Telegram()
    # telegram.send_message('Test',TG_Groups.DebugDima)

