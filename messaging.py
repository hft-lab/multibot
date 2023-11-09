import asyncio
import queue
import threading
import traceback
import json
import asyncpg
from aio_pika import connect_robust, ExchangeType, Message
from orjson import orjson
import requests
from core.queries import get_last_balance_jumps, get_total_balance, get_last_launch, get_last_deals
from clients.enums import RabbitMqQueues
from enum import Enum

import sys
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini', "utf-8")


# config.read(sys.argv[1], "utf-8")


class TG_Groups(Enum):
    _main_id = int(config['TELEGRAM']['CHAT_ID'])
    _main_token = config['TELEGRAM']['TOKEN']
    # self.daily_chat_id = int(config['TELEGRAM']['DAILY_CHAT_ID'])
    # self.inv_chat_id = int(config['TELEGRAM']['INV_CHAT_ID'])
    _alert_id = int(config['TELEGRAM']['ALERT_CHAT_ID'])
    _alert_token = config['TELEGRAM']['ALERT_BOT_TOKEN']
    _debug_id = int(config['TELEGRAM']['DIMA_DEBUG_CHAT_ID'])
    _debug_token = config['TELEGRAM']['DIMA_DEBUG_BOT_TOKEN']

    MainGroup = {'chat_id': _main_id, 'bot_token': _main_token}
    Alerts = {'chat_id': _alert_id, 'bot_token': _alert_token}
    DebugDima = {'chat_id': _debug_id, 'bot_token': _debug_token}


class Telegram:
    def __init__(self):
        self.tg_url = "https://api.telegram.org/bot"

    def send_message(self, message: str, group_obj: TG_Groups = None):
        group = group_obj.value if group_obj else TG_Groups.MainGroup.value
        url = self.tg_url + group['bot_token'] + "/sendMessage"
        message_data = {"chat_id": group['chat_id'], "parse_mode": "HTML","text": "<pre>"+message+"</pre>"}
        r = requests.post(url, json=message_data)
        return r.json()

    def send_tg_message_to_rabbit(self, text: str, chat_id: int = None, bot_token: str = None) -> None:
        chat_id = chat_id if chat_id is not None else self.chat_id
        bot_token = bot_token if bot_token is not None else self.bot_token

        message = {"chat_id": chat_id, "msg": text, 'bot_token': bot_token}
        self.messaging.add_task_to_queue(message, "TELEGRAM")

    def send_tg_message_directly(self, message: str, chat_id: int = None, bot_token: str = None) -> None:
        chat_id = chat_id if chat_id is not None else self.debug_chat_id
        bot_token = bot_token if bot_token is not None else self.debug_token_id

        url = self.tg_url + bot_token + "/sendMessage"
        message_data = {"chat_id": chat_id, "text": message}
        r = requests.post(url, json=message_data)
        return r.json()


class DB:
    def __init__(self, rabbit):

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

    def save_balance(self, multibot, parent_id) -> None:
        message = {
            'parent_id': parent_id,
            'context': 'post-deal',
            'env': multibot.env,
            'chat_id': multibot.chat_id,
            'telegram_bot': multibot.telegram_bot,
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
    telegram = Telegram()
    telegram.send_message('Test',TG_Groups.DebugDima)

