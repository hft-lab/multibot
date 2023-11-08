import asyncio
import queue
import threading
import traceback
from aio_pika import connect_robust, ExchangeType, Message
from orjson import orjson
import requests
from clients.enums import RabbitMqQueues

import sys
import configparser

config = configparser.ConfigParser()
config.read(sys.argv[1], "utf-8")

class Telegram:
    def __init__(self):
        # TELEGRAM
        # self.chat_id = int(config['TELEGRAM']['CHAT_ID'])
        # self.bot_token = config['TELEGRAM']['TOKEN']
        # self.daily_chat_id = int(config['TELEGRAM']['DAILY_CHAT_ID'])
        # self.inv_chat_id = int(config['TELEGRAM']['INV_CHAT_ID'])
        # self.alert_id = int(config['TELEGRAM']['ALERT_CHAT_ID'])
        # self.alert_token = config['TELEGRAM']['ALERT_BOT_TOKEN']
        self.debug_chat_id = config['TELEGRAM']['DIMA_DEBUG_CHAT_ID']
        self.debug_token_id = config['TELEGRAM']['DIMA_DEBUG_BOT_TOKEN']
        self.tg_url = "https://api.telegram.org/bot"

    def send_tg_message_directly(self, message: str, chat_id: int = None, bot_token: str = None) -> None:
        chat_id = chat_id if chat_id is not None else self.debug_chat_id
        bot_token = bot_token if bot_token is not None else self.debug_token_id

        url = self.tg_url + bot_token + "/sendMessage"
        message_data = {"chat_id": chat_id, "text": message}
        r = requests.post(url, json=message_data)
        return r.json()

class Rabbit():
    def __init__(self,loop):
        self.telegram = Telegram()
        rabbit = config['RABBIT']
        self.rabbit_url = f"amqp://{rabbit['USERNAME']}:{rabbit['PASSWORD']}@{rabbit['HOST']}:{rabbit['PORT']}/"
        self.mq = None
        self.tasks = queue.Queue()
        self.loop = loop
        print('label2')
        self.telegram.send_tg_message_directly('Test')
        # t = threading.Thread(target=self.run_await_in_thread, args=[self.__send_messages, self.loop])
        # t.start()
        # t.join()
        #

    def add_task_to_queue(self, message, queue_name):
        print('label3a' + str(message) + str(queue_name))
        self.telegram.send_tg_message_directly(str(message) + str(queue_name))
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
        print('Label6'+self.rabbit_url)
        print(str(loop))
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
