import asyncio
import queue
import traceback
from aio_pika import connect_robust, ExchangeType, Message
from orjson import orjson
from enums import RabbitMqQueues

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini', "utf-8")

from telegram import Telegram


class Rabbit:
    def __init__(self, loop):
        self.telegram = Telegram()
        rabbit = config['RABBIT']
        self.rabbit_url = f"amqp://{rabbit['USERNAME']}:{rabbit['PASSWORD']}@{rabbit['HOST']}:{rabbit['PORT']}/"
        self.mq = None
        self.tasks = queue.Queue()
        self.loop = loop

    @staticmethod
    def get_exchange_name(routing_key: str):
        routing_list = routing_key.split('.')

        if len(routing_list) > 1 and ('periodic' in routing_key or 'event' in routing_key):
            return routing_list[0] + '.' + routing_list[1]

        raise f'Wrong routing key:{routing_key}'

    def add_task_to_queue(self, message, queue_name):
        event_name = getattr(RabbitMqQueues, queue_name)
        if hasattr(RabbitMqQueues, queue_name):
            task = {
                'message': message,
                'routing_key': event_name,
                'exchange_name': self.get_exchange_name(event_name),
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

