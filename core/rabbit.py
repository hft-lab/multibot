import asyncio
import queue
from aio_pika import connect_robust, ExchangeType, Message
from orjson import orjson
from core.enums import RabbitMqQueues
import aiormq

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini', "utf-8")

from core.telegram import Telegram, TG_Groups
from core.wrappers import try_exc_regular, try_exc_async


class Rabbit:
    def __init__(self, loop):
        self.telegram = Telegram()
        rabbit = config['RABBIT']
        self.rabbit_url = f"amqp://{rabbit['USERNAME']}:{rabbit['PASSWORD']}@{rabbit['HOST']}:{rabbit['PORT']}/"
        self.mq = None
        self.tasks = queue.Queue() # точно ли здесь нужны очереди? Чтение из нее происходит в один поток, можно List + Append.
        self.loop = loop

    @staticmethod
    @try_exc_regular
    def get_exchange_name(routing_key: str):
        routing_list = routing_key.split('.')

        if len(routing_list) > 1 and ('periodic' in routing_key or 'event' in routing_key):
            return routing_list[0] + '.' + routing_list[1]

        raise f'Wrong routing key:{routing_key}'

    @try_exc_regular
    def add_task_to_queue(self, message, queue_name):
        if hasattr(RabbitMqQueues, queue_name):
            event_name = getattr(RabbitMqQueues, queue_name)
            task = {
                'message': message,
                'routing_key': event_name,
                'exchange_name': self.get_exchange_name(event_name),
                'queue_name': event_name
            }
            self.tasks.put(task)
        else:
            print(f"Method '{queue_name}' not found in RabbitMqQueues class")
            self.telegram.send_message(f"Method '{queue_name}' not found in RabbitMqQueues class", TG_Groups.Alerts)

    @try_exc_async
    async def setup_mq(self) -> None:
        self.mq = await connect_robust(self.rabbit_url, loop=self.loop)
        # print(f"SETUP MQ DONE")

    @try_exc_async
    async def send_messages(self):
        while self.tasks.qsize():
            task = self.tasks.get()
            # print('TASK TO MQ:\n\n',task)
            # self.telegram.send_message('TASK TO MQ:\n\n' + str(task), TG_Groups.DebugDima)
            await self.publish_message(**task)
        # self.tasks.task_done()
        # await asyncio.sleep(0.1)

    @try_exc_async
    async def publish_message(self, message, routing_key, exchange_name, queue_name):
        try:
            channel = await self.mq.channel()
            exchange = await channel.declare_exchange(exchange_name, type=ExchangeType.DIRECT, durable=True)
            # Точно ли нужны следующие 2 строчки, как будто эти привязки уже прописаны и так в Rabbit MQ?
            queue = await channel.declare_queue(queue_name, durable=True)
            await queue.bind(exchange, routing_key=routing_key)
            message_body = orjson.dumps(message)
            message = Message(message_body)
            await exchange.publish(message, routing_key=routing_key)
            await channel.close()
            return True
        except aiormq.exceptions.AMQPConnectionError as e:
            await asyncio.sleep(1)  # Wait for 5 seconds before retrying
            await self.setup_mq()
            await self.publish_message(message, routing_key, exchange_name, queue_name)


if __name__ == '__main__':
    pass


