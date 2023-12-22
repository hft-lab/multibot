import csv
import time
import asyncio
from datetime import datetime
from multibot.clients.core.enums import BotState
from core.queries import get_last_balance_jumps, get_total_balance, get_last_deals


# def __prepare_shifts(self):
#     time.sleep(10)
#     self.__rates_update()
#     # !!!!SHIFTS ARE HARDCODED TO A ZERO!!!!
#     for x, y in Shifts().get_shifts().items():
#         self.shifts.update({x: 0})
#     print(self.shifts)


def check_ob_slippage(multibot, client_sell, client_buy):
    client_slippage = None
    while True:
        ob_sell = client_sell.get_orderbook(client_sell.symbol)
        ob_buy = client_buy.get_orderbook(client_buy.symbol)
        if client_sell.EXCHANGE_NAME == 'APOLLOX':
            ob_sell_time_shift = 5 * multibot.deal_pause * 1000
        else:
            ob_sell_time_shift = multibot.deal_pause * 1000
        if client_buy.EXCHANGE_NAME == 'APOLLOX':
            ob_buy_time_shift = 5 * multibot.deal_pause * 1000
        else:
            ob_buy_time_shift = multibot.deal_pause * 1000
        current_timestamp = int(time.time() * 1000)
        if current_timestamp - ob_sell['timestamp'] > ob_sell_time_shift:
            if multibot.state == BotState.BOT:
                multibot.state = BotState.SLIPPAGE
                multibot.ob_alert_send(client_sell, client_buy, ob_sell['timestamp'])
                client_slippage = client_sell
            time.sleep(5)
            continue
        elif current_timestamp - ob_buy['timestamp'] > ob_buy_time_shift:
            if multibot.state == BotState.BOT:
                multibot.state = BotState.SLIPPAGE
                multibot.ob_alert_send(client_buy, client_sell, ob_buy['timestamp'])
                client_slippage = client_buy
            time.sleep(5)
            continue
        elif ob_sell['asks'] and ob_sell['bids'] and ob_buy['asks'] and ob_buy['bids']:
            if multibot.state == BotState.SLIPPAGE:
                multibot.state = BotState.BOT
                multibot.ob_alert_send(client_sell, client_buy, ob_sell['timestamp'], client_slippage)
                client_slippage = None
            return ob_sell, ob_buy


    @staticmethod
    def create_csv(filename):
        # Open the CSV file in write mode
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            # Write header row
            writer.writerow(['TimestampUTC', 'Time Stamp', 'Exchange', 'Coin', 'Flag'])

    @staticmethod
    def append_to_csv(filename, record):
        # Open the CSV file in append mode
        with open(filename, 'a', newline='') as file:
            writer = csv.writer(file)
            # Append new record
            writer.writerow(record)

        # parser = argparse.ArgumentParser()
        # parser.add_argument('-c1', nargs='?', const=True, default='apollox', dest='client_1')
        # parser.add_argument('-c2', nargs='?', const=True, default='binance', dest='client_2')
        # args = parser.parse_args()

        # import cProfile
        #
        #
        # def your_function():
        #
        #
        # # Your code here
        #
        # # Start the profiler
        # profiler = cProfile.Profile()
        # profiler.enable()
        #
        # # Run your code
        # your_function()
        #
        # # Stop the profiler
        # profiler.disable()
        #
        # # Print the profiling results
        # profiler.print_stats(sort='time')



    async def check_opposite_deal_side(self, exch_1, exch_2, ap_id):
        await asyncio.sleep(2)
        async with self.db.acquire() as cursor:
            for ap in await get_last_deals(cursor):
                if {exch_1, exch_2} == {ap['buy_exchange'], ap['sell_exchange']}:
                    if ap['id'] != ap_id:
                        low = int(round(datetime.utcnow().timestamp())) - 10
                        high = int(round(datetime.utcnow().timestamp())) + 10
                        if low < ap['ts'] < high:
                            # print(f"\n\n\nFOUND SECOND DEAL!\nDEAL: {ap}")
                            return True
        return False

    async def get_balance_percent(self) -> float:
        async with self.db.acquire() as cursor:
            self.finish, self.f_time = await self.get_total_balance_calc(cursor, 'desc')  # todo

            if res := await get_last_balance_jumps(cursor):
                self.start, self.s_time = res[0], res[1]
            else:
                self.start, self.s_time = await self.get_total_balance_calc(cursor, 'asc')
                await self.save_new_balance_jump()

            if self.start and self.finish:
                return abs(100 - self.finish * 100 / self.start)

            return 0

    async def get_total_balance_calc(self, cursor, asc_desc):
        result = 0
        exchanges = []
        time_ = 0
        for row in await get_total_balance(cursor, asc_desc):
            if not row['exchange_name'] in exchanges:
                result += row['total_balance']
                exchanges.append(row['exchange_name'])
                time_ = max(time_, row['ts'])

            if len(exchanges) >= self.exchanges_len:
                break

        return result, str(datetime.fromtimestamp(time_ / 1000).strftime('%Y-%m-%d %H:%M:%S'))


    def send_tg_message_to_rabbit(self, text: str, chat_id: int = None, bot_token: str = None) -> None:
        chat_id = chat_id if chat_id is not None else self.chat_id
        bot_token = bot_token if bot_token is not None else self.bot_token

        message = {"chat_id": chat_id, "msg": text, 'bot_token': bot_token}
        self.messaging.add_task_to_queue(message, "TELEGRAM")

    #
    # tg_message_to_mq=        {
    #         'exchange': 'logger.event',
    #         'queue': 'logger.event.send_to_telegram',
    #         'routing_key': 'logger.event.send_to_telegram',
    #         'interval': 1 * 5,
    #         'delay': 1 * 5,
    #         'payload': {
    #         "chat_id": -4073293077,
    #         "msg": "Hi from Dima",
    #         'bot_token': '6684267405:AAFf2z4yVXtW-afd3kM7vAfNkNipCJBAZbw'
    #         }
    #     }


   # async def __launch_and_run(self):
    #
    #     start = datetime.utcnow()
        # await self.db.log_launch_config()
        # start_shifts = self.shifts.copy()

        # try:
        #
        #     await self.db.update_launch_config(self)
        # except Exception:
        #     print(f"LINE 723:")
        #     traceback.print_exc()


        # Проверить, что сработает
        # self.db.update_balance_trigger('bot-launch', int(datetime.utcnow()), self.env)
        # async with aiohttp.ClientSession() as session:
        #     self.session = session
        #     time.sleep(1)
        #
        #     # self.db.update_config(self)
        #     # self.update_balances_trigger()
        #     while True:
        #         # if (datetime.utcnow()-start).total_seconds() >= 30:
        #         #     try:
        #         #         await self.db.update_launch_config(self)
        #         #     except Exception:
        #         #         traceback.print_exc()
        #         #     start = datetime.utcnow()
        #
        #         if not round(datetime.utcnow().timestamp() - self.start_time) % 92:
        #             self.start_time -= 1
        #             self.telegram.send_message(f"CHECK DEALS IS WORKING")
        #
        #
        #         await asyncio.sleep(1)

    # async def close_all_positions(self):
    #     async with aiohttp.ClientSession() as session:
    #         print('START')
    #         while abs(self.client_1.get_positions().get(self.client_1.symbol, {}).get('amount_usd', 0)) > 50 \
    #                 or abs(self.client_2.get_positions().get(self.client_2.symbol, {}).get('amount_usd', 0)) > 50:
    #             print('START WHILE')
    #             for client in self.clients-http:
    #                 print(f'START CLIENT {client.EXCHANGE_NAME}')
    #                 client.cancel_all_orders()
    #                 if res := client.get_positions().get(client.symbol, {}).get('amount'):
    #                     orderbook = client.get_orderbook()[client.symbol]
    #                     side = 'buy' if res < 0 else 'sell'
    #                     price = orderbook['bids'][0][0] if side == 'buy' else orderbook['asks'][0][0]
    #                     await client.create_order(abs(res), price, side, session)
    #                     time.sleep(7)

    def find_position_gap(self):
        position_gap = 0
        for client in self.clients:
            if res := client.get_positions().get(client.symbol):
                position_gap += res['amount']
        return position_gap

    def find_balancing_elements(self):
        position_gap = self.find_position_gap()
        amount_to_balancing = abs(position_gap) / len(self.clients)
        return position_gap, amount_to_balancing

    # @try_exc_regular
    # def check_last_ob(self, client_buy, client_sell, ob_sell, ob_buy):
    #     exchanges = client_buy.EXCHANGE_NAME + ' ' + client_sell.EXCHANGE_NAME
    #     last_obs = self.last_orderbooks.get(exchanges, None)
    #     self.last_orderbooks.update({exchanges: {'ob_buy': ob_buy['asks'][0][0], 'ob_sell': ob_sell['bids'][0][0]}})
    #     if last_obs:
    #         if ob_buy['asks'][0][0] == last_obs['ob_buy'] and ob_sell['bids'][0][0] == last_obs['ob_sell']:
    #             return False
    #         else:
    #             return True
    #     else:
    #         return True