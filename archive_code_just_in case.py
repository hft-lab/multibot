import csv
import time
import asyncio
from datetime import datetime
from clients.enums import BotState
from core.queries import get_last_balance_jumps, get_total_balance, get_last_launch, get_last_deals


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