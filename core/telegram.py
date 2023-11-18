import traceback
import datetime
import requests
from enum import Enum

from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini', "utf-8")


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
        message_data = {"chat_id": group['chat_id'], "parse_mode": "HTML", "text": "<pre>" + str(message) + "</pre>"}
        try:
            r = requests.post(url, json=message_data)
            return r.json()
        except Exception as e:
            return e

    @staticmethod
    def start_message(multibot):
        message = f'MULTIBOT INSTANCE #{multibot.setts["INSTANCE_NUM"]} STARTED\n'
        message += f'{" | ".join(multibot.exchanges)}\n'
        message += f"ENV: {multibot.env}\n"
        message += f"MARKETS: {'|'.join(list(multibot.markets.keys()))}\n"
        message += f"STATE: {multibot.setts['STATE']}\n"
        message += f"LEVERAGE: {multibot.setts['LEVERAGE']}\n"
        message += f"DEALS PAUSE: {multibot.setts['DEALS_PAUSE']}\n"
        message += f"ORDER SIZE: {multibot.setts['ORDER_SIZE']}\n"
        message += f"TARGET PROFIT: {multibot.setts['TARGET_PROFIT']}\n"
        message += f"CLOSE PROFIT: {multibot.setts['CLOSE_PROFIT']}\n"
        message += f"START BALANCE: {multibot.start}\n"
        message += f"CURRENT BALANCE: {multibot.finish}\n"
        return message

    @staticmethod
    def start_balance_message(multibot):
        message = f'START BALANCES AND POSITION\n'
        message += f"ENV: {multibot.setts['ENV']}\n"
        total_balance = 0
        total_position = 0
        abs_total_position = 0

        for client in multibot.clients:
            coins, total_pos, abs_pos = multibot.get_positions_data(client)
            try:
                message += f"   EXCHANGE: {client.EXCHANGE_NAME}\n"
                message += f"TOT BAL: {int(round(client.get_balance(), 0))} USD\n"
                message += f"ACTIVE POSITIONS: {'|'.join(coins)}\n"
                message += f"TOT POS, USD: {total_pos}\n"
                message += f"ABS POS, USD: {abs_pos}\n"
                message += f"AVL BUY:  {round(client.get_available_balance()['buy'])}\n"
                message += f"AVL SELL: {round(client.get_available_balance()['sell'])}\n"
                total_position += total_pos
                abs_total_position += abs_pos
                total_balance += client.get_balance()
            except:
                traceback.print_exc()
        try:
            message += f"   TOTAL:\n"
            message += f"START BALANCE: {int(round(total_balance))} USD\n"
            message += f"TOT POS: {int(round(total_position))} USD\n"
            message += f"ABS TOT POS: {int(round(abs_total_position))} USD\n"
        except:
            traceback.print_exc()
        return message

    @staticmethod
    def ap_executed_message(multibot, client_buy, client_sell, expect_buy_px, expect_sell_px):
        message = f"AP EXECUTED | ENV: {multibot.env}\n"
        message += f"ENV ACTIVE EXCHANGES: {multibot.setts['EXCHANGES']}\n"
        message += f"DT: {datetime.datetime.utcnow()}\n"
        message += f"B.E.: {client_buy.EXCHANGE_NAME} | S.E.: {client_sell.EXCHANGE_NAME}\n"
        message += f"B.P.: {str(expect_buy_px)} | S.P.: {str(expect_sell_px)}\n"
        return message

    @staticmethod
    def order_error_message(env, symbol, client, order_id):
        message = f"ALERT NAME: Order Mistake\nCOIN: {symbol}\nCONTEXT: BOT\nENV: {env}\n"
        message += f"EXCHANGE: {client.EXCHANGE_NAME}\nOrder Id:{str(order_id)}\nError:{str(client.error_info)}"
        return message

    @staticmethod
    def coin_threshold_message(coin, exchange, direction, position, available, max_position_part):
        message = f"ACTIVE POS {coin} > {max_position_part} %"
        message += f"Exchange: {exchange} \n Direction: {direction} \n POS: {position} \n AVAIL: {available}"
        return message

# Скрытые шаблоны
# def create_result_message(self, deals_potential: dict, deals_executed: dict, time: int) -> str:
#     message = f"For last 3 min\n"
#     message += f"ENV: {self.setts['ENV']}\n"
#
#     if self.__check_env():
#         message += f'SYMBOL: {self.client_1.symbol}'
#
#     message += f"\n\nPotential deals:"
#     for side, values in deals_potential.items():
#         message += f"\n   {side}:"
#         for exchange, deals in values.items():
#             message += f"\n{exchange}: {deals}"
#     message += f"\n\nExecuted deals:"
#     for side, values in deals_executed.items():
#         message += f"\n   {side}:"
#         for exchange, deals in values.items():
#             message += f"\n{exchange}: {deals}"
#     return message


# async def balance_jump_alert(multibot):
#     percent_change = round(100 - multibot.finish * 100 / multibot.start, 2)
#     usd_change = multibot.finish - multibot.start
#
#     message = f"ALERT NAME: BALANCE JUMP {'🔴' if usd_change < 0 else '🟢'}\n"
#     message += f'{" | ".join(multibot.exchanges)}\n'
#     message += f"ENV: {multibot.env}\n"
#
#     message += f"BALANCE CHANGE %: {percent_change}\n"
#     message += f"BALANCE CHANGE USD: {usd_change}\n"
#     message += f"PREVIOUS BAL, USD: {multibot.start}\n"
#     message += f"CURRENT BAL, USD: {multibot.finish}\n"
#     message += f"PREVIOUS DT: {multibot.s_time}\n"
#     message += f"CURRENT DT: {multibot.f_time}"
#     return message


# Сообщение о залипании стакана
# def ob_alert_send(self, client_slippage, client_2, ts, client_for_unstuck=None):
#     if self.state == BotState.SLIPPAGE:
#         msg = "🔴ALERT NAME: Exchange Slippage Suspicion\n"
#         msg += f"ENV: {self.env}\nEXCHANGE: {client_slippage.EXCHANGE_NAME}\n"
#         msg += f"EXCHANGES: {client_slippage.EXCHANGE_NAME}|{client_2.EXCHANGE_NAME}\n"
#         msg += f"Current DT: {datetime.datetime.utcnow()}\n"
#         msg += f"Last Order Book Update DT: {datetime.datetime.utcfromtimestamp(ts / 1000)}"
#     else:
#         msg = "🟢ALERT NAME: Exchange Slippage Suspicion\n"
#         msg += f"ENV: {self.env}\nEXCHANGE: {client_for_unstuck.EXCHANGE_NAME}\n"
#         msg += f"EXCHANGES: {client_slippage.EXCHANGE_NAME}|{client_2.EXCHANGE_NAME}\n"
#         msg += f"Current DT: {datetime.datetime.utcnow()}\n"
#         msg += f"EXCHANGES PAIR CAME BACK TO WORK, SLIPPAGE SUSPICION SUSPENDED"
#     message = {
#         "chat_id": self.alert_id,
#         "msg": msg,
#         'bot_token': self.alert_token
#     }
#     self.tasks.put({
#         'message': message,
#         'routing_key': RabbitMqQueues.TELEGRAM,
#         'exchange_name': RabbitMqQueues.get_exchange_name(RabbitMqQueues.TELEGRAM),
#         'queue_name': RabbitMqQueues.TELEGRAM
#     })
