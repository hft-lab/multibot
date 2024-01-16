import traceback
from datetime import datetime
import requests
from core.ap_class import AP
import sys
import aiohttp
import asyncio
from configparser import ConfigParser

config = ConfigParser()
config.read(sys.argv[1], "utf-8")


class TG_Groups:
    _main_id = int(config['TELEGRAM']['CHAT_ID'])
    _main_token = config['TELEGRAM']['TOKEN']
    # self.daily_chat_id = int(config['TELEGRAM']['DAILY_CHAT_ID'])
    # self.inv_chat_id = int(config['TELEGRAM']['INV_CHAT_ID'])
    _alert_id = int(config['TELEGRAM']['ALERT_CHAT_ID'])
    _alert_token = config['TELEGRAM']['ALERT_BOT_TOKEN']
    _debug_id = int(config['TELEGRAM']['DIMA_DEBUG_CHAT_ID'])
    _debug_token = config['TELEGRAM']['DIMA_DEBUG_BOT_TOKEN']
    _traces_id = int(config['TELEGRAM']['TRACES_CHAT_ID'])
    _traces_token = config['TELEGRAM']['DIMA_DEBUG_BOT_TOKEN']

    MainGroup = {'chat_id': _main_id, 'bot_token': _main_token}
    Alerts = {'chat_id': _alert_id, 'bot_token': _alert_token}
    DebugDima = {'chat_id': _debug_id, 'bot_token': _debug_token}
    Traces = {'chat_id': _traces_id, 'bot_token': _traces_token}


class Telegram:
    def __init__(self):
        self.tg_url = "https://api.telegram.org/bot"
        self.TG_DEBUG = bool(int(config['TELEGRAM']['TG_DEBUG']))
        self.env = config['SETTINGS']['ENV']

    @staticmethod
    async def async_send_message(url, message_data):
        async with aiohttp.ClientSession() as session:
            await session.get(url=url, json=message_data)
            await session.close()

    def send_message(self, message: str, tg_group_obj: TG_Groups = None,mode = 'sync'):
        if (not self.TG_DEBUG) and ((tg_group_obj is None) or (tg_group_obj == TG_Groups.DebugDima)):
            print("TG_DEBUG IS OFF. MESSAGE HASN'T SENT")
        else:
            group = tg_group_obj if tg_group_obj else TG_Groups.DebugDima
            url = self.tg_url + group['bot_token'] + "/sendMessage"
            message_data = {"chat_id": group['chat_id'], "parse_mode": "HTML",
                            "text": f"<pre>ENV: {self.env}\n{str(message)}</pre>"}
            try:
                r = requests.post(url, json=message_data)
                return r.json()
                #OPTION 1
                # loop = asyncio.get_event_loop()
                # loop.create_task(self.async_send_message(url, message_data))
            except Exception as e:
                print(f'TELEGRAM MESSAGE NOT SENT:')
                traceback.print_exc()

    def send_bot_launch_message(self, multibot, group: TG_Groups = None):
        message = f'MULTIBOT INSTANCE #{multibot.setts["INSTANCE_NUM"]} LAUNCHED\n'
        message += f'{" | ".join(multibot.exchanges)}\n'
        message += f"ENV: {multibot.env}\n"
        message += f"MARKETS: {'|'.join(list(multibot.markets.keys()))}\n"
        message += f"STATE: {multibot.setts['STATE']}\n"
        message += f"LEVERAGE: {multibot.setts['LEVERAGE']}\n"
        message += f"DEALS PAUSE: {multibot.setts['DEALS_PAUSE']}\n"
        message += f"ORDER SIZE: {multibot.setts['ORDER_SIZE']}\n"
        message += f"TARGET PROFIT: {multibot.setts['TARGET_PROFIT']}\n"
        message += f"CLOSE PROFIT: {multibot.setts['CLOSE_PROFIT']}\n"
        self.send_message(message, group)
        return message

    def send_parser_launch_message(self, multiparser, group: TG_Groups = None):
        message = f'MULTIPARSER INSTANCE #{multiparser.setts["INSTANCE_NUM"]} LAUNCHED\n'
        message += f'{" | ".join(multiparser.exchanges)}\n'
        message += f"ENV: {multiparser.env}\n"
        message += f"MODE: {multiparser.mode}\n"
        message += f"#Exchanges: {len(multiparser.exchanges)}\n"
        message += f"#RIBS: {int(len(multiparser.ribs)/2)}\n"
        message += f"#COINS: {len(list(multiparser.markets.keys()))}\n"
        message += f"TARGET PROFIT(S): {multiparser.profits_list}\n"
        self.send_message(message, group)
        return message

    def send_start_balance_message(self, multibot, group: TG_Groups = None):
        message = f'START BALANCES AND POSITION\n'
        message += f"ENV: {multibot.setts['ENV']}\n"
        total_balance = 0
        total_position = 0
        abs_total_position = 0

        for exchange, available_balance in multibot.available_balances.items():
            balance, total_pos, abs_pos, markets,*_ = multibot.positions[exchange].values()
            message += f"   EXCHANGE: {exchange}\n"
            message += f"TOT BAL: {int(round(balance, 0))} USD\n"
            message += f"TOT POS, USD: {total_pos}\n"
            message += f"ABS POS, USD: {abs_pos}\n"
            message += f"AVL BUY (NEW POS):  {round(available_balance['buy'])}\n"
            message += f"AVL SELL (NEW POS): {round(available_balance['sell'])}\n-\n"
            message += f"ACTIVE POSITIONS: {'|'.join(markets)}\n\n"
            total_position += total_pos
            abs_total_position += abs_pos
            total_balance += balance

        message += f"   TOTAL:\n"
        message += f"START BALANCE: {int(round(total_balance))} USD\n"
        message += f"TOT POS: {int(round(total_position))} USD\n"
        message += f"ABS TOT POS: {int(round(abs_total_position))} USD\n"
        self.send_message(message, group)
        return message

    def send_ap_executed_message(self, ap: AP, group: TG_Groups = None):
        message = f"AP EXECUTED\n"
        message += f"STATUS: {ap.status}\n"
        message += f"SYMBOL: {ap.coin}\n"
        message += f"DT: {datetime.utcnow()}\n"
        message += f"B.E.: {ap.buy_exchange} | S.E.: {ap.sell_exchange}\n"
        message += f"Deal direction: {ap.deal_direction}\n"
        message += f"Target profit: {ap.target_profit}\n"
        message += f"Expect profit: {ap.profit_rel_parser}\n"
        message += f"FACT profit: {ap.profit_rel_fact}\n"
        # message += f"B.Parser P.: {str(ap.buy_price_parser)} | S.Parser P.: {str(ap.sell_price_parser)}\n"
        message += f"B.TARGET P.: {str(ap.buy_price_parser)} | S.TARGET P.: {str(ap.sell_price_parser)}\n"
        message += f"B.Shifted P.: {str(ap.buy_price_shifted)} | S.Shifted P.: {str(ap.sell_price_shifted)}\n"
        message += f"B.Fitted P.: {str(ap.buy_price_fitted)} | S.Fitted P.: {str(ap.sell_price_fitted)}\n"
        message += f"B.REAL P.: {str(ap.buy_price_real)} | S.REAL P.: {str(ap.sell_price_real)}\n"
        message += f"Target Deal Size Amount: {ap.deal_size_amount_target}\n"
        message += f"Real DSA Buy: {ap.buy_amount_real}| Real DSA Sell: {ap.sell_amount_real}\n"
        self.send_message(message, group)
        return message

    def send_ap_expired_message(self, deal: AP, group: TG_Groups = None):
        message = f'ALERT NAME: AP EXPIRED AFTER OB UPDATE\n---\n' \
                  f'ACTUAL PROFIT: {round(deal.profit_rel_target, 5)}\n' \
                  f'TARGET PROFIT: {deal.target_profit}\n' \
                  f'PARSER PROFIT : {round(deal.profit_rel_parser, 5)}\n' \
                  f'DEAL DIRECTION: {deal.deal_direction}\n' \
                  f'EXCH_BUY: {deal.buy_exchange}, EXCH_SELL: {deal.sell_exchange}\n' \
                  f'MARKET_BUY: {deal.buy_market}, MARKET_SELL: {deal.sell_market}\n' \
                  f'ACTUAL PRICES: BUY: {deal.buy_price_target}, SELL: {deal.sell_price_target}\n' \
                  f'PARSER PRICES: BUY: {deal.buy_price_parser}, SELL: {deal.sell_price_parser}\n' \
                  f'TIMINGS:\n' \
                  f'CHOOSE TIME DURATION: {round(deal.time_choose, 4)}\n' \
                  # f'PARSE DURATION: {round(deal.time_parser, 4)}\n' \
                  # f'OB CHECK DURATION: {round(deal.time_check_ob, 4)}\n'
                  # f'DEFINE POT. DEALS DURATION {round(deal.time_define_potential_deals, 4)}\n' \

        self.send_message(message, group)
        return message

    def send_ap_still_active_parser(self, deal: AP, group: TG_Groups = None):
        message = f'ALERT NAME: PARSER. AP ACTIVE AFTER CHECK\n---\n' \
                  f'ACTUAL PROFIT: {round(deal.profit_rel_target, 5)}\n' \
                  f'TARGET PROFIT: {deal.target_profit}\n' \
                  f'BUY_E: {deal.buy_exchange}, SELL_E: {deal.sell_exchange}\n' \
                  f'BUY_M: {deal.buy_market}, SELL_M: {deal.sell_market}\n' \
                  f'ACTUAL PRICES: BUY: {deal.buy_price_target}, SELL: {deal.sell_price_target}\n' \
                  f'TIMINGS:\n' \
                  f'PARSE DURATION: {round(deal.time_parser, 4)}\n' \
                  f'DEFINE POT. DEALS DURATION {round(deal.time_define_potential_deals, 4)}\n' \
                  f'CHOOSE TIME DURATION: {round(deal.time_choose, 4)}\n' \
                  f'OB CHECK DURATION: {round(deal.time_check_ob, 4)}\n'

        self.send_message(message, group)
        return message

    def send_different_amounts_alert(self, chosen_deal: AP, rounded_deal_size_amount, group):
        client_buy, client_sell = chosen_deal.client_buy, chosen_deal.client_sell
        buy_exchange, sell_exchange = chosen_deal.buy_exchange, chosen_deal.sell_exchange
        buy_market, sell_market = chosen_deal.buy_market, chosen_deal.sell_market
        message = f'ALERT: Разошлись целевые объемы покупки, продажи и rounded_amount\n ' \
                  f'{client_buy.amount=}\n' \
                  f'{client_sell.amount=}\n' \
                  f'{rounded_deal_size_amount=}\n' \
                  f'{buy_exchange=}\n' \
                  f'{buy_market=}\n' \
                  f'{sell_market=}\n' \
                  f'{sell_exchange=}\n' \
                  f'ACTION: Проверить'
        self.send_message(message, group)
        return message

    def send_order_error_message(self, env, symbol, client, order_id, group: TG_Groups = None):
        message = f"ALERT NAME: Order Mistake\n" \
                  f"EXCHANGE: {client.EXCHANGE_NAME}\n" \
                  f"SYMBOL: {symbol}\n" \
                  f"Order Id:{str(order_id)}\n" \
                  f"Error:{str(client.error_info)}"
        self.send_message(message, group)
        return message

    # @staticmethod
    # def coin_threshold_message(coin, exchange, direction, position, available, max_position_part):
    #     message = f"ALERT: MARKET SHARE THRESHOLD EXCEEDED\n" \
    #               f"COIN: {coin} \nExchange: {exchange} \n" \
    #               f"MARKET SHARE {round(position/available,3)} > {max_position_part}%\n" \
    #               f"Direction: {direction} \nPOSITION: {round(position,1)} \n" \
    #               f"AVAILABLE BALANCE: {round(available,1)}\n" \
    #               f"ACTION: Рынок добавлен список исключений"
    #     return message


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

if __name__ == '__main__':
    tg = Telegram()
    tg.send_message('Hi All', TG_Groups.MainGroup)
