import traceback
def start_message(multibot):
    coin = multibot.clients[0].symbol.split('USD')[0].replace('-', '').replace('/', '')
    message = f'MULTIBOT {coin} STARTED\n'
    message += f'{" | ".join(multibot.exchanges)}\n'
    message += f"RIBS: {multibot.setts['RIBS']}\n"
    message += f"ENV: {multibot.env}\n"
    message += f"STATE: {multibot.setts['STATE']}\n"
    message += f"LEVERAGE: {multibot.setts['LEVERAGE']}\n"
    # message += f"EXCHANGES: {self.client_1.EXCHANGE_NAME} {self.client_2.EXCHANGE_NAME}\n"
    message += f"DEALS PAUSE: {multibot.setts['DEALS_PAUSE']}\n"
    message += f"ORDER SIZE: {multibot.setts['ORDER_SIZE']}\n"
    message += f"TARGET PROFIT: {multibot.setts['TARGET_PROFIT']}\n"
    message += f"CLOSE PROFIT: {multibot.setts['CLOSE_PROFIT']}\n"
    message += f"START BALANCE: {multibot.start}\n"
    message += f"CURRENT BALANCE: {multibot.finish}\n"
    return message
    # await self.send_message(message, int(config['TELEGRAM']['CHAT_ID']), config['TELEGRAM']['TOKEN'])


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

async def balance_jump_alert(multibot):
    percent_change = round(100 - multibot.finish * 100 / multibot.start, 2)
    usd_change = multibot.finish - multibot.start

    message = f"ALERT NAME: BALANCE JUMP {'🔴' if usd_change < 0 else '🟢'}\n"
    message += f'{" | ".join(multibot.exchanges)}\n'
    message += f"ENV: {multibot.env}\n"

    if not multibot.__check_env():
        message += "CHANGE STATE TO PARSER\n"

    message += f"BALANCE CHANGE %: {percent_change}\n"
    message += f"BALANCE CHANGE USD: {usd_change}\n"
    message += f"PREVIOUS BAL, USD: {multibot.start}\n"
    message += f"CURRENT BAL, USD: {multibot.finish}\n"
    message += f"PREVIOUS DT: {multibot.s_time}\n"
    message += f"CURRENT DT: {multibot.f_time}"
    return message

