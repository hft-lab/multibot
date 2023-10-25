import time
from clients.enums import BotState
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