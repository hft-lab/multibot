class AP_Status:
    SUCCESS = 'Success'
    FULL_FAIL = 'Full Fail'
    DISBALANCE = 'Disbalance'
    NEW = 'New'
    UNDEFINED = 'Undefined' #temp status


class RabbitMqQueues:
    TELEGRAM = 'logger.event.send_to_telegram'
    UPDATE_LAUNCH = 'logger.event.update_bot_launches'
    ARBITRAGE_POSSIBILITIES = 'logger.event.insert_arbitrage_possibilities'
    ORDERS = 'logger.event.insert_orders'
    UPDATE_ORDERS = 'logger.event.update_orders'
    CHECK_BALANCE = 'logger.event.check_balance'
    BALANCES = 'logger.event.insert_balances'
    # BALANCES = 'logger.event.insert_balances'
    # BALANCE_DETALIZATION = 'logger.event.insert_balance_detalization'
    # DISBALANCE = 'logger.event.insert_disbalances'
    # FUNDINGS = 'logger.event.insert_funding'
    # SAVE_MISSED_ORDERS = 'logger.event.save_missed_orders'
    # BOT_CONFIG = 'logger.event.insert_bot_config'
    # DEALS_REPORT = 'logger.event.insert_deals_reports'
    # BALANCING_REPORTS = 'logger.event.insert_balancing_reports'
    # PING = 'logger.event.insert_ping_logger'
    # BALANCE_JUMP = 'logger.event.insert_balance_jumps'


