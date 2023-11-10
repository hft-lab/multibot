class PositionSideEnum:
    LONG = 'LONG'
    SHORT = 'SHORT'
    BOTH = 'BOTH'

    @classmethod
    def all_position_sides(cls):
        return [cls.LONG, cls.SHORT, cls.BOTH]


class ConnectMethodEnum:
    PUBLIC = 'public'
    PRIVATE = 'private'


class EventTypeEnum:
    ACCOUNT_UPDATE = 'ACCOUNT_UPDATE'
    ORDER_TRADE_UPDATE = 'ORDER_TRADE_UPDATE'


class BotState:
    PARSER = 'PARSER'
    BOT = 'BOT'
    SLIPPAGE = 'SLIPPAGE'


class RabbitMqQueues:
    TELEGRAM = 'logger.event.send_to_telegram'
    DEALS_REPORT = 'logger.event.insert_deals_reports'

    BALANCING_REPORTS = 'logger.event.insert_balancing_reports'
    PING = 'logger.event.insert_ping_logger'
    BALANCE_JUMP = 'logger.event.insert_balance_jumps'


    # NEW -----------------------------------------------------------------
    ARBITRAGE_POSSIBILITIES = 'logger.event.insert_arbitrage_possibilities'
    ORDERS = 'logger.event.insert_orders'
    BALANCES = 'logger.event.insert_balances'
    BALANCE_DETALIZATION = 'logger.event.insert_balance_detalization'
    DISBALANCE = 'logger.event.insert_disbalances'
    FUNDINGS = 'logger.event.insert_funding'
    UPDATE_ORDERS = 'logger.event.update_orders'
    SAVE_MISSED_ORDERS = 'logger.event.save_missed_orders'
    CHECK_BALANCE = 'logger.event.check_balance'
    BOT_CONFIG = 'logger.event.insert_bot_config'
    UPDATE_LAUNCH = 'logger.event.update_bot_launches'

    @staticmethod
    def get_exchange_name(routing_key: str):
        routing_list = routing_key.split('.')

        if len(routing_list) > 1 and ('periodic' in routing_key or 'event' in routing_key):
            return routing_list[0] + '.' + routing_list[1]

        raise f'Wrong routing key:{routing_key}'


class ResponseStatus:
    SUCCESS = 'success'
    NO_CONNECTION = 'no_connection'
    ERROR = 'error'


class OrderStatus:
    NOT_EXECUTED = 'Not Executed'
    # DELAYED_FULLY_EXECUTED = 'Delayed Fully Executed'
    PARTIALLY_EXECUTED = 'Partially Executed'
    FULLY_EXECUTED = 'Fully Executed'
    PROCESSING = 'Processing'


class ClientsOrderStatuses:
    FILLED = 'FILLED'
    CANCELED = 'CANCELED'
    NEW = 'NEW'
    PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    EXPIRED = 'EXPIRED'
    PENDING = 'PENDING'

