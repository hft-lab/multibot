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


class RabbitMqQueues:
    TELEGRAM = 'logger.event.send_message'
    DEALS_REPORT = 'logger.event.insert_deals_reports'
    BALANCE_CHECK = 'logger.event.insert_balance_check'
    BALANCING_REPORTS = 'logger.event.insert_balancing_reports'
    PING = 'logger.event.insert_ping_logger'
    BALANCE_JUMP = 'logger.event.insert_balance_jumps'

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