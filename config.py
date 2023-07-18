import os
from os import getenv

from dotenv import load_dotenv

load_dotenv()

class Config:
    ENV = getenv('ENV')
    STATE = getenv("STATE")
    LEVERAGE = float(getenv("LEVERAGE", 1.0))
    EXCHANGES = getenv("EXCHANGES").split(",")
    DEALS_PAUSE = int(getenv("DEALS_PAUSE", 10))
    STOP_PERCENT = float(getenv("STOP_PERCENT", 0.5))
    ORDER_SIZE = float(getenv("ORDER_SIZE", 0))
    TARGET_PROFIT = float(getenv("TARGET_PROFIT", 0))
    LIMIT_SHIFTS = float(getenv("LIMIT_SHIFTS", 0))
    COIN = getenv('COIN')
    API_TOKEN = getenv('API_TOKEN')
    API_URL = getenv('API_TOKEN')

    TELEGRAM_TOKEN = getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = getenv("TELEGRAM_CHAT_ID") # this is tests chat
    TELEGRAM_DAILY_CHAT_ID = getenv("TELEGRAM_DAILY_CHAT_ID")
    TELEGRAM_INV_CHAT_ID = getenv("TELEGRAM_INV_CHAT_ID")
    ALERT_BOT_TOKEN = getenv('ALERT_BOT_TOKEN')
    ALERT_CHAT_ID = getenv('ALERT_CHAT_ID')

    POSTGRES = {
        'database': getenv('POSTGRES_NAME'),
        'user': getenv('POSTGRES_USER'),
        'password': getenv('POSTGRES_PASSWORD'),
        'host': getenv('POSTGRES_HOST'),
        'port': getenv('POSTGRES_PORT'),
    }

    RABBIT = {
        "host": getenv("RABBIT_HOST"),
        "port": getenv("RABBIT_PORT", 5672),
        "username": getenv("RABBIT_USERNAME"),
        "password": getenv("RABBIT_PASSWORD")
    }

    BITMEX = {
        "api_key":  getenv("BITMEX_API_KEY"),
        "api_secret": getenv("BITMEX_API_SECRET"),
        "symbol": getenv("BITMEX_SYMBOL"),
        "bitmex_shift": int(getenv("OKEX_SHIFT"))
    }

    DYDX = {
        "eth_address": getenv("DYDX_ETH_ADDRESS"),
        "privateKey":  getenv("DYDX_PRIVATE_KEY"),
        "publicKeyYCoordinate":  getenv("DYDX_PUBLIC_KEY_Y_COORDINATE"),
        "publicKey":  getenv("DYDX_PUBLIC_KEY"),
        "eth_private_key":  getenv("DYDX_ETH_PRIVATE_KEY"),
        "infura_key":  getenv("DYDX_INFURA_KEY"),
        "key":  getenv("DYDX_KEY"),
        "secret":  getenv("DYDX_SECRET"),
        "passphrase":  getenv("DYDX_PASSPHRASE"),
        "symbol":  getenv("DYDX_SYMBOL"),
        "dydx_shift": int(getenv("DYDX_SHIFT"))
    }

    BINANCE = {
        "api_key": getenv("BINANCE_API_KEY"),
        "secret_key": getenv("BINANCE_SECRET_API"),
        "symbol": getenv("BINANCE_SYMBOL"),
        "binance_shift": int(getenv("BINANCE_SHIFT"))
    }

    OKX = {
        "public_key": getenv("OKEX_PUBLIC_KEY"),
        "secret_key": getenv("OKEX_SECRET_KEY"),
        "passphrase": getenv("OKEX_PASSPHRASE"),
        "symbol": getenv("OKEX_SYMBOL"),
        "okex_shift": int(getenv("OKEX_SHIFT"))
    }

    KRAKEN = {
        "api_key": getenv("KRAKEN_API_KEY"),
        "secret_key": getenv("KRAKEN_SECRET_API"),
        "symbol": getenv("KRAKEN_SYMBOL"),
        "apollox_shift": int(getenv("KRAKEN_SHIFT"))
    }

    APOLLOX = {
        "api_key": getenv("APOLLOX_API_KEY"),
        "secret_key": getenv("APOLLOX_SECRET_API"),
        "symbol": getenv("APOLLOX_SYMBOL"),
        "apollox_shift": int(getenv("APOLLOX_SHIFT"))
    }

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '[%(asctime)s][%(threadName)s] %(funcName)s: %(message)s'
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'simple',
                'stream': 'ext://sys.stdout'
            },
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': False
            },
        }
    }