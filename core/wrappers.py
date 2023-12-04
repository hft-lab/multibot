import time
import traceback
from core.telegram import Telegram, TG_Groups

telegram = Telegram()


def timeit(func):
    async def wrapper(*args, **kwargs):
        ts_start = int(time.time() * 1000)
        result = await func(*args, **kwargs)
        result['ts_start'] = ts_start
        result['ts_end'] = int(time.time() * 1000)
        return result

    return wrapper


def try_exc_regular(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception:
            formatted_traceback = str(traceback.format_exc())
            print(formatted_traceback)
            telegram.send_message(formatted_traceback, TG_Groups.Alerts)

    return wrapper


def try_exc_async(func):
    async def wrapper(*args, **kwargs):
        try:
            result = await func(*args, **kwargs)
            return result
        except Exception:
            formatted_traceback = str(traceback.format_exc())
            print(formatted_traceback)
            telegram.send_message(formatted_traceback, TG_Groups.Alerts)
    return wrapper

