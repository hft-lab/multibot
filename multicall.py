import traceback
from threading import Thread
# import unittest
import time


"""Call bunch of targets at the same time
Sample usage:
    import multicall
    my_pool = multicall.Pool()
    my_pool.add(some_method, some_arg, kw1=1, kw2=2)
    my_pool.add(other_method, other_args)
    my_pool.add(third_method)
    my_pool.call_and_wait() # will start all targets and wait until all done.
Please note: if you need to call:
    some_method(some_arg, kw1=1, kw2=2)
You have to use following syntax:
    my_pool.add(some_method, some_arg, kw1=1, kw2=2)
"""


class Pool:

    def __init__(self):
        self.calls = []

    def add(self, target, *args, **kwargs):
        try:
            time_start = time.time()
            print(target)
            self.calls.append([target, args, kwargs])
            print(f"Pool adding time: {time.time() - time_start}")
        except Exception as e:
            print(f"ERROR: {e}")
            traceback.print_exc()

    def call_all_and_wait(self):
        time_start = time.time()
        threads = []
        for t in self.calls:
            thread = Thread(target=t[0], args=t[1], kwargs=t[2])
            thread.start()
            threads.append(thread)
        for t in threads:
            t.join()
        self.calls = []
        print(f"Pool call and wait time: {time.time() - time_start}")
        print()

    def call_all(self):
        time_start = time.time()
        for t in self.calls:
            thread = Thread(target=t[0], args=t[1], kwargs=t[2])
            thread.start()
        self.calls = []
        print(f"Pool calling time: {time.time() - time_start}")
        print()


# class PoolTest(unittest.TestCase):
#
#     def sample_func(self, seconds, kwarg1=1):
#         import time
#         from datetime import datetime
#         print("\nsleeping", seconds, kwarg1, datetime.now())
#         time.sleep(seconds)
#         print("\nsleeping done", seconds, kwarg1, datetime.now())
#
#     def test_pool(self):
#         p = Pool()
#         p.add(self.sample_func, 1, "test1")
#         p.add(self.sample_func, 2, kwarg1="test2")
#         p.call_all_and_wait()
#         print("all done")


# bot.client_Bitmex.create_order(1000, 12000, 'Buy', 'Limit')
# bot.client_DYDX.create_order(0.1, 25000, 'SELL', 'LIMIT')
# my_pool = Pool()
# # my_pool.add(bot.client_Bitmex.create_order, 1000, 12000, side='Buy', type='Limit', clOrdID='TEST2')
# my_pool.add(bot.client_DYDX.create_order, 0.1, 25000, 'SELL', 'LIMIT')
# my_pool.call_all()  # will start all targets and wait until all done.
# #
# time.sleep(1)
# print(bot.client_Bitmex.open_orders(clOrdIDPrefix=''))

# a = {'a': 1, 'B': 2}
# print(a.keys())
