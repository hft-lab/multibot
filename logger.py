import os
import uuid
from datetime import datetime
from core.wrappers import try_exc_regular, try_exc_async


class Logging:
    def __init__(self):
        self.log_folder_name = 'Logs'
        self.rates_log_file_name = 'rates_new.txt'
        self.rates_log_file_path = self.log_folder_name + "/" + self.rates_log_file_name
        self.launch_params_log_file_name = 'launch_params.txt'
        self.launch_params_log_file_path = self.log_folder_name + "/" + self.launch_params_log_file_name
        self.check_log_folder_and_files_exist()
        self.launch_id = self.get_launch_id()

    @try_exc_regular
    def log_rates(self, iteration, result):
        # input format: {'Kraken__CRV': {'top_bid': 0.4369, 'top_ask': 0.4377, 'bid_vol': 2062, 'ask_vol': 4182,
        # ts_exchange': 1694169801252, 'ts_start': , 'ts_end': , 'Status': 'Ok'},...}
        with open(self.rates_log_file_path, 'a') as file:
            for exchange__coin, ob in result.items():
                exchange, coin = exchange__coin.split('__')
                top_bid, top_ask = ob['top_bid'], ob['top_ask']
                bid_vol, ask_vol = ob['bid_vol'], ob['ask_vol']
                # ts_start, ts_end = ob['ts_start'], ob['ts_end']
                # utc_start_time = datetime.utcfromtimestamp(ts_start/1000).strftime("%Y-%m-%d %H:%M:%S")
                string_to_log = f"{self.launch_id},{iteration},{coin},ask,{exchange},{top_ask},{ask_vol}\n"
                string_to_log += f"{self.launch_id},{iteration},{coin},bid,{exchange},{top_bid},{bid_vol}\n"
                file.write(string_to_log)

    @try_exc_regular
    def log_launch_params(self, clients_list):
        self.check_log_folder_and_files_exist()
        launch_datetime = datetime.utcnow()
        with open(self.launch_params_log_file_path, 'a') as launch_params_log:
            for client in clients_list:
                launch_params_log.write(
                    f"{self.launch_id}, {launch_datetime},{client.EXCHANGE_NAME}, {client.taker_fee}\n")

    @try_exc_regular
    def get_launch_id(self):
        with open(self.launch_params_log_file_path, 'r') as launch_param_log:
            last_line = launch_param_log.readlines()[-1]
            try:
                return int(last_line.split(', ')[0]) + 1
            except:
                return 0

    @try_exc_regular
    def check_log_folder_and_files_exist(self):
        if not os.path.exists(self.log_folder_name):
            os.makedirs(self.log_folder_name)
        if not os.path.exists(self.rates_log_file_path):
            with open(self.rates_log_file_path, 'w') as rates_log:
                rates_log.write(
                    f"Launch Id,Iteration,Coin,Direction,Exchange,Top price,Start_dt,Request Time,Volume,Status\n")

        if not os.path.exists(self.launch_params_log_file_path):
            with open(self.launch_params_log_file_path, 'w') as launch_param_log:
                launch_param_log.write(f"Launch Id,DateTime,Exchange,Fee\n")


if __name__ == '__main__':
    logger = Logging()
    print(logger.get_launch_id())
