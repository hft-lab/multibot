#!/bin/bash

python3 multi_bot.py ./configs/eth_config_binance_apollox.ini &
python3 multi_bot.py ./configs/eth_config_dydx_apollox.ini &
python3 multi_bot.py ./configs/eth_config_dydx_binance.ini &
python3 multi_bot.py ./configs/eth_config_kraken_apollox.ini &
python3 multi_bot.py ./configs/eth_config_kraken_binance.ini &
python3 multi_bot.py ./configs/eth_config_dydx_kraken.ini &