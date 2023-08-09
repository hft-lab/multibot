#!/bin/bash

sudo apt update & wait
sudo apt upgrade -y & wait
sudo apt install python3-pip & wait
sudo python3 -m pip install --upgrade pip & wait
sudo pip3 install dydx_v3_python & wait
sudo pip3 install python-dotenv & wait
sudo pip3 install --no-cache-dir --user -r requirements.txt & wait
sudo pip3 install --upgrade requests & wait
touch rates.txt