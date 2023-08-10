#!/bin/bash

sudo apt update
sudo apt upgrade -y
sudo apt install python3-pip
sudo pip3 install --upgrade pip3
sudo pip3 install dydx_v3_python
sudo pip3 install python-dotenv
sudo pip3 install --no-cache-dir --user -r requirements.txt
sudo pip3 install --upgrade requests
touch rates.txt
