#!/usr/bin/env sh
# install python3.8
#sudo apt install python3.8-venv

# run
python3 -m venv venv
. venv/bin/activate
python3 -m thingsboard_gateway.tb_gateway
