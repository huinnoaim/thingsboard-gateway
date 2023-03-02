#!/usr/bin/env sh
# install python3.9
#sudo apt install python3.9-venv

# run
python3.9 -m venv venv
. venv/bin/activate
python -m thingsboard_gateway.tb_gateway
