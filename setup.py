# -*- coding: utf-8 -*-

#     Copyright 2019. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from setuptools import setup
from os import path

from thingsboard_gateway.gateway.constants import VERSION


this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    version=VERSION,
    name="thingsboard-gateway",
    author="ThingsBoard",
    author_email="info@thingsboard.io",
    license="Apache Software License (Apache Software License 2.0)",
    description="Thingsboard Gateway for IoT devices.",
    url="https://github.com/thingsboard/thingsboard-gateway",
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    python_requires=">=3.7",
    packages=['thingsboard_gateway', 'thingsboard_gateway.gateway',
              'thingsboard_gateway.storage', 'thingsboard_gateway.storage.memory',
              'thingsboard_gateway.storage.file', 'thingsboard_gateway.storage.sqlite',
              'thingsboard_gateway.connectors',
              'thingsboard_gateway.connectors.mqtt',
              'thingsboard_gateway.tb_utility'
              ],
    install_requires=[
        'cryptography',
        'jsonpath-rw',
        'regex',
        'pip',
        'PyYAML',
        'simplejson',
        'requests',
        'PyInquirer',
        'pyfiglet',
        'termcolor',
        'grpcio<=1.43.0',
        'protobuf',
        'tb-mqtt-client',
        'cache3',
        'cachetools'
    ],
    entry_points={
        'console_scripts': [
            'thingsboard-gateway = thingsboard_gateway.tb_gateway:daemon'
        ]
    })
