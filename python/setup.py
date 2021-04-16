#!/usr/bin/env python

#
# Copyright (C) 2021 The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from io import open
from os import path
from setuptools import setup
import sys

DESCRIPTION = "Delta Exchange"

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

try:
    exec(open('delta_exchange/version.py').read())
except IOError:
    print("Failed to load Delta Exchange version file for packaging.",
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__  # noqa

setup(
    name='delta-exchange',
    version=VERSION,
    packages=[
        'delta_exchange',
    ],
    python_requires='>=3.7,<3.10',
    install_requires=[
        'pandas',
        'pyarrow',
        'fsspec',
    ],
    author="Databricks",
    license='http://www.apache.org/licenses/LICENSE-2.0',
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
