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

DESCRIPTION = "Python Connector for Delta Sharing"

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

try:
    exec(open('delta_sharing/version.py').read())
except IOError:
    print("Failed to load Delta Sharing version file for packaging.",
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__  # noqa

setup(
    name='delta-sharing',
    version=VERSION,
    packages=[
        'delta_sharing',
    ],
    python_requires='>=3.7',
    install_requires=[
        'pandas',
        'pyarrow>=4.0.0',
        'fsspec>=0.7.4',
        'requests',
        'aiohttp',
        'dataclasses;python_version<"3.7"',
        'yarl>=1.6.0',
    ],
    extras_require={
        's3': ['s3fs'],
        'abfs': ['adlfs'],
        'adl': ['adlfs'],
        'gcs': ['gcsfs'],
        'gs': ['gcsfs'],
    },
    author="The Delta Lake Project Authors",
    author_email="delta-users@googlegroups.com",
    license="Apache-2.0",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    url="https://github.com/delta-io/delta-sharing/",
    project_urls={
        'Source': 'https://github.com/delta-io/delta-sharing',
        'Documentation': 'https://github.com/delta-io/delta-sharing',
        'Issues': 'https://github.com/delta-io/delta-sharing/issues'
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
