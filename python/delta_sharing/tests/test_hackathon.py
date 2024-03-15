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
import pytest

import os
import json
from datetime import date, datetime
from typing import Optional, Sequence

import pandas as pd

from delta_sharing.protocol import AddFile, AddCdcFile, CdfOptions, DeltaSharingProfile, Metadata, RemoveFile, Share, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import (
    ListFilesInTableResponse,
    ListTableChangesResponse,
    DataSharingRestClient,
)
from delta_sharing.tests.conftest import ENABLE_INTEGRATION, SKIP_MESSAGE


def test_to_pandas_non_partitioned(tmp_path):
    profile = DeltaSharingProfile.from_json('{"shareCredentialsVersion":1,"bearerToken":"xx","endpoint":"https://oregon.staging.cloud.databricks.com/api/2.0/delta-sharing/metastores/19a85dee-54bc-43a2-87ab-023d0ec16013","expirationTime":"9999-12-31T23:59:59.999Z"}')
    rest_client = DataSharingRestClient(profile)
    print("----[linzhou]----START-")
    delta_log_dir = "delta_log_for_dv_table_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    os.mkdir(delta_log_dir)
    os.chdir(delta_log_dir)
    print("----[linzhou]----getcwd-", os.getcwd())

#     print(rest_client.list_shares())
#     print(rest_client.list_all_tables(Share(name = "lin_dvsharing_bugbash_share_20231113")))
    rest_client.set_delta_format_header()
    filesResponse = rest_client.list_files_in_table(Table("dv_table", "lin_dvsharing_bugbash_share_20231113", "regular_schema"))
    f = open("0".zfill(20) + ".json", "w+")

    lines = filesResponse.lines
    protocol_json = json.loads(lines.pop(0))
    print("----[linzhou]----protocol_json:", protocol_json)
    deltaProtocol = {"protocol": protocol_json["protocol"]["deltaProtocol"]}
    json.dump(deltaProtocol, f)
    f.write("\n")

    metadata_json = json.loads(lines.pop(0))
    print("----[linzhou]----metadata_json:", metadata_json)
    deltaMetadata = {"metadata": metadata_json["metaData"]["deltaMetadata"]}
    json.dump(deltaMetadata, f)
    f.write("\n")
    for line in lines:
        file_json = json.loads(line)
        json.dump(file_json["file"]["deltaSingleAction"], f)
        f.write("\n")
    f.close()
    print("----[linzhou]----END---")

