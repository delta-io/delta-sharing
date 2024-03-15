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
import delta_kernel_python
import pyarrow as pa
import sys

import os
import json
from datetime import datetime

from delta_sharing.protocol import DeltaSharingProfile, Share, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import (
    DataSharingRestClient
)


def test_read_delta_sharing_dv_table():
    profile = DeltaSharingProfile.from_json('{"shareCredentialsVersion":1,"bearerToken":"xx","endpoint":"https://oregon.staging.cloud.databricks.com/api/2.0/delta-sharing/metastores/19a85dee-54bc-43a2-87ab-023d0ec16013","expirationTime":"9999-12-31T23:59:59.999Z"}')
    rest_client = DataSharingRestClient(profile)
    print("----[linzhou]----START-")
    delta_log_dir = "delta_log_for_dv_table_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    os.mkdir(delta_log_dir)
    os.chdir(delta_log_dir)
    table_path = "file:///" + os.getcwd()
    os.mkdir("_delta_log")
    os.chdir("_delta_log")
    print("----[linzhou]----_delta_log:", os.getcwd())

#     print(rest_client.list_shares())
    allTables = rest_client.list_all_tables(Share(name = "lin_dvsharing_bugbash_share_20231113")).tables
    for eachTable in allTables:
        print(eachTable)
    rest_client.set_delta_format_header()
    filesResponse = rest_client.list_files_in_table(Table("dv_table", "lin_dvsharing_bugbash_share_20231113", "regular_schema"))
    f = open("0".zfill(20) + ".json", "w+")

    lines = filesResponse.lines
    protocol_json = json.loads(lines.pop(0))
    deltaProtocol = {"protocol": protocol_json["protocol"]["deltaProtocol"]}
    json.dump(deltaProtocol, f)
    f.write("\n")

    metadata_json = json.loads(lines.pop(0))
    deltaMetadata = {"metaData": metadata_json["metaData"]["deltaMetadata"]}
    json.dump(deltaMetadata, f)
    f.write("\n")
    for line in lines:
        file_json = json.loads(line)
        json.dump(file_json["file"]["deltaSingleAction"], f)
        f.write("\n")
    f.close()
    print("----[linzhou]----MID---")

    print("----[linzhou]----tablepath:", table_path)

    interface = delta_kernel_python.PythonInterface(table_path)
    table = delta_kernel_python.Table(table_path)
    snapshot = table.snapshot(interface)
    print("Table Version %i" % snapshot.version())

    scan = delta_kernel_python.ScanBuilder(snapshot).build()
    table = pa.Table.from_batches(scan.execute(interface))
    print(table.to_pandas())

    print("----[linzhou]----END---")


test_read_delta_sharing_dv_table()