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

from datetime import date
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
    pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
    pdf2 = pd.DataFrame({"a": [4, 5, 6], "b": ["d", "e", "f"]})

    pdf1.to_parquet(tmp_path / "pdf1.parquet")
    pdf2.to_parquet(tmp_path / "pdf2.parquet")

    profile = DeltaSharingProfile.from_json('{"shareCredentialsVersion":1,"bearerToken":"xx","endpoint":"https://oregon.staging.cloud.databricks.com/api/2.0/delta-sharing/metastores/19a85dee-54bc-43a2-87ab-023d0ec16013","expirationTime":"9999-12-31T23:59:59.999Z"}')
    rest_client = DataSharingRestClient(profile)
    print("----[linzhou]----START-")
#     print(rest_client.list_shares())
#     print(rest_client.list_all_tables(Share(name = "lin_dvsharing_bugbash_share_20231113")))
    rest_client.set_delta_format_header()
    print(rest_client.list_files_in_table(Table("dv_table", "lin_dvsharing_bugbash_share_20231113", "regular_schema")))
    print("----[linzhou]----END---")
#     reader = DeltaSharingReader(Table("table_name", "share_name", "schema_name"), rest_client)
#     pdf = reader.to_pandas()
#     expected = pd.concat([pdf1, pdf2]).reset_index(drop=True)
#     pd.testing.assert_frame_equal(pdf, expected)
#
#     reader = DeltaSharingReader(
#         Table("table_name", "share_name", "schema_name"),
#         RestClientMock(),
#         jsonPredicateHints="dummy_hints"
#     )
#     pdf = reader.to_pandas()
#     expected = pd.concat([pdf1]).reset_index(drop=True)
#     pd.testing.assert_frame_equal(pdf, expected)
#
#     reader = DeltaSharingReader(
#         Table("table_name", "share_name", "schema_name"),
#         RestClientMock(),
#         predicateHints="dummy_hints"
#     )
#     pdf = reader.to_pandas()
#     expected = pd.concat([pdf2]).reset_index(drop=True)
#     pd.testing.assert_frame_equal(pdf, expected)

