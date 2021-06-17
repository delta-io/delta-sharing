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
from typing import Any, Callable, Dict, Optional, Sequence
from urllib.parse import urlparse
from json import loads

import fsspec
import pandas as pd
from pyarrow.dataset import dataset

from delta_sharing.converter import to_converters, get_empty_table
from delta_sharing.protocol import AddFile, Table
from delta_sharing.rest_client import DataSharingRestClient


class DeltaSharingReader:
    def __init__(
        self,
        table: Table,
        rest_client: DataSharingRestClient,
        *,
        predicateHints: Optional[Sequence[str]] = None,
        limitHint: Optional[int] = None
    ):
        self._table = table
        self._rest_client = rest_client

        if predicateHints is not None:
            assert isinstance(predicateHints, Sequence)
            assert all(isinstance(predicateHint, str) for predicateHint in predicateHints)
        self._predicateHints = predicateHints

        if limitHint is not None:
            assert isinstance(limitHint, int)
        self._limitHint = limitHint

    @property
    def table(self) -> Table:
        return self._table

    def predicateHints(self, predicateHints: Optional[Sequence[str]]) -> "DeltaSharingReader":
        return self._copy(predicateHints=predicateHints, limitHint=self._limitHint)

    def limitHint(self, limitHint: Optional[int]) -> "DeltaSharingReader":
        return self._copy(predicateHints=self._predicateHints, limitHint=limitHint)

    def to_pandas(self) -> pd.DataFrame:
        response = self._rest_client.list_files_in_table(
            self._table, predicateHints=self._predicateHints, limitHint=self._limitHint
        )

        schema_json = loads(response.metadata.schema_string)

        if len(response.add_files) == 0:
            return get_empty_table(schema_json)

        converters = to_converters(schema_json)

        return pd.concat(
            [DeltaSharingReader._to_pandas(file, converters) for file in response.add_files],
            axis=0,
            ignore_index=True,
            copy=False,
        )[[field["name"] for field in schema_json["fields"]]]

    def _copy(
        self, *, predicateHints: Optional[Sequence[str]], limitHint: Optional[int]
    ) -> "DeltaSharingReader":
        return DeltaSharingReader(
            table=self._table,
            rest_client=self._rest_client,
            predicateHints=predicateHints,
            limitHint=limitHint,
        )

    @staticmethod
    def _to_pandas(add_file: AddFile, converters: Dict[str, Callable[[str], Any]]) -> pd.DataFrame:
        protocol = urlparse(add_file.url).scheme
        filesystem = fsspec.filesystem(protocol)

        pdf = (
            dataset(source=add_file.url, format="parquet", filesystem=filesystem)
            .to_table()
            .to_pandas(
                date_as_object=True, use_threads=False, split_blocks=True, self_destruct=True
            )
        )

        for col, converter in converters.items():
            if col not in pdf.columns:
                if col in add_file.partition_values:
                    if converter is not None:
                        pdf[col] = converter(add_file.partition_values[col])
                    else:
                        raise ValueError("Cannot partition on binary or complex columns")
                else:
                    pdf[col] = None

        return pdf
