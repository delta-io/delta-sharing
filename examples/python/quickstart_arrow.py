#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os

import delta_sharing
import duckdb

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = os.path.dirname(__file__) + "/../open-datasets.share"
table_fqn = "delta_sharing.default.owid-covid-data"
limit = 10

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
print("########### All Available Tables #############")
print(client.list_all_tables())

# Create a first-class table handle with the new interface.
table = client.table(table_fqn)

# Load the same sample as an Arrow Table.
print(
    "########### Loading 10 rows from "
    + table_fqn
    + " as a PyArrow Table with client.table(...).snapshot(...).to_arrow #############"
)
arrow_table = table.snapshot(limit=limit).to_arrow()
print(arrow_table)

# Stream Arrow RecordBatches lazily.
print(
    "########### Reading the first Arrow RecordBatch from "
    + table_fqn
    + " with client.table(...).snapshot(...).to_record_batches #############"
)
arrow_batches = table.snapshot(limit=limit).to_record_batches()
print(next(arrow_batches))
# Close the iterator if you stop early so temporary resources are released promptly.
arrow_batches.close()

# Feed the Arrow RecordBatchReader into DuckDB so DuckDB consumes the stream.
print(
    "########### Querying "
    + table_fqn
    + " in DuckDB via client.table(...).snapshot(...).to_record_batch_reader #############"
)
arrow_batch_reader = table.snapshot(limit=limit).to_record_batch_reader()
duckdb_con = duckdb.connect()
duckdb_result = duckdb_con.from_arrow(arrow_batch_reader).limit(5).df()

print("########### Show the DuckDB result #############")
print(duckdb_result)

duckdb_result

# Change Data Feed (CDF) example.
# The bundled open-datasets.share profile does not expose a CDF-enabled table, so the example
# below is commented out by default. Replace `cdf_table_fqn` with a CDF-enabled table from your
# own share credentials before running it.
#
# cdf_table_fqn = "share.schema.cdf_table"
# cdf_changes = client.table(cdf_table_fqn).changes(starting_version=0)
#
# print(
#     "########### Loading table changes from "
#     + cdf_table_fqn
#     + " as a PyArrow Table with client.table(...).changes(...).to_arrow #############"
# )
# cdf_arrow_table = cdf_changes.to_arrow()
# print(cdf_arrow_table)
#
# print(
#     "########### Querying table changes from "
#     + cdf_table_fqn
#     + " in DuckDB via client.table(...).changes(...).to_record_batch_reader #############"
# )
# cdf_reader = cdf_changes.to_record_batch_reader()
# cdf_duckdb_result = duckdb_con.from_arrow(cdf_reader).limit(5).df()
# print(cdf_duckdb_result)
