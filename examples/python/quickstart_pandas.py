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

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = os.path.dirname(__file__) + "/../open-datasets.share"
table_fqn = "delta_sharing.default.owid-covid-data"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
print("########### All Available Tables #############")
print(client.list_all_tables())

# Create a first-class table handle.
table = client.table(table_fqn)

# Configure a scan and fetch 10 rows from a table as a Pandas DataFrame.
print(
    "########### Loading 10 rows from delta_sharing.default.owid-covid-data as a Pandas DataFrame with client.table(...).scan(...).to_pandas #############"
)
data = table.scan(limit=10).to_pandas()

# Print the sample.
print("########### Show the fetched 10 rows #############")
print(data)

# Materialize the full table as a Pandas DataFrame.
print(
    "########### Loading delta_sharing.default.owid-covid-data as a Pandas DataFrame with client.table(...).to_pandas #############"
)
data = table.to_pandas()

# Do whatever you want to your share data!
print("########### Show Data #############")
print(data[data["iso_code"] == "USA"].head(10))

# Older syntax, kept here for compatibility examples.
# A table path is the profile file path following with `#` and the fully qualified name of a table
# (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#" + table_fqn

print(
    "########### Loading 10 rows from delta_sharing.default.owid-covid-data as a Pandas DataFrame with load_as_pandas #############"
)
legacy_data = delta_sharing.load_as_pandas(table_url, limit=10)

print("########### Show the fetched 10 rows from the legacy interface #############")
print(legacy_data)

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
#     + " as a Pandas DataFrame with client.table(...).changes(...).to_pandas #############"
# )
# cdf_data = cdf_changes.to_pandas()
# print(cdf_data)
#
# print("########### Loading the same table changes with the legacy interface #############")
# cdf_table_url = profile_file + "#" + cdf_table_fqn
# legacy_cdf_data = delta_sharing.load_table_changes_as_pandas(
#     cdf_table_url,
#     starting_version=0,
# )
# print(legacy_cdf_data)
