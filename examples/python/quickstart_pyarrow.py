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

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
print("########### All Available Tables #############")
print(client.list_all_tables())

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#delta_sharing.default.owid-covid-data"

# As PyArrow Dataset.

# Create a lazy reference for delta sharing table as a PyArrow Dataset.
print(
    "########### Create a lazy reference for delta_sharing.default.owid-covid-data as a PyArrow Dataset #############")
data = delta_sharing.load_as_pyarrow_dataset(table_url)
print(data.schema)

# Create a lazy reference for delta sharing table as a PyArrow Dataset with additional pyarrow options.
print(
    "########### Create a lazy reference for delta_sharing.default.owid-covid-data as a PyArrow Dataset with additional options #############")
delta_sharing.load_as_pyarrow_dataset(table_url, pyarrow_ds_options={"partitioning": "hive"})

# As PyArrow Table.

# Fetch 10 rows from a delta sharing table as a PyArrow Table. This can be used to read sample data from a table that cannot fit in the memory.
print("########### Loading 10 rows from delta_sharing.default.owid-covid-data as a PyArrow Table #############")
data = delta_sharing.load_as_pyarrow_table(table_url, limit=10)

# Print the sample.
print("########### Show the number of fetched rows #############")
print(data.num_rows)

# Load full table as a PyArrow Table. This can be used to process tables that can fit in the memory.
print("########### Loading all data from delta_sharing.default.owid-covid-data as a PyArrow Table #############")
data = delta_sharing.load_as_pyarrow_table(table_url)
print(data.num_rows)

# Load full table as a PyArrow Table with additional pyarrow options.
print(
    "########### Loading all data from delta_sharing.default.owid-covid-data as a PyArrow Table with additional pyarrow options #############")
import pyarrow.dataset as ds

data = delta_sharing.load_as_pyarrow_table(
    table_url,
    pyarrow_ds_options={"exclude_invalid_files": False},
    pyarrow_tbl_options={
        "columns": ["date", "location", "total_cases"],
        "filter": ds.field("date") == "2020-02-24"
    }
)
print(data)

# Convert PyArrow Table to Pandas DataFrame.
print("########### Converting delta_sharing.default.owid-covid-data PyArrow Table to a Pandas DataFrame #############")
pdf = data.to_pandas()
print(pdf)
