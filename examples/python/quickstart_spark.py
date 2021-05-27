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
from pyspark.sql import SparkSession

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

# Create Spark with delta sharing connector
spark = SparkSession.builder \
	.appName("delta-sharing-demo") \
	.master("local[*]") \
	.getOrCreate()

# Read data using format "deltaSharing"
print("########### Loading delta_sharing.default.owid-covid-data with Spark #############")
df1 = spark.read.format("deltaSharing").load(table_url) \
	.where("iso_code == 'USA'") \
	.select("iso_code", "total_cases", "human_development_index") \
	.show()

# Or if the code is running with PySpark, you can use `load_as_spark` to load the table as a Spark DataFrame.
print("########### Loading delta_sharing.default.owid-covid-data with Spark #############")
data = delta_sharing.load_as_spark(table_url)
data.where("iso_code == 'USA'") \
	.select("iso_code", "total_cases", "human_development_index").show()
