import delta_sharing
from pyspark.sql import SparkSession


# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
print(client.list_all_tables())

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#delta_sharing.default.COVID_19_NYT"

# Create Spark with delta sharing connector
spark = SparkSession.builder.appName("delta-sharing-demo").config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:0.1.0").getOrCreate()

# Read data using format "deltaSharing"
spark.read.format("deltaSharing").load(table_url).where("cases < 100").show()

# Or if the code is running with PySpark, you can use `load_as_spark` to load the table as a Spark DataFrame.
data = delta_sharing.load_as_spark(table_url)
data.where("cases < 100").show()