import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
print(client.list_all_tables())

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#delta_sharing.default.COVID_19_NYT"

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
data = delta_sharing.load_as_pandas(table_url)

# Do whatever you want to your share data!
print(data[data["cases"] < 100].head(10))