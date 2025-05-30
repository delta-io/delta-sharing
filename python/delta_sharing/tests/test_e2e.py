import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "/Users/Moe.Derakhshani/Documents/oauth/demo/u2m.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)
#
# List all shared tables.
tables = client.list_all_tables()

print(tables)


#
# # Create a url to access a shared table.
# # A table path is the profile file path following with `#` and the fully qualified name of a table
# # (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#demo-d2o-identity-federation.my_schema.my_table"

# Fetch 10 rows from a table and convert it to a Pandas DataFrame. This can be used to read sample data
# from a table that cannot fit in the memory.
df = delta_sharing.load_as_pandas(table_url, limit=10)

print(df)

#
# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
delta_sharing.load_as_pandas(table_url)

# Load a table as a Pandas DataFrame explicitly using Delta Format
#delta_sharing.load_as_pandas(table_url, use_delta_format = True)

# # If the code is running with PySpark, you can use `load_as_spark` to load the table as a Spark DataFrame.
# delta_sharing.load_as_spark(table_url)



print("DONE")
