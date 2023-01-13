# Release Notes

## Delta Sharing 0.5.4 (Released on 2023-01-11)
Improvements:
- Spark connector changes to consume size from metadata.

## Delta Sharing 0.6.2 (Released on 2022-12-20)
Bug fixes:
- Fix comparison of the expiration time to current time for pre-signed urls.


## Delta Sharing 0.5.3 (Released on 2022-12-20)
Bug fixes:
- Extends DeltaSharingProfileProvider to customize tablePath and refresher.
- Refresh pre-signed urls for cdf queries.
- Fix partitionFilters issue for cdf queries.
- Fix comparison of the expiration time to current time for pre-signed urls.


## Delta Sharing 0.6.1 (Released on 2022-12-19)
Improvements:
- Spark connector changes to consume size from metadata.
- Improve delta sharing error messages.

Bug fixes:
- Extends DeltaSharingProfileProvider to customize tablePath and refresher.
- Refresh pre-signed urls for cdf and streaming queries.
- Allow 0 for versionAsOf parameter, to be consistent with Delta.
- Fix partitionFilters issue: apply it to all file indices.

## Delta Sharing 0.6.0 (Released on 2022-12-02)
Improvements:
- Support using a delta sharing table as a source in spark structured streaming, which allows recipients to stay up to date with the shared data.
- Fix a few nits in the PROTOCOL documentation.
- Support timestampAsOf parameter in delta sharing data source.

## Delta Sharing 0.5.2 (Released on 2022-10-10)
Fixes:
- Add a Custom Http Header Provider.

## Delta Sharing 0.5.1 (Released on 2022-09-08)
Improvements:
- Upgrade AWS SDK to 1.12.189.
- More tests on the error message when loading table fails.
- Add ability to configure armeria server request timeout.
- documentation improvements.

Bug fixes:
- Fix column selection bug on Delta Sharing CDF spark dataframe.
- Fix GCS path reading.

## Delta Sharing 0.5.0 (Released on 2022-08-30)
Improvements:
- Support for Change Data Feed which allows clients to fetch incremental changes for the shared tables.
- Include response body in HTTPError exception in Python library.
- Improve the error message for the /share/schema/table APIs.
- Protocol and REST API documentation improvements.
- Add query_table_version to the rest client.

## Delta Sharing 0.4.0 (Released on 2022-01-13)
Improvements:
- Support Google Cloud Storage on Delta Sharing Server.
- Add a new API to get the metadata of a Share.
- Protocol and REST API documentation enhancements.
- Allow for customization of recipient profile in Apache Spark connector.
  
Bug fixes:
- Block managed table creation for Delta Sharing to prevent user confusions.

## Delta Sharing 0.3.0 (Released on 2021-12-01)
Improvements:
- Support Azure Blob Storage and Azure Data Lake Gen2 in Delta Sharing Server.
- Apache Spark Connector now can send the limitHint parameter when a user query is using limit.
- `load_as_pandas` in Python Connector now accepts a limit parameter to allow users fetching only a few rows to explore.
- Apache Spark Connector will re-fetch pre-signed urls before they expire to support long running queries.
- Add a new API to list all tables in a share to save network round trips.
- Add a User-Agent header to request sent from Apache Spark Connector and Python.
- Add an optional expirationTime field to Delta Sharing Profile File Format to provide the token expiration time.

Bug fixes:
- Fix a corner case that list_all_tables may not return correct results in the Python Connector.

## Delta Sharing 0.2.0 (Released on 2021-08-10)
Improvements:
- Added official Docker images for Delta Sharing Server.
- Added an examples project to show how to try the open Delta Sharing Server.
- Added the conf directory to the Delta Sharing Server classpath to allow users to add their Hadoop configuration files in the directory.
- Added retry with exponential backoff for REST requests in the Python connector.

Bug fixes:
- Added the minimum fsspec requirement in the Python connector.
- Fixed an issue when files in a table have no stats in the Python connector.
- Improve error handling in Delta Sharing Server to report 400 Bad Request properly.
- Fixed the table schema when a table is empty in the Python connector.
- Fixed KeyError when there are no shared tables in the Python connector.

## Delta Sharing 0.1.0 (Released on 2021-05-25)
Components:
- Delta Sharing protocol specification.
- Python Connector: A Python library that implements the Delta Sharing Protocol to read shared tables as pandas DataFrame or Apache Spark DataFrames.
- Apache Spark Connector: An Apache Spark connector that implements the Delta Sharing Protocol to read shared tables from a Delta Sharing Server. The tables can then be accessed in SQL, Python, Java, Scala, or R.
- Delta Sharing Server: A reference implementation server for the Delta Sharing Protocol for development purposes. Users can deploy this server to share existing tables in Delta Lake and Apache Parquet format on modern cloud storage systems.