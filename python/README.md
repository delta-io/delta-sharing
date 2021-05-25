# Delta Sharing

[Delta Sharing](https://delta.io/sharing) is an open protocol for secure real-time exchange of large datasets, which enables secure data sharing across products for the first time. It is a simple [REST protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md) that securely shares access to part of a cloud dataset. It leverages modern cloud storage systems, such as S3, ADLS, or GCS, to reliably transfer large datasets.

With Delta Sharing, the user accessing shared data can directly connect to it through Pandas, Tableau, or dozens of other systems that implement the open protocol, without having to deploy a specific platform first. This reduces their access time from months to minutes, and makes life dramatically simpler for data providers who want to reach as many users as possible.

This is the Python Connector for Delta Sharing, a Python library that implements the [Delta Sharing Protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md) to read shared tables from a Delta Sharing Server. You can load shared tables as a [pandas](https://pandas.pydata.org/) DataFrame, or as an [Apache Spark](http://spark.apache.org/) DataFrame if running in PySpark with the Apache Spark Connector installed.

## Installation and usage

1. Install using `pip install delta-sharing`.
2. To use the Python Connector, see [the project doc](https://github.com/delta-io/delta-sharing) for details.

## Documentation

This README file only contains basic information related to Delta Sharing Python Connector. You can find the full documentation on [the project doc](https://github.com/delta-io/delta-sharing).
