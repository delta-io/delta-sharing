# Delta Sharing

[Delta Sharing](https://delta.io/sharing) is an open protocol for secure real-time exchange of large datasets, which enables secure data sharing across different computing platforms. It lets organizations share access to existing [Delta Lake](https://delta.io) and [Apache Parquet](https://parquet.apache.org) tables with other organizations, who can then directly read the table in Pandas, Apache Spark, or any other software that implements the open protocol.

This is the Python client library for Delta Sharing, which lets you load shared tables as [pandas](https://pandas.pydata.org/) DataFrames or as [Apache Spark](http://spark.apache.org/) DataFrames if running in PySpark with the [Apache Spark Connector library](https://github.com/delta-io/delta-sharing#set-up-apache-spark).

## Installation and Usage

1. Install using `pip install delta-sharing`.
    a. On some environments, you may also need to [install Rust](https://www.rust-lang.org/tools/install). This is because the `delta-sharing` package depends on the `delta-kernel-rust-sharing-wrapper` package, which does not have a pre-built Python wheel for all environments. As a result, pip will have to build `delta-kernel-rust-sharing-wrapper` from source.
2. To use the Python Connector, see [the project docs](https://github.com/delta-io/delta-sharing) for details.

## Documentation

This README only contains basic information about the Delta Sharing Python Connector. Please read [the project documentation](https://github.com/delta-io/delta-sharing) for full usage details.
