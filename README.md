<img src="https://docs.delta.io/latest/_static/delta-lake-white.png" width="400" alt="Delta Lake Logo"></img>

[![Build and Test](https://github.com/delta-io/delta-sharing/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/delta-io/delta-sharing/actions/workflows/build-and-test.yml)

[Delta Sharing](https://delta.io/sharing) is the industry's first open protocol for secure data sharing, making it simple to share data with other organizations regardless of which computing platforms they use. This repo includes the following components:

- Python Connector: A Python library that implements the [Delta Sharing Protocol](PROTOCOL.md) to read shared tables from a Delta Sharing Server. You can load shared tables as a [pandas](https://pandas.pydata.org/) DataFrame, or as an [Apache Spark](http://spark.apache.org/) DataFrame if running in PySpark with the following connector installed.
- [Apache Spark](http://spark.apache.org/) Connector: An Apache Spark connector that implements the [Delta Sharing Protocol](PROTOCOL.md) to read shared tables from a Delta Sharing Server. It's powered by Apache Spark and can be used in Python, Scala, Java, R, and SQL.
- Delta Sharing Server: A reference implementation server for the [Delta Sharing Protocol](PROTOCOL.md).

# Python Connector

## Requirement

Python 3.6+

## Install

```
pip install delta-sharing
```

If you are using [Databricks Runtime](https://docs.databricks.com/runtime/dbr.html), you can follow [Databricks Libraries doc](https://docs.databricks.com/libraries/index.html) to install the library on your clusters.

## Get the share profile file

The first step to read shared Delta tables is getting [a profile file](PROTOCOL.md#profile-file-format) from your share provider. The Delta Sharing library will need this file to access the shared tables.

We also host an open Delta Sharing Server with open datasets. You can download the profile file to access it [here](https://sharing.delta.io/open-profile.share).

## Usages

```python
import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "<profile-file-path>"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#<share-name>.<schema-name>.<table-name>"

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
delta_sharing.load_as_pandas(table_url)

# If the code is running with PySpark, you can use `load_as_spark` to load the table as a Spark DataFrame.
delta_sharing.load_as_spark(table_url)
```
### Table path

- The profile file path for `SharingClient` and `load_as_pandas` can be any url supported by [FSSPEC](https://filesystem-spec.readthedocs.io/en/latest/index.html) (such as `s3a://my_bucket/my/profile/file`). If you are using [Databricks File System](https://docs.databricks.com/data/databricks-file-system.html), you can also [preface the path with `/dbfs/`](https://docs.databricks.com/data/databricks-file-system.html#dbfs-and-local-driver-node-paths) to access the profile file as if it were a local file.  
- The profile file path for `load_as_spark` can be any url supported by Hadoop FileSystem (such as `s3a://my_bucket/my/profile/file`).
- A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).

# Apache Spark Connector

## Requirement

- Java 8+
- Scala 2.12.x
- Apache Spark 3+ or [Databricks Runtime](https://docs.databricks.com/runtime/dbr.html) 7+

## Get the share profile file

See [Get the share profile file](#get-the-share-profile-file) in Python Connector

## Set up Apache Spark

You can set up Apache Spark on your local machine in the following two ways:

- Run interactively: Start the Spark shell (Scala or Python) with Delta Sharing connector and run the code snippets interactively in the shell.
- Run as a project: Set up a Maven or SBT project (Scala or Java) with Delta Sharing connector, copy the code snippets into a source file, and run the project.


If you are using [Databricks Runtime](https://docs.databricks.com/runtime/dbr.html), you can skip this section and follow [Databricks Libraries doc](https://docs.databricks.com/libraries/index.html) to install the connector on your clusters.

### Set up interactive shell

To use Delta Sharing connector interactively within the Spark’s Scala/Python shell, you need a local installation of Apache Spark. Depending on whether you want to use Python or Scala, you can set up either PySpark or the Spark shell, respectively.

#### PySpark

Install or upgrade Pyspark (3.0 or above) by running the following:

```
pip install --upgrade pyspark
```

Then, run PySpark with the Delta Sharing package:

```
pyspark --packages io.delta:delta-sharing-spark_2.12:0.1.0
```

#### Spark Scala Shell

Download the latest version of Apache Spark (3.0 or above) by following instructions from [Downloading Spark](https://spark.apache.org/downloads.html), extract the archive, and run spark-shell in the extracted directory with the Delta Sharing package:

```
bin/spark-shell --packages io.delta:delta-sharing-spark_2.12:0.1.0
```

### Set up project

If you want to build a project using Delta Sharing connector from Maven Central Repository, you can use the following Maven coordinates.

#### Maven

You include Delta Sharing connector in your Maven project by adding it as a dependency in your POM file. Delta Sharing connector is compiled with Scala 2.12.

```xml
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>delta-sharing-spark_2.12</artifactId>
  <version>0.1.0</version>
</dependency>
```

#### SBT

You include Delta Sharing connector in your SBT project by adding the following line to your `build.sbt` file:

```scala
libraryDependencies += "io.delta" %% "delta-sharing-spark" % "0.1.0"
```

## Usages

### Python

```python
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_path = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"
df = spark.read.format("deltaSharing").load(table_path)
```

### Scala

```scala
// A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
val tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"
val df = spark.read.format("deltaSharing").load(tablePath)
```

### Java

```java
// A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
String tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>";
Dataset<Row> df = spark.read.format("deltaSharing").load(tablePath);
```

### R
```r
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_path <- "<profile-file-path>#<share-name>.<schema-name>.<table-name>"
df <- read.df(table_path, "deltaSharing")
```

### SQL
```sql
-- A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
CREATE TABLE mytable USING deltaSharing LOCATION '<profile-file-path>#<share-name>.<schema-name>.<table-name>';
SELECT * FROM mytable;
```

### Table path

- A profile file path can be any url supported by Hadoop FileSystem (such as `s3a://my_bucket/my/profile/file`).
- A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).

# Delta Sharing Server

Delta Sharing Server is a reference implementation server for the [Delta Sharing Protocol](PROTOCOL.md). This can be used to set up a small service to test your own connector that implements the [Delta Sharing Protocol](PROTOCOL.md). Note: It's not a completed implementation of secure web server. We highly recommend you to put this behind a secure proxy if you would like to expose it to public.

Here are the steps to setup a server to share your own data.

## Get the pre-built package

Download the pre-built package `delta-sharing-server-x.y.z.zip` from [GitHub Releases](https://github.com/delta-io/delta-sharing/releases).

## Config the shared tables for the server

- Unpack the pre-built package and copy the server config template file `conf/delta-sharing-server.yaml.template` to create your own server yaml file, such as `conf/delta-sharing-server.yaml`.
- Make changes to your yaml file, such as add the Delta tables you would like to share in this server. You may also need to update some server configs for special requirements.

## Config the server to access tables on S3

We only support sharing Delta tables on S3. There are multiple ways to config the server to access S3.

### EC2 IAM Metadata Authentication (Recommended)

Applications running in EC2 may associate an IAM role with the VM and query the [EC2 Instance Metadata Service](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html) for credentials to access S3.

### Authenticating via the AWS Environment Variables

We support configuration via [the standard AWS environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#cli-environment). The core environment variables are for the access key and associated secret:
```
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

### Other S3 authentication methods

The server is using `hadooop-aws` to read S3. You can find other approaches in [hadoop-aws doc](https://hadoop.apache.org/docs/r2.10.1/hadoop-aws/tools/hadoop-aws/index.html#S3A_Authentication_methods).

More cloud storage supports will be added in the future.

## Authorization

The server supports a basic authorization with pre-configed bearer token. You can add the following config to your server yaml file:

```yaml
authorization:
  bearerToken: <token>
```

Then any should send with the above token, otherwise, the server will refuse the request.

If you don't config the bearer token in the server yaml file, all requests will be accepted without authorization.

To be more secure, you recommend you to put the server behind a secure proxy such as [NGINX](https://www.nginx.com/) to set up [JWT Authentication](https://docs.nginx.com/nginx/admin-guide/security-controls/configuring-jwt-authentication/).

## Start the server

Run the following shell command:

```
bin/delta-sharing-server -- --config <the-server-config-yaml-file> 
```

`<the-server-config-yaml-file>` should be the path of the yaml file you created in the previous step. You can find options to config JVM in [sbt-native-packager](https://www.scala-sbt.org/sbt-native-packager/archetypes/java_app/index.html#start-script-options).

# Delta Sharing Protocol

[Delta Sharing Protocol](PROTOCOL.md) document provides a specification of the Delta Sharing protocol.

# Reporting issues

We use [GitHub Issues](https://github.com/delta-io/delta-sharing/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

# Building

## Python Connector

To execute tests, run

```
python/dev/pytest
```

To install in develop mode, run

```
cd python/
pip install -e .
```

To install locally, run

```
cd python/
pip install .
```

To generate a wheel file, run

```
cd python/
python setup.py sdist bdist_wheel
```

It will generate `python/dist/delta_sharing-0.1.0.dev0-py3-none-any.whl`.

## Apache Spark Connector and Delta Sharing Server

Apache Spark Connector and Delta Sharing Server are compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

To compile, run

```
build/sbt compile
```

To execute tests, run

```
build/sbt test
```

To generate the Apache Spark Connector, run

```
build/sbt spark/package
```

It will generate `spark/target/scala-2.12/delta-sharing-spark_2.12-0.1.0-SNAPSHOT.jar`.

To generate the pre-built Delta Sharing Server package, run

```
build/sbt server/universal:packageBin
```

It will generate `server/target/universal/delta-sharing-server-0.1.0-SNAPSHOT.zip`.

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

# Contributing 
We welcome contributions to Delta Sharing. See our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

We also adhere to the [Delta Lake Code of Conduct](https://github.com/delta-io/delta/blob/master/CODE_OF_CONDUCT.md).

# License
Apache License 2.0, see [LICENSE](LICENSE.txt).

# Community

We use the same community as the Delta Lake project.

- Public Slack Channel
  - [Register here](https://dbricks.co/delta-users-slack)
  - [Login here](https://delta-users.slack.com/)

- Public [Mailing list](https://groups.google.com/forum/#!forum/delta-users)
