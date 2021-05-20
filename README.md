<img src="https://docs.delta.io/latest/_static/delta-lake-white.png" width="400" alt="Delta Lake Logo"></img>

[![Build and Test](https://github.com/delta-io/delta-sharing/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/delta-io/delta-sharing/actions/workflows/build-and-test.yml)

[Delta Sharing](https://delta.io/sharing) is the industry's first open protocol for secure data sharing, making it simple to share data with other organizations regardless of which computing platforms they use. This repo includes the following components:

- Python Connector: A Python library that implements the [Delta Sharing Protocol](PROTOCOL.md) to read shared tables from a Delta Sharing Server.
- [Apache Spark](http://spark.apache.org/) Connector: An Apache Spark connector that implements the [Delta Sharing Protocol](PROTOCOL.md) to read shared tables from a Delta Sharing Server.
- Delta Sharing Server: A reference implementation server for the [Delta Sharing Protocol](PROTOCOL.md).

# Python Connector

## Requirement

Python 3.6+

## Install

```
pip install delta-sharing
```

## Get the share profile file

The first step to read shared Delta tables is getting a profile file from your share provider. It's a JSON file that contains the information to access a sharing server. There are three fields in this file.

Field Name | Descrption
-|-
shareCredentialsVersion | The file format version of the profile file. This version will be increased whenever non-forward-compatible changes are made to the profile format. When a client is running an unsupported profile file format version, it should show an error message instructing the user to upgrade to a newer version of their client.
endpoint | The url of the sharing server.
bearerToken | The [bearer token](https://tools.ietf.org/html/rfc6750) to access the server.

Example:

```
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing.delta.io/delta-sharing/",
  "bearerToken": "<token>"
}
```

The Delta Sharing library will need this file to access the shared tables. You can download the profile file to access our hosted open Delta Sharing Server [here](TODO).

## Usages

```python
import delta_sharing

profile_file = "<the-profile-file-path>"
client = delta_sharing.SharingClient(profile_file)
# List all shared tables
client.list_all_tables()

# Load a table as a Pandas DataFrame to process tables that can fit in the memory
table_url = "<the-profile-file-path>#<share-name>.<schema-name>.<table-name>"
delta_sharing.load_as_pandas(table_url)

# Load a table as a Spark DataFrame to process large tables (Require running with PySpark)
delta_sharing.load_as_spark(table_url)
```

# Apache Spark Connector

## Requirement

- Java 8+
- Scala 2.12.x
- Apache Spark 3.x.y or [Databricks Runtime](https://docs.databricks.com/runtime/dbr.html) 7.x/8.x

## Get the share profile file

See [Get the share profile file](#get-the-share-profile-file) in Python Connector

## Set up Apache Spark

You can run the steps on your local machine in the following two ways:

- Run interactively: Start the Spark shell (Scala or Python) with Delta Sharing connector and run the code snippets interactively in the shell.
- Run as a project: Set up a Maven or SBT project (Scala or Java) with Delta Sharing connector, copy the code snippets into a source file, and run the project.

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

You include Delta Sharing connector in your Maven project by adding it as a dependency in your POM file. Delta Sharing Connector is compiled with Scala 2.12.

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
# Access the table using DataFrame APIs
table_url = "<the-profile-file-path>#<share-name>.<schema-name>.<table-name>"
df = spark.read.format("deltaSharing").load(table_url)

# Register the table in catalog and use SQL to access it
spark.sql("CREATE TABLE mytable USING deltaSharing LOCATION '%s'" % (table_url))
spark.sql("SELECT * FROM mytable")
```

### Scala

```scala
// Access the table using DataFrame APIs
val tableUrl = "<the-profile-file-path>#<share-name>.<schema-name>.<table-name>"
df = spark.read.format("deltaSharing").load(tableUrl)

// Register the table in catalog and use SQL to access it
spark.sql(s"CREATE TABLE mytable USING deltaSharing LOCATION '$table_url'")
spark.sql("SELECT * FROM mytable")
```

### Java

```java
// Access the table using DataFrame APIs
String tableUrl = "<the-profile-file-path>#<share-name>.<schema-name>.<table-name>";
Dataset<Row> df = spark.read.format("deltaSharing").load(tableUrl);

// Register the table in catalog and use SQL to access it
spark.sql("CREATE TABLE mytable USING deltaSharing LOCATION '" + table_url + "'");
spark.sql("SELECT * FROM mytable");
```

# Delta Sharing Server

Here are the steps to setup a server to share your own data.

## Get the pre-built package

Download the pre-built package `delta-sharing-server-x.y.z.zip` from [GitHub Releases](https://github.com/delta-io/delta-sharing/releases).

## Config the shared tables for the server

- Unpack the pre-built package and copy the server config template file `conf/delta-sharing-server.yaml.templace` to create your own server yaml file, such as `conf/delta-sharing-server.yaml`.
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

To install locally, run

```
cd python/
pip install -e .
```

To execute tests, run

```
python/dev/pytest
```

## Apache Spark Connector and Delta Sharing Server

Apache Spark Connector and Delta Sharing Server are compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

To compile, run

```
build/sbt compile
```

To generate artifacts, run

```
build/sbt package
```

To execute tests, run

```
build/sbt test
```

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

# Contributing 
We welcome contributions to Delta Sharing. See our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

We also adhere to the [Delta Lake Code of Conduct](CODE_OF_CONDUCT.md).

# License
Apache License 2.0, see [LICENSE](LICENSE.txt).

# Community

We use the same community as the Delta Lake project.

- Public Slack Channel
  - [Register here](https://dbricks.co/delta-users-slack)
  - [Login here](https://delta-users.slack.com/)

- Public [Mailing list](https://groups.google.com/forum/#!forum/delta-users)
