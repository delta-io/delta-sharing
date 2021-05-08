

[![Build and Test](https://github.com/delta-io/delta-sharing/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/delta-io/delta-sharing/actions/workflows/build-and-test.yml)

### Install the Spark connector

```
build/sbt spark/publishLocal
```

Note: Make sure deleting the following directory when changing the Spark connector. Spark's `--packages` will cache it. 

```
rm -rf ~/.ivy2/cache/io.delta/delta-sharing-spark_2.12
```

### Install Python Pandas connector

```
cd python/
pip install -e .
```

Note: this installs the package in the current directory. Make sure staying in the `python` directory when testing it.

### Create a profile file.

Save the following content in a file.

```
{
  "version": 1,
  "endpoint": "https://ec2-18-237-148-30.us-west-2.compute.amazonaws.com/delta-sharing/",
  "bearerToken": "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
}
```

### Start PySpark

Make sure you are using PySpark 3.1.1 and still in the `python` directory. Adding `SPARK_LOCAL_IP` to fix the network issue in VPN.

```
SPARK_LOCAL_IP=127.0.0.1 pyspark --packages io.delta:delta-sharing-spark_2.12:0.1.0-SNAPSHOT
```

### Try the folllowing codes

Make sure you connect to VPN. `load_as_pandas` doesn't require PySpark.

```
>>> from delta_sharing import DeltaSharing
>>> delta = DeltaSharing("<the-profile-file>")
>>> delta.list_all_tables()
[Table(name='table1', share='share1', schema='default'), Table(name='table3', share='share1', schema='default'), Table(name='table2', share='share2', schema='default')]
>>> DeltaSharing.load_as_pandas('<the-profile-file>#share1.default.table1')
                eventTime        date
0 2021-04-28 06:32:22.421  2021-04-28
1 2021-04-28 06:32:02.070  2021-04-28
>>> DeltaSharing.load_as_spark('<the-profile-file>#share1.default.table1').show()
+--------------------+----------+                                               
|           eventTime|      date|
+--------------------+----------+
|2021-04-27 23:32:...|2021-04-28|
|2021-04-27 23:32:...|2021-04-28|
+--------------------+----------+
```

