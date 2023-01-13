## Delta Sharing examples
In this folder there are examples taken from the delta.io/delta-sharing quickstart guide and docs. They are available in Python and can be run if the prerequisites are satisfied.  
The profile file from the open, example Delta Sharing Server is downloaded and located in this folder.

### Prerequisites
* For Python examples, Python3.6+, Delta-Sharing Python Connector, PySpark need to be installed, see [the project docs](https://github.com/delta-io/delta-sharing) for details.

### Instructions
* To run the example of PySpark in Python run `spark-submit --packages io.delta:delta-sharing-spark_2.12:0.6.2 ./python/quickstart_spark.py`
* To run the example of pandas DataFrame in Python run `python3 ./python/quickstart_pandas.py`