## Delta Sharing examples

The profile file(open-datasets.share) from the open, example Delta Sharing Server is downloaded and located in this folder.

### Python examples
In this folder there are examples taken from the delta.io/delta-sharing quickstart guide and docs. They are available in Python and can be run if the prerequisites are satisfied.


### Scala example
In addition, there is a Scala example(`main.scala`). You can easily run the example using [Scala-CLI](https://scala-cli.virtuslab.org/).

```sh
curl -sSLf https://virtuslab.github.io/scala-cli-packages/scala-setup.sh | sh
```

```sh
scala-cli run main.scala
```

When using Java 9 or later, remove comment-out from the lines(L17~18) as shown below to add java options

```diff
- ///> using javaOptions "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
+ //> using javaOptions "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
- ///> using javaOptions "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
+ //> using javaOptions "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
```
to avoid the following exception.

```
Exception in thread "main" java.lang.IllegalAccessError:
class org.apache.spark.storage.StorageUtils$ (in unnamed module @0xa803f94)
cannot access class sun.nio.ch.DirectBuffer
```



### Prerequisites
* For Python examples, Python3.6+, Delta-Sharing Python Connector, PySpark need to be installed, see [the project docs](https://github.com/delta-io/delta-sharing) for details.
* For Scala example
    * [Scala-CLI](https://scala-cli.virtuslab.org/), which downloads Scala compiler and all the dependencies. See the installation guide at the official website → https://scala-cli.virtuslab.org/install
    * Java 8 or later

### Instructions

Python
* To run the example of PySpark in Python run `spark-submit --packages io.delta:delta-sharing-spark_2.12:0.6.2 ./python/quickstart_spark.py`
* To run the example of pandas DataFrame in Python run `python3 ./python/quickstart_pandas.py`

Scala
- To run the example of Scala with Spark, run `scala-cli run main.scala`
- To enable editor support(completion, jump to definition, etc.) for the Scala example, setup [Metals](https://scalameta.org/metals/)(Scala language server) and run `scala-cli setup-ide .`
