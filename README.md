To complile the project, run

```
build/sbt compile
```

To generate JAR file, run

```
build/sbt publishLocal
```

The jar file will be generated in the target folder of server directory.

To install the generated jar file in local Maven repo, run 

```
mvn install:install-file -Dfile=<path-to-jar> -DgroupId=io.delta -DartifactId=delta-sharing-server_2.12 -Dversion=1.0.0-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
```

After installing jar, the JUnit test can be run by following below steps:

``` 
cd jarTest
mvn test
```
