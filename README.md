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
cd server/target/scala-2.12
mvn install:install-file -Dfile=delta-sharing-server_2.12-1.0.0-SNAPSHOT.jar -DgroupId=io.delta -DartifactId=delta-sharing-server_2.12 -Dversion=1.0.0-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
```

To access data from AWS S3, AWS access key id and key needs to be exported using below commands:

```
export AWS_ACCESS_KEY_ID=<aws_key_id>
export AWS_SECRET_ACCESS_KEY=<aws_acess_key>
```

After installing jar, the JUnit test can be run by following below steps:

``` 
cd jarTest
mvn test
```
