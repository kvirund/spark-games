# Sample Spark data source

Inspiration:
* [Extending Spark SQL 2 4 with New Data Sources Live Coding Session -Jacek Laskowski](https://youtu.be/YKkgVEgn2JE)
* [Extending Spark SQL 2 4 with New Data Sources Live Coding Session—continues -Jacek Laskowski](https://youtu.be/vfd83ELlMfc)

## Setting up Spark cluster

See https://raw.githubusercontent.com/bitnami/bitnami-docker-spark for the details. Briefly:

```
$ curl -LO https://raw.githubusercontent.com/bitnami/bitnami-docker-spark/master/docker-compose.yml
```

Then

```
$ docker-compose up
```

Spark UI will be available at http://localhost:8080/.

## Building and starting application

```
$ ./gradlew :spark-client:run
```

Among the other output you should see something like

```
21/09/12 16:01:45 INFO CodeGenerator: Code generated in 10.1196 ms
+---+------+
| id|  name|
+---+------+
|  1|Sample|
+---+------+

21/09/12 16:01:45 INFO SparkContext: Invoking stop() from shutdown hook
```

At this point, this sample Spark connector generates one row of a data on the fly.
The Spark client just shows the content of that data source.