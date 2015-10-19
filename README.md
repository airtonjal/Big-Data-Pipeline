# POC Big Data

This project aims to provide information and insights regarding fresh technologies that might help stakeholders in every level of a company's software development lifecycle. The target of our studies are backend requirements, ranging from functionalities and ease of use, to performance and cost based optimization. The main questions we are attempting to answer, requirements to fulfill and overall objectives of this research are the following:

- Send and resend messages in a high level fashion. This system should be configurable to retain messages as well for a period of time (week, month)
- A queue system that persists messages and dispatches them when needed, to multiple clients in a distributed manner
- This queue should be distributable, in the sense that the number of messages might grow and should be persisted throughout a set of common servers and disks

[Apache Kafka](http://kafka.apache.org) has been the primary candidate for the aforementioned aspects

- Process, geolocate, count, aggregate tons of messages
- Computation units should be close to the idea of a [CEP](http://en.wikipedia.org/wiki/Complex_event_processing) system and [stream processing](http://en.wikipedia.org/wiki/Stream_processing) or the [actor model](http://en.wikipedia.org/wiki/Actor_model)
- Ideally the tool should allow the compute units to be implemented in a range of programming languages

[Apache Storm](http://storm.apache.org) has been the primary candidate for the aforementioned aspects, other tools include Typesafe's [Akka](http://akka.io) and Linkedin's [Samza](http://samza.incubator.apache.org)

- Store document-based timestamped data
- Provide out-of-the-box aggregation functionalities
- Index and search events using a wide range of operators (equality, regexes, booleans)

[Elasticsearch](http://elasticsearch.org) has been the primary candidate for the aforementioned aspects, other tools include [Redis](http://redis.io), [MongoDB](http://www.mongodb.org), [HBase](http://hbase.apache.org). These requirements could demand more than one technology

All of the tools should ideally:

- Scale to the order of magnitude of billions of messages per day
- Be well instrumented, tested, documented and broadly used by the Big Data community
- Guarantee message delivery even in the face of errors to avoid inconsistencies, with [fault-tolerance mechanisms](http://en.wikipedia.org/wiki/Fault_tolerance)
- Process data in parallel, in order to take advantage of multi-core or multiprocessor architectures
- Be vertically scalable
- Be runnable in [commodity hardware](http://en.wikipedia.org/wiki/Commodity_computing)
- Provide near [linear scalability](http://natishalom.typepad.com/nati_shaloms_blog/2007/07/the-true-meanin.html), horizontally

<!---
When conducting [performance tests](http://en.wikipedia.org/wiki/Software_performance_testing) a fully isolated environment is needed, with dedicated network devices and no interference from the outside. Tests should be reproducible and repeated several times.
-->
### Dependencies

The following applications/frameworks/APIs are needed to run the pipeline project

- Java JDK 7 or 8 
- [Scala](http://www.scala-lang.org) 2.10
- [Kafka](http://kafka.apache.org) 0.8
- [Storm](http://storm.apache.org) 0.9
- [Elasticsearch](http://elasticsearch.org) 1.4

In addition to local package componentes, a cluster of Kafka, Storm and Elasticsearch is needed to run the complete stack. To change any of the dependencies version, edit pipeline/build.gradle `ext` section. Other dependencies include:

- [scala-csv](https://github.com/tototoshi/scala-csv), a csv parser
- Log4j2 as the logging mechanism
- Jackson mapper for JSON serialization
- Apache commons for mathematical functionalities

## Building

To build the project in Unix systems:

```shell
cd pipeline
./gradlew clean assemble %PROFILE%
```

where %PROFILE% is the environment profile, and should be replaced by either `dev` or `local`. This will download and install dependencies on the local machine. A directory named _build_ will be generated under pipeline, with the following structure:

- build/classes: .class bytecode files
- build/libs: Generated project jars
- build/resources: Project needed resources, such as properties file, images, csv, etc

If you have any questions, refer to the [gradle documentation](https://www.gradle.org/documentation)

To build a Storm topology executable, it is needed to package all dependencies into one single jar, as pointed by the [documentation](https://storm.apache.org/documentation/Running-topologies-on-a-production-cluster.html). Our build script already contains such task, and the command is:

```shell
./gradlew clean %PROFILE% stormJar
```

This will generate a [fat jar](http://maven.apache.org/plugins/maven-assembly-plugin/descriptor-refs.html#jar-with-dependencies) under build/libs ready to be submitted to a Storm cluster.

<!--## DevOps

Kafka and Storm are currently installed in `newton.poc.wr01.wradar.br` and Elasticsearch in `ampere.poc.wr01.wradar.br`. Logs are under /var/logs/

Scripts to manage these services are under /root/scripts. Commands use the following syntax (as root):

```shell
sh {storm|kafka|zookeeper}/{start|stop|status}.sh
```

To access Storm's UI use [](http://newton.poc.wr01.wradar.br:8080/)

Both Storm and Kafka are installed in /opt, so if you need to further configure these components or use their scripts to inspect their contents/execution use these directories.

To manage elasticsearch, use CentOS service command (as root):

```shell
sudo -u elasticsearch sudo service elasticsearch {start|stop|restart|status}
```

Logs are also under /var/log/elasticsearch. To control Elasticsearch and Storm JVM parameters edit the /etc/sysconfig/{elasticsearch|storm} file
### Cleaning up Kafka queue

There is no straightforward way to cleanup a Kafka topic, so you have to do it manually. To delete the queue, first stop both Kafka and ZooKeeper:

```shell
cd /root/scripts
sh kafka/stop.sh
sh zookeeper/stop.sh
```

Then delete these directories files:

```shell
rm -rf /var/lib/zookeeper/version-2/*
rm -rf /tmp/kafka-logs/*
rm -rf /tmp/kafka-logs/.lock
rm -rf /tmp/kafka-logs/.kafka_cleanshutdown
```

Restart Kafka and Zookeeper and recreate the topic with the correct number of partitions:

```shell
cd /root/scripts
sh zookeeper/start.sh
sh kafka/start.sh
su - kafka
cd /opt/kafka
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic pchr --partitions 10 --replication-factor 1
```
-->
## Components

The pipeline project is comprised by three main software modules, described below:

#### Pipeline (parent module)

Application entry point, responsible for holding resources needed by other components and being the application entry point. Featured files:

- FileDispatcher: Dispatches application execution, either by calling producer/consumer or starting a Storm topology
- {storm|elasticsearch|producer|consumer}.properties: Properties files used by other components
- Log4j2.xml: Log4j2 configuration file
<!-- - {cells\_info.csv|tac}.csv: Needed to find location of cells and phone models information, respectively -->
- build.gradle: Gradle build script 

#### Commons

Contains overall functionalities, such as:

- CellsInfo and PhoneInfo: CSV data accessors, provide a map to find relevant data for geolocation and enrichment
- JsonUtils: Our very own json manipulation library (based on Jackson)
- Normalization, events creation, enrichment and geolocation: PCHR document-based json processing and transformation. These are the computation units that run on a Storm topology

#### Producer

Produces PCHR messages (calls) from a set of txt files (monkey output) to a Kafka cluster in the JSON format. Featured files:

<!-- - PCHRParser: Java vanilla implementation of a txt to json CHR parser-->
- PCHRProducer: Calls parser and sends each call as a message to Kafka

#### Consumer

Consumes PCHR messages either locally or in a Storm cluster. Featured files:

- PCHRConsumer: Example of a Kafka consumer that reads a JSON tree into a Java JsonNode object
- PCHRTopology: Storm topology configuration and creation, reading from a Kafka topic, processing and inserting data into Elasticsearch
- {Normalization|Events|Enrich|Geolocation|Persistence}Bolt: Storm bolts for classifying and persisting PCHR calls

## Running the modules

For now, there are two of ways of running the project modules: use Intellij Idea builtin run/debug command or build a fat jar and pass parameters to it. Only the latter will be considered here, and its build instructions are already described in the section above.

#### Running the producer

To produce pchr messages with the JSON format to Kafka, do the following:

```shell
java -jar pipeline-pchr-storm.jar-0.1.jar producer %PATH%
```

where %PATH% should be replaced by either a file or a directory. You can also match a set of files by using the * wildcard. Each file will parsed and sent to Kafka in a serial fashion.

#### Running the consumer

To consume messages from a Kafka topic, do the following:

```shell
java -jar pipeline-pchr-storm-jar-0.1.jar consumer
```

This will connect the program to Kafka and read every message from the queue since the beginning. Connection parameters should be configured through the equivalent properties files.

#### Submitting a topology

To submit a topology to a Storm cluster, you have to first copy and place the fat jar into Storm's home directory. The following commands show how to copy Storm's jar and run the complete topology (normalizer, events creation, enrichment, geolocation):

```shell
./gradlew clean dev stormJar
scp build/libs/pipeline-pchr-storm.jar-0.1.jar root@10.0.40.10:/root/ 
chown storm:storm pipeline-pchr-storm.jar-0.1.jar
mv pipeline-pchr-storm.jar-0.1.jar /home/storm/
su - storm
/opt/storm/bin/storm jar pipeline-pchr-storm.jar-0.1.jar com.webradar.poc.dispatcher.FileDispatcher topology 
```

This will submit the jar to Storm, which will read from the Kafka topic and process the messages. You can verify how the execution is going by using the tail command on the worker logs, such as:

```shell
tail -f /var/log/storm/pchr-topology-2-1421340370worker-6703.log
```

