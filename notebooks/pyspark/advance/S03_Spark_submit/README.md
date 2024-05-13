# Spark submit introduction

The **spark-submit** command is utilized for submitting Spark applications written in various languages (e.g. Scala, 
Java, and Python.), to a Spark cluster. In this article, I will demonstrate several examples 
of how to submit a spark applications.

> R is not supported for now (sparklyr or sparkR).

## Advantage of spark submit

We can use deploy-mode **cluster**, the spark-driver can run inside the cluster.

> You will see an **AM(ApplicationMaster)** container created. For more details, visit this page https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html

## Spark submit scala/java jar


## Spark submit python script

https://sparkbyexamples.com/pyspark/spark-submit-python-file/
Python (.py), which is a PySpark program file, by using different options and configurations.

Key Points:

- Configure the cluster settings, such as the number of executors, memory allocation, and other Spark properties, either programmatically using SparkConf or through configuration files like spark-defaults.conf.
Ensure that all necessary dependencies for your PySpark application are included or available on the Spark cluster’s environment. This includes Python packages, libraries, and any external resources required for the application to run successfully.
Monitor the execution of your PySpark application using Spark’s built-in monitoring tools, such as Spark UI, to track job progress, resource utilization, task execution, and other metrics.
Implement error handling mechanisms within your PySpark application to gracefully handle exceptions, failures, and unexpected conditions during execution.

### Spark Submit Python File
Apache Spark binary comes with spark-submit.sh script file for Linux, Mac, and spark-submit.cmd command file for windows, these scripts are available at $SPARK_HOME/bin directory which is used to submit the PySpark file with .py extension (Spark with python) to the cluster.

Below is a simple spark-submit command to run python file with the command options that are used most of the time.



## How to submit a job via spark submit

The general syntax of spark submit **Java/Scala** version

```shell
./bin/spark-submit \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  --driver-memory <value>g \
  --executor-memory <value>g \
  --executor-cores <number of cores>  \
  --jars  <comma separated dependencies>
  --class <main-class> \
  <application-jar> \
  [application-arguments]
```

The general syntax of spark submit **python** version

```shell
./bin/spark-submit \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  --driver-memory <value>g \
  --executor-memory <value>g \
  --executor-cores <number of cores>  \
  /path/to/script.py [application-arguments]
```

For example,  

In spark submit cluster mode, we cant put the conf in the source code (e.g. sparkSession.builder.config()).It has no effect
at all. **The configuration such as spark.yarn.am.cores and spark.yarn.am.memory only works in client mode**
```shell

./bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.yarn.queue=prod \
  --conf spark.yarn.archive=hdfs:///system/libs/spark_libs.zip \
  --driver-memory 4G \
  --executor-memory 4G \
  --executor-cores 4 \
  --num-executors 4 \
  /path/to/pi.py 1000
```

