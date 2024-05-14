# Spark submit introduction

The **spark-submit** command is utilized for submitting Spark applications written in various languages (e.g. Scala, 
Java, and Python.), to a Spark cluster. In this article, I will demonstrate several examples 
of how to submit a spark applications.

> R is not supported for now (sparklyr or sparkR).

## Advantage of spark submit

We can use deploy-mode **cluster**, the spark-driver can run inside the cluster.

> You will see an **AM(ApplicationMaster)** container created. For more details, visit this page https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html

## Spark submit yarn

Spark submit by using **Yarn** is quite different from the other resource manager. It has two modes:
- client mode: The spark driver runs at the machine where the spark-submit is executed
- cluster mode: The spark driver runs at a container located at one of the worker nodes

You can find the full official doc here https://spark.apache.org/docs/latest/running-on-yarn.html#spark-properties

### Client mode
As the driver is at the local machine, your local machine must have python or R interpreter when your spark script is in python
or R, and all the required dependencies. 

If you use python, you also need to set up the env var such as **PYSPARK_PYTHON=path/to/python**

### Cluster mode

In cluster mode, although your driver is running on the worker node, the spark-submit will still send your local config to the worker

In my case, the spark-submit is launched at a Windows server with a **PYSPARK_PYTHON=C:\path\to\python**, the container
which runs on the worker nodes are in Debian. So naturally an error message is sent `can't find file C:\path\to\python`.

To resolve the similar problems, don't put any config in the script when creating the spark session. Put all config as the 
argument of the spark submit. For example

```shell
./bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.yarn.queue=prod \
  --conf spark.yarn.archive=hdfs:///system/libs/spark_libs.zip \
  --conf spark.pyspark.driver.python=/usr/bin/python3 \
  --conf spark.pyspark.python=/usr/bin/python3 \
  --conf spark.yarn.am.memory=4g \
  --conf spark.yarn.am.cores=2 \
  --driver-memory 4G \
  --driver-cores 2 \
  --executor-memory 4G \
  --executor-cores 4 \
  --num-executors 4 \
  /path/to/pi.py 1000
```

- **spark.yarn.queue=prod**: yarn has the queue notion. This conf specify this job will use the resource of the `prod` queue
- **spark.yarn.archive=hdfs:///system/libs/spark_libs.zip**: To avoid spark send the dependencies jars to hdfs for each job submitted
- **spark.pyspark.driver.python=/usr/bin/python3**: Specifies the python path of the driver, it overrides the client local config send by spark-submit
- **spark.pyspark.python=/usr/bin/python3**: Specifies the python path of the worker, it overrides the client local config send by spark-submit
- **spark.yarn.am.memory=4g**: Amount of memory to use for the YARN Application Master in client mode, in the same format as JVM memory strings (e.g. 512m, 2g). In cluster mode, use spark.driver.memory instead.
- **spark.yarn.am.cores=2**: Number of cores to use for the YARN Application Master in client mode. In cluster mode, use spark.driver.cores instead.



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

