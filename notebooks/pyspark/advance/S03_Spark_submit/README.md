# Spark submit introduction

## Advantage of spark submit

We can use deploy-mode **cluster**, the spark-driver can run inside the cluster.

> You will see an **AM(ApplicationMaster)** container created. For more details, visit this page https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html

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
```shell

./bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4G
  --executor-memory 4G \
  --executor-cores 4
  --num-executors 4 \
  /path/to/pi.py 1000
```

