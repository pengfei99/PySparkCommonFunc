# Install spark 4.0 on debian

## Install Java(jdk)

In general, your system package manager should provide a jdk. So the easiest way is to use your system package manager. 

spark 4.0 uses jdk-21. So we need to install the jdk-21

## Download spark 4.0 binary



```shell
# get source
wget https://dlcdn.apache.org/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz

# we recommand you to install spark in /opt
sudo mkdir /opt/spark
sudo cp spark-4.0.0-bin-hadoop3.tgz /opt/spark/

cd /opt/spark/
sudo tar -xzvf spark-4.0.0-bin-hadoop3.tgz

```

## Set up spark 

create **spark.sh** in `/etc/profile.d/` and put below text in it.

```shell
# create the file
sudo vim /etc/profile.d/spark.sh

# add below text
export SPARK_HOME=/opt/spark/spark-4.0.0
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# refresh profile
source /etc/profile.d/spark.sh
```

## Test your spark

```shell
# open a spark shell
spark-shell --version
```

## Install hadoop

```shell
# get the hadoop bin
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
```

### Setup hadoop home

```shell
# create the file
sudo vim /etc/profile.d/hadoop.sh

# add below text
export HADOOP_HOME=/opt/hadoop/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin

# refresh profile
source /etc/profile.d/hadoop.sh
```

