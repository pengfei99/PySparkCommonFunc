# Install spark on debian

## Install Java

In general, you system package manager should provide a jdk. So the easiest way is to use your system package manager. 

For example, below script works on Ubuntu and debian

```shell
# upgrade your package manager index and upgrade the packages
sudo apt update && sudo apt -y full-upgrade

# install other usefull tools
sudo apt install curl mlocate -y

# install open-jdk
sudo apt install default-jdk -y

# check your java version
$ java -version
openjdk version "11.0.14.1" 2022-02-08

# if JAVA_HOME is not set, you need to add it to your .bashrc
```

## Download spark 

```shell
# get source
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
# wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

# extract
tar xvf spark-3.5.0-bin-hadoop3.tgz

# copy source to target path
sudo mv spark-3.5.0-bin-hadoop3/ /opt/spark 
```

## Set up spark 

create **spark.sh** in `/etc/profile.d/` and put below text in it.

```shell
# create the file
sudo vim /etc/profile.d/spark.sh

# add below text
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# refresh profile
source /etc/profile.d/spark.sh
```

## Test your spark

```shell
# open a spark shell
spark-shell
```
