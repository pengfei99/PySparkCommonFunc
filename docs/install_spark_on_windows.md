# Install spark on windows

This tutorial has been tested on windows 10 and windows server 2019.

## 1. Download and Install jdk

Spark requires jdk to run, so we need to install jdk first. Check your current system situation.

Open the command line by clicking **Start** > type cmd > click **Command Prompt**.

```shell
java -version
```

If java is installed, you should see the version, otherwise you should see `commnad not found`.

Choose the version that your spark requires: `Spark 3.*` requires JDK 8 or later. In this tutorial, we choose JDK 11 (LTS).

You can use this [link](https://www.oracle.com/java/technologies/downloads/#java11) to download it from Oracle. 

After installation, rerun 

```shell
java -version
```

By default, the installer will set java home folder at `C:\Program Files\Java\jdk-11.0.17` 

## 2. Download and Install spark

1. Open a browser and navigate to https://spark.apache.org/downloads.html.
2. Choose a stable version. In this tutorial we have choosen `spark 3.2.3 with hadoop 3.2`
3. Download .tgz
4. Extract it to a path that you prefered. In this tutorial, we choose `C:\opt\spark-3.2.3`
5. Add Hadoop dependencies. Spark requires some hadoop dependencies to run. For windows, we need to download a **winutils.exe**. You can find a [repo](https://github.com/cdarlint/winutils) which build this for all the hadoop version. Get the appropriate version based on your spark version. Then put it at a folder (consider it as the hadoop home folder). In this tutorail we choose `C:\opt\hadoop` as the `HADOOP_HOME`, so the `winutils.exe` goes into `C:\opt\hadoop\bin\winutils.exe`.

## 3. Setup Environment Variables

We need to setup three environment variables here:
- JAVA_HOME
- SPARK_HOME
- HADOOP_HOME

You can follow below steps to create environment variables:
1. Click on `search` toolbar, and type `environment`
2. Select the result `Edit the system environment variables`
3. A System Properties dialog box appears. In the lower-right corner, click Environment Variables and then click New in the next window.



