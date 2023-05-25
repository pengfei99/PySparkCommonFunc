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

### 2.1 Build hadoop dependencies

Spark requires some hadoop dependencies to run. For linux, the required hadoop dpendencies are already included in the .tgz. 

For windows, we need to download these dependencies manually (e.g. **winutils.exe, hadoop.dll, etc.**. You can find a [repo](https://github.com/cdarlint/winutils) which build the dependencies for all the hadoop version. Git clone the repo, and get the appropriate hadoop version based on your spark binary version. Then put all the required dependencies at a folder (consider it as the hadoop home folder). In this tutorail we choose `C:\opt\hadoop` as the `HADOOP_HOME`, so the `winutils.exe` goes into `C:\opt\hadoop\bin\winutils.exe`, and the `hadoop.dll` goes into `C:\opt\hadoop\bin\hadoop.dll`

## 3. Setup Environment Variables

We need to setup three environment variables here:
- JAVA_HOME
- SPARK_HOME
- HADOOP_HOME

You can follow below steps to create environment variables:
1. Click on `search` toolbar, and type `environment`
2. Select the result `Edit the system environment variables`
3. A System Properties dialog box appears. In the lower-right corner, click `Environment Variables` and then click `New` in the next window.

![windows-new-environment-variable1.png](../images/windows-new-environment-variable1.png)

4. Create a new `environment variables` called `SPARK_HOME`, and put the path of the spark binary which you just downloaded. In our case it's `C:\opt\spark-3.2.3`

![windows-new-spark-env-variable.png](../images/windows-new-spark-env-variable.png)

5. In the top box, click on the `Path` entry, then click `Edit`. Then add the `%SPARK_HOME%\bin` to the `Path`

![windows-edit-path-variable-spark.png](../images/windows-edit-path-variable-spark.png)

Be careful here, don't try to delete the existing rows in the system path. Just add Spark.

![windows-add-spark-to-path.png](../images/windows-add-spark-to-path.png)


> Note In the `Environment Variables` view, you have the upper part which edit the User env var(only effective for the current user). The lower part which edit the system env var (effective for all users).  

You can repeat the above process for `JAVA_HOME` and `HADOOP_HOME`. For the Hadoop, suppose we put the `winutils.exe` in `C:\opt\hadoop\bin\winutils.set`. Then we should set `C:\opt\hadoop` as the env var, and add the `%HADOOP_HOME%\bin` to the `Path`.

## 4. Test your Spark installation

1. Open a new `command-prompt` window using the right-click and Run as `administrator`

2. To start Spark, enter:

```shell
C:\opt\spark-3.2.3\bin\spark-shell
```

3. The system should display several lines indicating the status of the application. You may get a Java pop-up. Select Allow access to continue.

Finally, the Spark logo appears, and the prompt displays the `Scala shell`.

4. You can view the spark web ui by using the  http://localhost:4040/. (You can replace localhost with the name of your system.)

5. To test the spark functionality, Use below script
```scala
// create a RDD based on a list
val rdd = sc.parallelize(List(1,2,3,4))
// do a map operation             
val square = rdd.map(x=>x*x)
// do a spark action and print the result 
println(square.collect().mkString(","))    
```

6. To exit Spark and close the Scala shell, press `ctrl-d` in the command-prompt window.

## 5. Trouble shoot

### 5.1 java.io.IOException: Cannot run program "python3"

This error is caused by the way of how python and pyspark are installed in the OS. And how the env var is configured. 

Two possible solutions:
1. Set up an env var **PYSPARK_PYTHON** = python or the python executable path (works for linux, window, MacOS)
2. Before you create your spark session, add below instructions

```python
import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.getOrCreate()
```
