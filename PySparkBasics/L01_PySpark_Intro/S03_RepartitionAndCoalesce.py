""" Spark is a distributed calculation framework. So data are divided into parts. Each parts is called a partition
When we create a RDD or data frame, data set. A default partition is given. This partition can be modified by two
methods:
- repartition: is used to increase or decrease the partitions
- coalesce: is used only to reduce the number of partitions. Note coalesce is more efficient than repartition which
           means less data movement across the cluster

As a result, if you can use coalesce, do not use repartition.
"""
from pyspark.sql import SparkSession

"""Exp1: RDD repartition and coalesce"""


def exp1(spark: SparkSession):
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd = spark.sparkContext.parallelize(data)
    # The default partition is 4 because I have 4 executor in my cluster(i.e. local[4])
    print("Default rdd partition: {}".format(rdd.getNumPartitions()))

    # if you don't want to use the default partition, you can give a specific partition
    rdd_fix_partition = spark.sparkContext.parallelize(data, 8)
    print("rdd_fix_partition partition: {}".format(rdd_fix_partition.getNumPartitions()))

    # The same patter also works for textFile(), I will not show them here

    # When you save data to disk, the number of partition is the number of part file. Below will generate four files
    # rdd.saveAsTextFile("/tmp/partition")
    # You will find 4 files in /tmp/partition
    # - part-00000:1,2,3
    # - part-00001:4,5,6
    # - part-00002:7,8,9
    # - part-00003:10,11,12
    # The skewness of data is 0. Because data is split into 4 partition evenly.

    # We can change the partition to 12
    rdd_repart = rdd.repartition(12)
    print("After repartition rdd has partition: {}".format(rdd_repart.getNumPartitions()))

    # We can change the partition to 2
    rdd_coalesce = rdd.coalesce(2)
    print("After coalesce rdd has partition: {}".format(rdd_coalesce.getNumPartitions()))


"""Exp2: DataFrame repartition 
We can create a data frame by using spark session, or RDD. You can notice, none of them allow us to give a specific 
partition number. So the spark session will give us a default partition number, which is the number of executor.
In our case, it's 4
- SPARKSESSION: createDataFrame(rdd)/(dataList)/(rowData,columns)/(dataList,schema)	
                read()
- RDD: toDF()/(*cols)

"""


def exp2(spark: SparkSession):
    df = spark.range(0, 20)
    df.show()
    # note data frame does not provide function to get partition number, we need to convert dataframe to rdd first
    # data frame is build on top of rdd. So it has the same partition number as the base rdd.
    print("Default data frame partition: {}".format(df.rdd.getNumPartitions()))
    df_repart = df.repartition(8)
    print("After repartition(8) data frame partition: {}".format(df_repart.rdd.getNumPartitions()))
    df_coalesce = df.coalesce(2)
    print("After coalesce(2) data frame partition: {}".format(df_coalesce.rdd.getNumPartitions()))

    # Note if you don't do data transformation which triggers shuffling, the partition of the result data frame does
    # not change.
    df_no_shuffle = df.withColumn("plus", df.id + 2)
    df_no_shuffle.show()
    print("After withColumn data frame partition: {}".format(df_no_shuffle.rdd.getNumPartitions()))

    # Before spark 3.0, If the operation(e.g. groupBy, union, join) trigger a shuffle, the data frame will be
    # transferred between multiple executors and even machines and finally repartition data into 200 partitions
    # by default. PySpark default defines shuffling partition to 200 using "spark.sql.shuffle.partitions" configuration.
    # After spark 3.0, it's not the case, after shuffle, the data frame has same partition.
    df_with_shuffle = df.groupBy("id").count()
    df_with_shuffle.show()
    print("After GroupBy data frame partition: {}".format(df_with_shuffle.rdd.getNumPartitions()))


def main():
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName("RepartitionAndCoalesce") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    # run exp1
    # exp1(spark)

    # run exp2
    exp2(spark)


if __name__ == "__main__":
    main()
