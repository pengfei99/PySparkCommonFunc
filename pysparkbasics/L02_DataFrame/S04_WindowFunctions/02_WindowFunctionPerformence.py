from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import spark_partition_id, row_number, rank, col, lit, coalesce, broadcast, max, sum
from pyspark.sql.window import Window

""" Window function Performance issus. 
You could notice that the window specification change the partition of data frame. If your window specification 
partitionBy a column which only has one value or you don't use a partitionBy. This will cause entire data set to 
get shuffled to a single executor and the job fails with OOM errors.

If we use another column to do the partitionBy. The global order of the column that we want to sort will be lost.
So we need to partition the dataframe by conserving the global order of the the sort column. Which means
all_values_in_sort_column(partition_0) < all_values_in_sort_column(partition_1) < all_values_in_sort_column(partition_2)
And all values in one partition are sorted, so we keep the global order of the sorted column. 

Note in spark 3.1.2. only the same value will be put in the same partition. If column price has 12 distinct value, the 
dataframe after orderBy("price") will have 12 partition.



  
"""

""" Exp1
Check the partition change after applying window functions, 
You can notice the warning on window spec without partition
"""


def exp1(df: DataFrame):
    # Check the default partition of a dataframe
    print("Exp1 default partition of source data frame")
    df.withColumn("partition_id", spark_partition_id()).show(truncate=False)

    # define an orderBy window specification without partitionBy
    # You can notice all rows are in the same partition 0 which will be shuffled to the same executor.
    # If you have millions of rows in the same executor, you will get an OOM error
    win_order = Window.orderBy("price")
    # apply window function by using orderBy window spec
    df1 = df.withColumn("row_number", row_number().over(win_order)).withColumn("partition_id", spark_partition_id())
    print("Exp1 repartition of data frame after applying window function")
    df1.show()


"""
In the following example, we want to calculate the rank of price column, without separate them into different windows.
In a small dataset we can directly use groupBy specification without partition. But with a big data set, we will have 
OOM error. To avoid this, we do the following
Step1: use orderBy("price") to partition the dataframe, create a column "partition_id" by using function 
        spark_partition_id,
Step2: Create a local_rank column to rank each row inside its partition.
Step3: Get max rank of each partition by grouping the partition_id column
Step4: Use a window spec to get cumulative rank number for each partition
Step5: calculate the a sum factor to which can sum to local rank to become a global rank 
Step6: join the sum_factor with local rank and calculate the global rank
"""


def exp2(df: DataFrame):
    # Step 1:
    # To partition the dataframe and conserving the global sort order, we can use orderBy("sort_column_name")
    # In our case, the column name is price which we want to sort.
    print("Step1 new repartition of data frame after applying orderBy")
    df_sort_part = df.orderBy("price").withColumn("partition_id", spark_partition_id())
    df_sort_part.show(truncate=False)

    # Step 2: Create a local_rank column to rank each row inside its partition.
    # We create a window specification partitionBy the partition_id which is created by orderBy()
    win_part_id = Window.partitionBy("partition_id").orderBy("price")
    df_rank = df_sort_part.withColumn("local_rank", rank().over(win_part_id))
    print("Step2 Create a local_rank column to rank each row inside its partition.")
    df_rank.show()

    # Step 3: Get max rank of each partition by grouping the partition_id column
    df_tmp = df_rank.groupBy("partition_id").agg(max("local_rank").alias("max_rank"))
    print("Step 3: Get max rank of each partition by grouping the partition_id column")
    df_tmp.show()

    # Step 4: Use a window spec to get cumulative rank number for each partition
    win_rank = Window.orderBy("partition_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_stats = df_tmp.withColumn("cum_rank", sum("max_rank").over(win_rank))
    print("Step 4: Use a window spec to get cumulative rank number for each partition")
    df_stats.show()

    # Step 5 calculate the a sum factor to which can sum to local rank to become a global rank
    # tmp1 is a self join with the join condition, l.partition_id == r.partition_id +1
    # this means we shift the cumulative sum by 1 row on the right data frame
    tmp1 = df_stats.alias("l").join(df_stats.alias("r"), col("l.partition_id") == col("r.partition_id") + 1, "left")
    tmp1.show()
    join_df = tmp1.select(col("l.partition_id"), coalesce(col("r.cum_rank"), lit(0)).alias("sum_factor"))
    print("Step 5 calculate the a sum factor to which can sum to local rank to become a global rank ")
    join_df.show()

    # Step 6 join the sum_factor with local rank and calculate the global rank
    df_final = df_rank.join(broadcast(join_df), "partition_id", "inner") \
        .withColumn("rank", col("local_rank") + col("sum_factor"))
    print("Step 6 join the sum_factor with local rank and calculate the global rank")
    df_final.show()


"""
pyspark.sql.functions.coalesce(*cols): is a function which takes multiple column and returns the first column value 
                that is not null. 
                
Important note, don't mix with the rdd, dataframe class method coalesce. It's used to decrease the partition of a rdd
or data frame. for example: 
- rdd.coalesce(4)
- df.coalesce(4)
"""


def coalesce_exp(spark: SparkSession):
    df = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
    print("coalesce example source data frame")
    df.show()

    print("coalesce example concat column a and b")
    df.select(coalesce(col("a"), col("b"))).show()

    print("coalesce example concat column a, b and lit(0)")
    df.select(coalesce(col("a"), col("b"), lit(0))).show()


def main():
    spark = SparkSession.builder.master("local[4]").appName("Window_Function_performance").getOrCreate()
    data = [('Alex', '2018-10-10', 'Paint', 5),
            ('Alex', '2018-04-02', 'Ladder', 10),
            ('Alex', '2018-06-22', 'Stool', 15),
            ('Alex', '2018-12-09', 'Vacuum', 20),
            ('Alex', '2018-07-12', 'Bucket', 20),
            ('Alex', '2018-02-18', 'Gloves', 20),
            ('Alex', '2018-03-03', 'Brushes', 20),
            ('Alex', '2018-09-26', 'Sandpaper', 30),
            ('Alex', '2018-12-09', 'Vacuum', 30),
            ('Alex', '2018-07-12', 'Bucket', 30),
            ('Alex', '2018-02-18', 'Gloves', 30),
            ('Alex', '2018-03-03', 'Brushes', 5),
            ('Alex', '2018-09-26', 'Sandpaper', 5)]

    df = spark.createDataFrame(data, schema=['name', 'date', 'product', 'price'])

    # exp1(df)

    # run exp2
    # exp2(df)

    coalesce_exp(spark)


if __name__ == "__main__":
    main()
