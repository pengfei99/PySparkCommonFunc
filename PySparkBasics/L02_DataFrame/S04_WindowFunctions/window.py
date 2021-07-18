from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead, col, avg, \
    min, max, sum, round, count, datediff

""" Window function in spark
A window function performs a calculation across a set of rows(groups, partitions, etc.). The built-in 
window functions provided by Spark SQL include two categories:
- Ranking functions:
- Analytic functions:

Window specification
To use window functions, we need to create a window specification. A window specification defines which rows
are included in the frame associated with a given input row. A window specification includes three parts:
 1. Partitioning specification: controls which rows will be in the same partition with the given row.
              Also, the user might want to make sure all rows having the same value for the category column are
              collected to the same machine before ordering and calculating the frame. If no partitioning specification
              is given, then all data must be collected to a single machine.
    
 2. Ordering specification: controls the way that rows in a partition are ordered, determining the position of the
                               given row in its partition.
    
 3. Frame specification: states which rows will be included in the frame for the current input row, based on their
                            relative position to the current row. For example, "the three rows preceding the current
                            row to current row" describes a frame including the current input row and three rows
                            appearing before the current row.
    
In spark SQL, the partition specification are defined by keyword "partitionBy", ordering specification is defined by
keyword "orderBy". 

1. Ranking functions:
     - rank: returns the rank of rows within a window partition
     - dense_rank: returns the rank of rows within a window partition, without any gaps. For example,
                  if you were ranking a competition using dense_rank and had three people tie for second place,
                  you would say that all three were in second place and that the next person came in third.
                  Rank would give me sequential numbers, making the person that came in third place (after the ties)
                  would register as coming in fifth.

     - percent_rank: returns the relative rank (i.e. percentile) of rows within a window partition.
     - ntile(n:Int): returns the ntile group id (from 1 to n inclusive) in an ordered window partition. For
                     example, if n is 4, the first quarter of the rows will get rank 1, the second quarter will
                     get 2, the thirds quarter will get 3, and the last will get 4. If the rows are less than n, it
                     works too.
    - row_number: returns a sequential number starting at 1 within a window partition.

2. Analytic functions:
    - cume_dist: returns the cumulative distribution of values within a window partition, i.e. the fraction
                 of rows that are below the current row. N = total number of rows in the partition.
                 cumeDist(x) = number of values before (and including) x / N. similar to percent_rank()
    - first()
    - last()
    - lag(e:Column,offset:Int,defaultValue:Object): returns the value that is offset rows before the current row, 
                and null if there is less than offset rows before row. For example, an offset of one will return 
                the previous row at any given point in the window partition. The defaultValue is optional
    - lead(e:Column,offset:Int): returns the value that is offset rows after the current row, and null if
               there is less than offset rows after the current row. For example, an offset of one will return
               the next row at any given point in the window partition.
    - currentRow(): Window function: returns the special frame boundary that represents the current row in
                          the window partition.
3. Aggregation functions:
     All the aggregation function that we showed in S03_GroupByAndAggregation can be used here.
    - sum(e:Column): returns the sum of selecting column for each partitions.
    - first(e:Column): returns the first value within each partition.
    - last(e:Column): returns the last value within each partition.

"""

""" Exp1

We show Ranking functions:
- row_number
- rank
- dense_rank

"""


def exp1(df: DataFrame):
    # create a window specification
    # This specification contains two partition "Alex", "Bob", each partition is ordered by price in ascending order.
    win_name = Window.partitionBy("name").orderBy("price")

    # Create a column with row number
    # You can notice the row number restarted from 1 for Bob, because it's in a new partition
    df1 = df.withColumn("row_number", row_number().over(win_name))
    print("Exp1: row number over name window order by price")
    df1.printSchema()
    df1.show()

    # create a column with rank
    # Note that for Alex partition, there is no rank2, because we have two items in rank 1, the third item goes to
    # rank 3. If you want compact rank number, use dense rank
    df2 = df.withColumn("rank", rank().over(win_name))
    print("Exp1: rank over name window order by price")
    df2.printSchema()
    df2.show()

    # create a column with dense rank
    # Note that for Alex partition, even thought we have two items in rank 1, but the third item goes to
    # rank 2 not 3.
    df3 = df.withColumn("dense_rank", dense_rank().over(win_name))
    print("Exp1: dense rank over name window order by price")
    df3.printSchema()
    df3.show()

    # create a column with percent rank, the percent is calculate by dense_rank_number/total_item_number
    df4 = df.withColumn("percent_rank", percent_rank().over(win_name))
    print("Exp1: percent rank over name window order by price")
    df4.printSchema()
    df4.show()

    # create a column with ntile
    df4 = df.withColumn("ntile_rank", ntile(3).over(win_name))
    print("Exp1: ntile over name window order by price")
    df4.printSchema()
    df4.show()


""" Exp2

show example of the following functions
- cume_dist
- lag
- lead
"""


def exp2(df: DataFrame):
    win_name = Window.partitionBy("name").orderBy("price")
    win0 = Window.partitionBy("name")

    # create a cumulative_distribution column
    df1 = df.withColumn("cumulative_distribution", cume_dist().over(win_name))
    print("Exp2 create a cumulative_distribution column")
    df1.printSchema()
    df1.show()

    # create a lag column by using price.
    # note if we set offset as 2, the first two row of lag is null, and the third rows gets the first row value of the
    # price column. If we set offset as 3, the first three rows will be null, and the fourth rows get the first row
    # value.
    df2 = df.withColumn("lag", lag("price", 3).over(win_name))
    print("Exp2 create a lag column")
    df2.printSchema()
    df2.show()

    # create a lead column by using price.
    # note if we set offset as 2, the last two row of lead is null in each partition, and the last third row gets the
    # value of last row of the price column. If we set offset as 3, the last three rows will be null, and the last
    # fourth rows get the last row value.
    df3 = df.withColumn("lead", lead("price", 3).over(win_name))
    print("Exp2 create a lead column")
    df3.printSchema()
    df3.show()

    # here we set lag on date column with offset 1, it means the second row will have the value of first row, then
    # apply the datediff function on this value with the current row date value, then we get days from the last
    # purchase.
    # Use the same logic by using lead, we get the days before next purchase, if we set offset as 2, we will get
    # the days before next 2 purchase
    df4 = df.withColumn('days_from_last_purchase', datediff('date', lag('date', 1).over(win0.orderBy(col('date'))))) \
        .withColumn('days_before_next_purchase', datediff(lead('date', 1).over(win0.orderBy(col('date'))), 'date'))
    print("Exp2 Practical example of lead and lag")
    df4.show()


"""Exp3
Aggregation functions:
- avg/mean
- min
- max
- sum
"""


def exp3(df: DataFrame):
    win_name = Window.partitionBy("name")
    # if you apply aggregation function on a windows spec with order, you will get a cumulative result for each rows
    df1 = df.withColumn("row", row_number().over(win_name.orderBy("price"))) \
        .withColumn("avg", avg(col("price")).over(win_name)) \
        .withColumn("sum", sum(col("price")).over(win_name)) \
        .withColumn("min", min(col("price")).over(win_name)) \
        .withColumn("max", max(col("price")).over(win_name)) \
        .withColumn("cumulative_avg", avg(col("price")).over(win_name.orderBy("price")))
    print("Exp3 show aggregation function example OrderBy price")
    df1.show()
    df2 = df.withColumn('avg_to_date', round(avg('price').over(win_name.orderBy(col('date'))), 2)) \
        .withColumn('accumulating_sum', sum('price').over(win_name.orderBy(col('date')))) \
        .withColumn('max_to_date', max('price').over(win_name.orderBy(col('date')))) \
        .withColumn('max_of_last2', max('price').over(win_name.orderBy(col('date')).rowsBetween(-1, Window.currentRow))) \
        .withColumn('items_to_date', count('*').over(win_name.orderBy(col('date'))))
    print("Exp3 show aggregation function example OrderBy date")
    df2.show()


""" Exp4
Rows between, Range between

"""


def exp4(df: DataFrame):
    pass


def main():
    spark = SparkSession.builder.master("local[2]").appName("Windows functions").getOrCreate()
    data = [('Alex', '2018-10-10', 'Paint', 80),
            ('Alex', '2018-04-02', 'Ladder', 20),
            ('Alex', '2018-06-22', 'Stool', 20),
            ('Alex', '2018-12-09', 'Vacuum', 40),
            ('Alex', '2018-07-12', 'Bucket', 5),
            ('Alex', '2018-02-18', 'Gloves', 5),
            ('Alex', '2018-03-03', 'Brushes', 30),
            ('Alex', '2018-09-26', 'Sandpaper', 10),
            ('Bob', '2018-12-09', 'Vacuum', 40),
            ('Bob', '2018-07-12', 'Bucket', 5),
            ('Bob', '2018-02-18', 'Gloves', 5),
            ('Bob', '2018-03-03', 'Brushes', 30),
            ('Bob', '2018-09-26', 'Sandpaper', 10)]

    df = spark.createDataFrame(data, schema=['name', 'date', 'product', 'price'])
    print("source data frame: ")
    df.printSchema()
    df.show(truncate=False)

    # run exp1
    # exp1(df)

    # run exp2
    exp2(df)

    # run exp3
    # exp3(df)


if __name__ == "__main__":
    main()
