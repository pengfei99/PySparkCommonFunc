from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead, col, avg, \
    min, max, sum, round, count, datediff, unix_timestamp, stddev, collect_list, element_at, size, sort_array, \
    broadcast, spark_partition_id, lit, coalesce

from pyspark.sql.window import Window

""" 1. Window function in spark
A window function performs a calculation across a set of rows(aka. Frame). The built-in 
window functions provided by Spark SQL include two categories:
- Ranking functions:
- Analytic functions:


Window specification
To use window functions, we need to create a window specification. A window specification defines which rows
are included in the frame associated with a given input row. In another word, the window specification defines
the default frame of a window. A window specification can be classified into three categories:
1. PartitionBy specification: 
       - Created with Window.partitionBy on one or more columns
       - All rows that have the same value on the partitionBy column will be in the same frame.
       - The aggregation functions can be applied on each frame
       - The windows functions can not be applied.  
             
2. Ordered specification: 
       - Created by using a partitionBy specification, followed by an orderBy specification
       - The frame is not static, it moves when we iterate each row. By default, the frame contains 
         all previous rows and the currentRow.
       - The window function can be applied to each moving frame (i.e. currentRow+allPreviousRow)
       - The aggregation functions can be applied to each moving frame. As each row has a different
         frame, the result of the aggregation is different for each row. Unlike the partitionBy
         specification, all rows in the same partition has the same result. 
    
3. Custom Range Frame specification: (check exp4)
       - Created by using a partitionBy specification, 
       - Usually followed by an orderBy specification,
       - Then followed by "rangeBetween" or "rowsBetween"
       - Each row has a corresponding frame which is controlled by rangeBetween or rowsBetween. For example, 
         rowsBetween(-3,Window.currentRow) means the three rows preceding the current row to the current row.
         It defines a frame including the current input row and three rows appearing before the current row.
       - Aggregation can be applied on each frame.
       
        
    
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

We show examples of Ranking functions on ordered frame:
- row_number
- rank
- dense_rank
- percent_rank
- ntile

Note all above window functions require that the frame are ordered. You can try to
replace win_name_ordered by win_name and see what happens. 
"""


def exp1(df: DataFrame):
    # create a window specification
    # This specification contains two partition "Alex", "Bob", each partition is ordered by price in ascending order.
    win_name = Window.partitionBy("name")
    win_name_ordered = win_name.orderBy("price")

    # Create a column with row number
    # You can notice the row number restarted from 1 for Bob, because it's in a new partition
    df1 = df.withColumn("row_number", row_number().over(win_name_ordered))
    print("Exp1: row number over name window order by price")
    df1.printSchema()
    df1.show()
    print("Exp1: show partition id after window functions")
    df1.withColumn("partition_id", spark_partition_id()).show(truncate=False)

    # create a column with rank
    # Note that for Alex partition, there is no rank2, because we have two items in rank 1, the third item goes to
    # rank 3. If you want compact rank number, use dense rank
    df2 = df.withColumn("rank", rank().over(win_name_ordered))
    print("Exp1: rank over name window order by price")
    df2.printSchema()
    df2.show()

    # create a column with dense rank
    # Note that for Alex partition, even thought we have two items in rank 1, but the third item goes to
    # rank 2 not 3.
    df3 = df.withColumn("dense_rank", dense_rank().over(win_name_ordered))
    print("Exp1: dense rank over name window order by price")
    df3.printSchema()
    df3.show()

    # create a column with percent rank, the percent is calculate by dense_rank_number/total_item_number
    df4 = df.withColumn("percent_rank", percent_rank().over(win_name_ordered))
    print("Exp1: percent rank over name window order by price")
    df4.printSchema()
    df4.show()

    # create a column with ntile
    df4 = df.withColumn("ntile_rank", ntile(3).over(win_name_ordered))
    print("Exp1: ntile over name window order by price")
    df4.printSchema()
    df4.show()


""" Exp2

show example of the analytic functions on ordered frame
- cume_dist
- lag
- lead

Note all above window functions require that the frame are ordered.
"""


def exp2(df: DataFrame):
    win_name = Window.partitionBy("name")
    win_name_ordered = win_name.orderBy("price")

    # create a cumulative_distribution column
    df1 = df.withColumn("cumulative_distribution", cume_dist().over(win_name_ordered))
    print("Exp2 create a cumulative_distribution column")
    df1.printSchema()
    df1.show()

    # create a lag column by using price.
    # note if we set offset as 2, the first two row of lag is null, and the third rows gets the first row value of the
    # price column. If we set offset as 3, the first three rows will be null, and the fourth rows get the first row
    # value.
    df2 = df.withColumn("lag", lag("price", 3).over(win_name_ordered))
    print("Exp2 create a lag column")
    df2.printSchema()
    df2.show()

    # create a lead column by using price.
    # note if we set offset as 2, the last two row of lead is null in each partition, and the last third row gets the
    # value of last row of the price column. If we set offset as 3, the last three rows will be null, and the last
    # fourth rows get the last row value.
    df3 = df.withColumn("lead", lead("price", 3).over(win_name_ordered))
    print("Exp2 create a lead column")
    df3.printSchema()
    df3.show()

    # here we set lag on date column with offset 1, it means the second row will have the value of first row, then
    # apply the datediff function on this value with the current row date value, then we get days from the last
    # purchase.
    # Use the same logic by using lead, we get the days before next purchase, if we set offset as 2, we will get
    # the days before next 2 purchase
    df4 = df.withColumn('days_from_last_purchase', datediff('date', lag('date', 1).over(win_name.orderBy(col('date'))))) \
        .withColumn('days_before_next_purchase', datediff(lead('date', 1).over(win_name.orderBy(col('date'))), 'date'))
    print("Exp2 Practical example of lead and lag")
    df4.show()


"""Exp3
Show aggregation functions on ordered frame and basic partitionBy frame
- avg/mean
- min
- max
- sum

In df1, we use a partition window specification, so the result is the same for all rows
that are in the same partition.

In df2, we use an ordered window specification, the result is different for each rows. 

"""


def exp3(df: DataFrame):
    win_name = Window.partitionBy("name")
    win_name_ordered = win_name.orderBy("date")
    df1 = df.withColumn("avg", avg(col("price")).over(win_name)) \
        .withColumn("sum", sum(col("price")).over(win_name)) \
        .withColumn("min", min(col("price")).over(win_name)) \
        .withColumn("max", max(col("price")).over(win_name)) \
        .withColumn("item_number", count("*").over(win_name)) \
        .withColumn("item_list", collect_list(col("product")).over(win_name))
    print("Exp3 show aggregation function example on partition window specification")
    df1.show(truncate=False)

    # if you apply aggregation function on a windows spec with order, you will get a cumulative result for each rows
    df2 = df.withColumn('avg_to_date', round(avg('price').over(win_name_ordered), 2)) \
        .withColumn('sum_to_date', sum('price').over(win_name_ordered)) \
        .withColumn('max_to_date', max('price').over(win_name_ordered)) \
        .withColumn('min_to_date', max('price').over(win_name_ordered)) \
        .withColumn('item_number_to_date', count('*').over(win_name_ordered)) \
        .withColumn("item_list_to_date", collect_list(col("product")).over(win_name_ordered))

    print("Exp3 show aggregation function example on ordered window specification")
    df2.show(truncate=False)


""" Exp4
To build range window specifications, we need to use the two following functions 
- rowsBetween(start:Long,end:Long)->WindowSpec : Here start, end are the index of rows relative to current rows, -1 
    means 1 row before current row, 1 mean 1 row after current row
- rangeBetween(start:Long, end:Long)->WindowSpec : The start, end boundary in rangeBetween is based on row value 
    relative to currentRow. The value definition of the constant values used in range functions:
     -- Window.currentRow = 0
     -- Window.unboundedPreceding = Long.MinValue
     -- Window.unboundedFollowing = Long.MaxValue

The [start, end] index are all inclusive. Their value can be 
- Window.unboundedPreceding
- Window.unboundedFollowing
- Window.currentRow. 
- Or a value relative to Window.currentRow, either negative or positive.

Some examples of rowsBetween:
- rowsBetween(Window.currentRow, 2): From current row to the next 2 rows 
- rowsBetween(-3, Window.currentRow): From the previous 3 rows to the current row. 
- rowsBetween(-1, 2): Frame contains previous row, current row and the next 2 rows 
- rowsBetween(Window.currentRow, Window.unboundedFollowing): From current row to all next rows 
- rowsBetween(Window.unboundedPreceding, Window.currentRow): From all previous rows to the current row. 
- rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing): all rows in the window. 
"""


# we have 86400 seconds in a day
def day_to_seconds(day_num: int):
    return day_num * 86400


def exp4(df: DataFrame):
    win_name = Window.partitionBy("name")
    win_name_ordered = win_name.orderBy("date")

    # Example of rowsBetween
    # last 2 row(current and the row before it) range window specification
    last2 = win_name_ordered.rowsBetween(-1, Window.currentRow)
    df.withColumn("max_of_last2", max("price").over(last2)).show(truncate=False)

    # max of all following row
    following = win_name_ordered.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    df.withColumn("max_of_following", max("price").over(following)).show(truncate=False)

    #
    df1 = df.withColumn("unix_date", unix_timestamp("date", "yyyy-MM-dd"))
    print("Exp4 convert string date to long unix timestamp")
    df1.show(5, truncate=False)

    # Example of rangeBetween
    # get the avg of a specific range of a window
    # 0 is the relative unix_date of current row, the frame boundary of rangeBetween(-day_to_seconds(30), 0)
    # for row "Alex|2018-02-18|Gloves |5 |1518908400|" will be (1518908400-(30*86400),1518908400). All rows that
    # have unix_date column value in this frame boundary will be included in the frame.
    range_30 = win_name.orderBy(col("unix_date")).rangeBetween(-day_to_seconds(30), 0)
    df2 = df1.withColumn("30day_moving_avg", avg("price").over(range_30))
    print("Exp4 create a column that shows last 30 day avg before current row date")
    df2.show(10, truncate=False)

    # get the avg of 30 day before and 15 days after the current row date
    # Note that stddev of some row will return null. Because it requires at least two
    # observations to calculate standard deviation.
    range_45 = win_name.orderBy("unix_date").rangeBetween(-day_to_seconds(30), day_to_seconds(15))
    df3 = df1.withColumn("45day_moving_avg", avg("price").over(range_45)) \
        .withColumn("45day_moving_std", stddev("price").over(range_45))
    print("Exp4 create a column that shows the avg of 30 day before and 15 days after the current row date")
    df3.show(10, truncate=False)


""" Exp5 Calculate Median in a window
mean(avg) and median are commonly used in statistics. 
- mean is cheap to calculate, but outliers can have large effect. For example, the income of population, if we have 9 
  people has 10 dollar, and 1 person has 1010 dollar. The mean is 1100/10= 110. It does not represent any group's income. 
- Median is expansive to calculate. But in certain cases median are more robust comparing to mean, since it will 
  filter out outlier values. If we retake the previous example, the median will be 10 dollar, which represent a 
  group's income
"""


def exp5(df: DataFrame):
    win_name = Window.partitionBy("name")
    win_name_ordered = win_name.orderBy("price")
    # Rolling median
    # we create a column of price list, then we use function element_at to get the middle element of the list
    print("Exp5 Calculate rolling median for price column")
    df.withColumn("price_list", collect_list("price").over(win_name_ordered)) \
        .withColumn("rolling_median", element_at("price_list", (size("price_list") / 2 + 1).cast("int"))) \
        .show(truncate=False)

    # Global median with partition frame,
    # as the window is not ordered, all element of the partition are in the same frame. The problem is the element
    # of the list is not ordered, so the middle element of the list is not the median. To correct this, we need to
    # sort the list
    print("Exp5 Calculate global median for price column by using partition window and sort_array ")
    df.withColumn("price_list", sort_array(collect_list("price").over(win_name))) \
        .withColumn("sort_list_median", element_at("price_list", (size("price_list") / 2 + 1).cast("int"))) \
        .show(truncate=False)

    # Global median with range frame
    # After orderBy, each row will have a rolling frame. To include all rows of the partition, we need to use range
    # specification to change the default frame after orderBy.
    # We use rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing) to include all rows of the partition
    print("Exp5 Calculate global median for price column by using range frame")
    # create a range window spec that contains all rows of the partition
    win_range = win_name_ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df.withColumn("price_list", collect_list("price").over(win_range)) \
        .withColumn("range_window_median", element_at("price_list", (size("price_list") / 2 + 1).cast("int"))) \
        .show(truncate=False)

    # We can also use groupBy and join to get the same result
    df1 = df.groupBy("name").agg(sort_array(collect_list("price")).alias("price_list")) \
        .select("name", "price_list",
                element_at("price_list", (size("price_list") / 2 + 1).cast("int")).alias("groupBy_median"))
    df1.show(truncate=False)
    # The pyspark.sql.functions.broadcast(df) marks a DataFrame as small enough for use in broadcast joins.
    df.join(broadcast(df1), "name", "inner").show(truncate=False)


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
    # exp2(df)

    # run exp3
    # exp3(df)

    # run exp4
    # exp4(df)

    # run exp5
    # exp5(df)


if __name__ == "__main__":
    main()
