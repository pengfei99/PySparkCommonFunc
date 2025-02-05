from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, countDistinct, approx_count_distinct, avg, min, max, mean, collect_list, \
    collect_set, grouping, first, last, sum, sumDistinct, skewness, kurtosis, stddev, stddev_samp, stddev_pop, \
    variance, var_samp, var_pop

"""
Aggregate functions operate on a group of rows and calculate a single return value for every group.

In previous GroupByExp, we have seen some aggregation example. In this tutorial, we will examine all existing 
aggregation functions in spark. The full list in alphabetic order:

- approx_count_distinct, count, countDistinct
- avg/mean, max, min, 
- collect_list, collect_set
- grouping : Check if a column is created by aggregation function or not, returns 1 for aggregated, 0 for not aggregated
- first,last
- sum, sumDistinct
- kurtosis, skewness
- stddev, stddev_samp, stddev_pop
- variance, var_samp, var_pop
"""

""" Exp1
The classic count() will just count row numbers of a group.

The approx_count_distinct is implemented to avoid count(distinct()) operations. The approx_count_distinct uses
an algorithm called HyperLogLog. This algorithm can estimate the number of distinct values of greater than 
1,000,000,000, where the accuracy of the calculated approximate distinct count value is within 2% of the actual 
distinct count value. It can do this while using much less memory.

Because count(distinct()) requires more and more memory as the number of distinct values increases. 

This tutorial shows how the approx_count_distinct function is implemented
https://mungingdata.com/apache-spark/hyperloglog-count-distinct/#:~:text=approx_count_distinct%20uses%20the%20
HyperLogLog%20algorithm,count()%20will%20run%20slower).

"""


def exp1(df: DataFrame):
    # Aggregation function can be used after groupBy or on the whole data frame.
    print("Exp1 show count example")
    df.select(count("salary")).show()
    df.groupBy("department").count().show()

    # countDistinct can take multiple column as argument. If input column number is greater than 1, then
    # the value combination (col1, col2, ...) must be distinct.
    print("Exp1 show countDistinct example")
    # salary distinct value is 6, but (department, salary) distinct value is 10
    df.select(countDistinct("salary")).show()
    df.select(countDistinct("department", "salary")).show()

    print("Exp1 show approx_count_distinct example")
    df.select(approx_count_distinct("salary")).show()


""" Exp2 
Get basic stats of a column by using avg/mean, min, max

Note mean is the alias of avg. It's not the median function.

"""


def exp2(df: DataFrame):
    print("Exp2, show avg example")
    # avg can only apply on digit column, if type is mismatch, it returns null
    df.select(avg("salary")).show()
    df.select(avg("name")).show()

    print("Exp2, show mean example")
    # mean is the alias of avg. so it works like avg. It's not the median.
    df.select(mean("salary")).show()
    df.select(mean("name")).show()

    print("Exp2, show min example")
    # min can apply on digit and string column, it uses default sorting order (ascending) to find min
    df.select(min("salary")).show()
    df.select(min("name")).show()

    print("Exp2, show max example")
    # max can apply on digit and string column, it uses default sorting order (ascending) to find max
    df.select(max("salary")).show()
    df.select(max("name")).show()


""" Exp3
collect_list() function returns a list that contains all values from an input column with duplicates.
collect_set() function returns a list that contains all values from an input column without duplicates
"""


def exp3(df: DataFrame):
    print("Exp3 show collect_list example")
    df.select(collect_list("salary")).show(truncate=False)
    print("Exp3 show collect_set example")
    df.select(collect_set("salary")).show(truncate=False)


""" Exp4
Can't make it work, always get "grouping() can only be used with GroupingSets/Cube/Rollup;" error
"""


def exp4(df: DataFrame):
    # we can't use withColumn to create a column by using aggregation function
    df1 = df.groupBy("department").avg("salary")
    df1.show()
    df1.select(grouping("avg(salary)")).show()


""" Exp5 
- first(): it returns the first element in a column when ignoreNulls is set to true, 
          it returns the first non-null element.
- last(): returns the last element in a column. when ignoreNulls is set to true, 
          it returns the last non-null element.
"""


def exp5(df: DataFrame):
    df.select(first("salary"), last("department")).show()


""" Exp6
sum: returns the sum of all values in a column.
sumDistinct: returns the sum of all distinct values in a column.
"""


def exp6(df: DataFrame):
    df.select(sum("salary"), sumDistinct("salary")).show()


""" Exp7
When we do descriptive analysis, we want to know the skewness of a distribution. If a distribution is not skewed, which
is a normal distribution, then we want to know the Kurtosis(峰度) of the distribution.

Skewness is a measure of symmetry, or more precisely, the lack of symmetry. A distribution, or data set, is symmetric 
if it looks the same to the left and right of the centre point. 

Kurtosis measures whether your dataset is heavy-tailed or light-tailed compared to a normal distribution. 
Data sets with high kurtosis have heavy tails and more outliers and data sets with low kurtosis tend to have 
light tails and fewer outliers.
"""


def exp7(df: DataFrame):
    # note the salary distribution is negatively skewed, it means mean<median<Mode
    # The kurtosis is negative too, it means the distribution is very flat and dispersed
    df.select(skewness("salary"), kurtosis("salary")).show()


""" Exp 8
Standard deviation is a measure of the amount of variation or dispersion of a set of values.
- A low standard deviation indicates that the values tend to be close to the mean (also called the expected value) 
     of the set
- A high standard deviation indicates that the values are spread out over a wider range.

Spark provides three methods:
- stddev(): alias for stddev_samp.
- stddev_samp(): returns the unbiased sample standard deviation of the expression in a group.
- stddev_pop(): returns population standard deviation of the expression in a group.

"""


def exp8(df: DataFrame):
    df.select(stddev("salary"), stddev_samp("salary"), stddev_pop("salary")).show()


""" Exp9
Variance is also a measure of the amount of variation or dispersion of a set of values. The difference with standard
deviation is that square functions are used during the calculation because they weight outliers more heavily than 
points that are near to the mean. This prevents that differences above the mean neutralize those below the mean.

But the square function makes the Variance change the unit of measurement of the original data. For example, a column
contains centimeter values. Your variance would be in squared centimeters and therefore not the best measurement. 

Spark provides three methods:
- variance(): alias for var_samp.
- var_samp(): returns the unbiased sample variance of the values in a group.
- var_pop(): returns the population variance of the values in a group.
"""


def exp9(df: DataFrame):
    df.select(variance("salary"), var_samp("salary"), var_pop("salary")).show()


def main():
    spark = SparkSession.builder.master("local[2]").appName("AggregationFunction").getOrCreate()
    data = [("Alice", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "IT", 4100),
            ("Maria", "Finance", 3000),
            ("Haha", "IT", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Haha", "Sales", 4100)
            ]
    schema = ["name", "department", "salary"]
    df = spark.createDataFrame(data=data, schema=schema)
    print("Source data frame: ")
    df.printSchema()
    df.show(truncate=False)

    # exp1
    # exp1(df)

    # exp2
    # exp2(df)

    # run exp3
    # exp3(df)

    # run exp4
    # exp4(df)

    # run exp5
    # exp5(df)

    # run exp6
    # exp6(df)

    # run exp7
    # exp7(df)

    # run exp8
    # exp8(df)

    # run exp9
    exp9(df)


if __name__ == "__main__":
    main()
