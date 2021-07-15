from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, countDistinct, approx_count_distinct, avg, min, max, mean, collect_list, \
    collect_set

"""
Aggregate functions operate on a group of rows and calculate a single return value for every group.

In previous GroupByExp, we have seen some aggregation example. In this tutorial, we will examine all existing 
aggregation functions in spark. The full list in alphabetic order:

- approx_count_distinct, count, countDistinct
- avg, max, min, mean
- collect_list, collect_set
- grouping
- first
- last
- kurtosis
- skewness
- stddev
- stddev_samp
- stddev_pop
- sum
- sumDistinct
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
    exp3(df)


if __name__ == "__main__":
    main()
