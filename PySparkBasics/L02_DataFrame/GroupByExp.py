""" Introduction
Similar to SQL GROUP BY clause, PySpark groupBy() function is used to collect the identical data into
groups on DataFrame and perform aggregate functions on the grouped data.

roupBy(col1 : scala.Predef.String, cols : scala.Predef.String*) :
      org.apache.spark.sql.RelationalGroupedDataset

Note that, it and take one or more column names and returns GroupedData object which can use below aggregate functions.
- count() - Returns the count of rows for each group.
- mean() - Returns the mean of values for each group.
- max() - Returns the maximum of values for each group.
- min() - Returns the minimum of values for each group.
- sum() - Returns the total for values for each group.
- avg() - Returns the average for values for each group.
- agg() - Using agg() function, we can calculate more than one aggregate at a time.
- pivot() - This function is used to Pivot the DataFrame.
"""
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

""" Exp 1, groupby single column and aggregate single/multi column

"""


def exp1(df: DataFrame):
    # Get the employee number by department, note that count() does not need argument
    df.groupBy("department").count().show()

    # Get the sum of salary and bonus by state
    df.groupBy("state").sum("salary", "bonus").show()


""" Exp 2, GroupBy multiple column, 
Suppose we do a groupBy on two column A, B. If A has m distinct value, and B has n distinct value. Then after groupby, 
we will have m*n rows

"""


def exp2(df: DataFrame):
    # Get max salary and bonus by department and state
    df.groupBy("department", "state").max("salary", "bonus").show()


""" Exp 3, GroupBy and show various stats on multiple column
The default min, max, can only show one type of stats. If you want to show multiple column with different stats, you 
need to use agg().  

Note here, avg and mean returned the same result.
"""


def exp3(df: DataFrame):
    df.groupBy("department").agg(
        f.sum("salary").alias("sum_salary"),
        f.min("salary").alias("min_salary"),
        f.avg("bonus").alias("avg_bonus"),
        f.mean("bonus").alias("mean_bonus"),
        f.sum("bonus").alias("sum_bonus")
    ).show(truncate=False)


""" Exp4, other aggregation functions inside agg()
We can use other aggregation functions inside agg, the full list of predefine aggregation function is here 
https://sparkbyexamples.com/pyspark/pyspark-aggregate-functions/ 

Below shows collect_list
"""


def exp4(df: DataFrame):
    # get the employee list by department
    df.groupBy("department").agg(f.collect_list("employee_name")).show(truncate=False)


def main():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("GroupByExp") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    data = [("James", "Sales", "NY", 90000, 34, 10000),
            ("Michael", "Sales", "NY", 86000, 56, 20000),
            ("Robert", "Sales", "CA", 81000, 30, 23000),
            ("Maria", "Finance", "CA", 90000, 24, 23000),
            ("Raman", "Finance", "CA", 99000, 40, 24000),
            ("Scott", "Finance", "NY", 83000, 36, 19000),
            ("Jen", "Finance", "NY", 79000, 53, 15000),
            ("Jeff", "Marketing", "CA", 80000, 25, 18000),
            ("Kumar", "Marketing", "NY", 91000, 50, 21000)
            ]

    schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)

    # run exp1
    # exp1(df)

    # run exp2
    # exp2(df)

    # run exp3
    # exp3(df)

    # run exp4
    exp4(df)


if __name__ == "__main__":
    main()
