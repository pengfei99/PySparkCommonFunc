from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f

"""Exp1 : Simple pivot example, understand what pivot will do"""


def exp1(df):
    print("Exp1: show the country list groupby Product")
    df.groupBy("Product").agg(f.collect_list("Country")).show(truncate=False)
    print("Exp1: show the Amount by Product and country")
    df.groupBy("Product", "Country").sum("Amount").show(truncate=False)
    # The pivot function will transform the country list into columns with
    pivot_country = df.groupBy("Product").pivot("Country").sum("Amount")
    pivot_country.printSchema()
    pivot_country.show(truncate=False)
    return pivot_country


""" Exp2 : Improve pivot performance
Pivot is a very expensive operation, you may want to optimize it by using following solutions.
Solution 1: We can provide a list of the column name(row value) that we want to pivot.
Solution 2: Two phrase groupby
"""


def exp2(df):
    # Solution 1: We can provide a list of the column name(row value) that we want to pivot.
    # You don't have to have all distinct value in the list, try to remove one country and re-run the example
    country_list = ["USA", "China", "Canada", "Mexico"]
    pivot_country = df.groupBy("Product").pivot("Country", country_list).sum("Amount")
    print("Exp2: Optimize pivot with country list")
    pivot_country.show(truncate=False)

    # Solution 2 with custom col name:
    pivot_2 = df.groupBy("Product", "Country").agg(f.sum("Amount").alias("sum_amount")) \
        .groupBy("Product").pivot("Country").sum("sum_amount")
    print("Exp2: Optimize pivot with two groupBy and custom column name")
    pivot_2.show(truncate=False)

    # solution 2 with auto col name:
    pivot_3 = df.groupBy("Product", "Country").sum("Amount") \
        .groupBy("Product").pivot("Country").sum("sum(Amount)")
    print("Exp2: Optimize pivot with two groupBy and auto gen column name")
    pivot_3.show(truncate=False)


""" Exp3 : Unpivot 
Unpivot is a reverse operation, we can achieve by rotating column values into rows values. 
PySpark SQL doesn’t have unpivot function hence will use the stack() function. Below code converts 
column countries to row.
"""


def exp3(pivoted_df: DataFrame):
    unpivot_expr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
    unpivot_df = pivoted_df.select("Product", f.expr(unpivot_expr)).where("Total is not null")
    unpivot_df.show(truncate=False)
    unpivot_df.printSchema()


def main():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("Pivot and UnPivot") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    data = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
            ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
            ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
            ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico")]

    columns = ["Product", "Amount", "Country"]
    df = spark.createDataFrame(data=data, schema=columns)
    print("main output: source data schema")
    df.printSchema()
    print("main output: source data")
    df.show(truncate=False)

    # run exp1
    pivot_df = exp1(df)

    # run exp2
    # exp2(df)

    # run exp3
    exp3(pivot_df)


if __name__ == "__main__":
    main()
