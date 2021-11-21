from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col,collect_list


def main():
    spark = SparkSession.builder.master("local[2]").appName("reform_row_to_column").getOrCreate()
    path = "data/department.csv"
    df = spark.read.option("header", "true").csv(path)
    df.show()
    df.printSchema()
    # The best solution is to use window function
    # ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    # Step 1. build the month_revenue column
    df1 = df.withColumn("Jan_Revenue", when(col("month") == "Jan", col("revenue")).otherwise(None)) \
        .withColumn("Feb_Revenue", when(col("month") == "Feb", col("revenue")).otherwise(None)) \
        .withColumn("Mar_Revenue", when(col("month") == "Mar", col("revenue")).otherwise(None)) \
        .withColumn("Apr_Revenue", when(col("month") == "Apr", col("revenue")).otherwise(None)) \
        .withColumn("May_Revenue", when(col("month") == "May", col("revenue")).otherwise(None)) \
        .withColumn("Jun_Revenue", when(col("month") == "Jun", col("revenue")).otherwise(None)) \
        .withColumn("Jul_Revenue", when(col("month") == "Jul", col("revenue")).otherwise(None)) \
        .withColumn("Aug_Revenue", when(col("month") == "Aug", col("revenue")).otherwise(None)) \
        .withColumn("Sep_Revenue", when(col("month") == "Sep", col("revenue")).otherwise(None)) \
        .withColumn("Oct_Revenue", when(col("month") == "Oct", col("revenue")).otherwise(None)) \
        .withColumn("Nov_Revenue", when(col("month") == "Nov", col("revenue")).otherwise(None)) \
        .withColumn("Dec_Revenue", when(col("month") == "Dec", col("revenue")).otherwise(None))
    df1.show()
    # we can notice that we duplicate rows that has the same id. The duplicated rows for each month, only one row has
    # value, others are all null. So we need to group by rows by id and remove the duplicated null value rows
    # Step2. use groupBy to remove all duplicated rows that only contains null
    df2 = df1.groupBy(col("id")).agg(collect_list(col("Jan_Revenue")).getItem(0).alias("Jan_Revenue"),
                                     collect_list(col("Feb_Revenue")).getItem(0).alias("Feb_Revenue"),
                                     collect_list(col("Mar_Revenue")).getItem(0).alias("Mar_Revenue"),
                                     collect_list(col("Apr_Revenue")).getItem(0).alias("Apr_Revenue"),
                                     collect_list(col("May_Revenue")).getItem(0).alias("May_Revenue"),
                                     collect_list(col("Jun_Revenue")).getItem(0).alias("Jun_Revenue"),
                                     collect_list(col("Jul_Revenue")).getItem(0).alias("Jul_Revenue"),
                                     collect_list(col("Aug_Revenue")).getItem(0).alias("Aug_Revenue"),
                                     collect_list(col("Sep_Revenue")).getItem(0).alias("Sep_Revenue"),
                                     collect_list(col("Oct_Revenue")).getItem(0).alias("Oct_Revenue"),
                                     collect_list(col("Nov_Revenue")).getItem(0).alias("Nov_Revenue"),
                                     collect_list(col("Dec_Revenue")).getItem(0).alias("Dec_Revenue")
                                     ).orderBy(col("id"))
    df2.show()


if __name__ == "__main__":
    main()
