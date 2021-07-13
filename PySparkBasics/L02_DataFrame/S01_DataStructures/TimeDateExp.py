from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, unix_timestamp, col, from_unixtime, to_timestamp, date_format, to_date

"""

Date and time has multiple type. 
- String (date_format())
- Long (unix_timestamp)
- timestamp (to_timestamp): can be converted to unix_timestamp by using cast("long"), can be converted to string by 
                           using cast("string") 
- date(to_date()): 

In exp1, exp2, we deal with unix timestamp type

In exp3, we deal with timestamp type

In exp4, we deal with date type
"""

"""
Exp1: unix_timestamp() function is used to get the current time and to convert the time string in format 
      yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds) by using the current timezone of the system.
1: def unix_timestamp(): returns the current time in seconds (LongType)
2) def unix_timestamp(date: Column): take the string column date as input, output it in seconds
3) def unix_timestamp(date: Column, format: String): here format gives explicitly the format of the date string.
"""


def exp1(df: DataFrame):
    df1 = df.withColumn("timestamp_4", lit(unix_timestamp()))
    print("Exp1: use unix_timestamp() to get current time")
    df1.printSchema()
    df1.show()

    # note we did not give format for timestamp_1, it works because the default format is yyyy-MM-dd HH:mm:ss
    # for timestamp_2, timestamp_3, if we don't give format, it returns null, because the string does not have
    # the default date format
    # If we give a format that is wrong, the spark job will fail. For example, try to remove SSS from timestamp_2
    # format
    df2 = df.withColumn("timestamp_1", unix_timestamp("timestamp_1")) \
        .withColumn("timestamp_2", unix_timestamp("timestamp_2", "MM-dd-yyyy HH:mm:ss.SSS")) \
        .withColumn("timestamp_3", unix_timestamp("timestamp_3"))
    print("Exp1: covert date string to seconds with some erreurs")
    df2.printSchema()
    df2.show()

    df3 = df.withColumn("timestamp_1", unix_timestamp("timestamp_1")) \
        .withColumn("timestamp_2", unix_timestamp("timestamp_2", "MM-dd-yyyy HH:mm:ss.SSS")) \
        .withColumn("timestamp_3", unix_timestamp("timestamp_3", "MM-dd-yyyy")) \
        .withColumn("timestamp_4", lit(unix_timestamp()))
    print("Exp1: covert date string to seconds with success")
    df3.printSchema()
    df3.show()
    return df3


"""Exp2
We have seen how to convert string(date) to long(second) in exp2, now we need to covert it back.
Todo this we use from_unixtime(unix_time: Column, format: String) 
"""


def exp2(df: DataFrame):
    df1 = df.select(
        from_unixtime(col("timestamp_1")).alias("timestamp_1"),
        from_unixtime(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
        from_unixtime(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
        from_unixtime(col("timestamp_4")).alias("timestamp_4")
    )
    print("Exp2 Convert unix timestamp in second to string date: ")
    df1.printSchema()
    df1.show(truncate=False)


""" Exp3 
to_timestamp(col, format): the default format is yyyy-MM-dd HH:mm:ss It converts a string date to spark timestamp.
If failed, return null.

To convert timestamp back to string, we have two options 
- cast("String"): It converts a timestamp column to default date format yyyy-MM-dd HH:mm:ss
- date_format(column,format): It converts a timestamp column to any Java Date formats specified in DateTimeFormatter.
"""


def exp3(df: DataFrame, spark: SparkSession):
    # try to remove the format of timestamp_2 or 3, and see what happens
    df1 = df.withColumn("timestamp_1", to_timestamp("timestamp_1")) \
        .withColumn("timestamp_2", to_timestamp("timestamp_2", "MM-dd-yyyy HH:mm:ss.SSS")) \
        .withColumn("timestamp_3", to_timestamp("timestamp_3", "MM-dd-yyyy"))
    # note the column type is not string or long. Its a spark timestamp type
    print("Exp3 Convert string to spark time stamp")
    df1.printSchema()
    df1.show(truncate=False)

    df2 = df1.select(col("timestamp_3").cast("string"))
    print("Exp3 convert spark time stamp back to string")
    df2.printSchema()
    df2.show()

    df3 = df1.withColumn("str_date_yyyy_MM_dd", date_format(col("timestamp_2"), "yyyy MM dd")) \
        .withColumn("str_date_MM/dd/yyyy_hh:mm", date_format(col("timestamp_2"), "MM/dd/yyyy hh:mm")) \
        .withColumn("str_date_yyyy_MMM_dd", date_format(col("timestamp_2"), "yyyy MMM dd")) \
        .withColumn("str_date_yyyy_MMMM_dd_E", date_format(col("timestamp_2"), "yyyy MMMM dd E"))
    print("Exp3 use date_format() to convert spark timestamp to various string date")
    df3.printSchema()
    df3.show(truncate=False)

    print("Exp3 use to_timestamp in sql")
    spark.sql("select to_timestamp('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as timestamp")


def exp4(spark: SparkSession):
    df = spark.createDataFrame([["02-03-2013"], ["05-06-2023"]], ["input"])
    df1 = df.withColumn("date", to_date("input", "MM-dd-yyyy"))
    print("Exp4 use to_date() function to convert string to date type")
    df1.printSchema()
    df1.show()


def main():
    spark = SparkSession.builder.master("local[2]").appName("TimeDateExp").getOrCreate()
    data = [("2019-07-01 12:01:19", "07-01-2019 12:01:19.888", "07-01-2019"),
            ("2018-07-01 12:01:19", "07-01-2018 12:01:19.666", "07-01-2018"),
            ("2017-07-01 12:01:19", "07-01-2017 12:01:19.111", "07-01-2017")]
    columns = ["timestamp_1", "timestamp_2", "timestamp_3"]
    df = spark.createDataFrame(data=data, schema=columns)
    print("Source data: ")
    df.printSchema()
    df.show(truncate=False)

    # run exp1
    # df_seconds = exp1(df)

    # run exp2
    # exp2(df_seconds)

    # run exp3
    # exp3(df, spark)

    # run exp4
    exp4(spark)


if __name__ == "__main__":
    main()
