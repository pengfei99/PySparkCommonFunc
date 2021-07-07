from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, unix_timestamp, col, from_unixtime

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
    df2 = df.withColumn("timestamp_1", unix_timestamp("timestamp_1")) \
        .withColumn("timestamp_2", unix_timestamp("timestamp_2", "MM-dd-yyyy HH:mm:ss")) \
        .withColumn("timestamp_3", unix_timestamp("timestamp_3"))
    print("Exp1: covert date string to seconds with some erreurs")
    df2.printSchema()
    df2.show()

    df3 = df.withColumn("timestamp_1", unix_timestamp("timestamp_1")) \
        .withColumn("timestamp_2", unix_timestamp("timestamp_2", "MM-dd-yyyy HH:mm:ss")) \
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


def main():
    spark = SparkSession.builder.master("local[2]").appName("TimeDateExp").getOrCreate()
    data = [("2019-07-01 12:01:19", "07-01-2019 12:01:19", "07-01-2019"),
            ("2018-07-01 12:01:19", "07-01-2018 12:01:19", "07-01-2018"),
            ("2017-07-01 12:01:19", "07-01-2017 12:01:19", "07-01-2017")]
    columns = ["timestamp_1", "timestamp_2", "timestamp_3"]
    df = spark.createDataFrame(data=data, schema=columns)
    print("Source data: ")
    df.printSchema()
    df.show(truncate=False)

    # run exp1
    df_seconds = exp1(df)

    # run exp2
    exp2(df_seconds)


if __name__ == "__main__":
    main()
