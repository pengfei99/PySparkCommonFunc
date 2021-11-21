from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col


def main():
    spark = SparkSession.builder.master("local[2]").appName("get_consecutive_numbers").getOrCreate()
    path = "data/logs.csv"
    df = spark.read.option("header", "true").csv(path)
    df.show()
    df.printSchema()
    # The solution use inner self join. note use alias to rename the df to avoid conflict in self join
    df_res = df.alias("df1").join(df.alias("df2"),
                                  [(col("df1.num") == col("df2.num")) & (col("df1.id") == col("df2.id") - 1)],
                                  "inner") \
        .join(df.alias("df3"), [(col("df1.num") == col("df3.num")) & (col("df1.id") == col("df3.id") - 2)], "inner")
    # note even the column name printed on the data frame only shows id, num. But the spark backend knows
    # the difference between df1.id and df2.id
    df_final = df_res.select("df1.num").withColumnRenamed("num", "ConsecutiveNums")
    df_final.show()


if __name__ == "__main__":
    main()
