from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col


def main():
    spark = SparkSession.builder.master("local[2]").appName("change_seat").getOrCreate()
    path = "data/seat.csv"
    df_seat = spark.read.option("header", "true").csv(path)
    df_seat.show()
    df_seat.printSchema()
    # The best solution is directly change the id
    df1 = df_seat.withColumn("new_id", when((((col("id")) % 2) == 1) & (col("id") == df_seat.count()), col("id"))
                             .when(((col("id")) % 2) == 0, col("id") - 1)
                             .otherwise(col("id") + 1)
                             ) \
        .drop("id").withColumnRenamed("new_id", "id")
    df2 = df1.selectExpr("cast(id as int) id", "student").orderBy("id")
    df2.show()


if __name__ == "__main__":
    main()
