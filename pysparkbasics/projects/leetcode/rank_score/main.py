from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, dense_rank
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder.master("local[2]").appName("change_seat").getOrCreate()
    path = "data/scores.csv"
    df = spark.read.option("header", "true").csv(path)
    df.show()
    df.printSchema()
    # The best solution is to use window function
    # build window spec
    win_spec=Window.orderBy(col("score").desc())
    df1 = df.withColumn("rank", dense_rank().over(win_spec)).drop("id")
    df1.show()


if __name__ == "__main__":
    main()
