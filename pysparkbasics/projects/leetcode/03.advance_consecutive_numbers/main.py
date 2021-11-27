from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, dense_rank
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder.master("local[2]").appName("advance_consecutive_numbers").getOrCreate()
    path = "data/stadium.csv"
    df = spark.read.option("header", "true").csv(path)
    df.show()
    df.printSchema()


if __name__ == "__main__":
    main()
