from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.master("local[2]").appName("change_seat").getOrCreate()
    path = "data/seat.csv"
    df_seat = spark.read.option("header", "true").csv(path)
    df_seat.show()
    df_seat.printSchema()


if __name__ == "__main__":
    main()
