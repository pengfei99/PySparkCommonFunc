from pyspark.sql import SparkSession, DataFrame

"""Random split is very useful when you need to split your data frame into train, validation, test
randomSplit(weightslist, seed)
- weightslist: list of doubles as weights with which to split the DataFrame. Weights will be normalized 
               if they don’t sum up to 1.0.

- seedint, optional. The seed for sampling
"""


def exp1(df: DataFrame):
    seed = 38
    train, val, test = df.randomSplit([0.7, 0.2, 0.1], seed)
    print("train data frame has {} rows".format(train.count()))
    print("val data frame has {} rows".format(val.count()))
    print("test data frame has {} rows".format(test.count()))


def main():
    spark = SparkSession.builder.master("local[2]").appName("RandomSplit").getOrCreate()
    df = spark.range(0, 10000)
    print("source dataframe has {} rows".format(df.count()))
    df.printSchema()
    df.show(5)

    # run exp1
    exp1(df)


if __name__ == "__main__":
    main()
