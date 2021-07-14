from pyspark.sql import SparkSession, DataFrame

"""
When you train a model, you often needs to split your data set into 3 sub data set:
- train
- validation
- test

We have two methods that can do this job easily:
- randomSplit(weights, seed): It takes two arguments 
          -- weights: a list of doubles as weights with which to split the DataFrame. Weights will be normalized if 
                      they don’t sum up to 1.0.
          -- seed: int, optional. If you give a fix value, spark should produce the same split result every time.

- sample(withReplacement, fraction, seed=None): 
          -- withReplacement: bool, optional. Sample with replacement or not (default False).
          -- fraction: float, optional. Fraction of rows to generate, range [0.0, 1.0].
          -- seed: int, optional. Seed for sampling (default a random seed).

Important note: Even you set seed to a fix value in your randomSplit or sample, your split may not be consistent. 
                These inconsistencies may not happen on every run, but it will happen.
- randomSplit() is equivalent to applying sample() on your data frame multiple times. When randoSplit calls sample, each 
  sample re-fetching, partitioning, and sorting your data frame within partitions.
- The data distribution across partitions and sorting order will decide which rows goes to which split for both 
  randomSplit() and sample(). If either change upon data re-fetch, there may be duplicates or missing values across 
  splits. Thus, the same sample using the same seed may produce different results.
- To eliminate the inconsistency of split completely, you have three options to fix the partition and ordering
   -- persist (aka cache) your data frame in memory df = raw.persist(spark.StorageLevel.MEMORY_AND_DISK)
   -- repartition on a column(s). df = raw.repartition(100, 'col1')
   -- apply aggregate functions such as groupBy. df = raw.groupBy('col1').count()


Spark utilizes "Bernoulli sampling" to split rows, which can be summarized as 
1. Generating a random number for each row 
2. Accepting the row into a split if the generated number falls within a certain range, determined by the split ratio. 
For example a 0.8 ratio split of a data frame, the acceptance range for the row would be [0.0,0.80].
If the generated number is in this range, then accept, otherwise reject.
"""


def exp1(df: DataFrame):
    seed = 888
    df1 = df.sample(0.1, seed)
    print("Exp1: 10% sample has {} rows".format(df1.count()))


def exp2(df: DataFrame):
    seed = 666
    train, validation, test = df.randomSplit([0.7, 0.2, 0.1], seed)
    train_num = train.count()
    val_num = validation.count()
    test_num = test.count()
    print("Exp2: train sample has {} rows".format(train_num))
    print("Exp2: validation sample has {} rows".format(val_num))
    print("Exp2: test sample has {} rows".format(test_num))
    print("Exp2: Sum of sample rows: {}".format(test_num + val_num + train_num))


def main():
    spark = SparkSession.builder.master("local[2]").appName("SplitDataToMultipleGroups").getOrCreate()
    df: DataFrame = spark.range(0, 10000)
    print("Source data frame has {} rows".format(df.count()))
    df.printSchema()
    df.show(5)

    # run exp1
    exp1(df)

    # run exp2
    exp2(df)


if __name__ == "__main__":
    main()
