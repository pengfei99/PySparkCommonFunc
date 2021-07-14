from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

"""
When you train a model, you often needs to split your data set into 3 sub data set:
- train
- validation
- test

We have three methods that can split a dataframe:
- randomSplit(weights, seed): It takes two arguments 
          -- weights: a list of doubles as weights with which to split the DataFrame. Weights will be normalized if 
                      they don’t sum up to 1.0.
          -- seed: int, optional. If you give a fix value, spark should produce the same split result every time.

- sample(withReplacement, fraction, seed=None): 
          -- withReplacement: bool, optional. Sample with replacement or not (default False). If you want duplicates 
                              rows in a split, set it to true 
          -- fraction: float, optional. Fraction of rows to generate, range [0.0, 1.0].
          -- seed: int, optional. Seed for sampling (default a random seed).

- sampleBy(col, fractions, seed=None): sample rows by filtering specific values of a row. Check exp2()
          -- col: Column 
          -- fractions: dict. sampling fraction for each stratum. If a stratum is not specified, we treat its 
                        fraction as zero.
          -- seed: int, optional. random seed
          
For rdd, we have two methods:
- sample(withReplacement, fraction, seed=None): The sample syntaxe and argument as for dataframe
- takeSample(self, withReplacement, number, seed=None): Unlike other methods are transformation, this takeSample() is 
           an action. Hence you may have out-of-memory error in your driver, if you return too much data results.
            

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


# sample example
def exp1(df: DataFrame):
    seed = 888
    df1 = df.sample(0.1, seed)
    print("Exp1: 10% sample has {} rows".format(df1.count()))


# sampleBy example
def exp2(df: DataFrame):
    print("exp2: show transformed data frame")
    # we transform the value of id to 0,1,2
    dataset = df.select((col("id") % 3).alias("key"))
    dataset.show(10)
    print("exp2: show the count of the transformed data frame")
    dataset.groupBy("key").count().orderBy("key").show()
    # we use column key to filter rows, if the value is 0, we take all. if the value is 1, we take 30%, if the value
    # is 2, we take 20 percent
    sampled = dataset.sampleBy("key", fractions={0: 1, 2: 0.2, 1: 0.3}, seed=0)
    print("exp2: show the sampleBy result data frame")
    sampled.show(10)
    print("exp2: show the count of the result data frame")
    sampled.groupBy("key").count().orderBy("key").show()


# random split example
def exp3(df: DataFrame):
    seed = 666
    train, validation, test = df.randomSplit([0.7, 0.2, 0.1], seed)
    train_num = train.count()
    val_num = validation.count()
    test_num = test.count()
    print("Exp3: train sample has {} rows".format(train_num))
    print("Exp3: validation sample has {} rows".format(val_num))
    print("Exp3: test sample has {} rows".format(test_num))
    print("Exp3: Sum of sample rows: {}".format(test_num + val_num + train_num))


# rdd sample and takeSample example
def exp4(spark: SparkSession):
    rdd = spark.sparkContext.range(0, 100)
    print("Exp4 get sample without duplicates by using sample()")
    print(rdd.sample(False, 0.1, 0).collect())
    print("Exp4 get sample with duplicates by using sample()")
    print(rdd.sample(True, 0.3, 0).collect())

    # takeSample does not need collect to start the calculation
    # note we dont use fraction, but directly the sample number that we want
    print("Exp4 get sample without duplicates by using takeSample()")
    print(rdd.takeSample(False, 10, 0))
    print("Exp4 get sample with duplicates by using takeSample()")
    print(rdd.takeSample(True, 30, 0))


def main():
    spark = SparkSession.builder.master("local[2]").appName("SplitDataToMultipleGroups").getOrCreate()
    df: DataFrame = spark.range(0, 100)
    print("Source data frame has {} rows".format(df.count()))
    df.printSchema()
    df.show(5)

    # run exp1
    # exp1(df)

    # run exp2
    # exp2(df)

    # run exp3
    # exp3(df)

    # run exp4
    exp4(spark)


if __name__ == "__main__":
    main()
