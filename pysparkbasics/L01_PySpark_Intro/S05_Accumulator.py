""" Accumulators
Accumulators are write-only and initialize once variables where only tasks that are running on workers are
allowed to update and updates from the workers get propagated automatically to the driver program. But, only the
driver program is allowed to access the Accumulator variable using the value property.

We can create Accumulators in PySpark for primitive types int and float. Users can also create Accumulators for
custom types using AccumulatorParam class of PySpark.

Three important steps:
1. Define accumulator variable a sparkContext.accumulator(x), x is the init value
2. add/update a value in accumulator by using add() function
3. Retrieve the value of the accumulator by using value property.
"""
from pyspark.sql import SparkSession

""" In exp1, we use an accumulator to calculate the sum of a int list

"""


def exp1(spark: SparkSession, data):
    # create an int accumulator and assign it with 0
    accu_sum = spark.sparkContext.accumulator(0)
    rdd = spark.sparkContext.parallelize(data)
    rdd.foreach(lambda x: accu_sum.add(x))
    print("Exp1: Sum of the items in the list: {}".format(accu_sum.value))


""" In exp2, we use an accumulator to count the element of a int list

"""


def exp2(spark: SparkSession, data):
    accu_count = spark.sparkContext.accumulator(0)
    rdd = spark.sparkContext.parallelize(data)
    rdd.foreach(lambda x: accu_count.add(1))
    print("Exp2: items in the list: {} ".format(accu_count.value))


def main():
    spark = SparkSession.builder.master("local[4]").appName("Accumulator").getOrCreate()
    data = [1, 2, 3, 4, 5]
    print("source list: {}".format(data))
    # run example 1
    exp1(spark, data)

    # run example 2
    exp2(spark, data)


if __name__ == "__main__":
    main()
