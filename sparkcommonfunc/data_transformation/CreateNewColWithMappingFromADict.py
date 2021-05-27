from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col, create_map, lit
from itertools import chain

""" Context:
Suppose that we have a dataframe which has a key column, and we have a dictionary which uses these keys to map some 
values. Now we want to create a new column which contains the value that matches the key on the same rows   

"""

"""Solution 1: Use a udf which"""


def translate(mapping):
    def translate_(col):
        return mapping.get(col)

    return udf(translate_, StringType())


def solution1(df, mapping):
    return df.withColumn("value", translate(mapping)("key"))


"""Solution 2: Use MapType literal instead of a udf"""


def solution2(df, mapping):
    for x in chain(*mapping.items()):
        print(x)
    mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])
    res = mapping_expr.getItem("k1")
    print(res)
    return df.withColumn("value", mapping_expr.getItem(col("key")))


"""Solution 3: In Spark >= 3.0 getItem should be replaced with __getitem__ ([])"""


def solution3(df, mapping):
    mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])
    return df.withColumn("value", mapping_expr[col("key")])


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("createNewColWithMapping") \
        .getOrCreate()
    data = [('k1',),
            ('k3',),
            ('INVALID',)]
    df = spark.createDataFrame(data, ["key"])
    df.show(5, False)
    mapping = {
        'k1': 'v1', 'k2': 'v2', 'k3': 'v3', 'k4': 'v4', 'k5': 'v5'}

    # run solution1
    res1 = solution1(df, mapping)
    res1.show(5, False)
    res1.explain(extended='formatted')

    # run solution2
    res2 = solution2(df, mapping)
    res2.show(5, False)
    res2.explain(extended='formatted')

    # run solution3
    res3 = solution3(df, mapping)
    res3.show(5, False)
    res3.explain(extended='formatted')


if __name__ == "__main__":
    main()
