""" UDF is called user define function
UDF is very useful when you want to transform your data frame, and there is no pre-defined
Spark sql functions already available.

To define a spark udf, you have three options:
1. use pyspark.sql.functions.udf, this works for select, withColumn.
   udf(lambda_function, return_type). The default return_type is String. If you omit
   return_type, the value returned by lambda function will be convert it to String.
2. use @udf(returnType=<>) annotation, this works for select, withColumn.
3. use spark.udf.register, this works for sql.

But, remember two important things about UDF
- UDF is not optimized at all. So you can quickly come across performance issues.
- UDF need to treat null value explicitly.

"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType


def name_count(name: str) -> int:
    return len(name)


# The pyspark.sql.functions.udf function takes a python function, and it returns
# org.apache.spark.sql.expressions.UserDefinedFunction class object. In our case
# it's Name_Count_UDF. And this object can used inside select or withColumn.
Name_Count_UDF = udf(lambda x: name_count(x), IntegerType())
Null_Safe_Name_Count_UDF = udf(lambda x: name_count(x) if not (x is None) else None, IntegerType())


# We can also use @udf to define a spark udf.
@udf(returnType=StringType())
def add_hello(name: str) -> str:
    return "{} {}".format("hello", name)


""" Exp1,
In this example, we show how to use udf inside a select
"""


def exp1(df: DataFrame):
    df1 = df.select("name", Name_Count_UDF("name").alias("length"), add_hello("name").alias("msg"))
    print("Exp1 udf in select")
    df1.printSchema()
    df1.show()


""" Exp2,
In this example, we show how to use udf inside a withColumn
"""


def exp2(df: DataFrame):
    df1 = df.withColumn("length", Name_Count_UDF("name")).withColumn("msg", add_hello("name"))
    print("Exp2 udf in withColumn")
    df1.printSchema()
    df1.show()


""" Exp3
In this example, we show how to register and use udf inside sql
"""


def exp3(spark: SparkSession, df: DataFrame):
    # register the function for sql
    spark.udf.register("Count_Name_UDF", name_count, IntegerType())
    df.createOrReplaceTempView("name_table")
    df1 = spark.sql("select name, Count_Name_UDF(name) as length, from name_table")
    print("Exp3 udf in sql statement: ")
    df1.show()


def exp4(spark: SparkSession):
    data1 = [("haha ",),
             ("toto",),
             ("titi",),
             (None,)]
    df1 = spark.createDataFrame(data1, schema=['name'])
    print("Source data frame: ")
    df1.printSchema()
    df1.show()
    # try to replace Null_Safe_Name_Count_UDF by Name_Count_UDF, and see what happens
    #
    try:
        df1.select("name", Null_Safe_Name_Count_UDF("name")).show()
    except exeception as e:
        print("udf failed")

def exp5()


def main():
    spark = SparkSession.builder.master("local[2]").appName("UdfExample").getOrCreate()
    data = [("haha ",),
            ("toto",),
            ("titi",)]
    df = spark.createDataFrame(data, schema=['name'])
    print("Source data frame: ")
    df.printSchema()
    df.show()

    # exp1
    # exp1(df)

    # exp2
    # exp2(df)

    # exp3
    # exp3(spark, df)

    # exp4
    exp4(spark)


if __name__ == "__main__":
    main()
