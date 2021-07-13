from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType
from pyspark.sql.functions import explode, split, array, array_contains

""" Array Type Column
pyspark.sql.types.ArrayType (ArrayType extends DataType class) is used to define an array data type column on 
DataFrame that holds the same type of elements. In this tutorial, we will learn the following points

1. Create ArrayType column in a data frame check main()
2. Access ArrayType column value. Exp1
3. Split a string column to ArrayType column (list of tokens) Exp2
4. Explode an arrayType column to multiple rows Exp3
5. Merging multiple column into an array column Exp4
6. Check if an array column contains a specific value Exp6

"""


def exp1(df: DataFrame):
    # to access a value an array column, we can use getItem() and the index of the value in the array
    print("Exp1 access values of an array column")
    df.select(df.languages.getItem(0), df.score.getItem(0)).show()


def exp2(df: DataFrame):
    print("Exp2 split names into array of tokens")
    # , is the separator
    df.select(split(df.name, ",")).show()


def exp3(df: DataFrame):
    # explode one column
    print("Exp3 explode single array column to primitive column")
    df.select("name", explode(df.languages)).show()

    print("Exp3 explode two array columns to primitive column")
    # note we can't do two explode in one select. But we can use withColumn to explode multiple columns.
    # df.select("name", explode(df.languages), explode(df.score)).show()
    df.withColumn("lang_explode", explode(df.languages)).withColumn("score_explode", explode(df.score)).show()


""" Exp4
array(*column): It takes multiple column as argument, and put each column value in an array.

"""


def exp4(df: DataFrame):
    print("Exp4 add two columns value into an array column")
    df.withColumn("stats", array(df.currentState, df.previousState)).show()
    print("Exp4 add three columns value into an array column")
    df.withColumn("three", array(df.name, df.currentState, df.previousState)).show(truncate=False)


""" Exp5
array_contains(col,val): It takes a column and a value as argument, and checks if an array column contains a specific 
                         value. If yes, it returns true, otherwise it returns false
"""


def exp5(df: DataFrame):
    print("Exp5 use array_contains() to check if an array column contains a specific value")
    df.withColumn("has_java", array_contains(df.languages, "Java")).show(truncate=False)


def main():
    spark = SparkSession.builder.master("local[2]").appName("ArrayTypeColumn").getOrCreate()
    # define a schema with various array type, we must specify the data type that an array contains
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("languages", ArrayType(StringType()), True),
        StructField("score", ArrayType(IntegerType()), True),
        StructField("currentState", StringType(), True),
        StructField("previousState", StringType(), True)
    ])

    data = [
        ("James,,Smith", ["Java", "Scala", "C++", "Spark"], [10, 20, 30], "OH", "CA"),
        ("Michael,Rose,", ["Spark", "Java", "C++"], [20, 30, 40], "NY", "NJ"),
        ("Robert,,Williams", ["CSharp", "VB", "Spark", "Python"], [21, 22, 23], "UT", "NV")
    ]
    df = spark.createDataFrame(data, schema=schema)
    print("Source data frame: ")
    df.printSchema()
    df.show()

    # run exp1
    # exp1(df)

    # run exp2
    # exp2(df)

    # run exp3
    # exp3(df)

    # run exp4
    exp4(df)

    # run exp5
    # exp5(df)


if __name__ == "__main__":
    main()
