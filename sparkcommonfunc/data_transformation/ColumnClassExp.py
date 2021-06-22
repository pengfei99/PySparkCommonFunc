from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

# from pyspark.sql import

"""Exp1: Create a column by using lit"""


def exp1():
    col_obj1 = lit("toto")
    print(col_obj1)
    print(str(type(col_obj1)))

    col_obj2 = (lit(i) for i in range(10))
    print(col_obj2)
    print(str(type(col_obj2)))


""" Exp2: Column name has . in it
When you have . in your column name, if you use the normal way to get the column, you will receive an error. Because
expression such as df.name.first_name, df["name.first_name"], col("name.first_name") will consider that "name" is the 
column name, this column is a struct type and first_name is a field of the struct.

To tell spark that name.first_name is the complete column name we need to use `name.first_name`. Check below example 
"""


def exp2(spark):
    data = [("James", 23), ("Ann", 40)]
    df = spark.createDataFrame(data).toDF("name.first_name", "age")
    df.printSchema()
    # notice the schema, the column name is name.first_name
    # Solution 1 Using DataFrame object (df)
    # try to run the following two line
    try:
        df.select(df.name.first_name).show()
    # catch all exception
    except:
        print("failed on df.name.first_name")

    try:
        df.select(df["name.first_name"]).show()
    except:
        print("failed on df[\"name.first_name\"]")
    # Accessing column name with dot (with backticks)
    df.select(df["`name.first_name`"]).show()

    # Solution 2 Using SQL col() function
    try:
        df.select(col("name.first_name")).show()
    except:
        print("failed on col(\"name.first_name\")")
    # Accessing column name with dot (with backticks)
    df.select(col("`name.first_name`")).show()


""" Exp3: Struct type column

We can access fields of a struct type column by using "." 
Check exp2 how to avoid this when column name contains "."
"""


def exp3(spark):
    # with row type, we don't need to declare schema anymore
    data = [Row(name="James", prop=Row(hair="black", eye="blue")),
            Row(name="Ann", prop=Row(hair="grey", eye="black"))]
    df = spark.createDataFrame(data)
    df.printSchema()
    # get a field of a struct type column
    print("exp3: Get a field hair of column prop")
    df.select("prop.hair").show()
    df.select(df["prop.hair"]).show()
    df.select(col("prop.hair")).show()

    print("exp3: Get all fields of column prop")
    # We can use * to get all fields
    df.select(col("prop.*"), "name").show()


"""Exp4 arithmetic operations on numeric columns"""


def exp4(spark):
    data = [(100, 2, 3), (200, 3, 4), (300, 4, 5)]
    df = spark.createDataFrame(data).toDF("x", "y", "z")
    # basic arithmetic operations
    df.select(df.x+df.y).show()
    df.select(df.x - df.y).show()
    df.select(df.x * df.y).show()
    df.select(df.x / df.y).show()
    df.select(df.x % df.y).show()

    # we can use eval function to return a bool value
    df.select(df.y > df.z).show()
    df.select(df.y < df.z).show()
    df.select(df.y == df.z).show()


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("ColumnClassExample") \
        .getOrCreate()

    # run exp1
    # exp1()

    # run exp2
    # exp2(spark)

    # run exp3
    # exp3(spark)

    # run exp4
    exp4(spark)


if __name__ == "__main__":
    main()
