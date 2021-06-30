from pyspark.sql import SparkSession, DataFrame
from pyspark import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, MapType, LongType

""" Exp1 Select

When you working on a tabular data set, the most common function that you will use is select. It allows you select 
single, multiple column and the struct filed of nested columns from a DataFrame.

In spark, select is an transformation which will not trigger computation, you need to add an action after select.
It returns an new dataframe based on the selected columns.
"""


def exp1(df: DataFrame):
    # select all columns,below shows 3 ways, they all output the same result
    print("Exp1: print all columns of origin data frame")
    df.select([cols for cols in df.columns]).show()
    # df.select("*").show()
    # df.select(*df.columns).show()

    # use 3 different ways to quote column names
    print("Exp1: use 3 different ways to quote column names")
    df.select(col("name"), df.department, df["properties"]).show()

    # use regular expression to quote column names
    print("Exp1: use regular expression to quote column names")
    df.select(df.colRegex("`^.*name*`")).show()

    # We can also get columns by their index, note df.columns returns a list of all column names
    print("Exp1: Get column by their index")
    # Get the first 2 column
    df.select(df.columns[:2]).show()
    # Get the 2nd and 3rd column
    df.select(df.columns[1:3]).show()

    # select field of the struct. For details, please visit ColumnClassExp
    df.select(df.name.fname.alias("first_name"), df.properties.getItem("eye").alias("eye_color"),
              df.score.getItem(0).alias("first_score")).show()


"""Exp2: Collect
Collect is an Action that is used to retrieve all the elements of the dataset (from all nodes) to the driver node. 
We should use the collect() on smaller dataset usually after filter(), group() e.t.c. Retrieving larger datasets 
results in OutOfMemory error.

Collect(): returns an array of Row type

Use Collect with caution, you may kill your driver. 
"""


def exp2(df: DataFrame):
    data = df.collect()
    print("Exp2: the data after collect")
    print("It has format: {}".format(str(type(data))))
    # Because it's a list of rows, so we can use list loop to iterate it
    for row in data:
        # for each row, we can access it's element by using following method
        # for more details, please check the RowClass
        print("first_name: {}, department: {}, eye: {}, score:{}".format(row.name.fname, row.department,
                                                                         row.properties.get("eye"), row.score[0]))


def main():
    spark = SparkSession.builder.master("local[2]").appName("GetResultWithCollectAndSelect").getOrCreate()
    Person = Row("name", "department", "properties", "score")
    data = [Person(Row(fname="James", lname="Smith"), "Finance", {'hair': 'black', 'eye': 'black'}, [10, 20, 30]),
            Person(Row(fname="Michael", lname="Rose"), "Marketing", {'hair': 'bleu', 'eye': 'yellow'}, [20, 30, 40]),
            Person(Row(fname="Robert", lname="Williams"), "Sales", {'hair': 'black', 'eye': 'bleu'}, [10, 30, 40]),
            Person(Row(fname="Maria", lname="Jones"), "IT", {'hair': 'black', 'eye': 'black'}, [10, 30, 40])
            ]
    # The above syntax allows spark to infer the schema of the data frame, but with a specific schema we can override
    # the infer schema
    schema = StructType([
        StructField('name', StructType([
            StructField('fname', StringType(), True),
            StructField('lname', StringType(), True)
        ])),
        StructField('department', StringType(), True),
        StructField('properties', MapType(StringType(), StringType()), True),
        StructField('score', ArrayType(LongType()), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.printSchema()
    print("Source data frame: ")
    df.show()

    # run exp1
    # exp1(df)

    # run exp2
    exp2(df)


if __name__ == "__main__":
    main()
