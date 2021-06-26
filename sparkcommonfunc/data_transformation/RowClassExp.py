from pyspark import Row
from pyspark.sql import SparkSession

"""
Row class introduction:

Row class extends the tuple hence it takes variable number of arguments, Row() is used to create the row object. 
Once the row object created, we can retrieve the data from Row using index similar to tuple.

Key Points of Row Class:

- Earlier to Spark 3.0, when used Row class with named arguments, the fields are sorted by name.
- Since 3.0, Rows created from named arguments are not sorted alphabetically instead they will be ordered in 
  the position entered.
- To enable sorting by names, set the environment variable PYSPARK_ROW_FIELD_SORTING_ENABLED to true.
- Row class provides a way to create a struct-type column as well.

Note that, here we called the elements in a row "field", not column. Because column only make sense in a dataframe.
"""

"""Exp1 Row Object creation
Row object can have primitive field, array field, map field and struct field
"""


def exp1():
    # We can create row object without giving a field name, and accessing field by using index
    print("Exp1 output simple row object without name: ")
    row1 = Row("Alice", 18)
    print("name:{},age:{}".format(row1[0], str(row1[1])))

    # We can also specify field name when create a row object, then we can access it by using its name
    print("Exp1 output row object with field name: ")
    row2 = Row(name="Bob", age=38)
    print("name:{},age:{}".format(row2.name, str(row2.age)))

    # Row object can have primitive field, array field, map field and struct field
    # To access struct field, use ".", to access array field use [index], to access map field use .get(key)
    row3 = Row(name=Row(fname="Alice", lname="Liu"), score=[10, 20, 40], properties={"hair": "black", "eye": "bleu"})
    print("first_name:{}, last_name:{}, 1st_score:{}, eye:{}".format(row3.name.fname, row3.name.lname, row3.score[0],
                                                                     row3.properties.get("eye")))


""" Exp2 : Create custom class from Row
We can create a custom class by using Row(*fieldName)

"""


def exp2():
    #
    Student = Row("name", "age")
    s1 = Student("alice", 18)
    s2 = Student("Bob", 38)
    print("Student1: name={},age={}".format(s1.name, str(s1.age)))
    print("Student2: name={},age={}".format(s2.name, str(s2.age)))


""" Exp3: Create RDD by using row
We can create an RDD by using a list of Rows. rdd.collect() will return a list of row.
"""


def exp3(spark):
    # data is a list of rows
    data = [Row(name="James,,Smith", lang=["Java", "Scala", "C++"], state="CA"),
            Row(name="Michael,Rose,", lang=["Spark", "Java", "C++"], state="NJ"),
            Row(name="Robert,,Williams", lang=["CSharp", "VB"], state="NV")]
    # parallelize turn the list to rdd
    rdd = spark.sparkContext.parallelize(data)
    print("Exp3 rdd has type:{}".format(str(type(rdd))))
    # collect turn the rdd back to list
    rowList = rdd.collect()
    print("Exp3 row has type:{}".format(str(type(rowList))))
    print("Exp3 row has value:")
    for row in rowList:
        print("name: {}, lang: {}, state: {}".format(row.name, str(row.lang), row.state))


""" Exp4 : Create a dataframe by using row
"""


def exp4(spark):
    # we use custom class to create a list of Student(row)
    # the advantage of custom class is that we don't need to repeat filed name in each row.
    Student = Row("name", "score", "properties")
    data = [Student(Row(fname="James", lname="Smith"), [10, 20, 30], {'hair': 'black', 'eye': 'brown'}),
            Student(Row(fname="Michael", lname="Rose"), [20, 30, 40], {'hair': 'brown', 'eye': 'black'}),
            Student(Row(fname="Robert", lname="Williams"), [30, 20, 50], {'hair': 'black', 'eye': 'blue'})]
    df = spark.createDataFrame(data)
    df.printSchema()
    df.show(truncate=False)
    # we can access these field
    df.select(df.name.fname.alias("first_name"), df.name.lname.alias("last_name"), df.score.getItem(0).alias("score_0"),
              df.properties.getItem("hair").alias("hair")).show()


def main():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("Row class example") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    # run exp1
    # exp1()

    # run exp2
    # exp2()

    # run exp3
    # exp3(spark)

    # run exp4
    exp4(spark)


if __name__ == "__main__":
    main()
