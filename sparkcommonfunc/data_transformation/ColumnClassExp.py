from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, expr, when
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, MapType

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
    print("exp4 output: arithmetic operations on two numeric columns")
    df.select(df.x + df.y).show()
    df.select(df.x - df.y).show()
    df.select(df.x * df.y).show()
    df.select(df.x / df.y).show()
    df.select(df.x % df.y).show()

    # we can use eval function to return a bool value
    print("exp4 output: comparison operations on two numeric columns")
    df.select(df.y > df.z).show()
    df.select(df.y < df.z).show()
    df.select(df.y == df.z).show()


""" Exp5 Rename column 
To rename column, we have two functions alias and name
"""


def exp5(df):
    print("exp5 output: rename column with name function")
    df.select(df.fname.name("first_name")).show()
    print("exp6 output: rename column with alias function")
    df.select(expr("fname ||','|| lname").alias("full_name")).show()


""" Exp6 Sort column by descending and ascending order 
- asc(): Returns ascending order of the column.
- asc_nulls_first(): Returns null values first then non-null values.
- asc_nulls_last(): Returns null values after non-null values.
- desc(): Returns descending order of the column.
- desc_nulls_first(): null values appear before non-null values.
- desc_nulls_last(): null values appear after non-null values.
"""


def exp6(df):
    print("exp6 output: sort column by ascending order")
    df.select(df.fname.asc()).show()
    print("exp6 output: sort column by ascending order with null first")
    # df.select(df.id.asc_nulls_first()).show()
    print("exp6 output: sort column by ascending order with null last")
    # df.select(df.id.asc_nulls_last()).show()
    print("exp6 output: sort column by descending order")
    df.select(df.fname.desc()).show()


""" Exp 7 Convert the column's data Type by using cast() & astype()

"""


def exp7(df):
    print("exp7 output: convert type by using cast")
    df1 = df.select(df.fname, df.id.cast("int"))
    df1.printSchema()
    df1.show()

    print("exp7 output: convert type by using astype")
    df2 = df.select(df.fname, df.id.astype("int"))
    df2.printSchema()
    df2.show()


""" Exp 8 filter column with specific values
We have several functions which can evaluate column value with specific conditions and return a bool, as they return a
bool value, we can use them inside filter. 
- between(lowerBound, upperBound): Checks if the columns values are between lower and upper bound. Returns 
                                  boolean value.
- contains(arg): Check if String contains in another string. Returns boolean value.
- startswith(arg): Check if String starts with another string. Returns boolean value
- endswith(other): Check if String starts with another string. Returns boolean value
- isNotNull(): Returns True if the current expression is NOT null.
- isNull(): Returns True if the current expression is null.	
- isin(*cols):	A boolean expression that is evaluated to true if the value of this expression is contained by the 
                evaluated values of the arguments(a list of values).
"""


def exp8(df):
    # Between function
    print("exp8 output: check id between 100, 300")
    # note the id column has type string, not int.
    df.printSchema()
    df.withColumn("between", df.id.between(100, 300)).show()
    print("exp8 output: use between inside a filter")
    df.filter(df.id.between(100, 300)).show()

    # Contains function
    print("exp8 output: check name contains e")
    df.withColumn("has_e", df.fname.contains("e")).show()

    # starts/end with
    print("exp8 output: check name starts with Tom")
    df.withColumn("starts_with", df.fname.startswith("Tom")).show()
    print("exp8 output: check name ends with and")
    df.withColumn("starts_with", df.fname.endswith("and")).show()

    # check null/non null
    print("exp8 output: check id column contains null value or not")
    df.withColumn("null", df.id.isNull()).show()
    print("exp8 output: check id column contains not null value or not")
    df.withColumn("not_null", df.id.isNotNull()).show()

    # check a column value in a list
    print("exp8 output: check id column is in string list of 100, 200")
    li1 = ["100", "200"]
    df1 = df.withColumn("inList", df.id.isin(li1))
    print("exp8 output: check id column is in int list ")
    df.withColumn("inList", df.id.cast("int").isin([100, 300])).show()
    # Reverse the bool result in a filter, note the null value row is dropped too
    print("exp8 output: filter id column not in the list ")
    df.filter(~df.id.isin(li1)).show()


""" Exp9: Substring a string column
- substr(startPos, length): Return a Column which is a substring of the column. startPos starts with 0.
"""


def exp9(df):
    print("exp9 output: sub string example ")
    df.withColumn("substr", df.fname.substr(0, 3)).show()


""" Exp10: Fuzz search
- like(other): Similar to SQL like expression.
- rlike(other): Similar to SQL RLIKE expression (LIKE with Regex).
"""


def exp10(spark):
    data = [("James", "fun|stuff "),
            ("Ann", "dancing is fun"),
            ("Tom Cruise", "you have stuff"),
            ("Tom Brand", None)]

    columns = ["fname", "phrase"]
    df = spark.createDataFrame(data, columns)
    print("exp10 output:  like example ")
    df.withColumn("like", df.fname.like("%om%")).show()
    print("exp10 output:  rlike example ")
    df.withColumn("rlike", df.fname.rlike("Tom")).show()
    print("exp10 output:  rlike with or condition example ")
    # "|".join(keywords) is equivalent to keywords.mkString("|") in scala
    keywords = ["Tom", "James"]
    df.withColumn("rlike", df.fname.rlike("|".join(keywords))).show()

    # if you have special character like |%$* in target string. You need to protect them to avoid regular expression
    # translate them. For example, below will translate and we will see three rows returns true
    print("exp10 output:  rlike with protected special character ")
    df.withColumn("special", df.phrase.rlike("fun|stuff")).show()
    print("exp10 output:  rlike with protected special character ")
    # After protection, only one row returns true
    df.withColumn("special", df.phrase.rlike("fun\\|stuff")).show()


""" Exp11: Modify each row value by using When otherwise
- when(condition, value),otherwise(value): Similar to SQL CASE WHEN, Executes a list of conditions and returns 
                              one of multiple possible result expressions.

Following example, change the gender column value "M"->"Male", "F"->"Female", None->"", otherwise->origin_column_value
"""


def exp11(df):
    print("exp11 output:  when, otherwise example ")
    df.select(df.fname, df.lname, when(df.gender == "M", "Male")
              .when(df.gender == "F", "Female")
              .when(df.gender == None, "")
              .otherwise(df.gender).alias("new_gender")
              ).show()


""" Exp12: Get/set struct field value of a column 
We can use . to get field implicitly. Or we can use the following methode to get them explicitly
- getField(name): Returns a field by name in a StructField and by key in Map.
- getItem(key): Returns a values from Map/Key at the provided position.
- dropFields(*fieldNames)	Used to drops fields in StructType by name.
- withField(fieldName, col)	An expression that adds/replaces a field in StructType by name.
"""


def exp12(spark):
    data = [(("James", "Bond"), ["Java", "C#"], {'hair': 'black', 'eye': 'brown'}),
            (("Ann", "Varsa"), [".NET", "Python"], {'hair': 'brown', 'eye': 'black'}),
            (("Tom Cruise", ""), ["Python", "Scala"], {'hair': 'red', 'eye': 'grey'}),
            (("Tom Brand", None), ["Perl", "Ruby"], {'hair': 'black', 'eye': 'blue'})]

    schema = StructType([
        StructField('name', StructType([
            StructField('fname', StringType(), True),
            StructField('lname', StringType(), True)])),
        StructField('languages', ArrayType(StringType()), True),
        StructField('properties', MapType(StringType(), StringType()), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.printSchema()

    # Get item of array, map column
    # For the array column, getItem takes the index of item as argument
    # For the map column, getItem takes the key of item as argument
    print("exp12 output:  Get value of array, map type column by using getItem.")
    df.select(df.languages.getItem(1), df.properties.getItem("hair")).show()

    # Get field of struct column by using implicit method
    print("exp12 output:  get field value by using . ")
    df.select(df.name.fname.alias("first_name"), df.name.lname.alias("last_name")).show()
    print("exp12 output:  get field value by using getField ")
    df.select(col("name").getField("fname").alias("first_name"), df.name.getField("lname").alias("last_name")).show()

    # create a new column new_name by using dropFields to drop fname field,
    print("exp12 output:  drop field fname ")
    df.withColumn("new_name", df['name'].dropFields("fname")).show()
    # Create a new column complete_name by using withField to add a new field middle_name
    print("exp12 output:  add field value to column name")
    df.withColumn("complete_name", df['name'].withField("middle_name", df.properties.eye)).show(truncate=False)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("ColumnClassExample") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    # run exp1
    # exp1()

    # run exp2
    # exp2(spark)

    # run exp3
    # exp3(spark)

    # run exp4
    # exp4(spark)

    data = [("James", "Bond", "100", None),
            ("Ann", "Varsa", "200", 'F'),
            ("Tom Cruise", "XXX", "400", ''),
            ("Tom Brand", None, None, 'M')]

    columns = ["fname", "lname", "id", "gender"]
    df = spark.createDataFrame(data, columns)
    # run exp5
    # exp5(df)

    # run exp6, failed do not know why
    # exp6(df)

    # run exp7
    # exp7(df)

    # run exp8
    # exp8(df)

    # run exp9
    # exp9(df)

    # run exp10
    # exp10(spark)

    # run exp11
    # exp11(df)

    # run exp12
    exp12(spark)


if __name__ == "__main__":
    main()
