from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
from pyspark.sql.functions import lit, col, when, concat, udf

"""
Null value is very common in data science. All your code should gracefully handle these null values. Three main strategy
to handle null values:
1. keep null values in data frame, all functions which worked with data frame need to handle the null value gracefully
2. Remove all null values
3. Use Imputation process to fill null values with non null values.

Exp1 shows how to handle null in data frame creation

Exp2 shows how to test if a column/row has null value, and the default strategy of null value handling in spark built 
     in function
     
Exp3 shows how to do equality test with null values

Exp4 shows how to define udf with null value handling logic      

Exp5. use df.na.drop() and df.dropna() to remove all null values

Exp6. use fill() and fillna() to impute null values
"""

"""Exp1: Null value in data frame creation
When you read source data from file, or you create dataframe from list. You can use schema to control how you want to
handle the null values.

In the schema definition, each column in a DataFrame has a nullable property that can be set to True or False.
If nullable is set to False then the column cannot contain null values. 

In the following example, the column id and zip_code is set to nullable=False. If your data contains null in these two
column, you can't create a dataframe with these data. 

"""


def exp1(spark: SparkSession, schema: StructType):
    test_data1 = [(1, None, "STANDARD", None, "PR", 30100)]
    test_data2 = [(None, 705, "STANDARD", None, "PR", 30100)]
    print("Exp1 create null column with non nullable schema")
    try:
        spark.createDataFrame(test_data1, schema=schema)
    except Exception as e:
        print("Create data frame failed. error msg: {}".format(e))
    try:
        spark.createDataFrame(test_data2, schema=schema)
    except Exception as e:
        print("Create data frame failed. error msg: {}".format(e))


""" Exp2 
The built in spark functions handle null value gracefully, so we dont need to worry.
- isNull(): called by a column, return true if value is null

This function can be used in filter to remove rows with null values

The default behavior of spark built in function to handle null value is that if either, or both, of the operands 
column are null, then function returns null
"""


def exp2(df: DataFrame):
    # isNull check a column value is null or not, if yes, return true, otherwise return false
    print("Exp2 check a column value is null or not")
    df.withColumn("type_is_null", df.type.isNull()).show()

    # if either, or both, of the operands column are null, then function returns null.
    print("Exp2 concat two columns city and type which may contain null values")
    df.withColumn("city_and_type", concat(df.city, df.type)).show()

    print("Exp2 overwrite the default null handling behaviour")
    df.withColumn("city_and_type",
                  when(df.city.isNull() & ~df.type.isNull(), concat(lit(" "), df.type))
                  .when(~df.city.isNull() & df.type.isNull(), concat(df.city, lit(" ")))
                  .when(df.city.isNull() & df.type.isNull(), None)
                  .otherwise(concat(df.city, df.type))).show(truncate=False)


"""Exp3: Equality check with null values
You can noticed in below example, if either, or both, of the operands are null, then == returns null, not a boolean.

In some case, you want the == behave like this :
- When one value is null and the other is not null, return False
- When both values are null, return True

eqNullSafe(col_name): It's called by a column, takes a column as argument and produce a bool value by using above rules 
"""


def exp3(df: DataFrame, spark):
    # equality with null value
    print("Exp3 equality check with null values")
    df.withColumn("type_test", lit("STANDARD")).withColumn("type_equality_test", df.type == col("type_test")).show()

    # null safe equality
    print("Exp3 null safe equality check")
    df1 = spark.createDataFrame([(1, None), (2, 2), (1, 2), (None, None)], ["num1", "num2"])
    df1.show()
    # we can write the logic by ourself
    df2 = df1.withColumn("num1_eq_num2",
                         when(df1.num1.isNull() & df1.num2.isNull(), True).when(df1.num1.isNull() | df1.num2.isNull(),
                                                                                False).otherwise(df1.num1 == df1.num2))
    print("Exp3 hand write null safe equality returns: ")
    df2.show()

    # spark provide a built in null safe equality function
    df3 = df1.withColumn("num1_eqNullSafe_num2", df1.num1.eqNullSafe(df1.num2))
    print("Exp3 spark built in null safe equality returns: ")
    df3.show()


"""Exp4 Udf null value handling
hi_city_bad udf does not handles null value, so we will get exception when null values occurs

hi_city udf handles null values with spark default strategy. Thus its null safe
"""


@udf(returnType=StringType())
def hi_city_bad(city_name: str) -> str:
    return "hi " + city_name


@udf(returnType=StringType())
def hi_city(city_name: str) -> str:
    return None if city_name is None else "hi " + city_name


def exp4(df: DataFrame):
    try:
        df.withColumn("hi_city", hi_city_bad(df.city)).show()
    except Exception as e:
        print("Exp4 udf failed due to null value, error msg: {}".format(e))

    print("Exp4 udf success by handling null values")
    df.withColumn("hi_city", hi_city(df.city)).show()


"""Exp5: Remove null values
- drop(how='any', thresh=None, subset=None): function is used to remove/drop rows with NULL values in DataFrame columns
                   It has three 3 optional parameters:
                   -- how: This takes values ‘any’ or ‘all’. By using ‘any’, drop a row if it contains NULLs on 
                           any columns. By using ‘all’, drop a row only if all columns have NULL values. Default is ‘any’.
                   -- thresh: This takes int value, Drop rows that have less than thresh hold non-null values. 
                              Default is ‘None’.
                   -- subset: Use this to select the columns for NULL values. Default is ‘None.
- df.dropna() : is equivalent to df.na.drop()
"""


def exp5(df: DataFrame):
    print("Exp5 drop() without arguments remove all rows that have null values")
    df.na.drop().show(truncate=False)

    print("Exp5 drop() with how=all arguments remove all rows that have null values on all columns")
    df.na.drop(how="all").show(truncate=False)

    print("Exp5 drop() with subset arguments remove all rows that have null values on selected columns")
    df.na.drop(subset=["population", "type"]) \
        .show(truncate=False)

    # note dropna() is equivalent to df.na.drop()
    print("Exp5 dropna() removes all rows that have null values")
    df.dropna().show(truncate=False)


"""Exp6 Imputation of null value
These two function are aliases of each other and returns the same results.
- fillna(value, subset=None) : it replaces NULL/None with a specific value, It has two arguments:
            -- value: Value should be the data type of int, long, float, string, or dict. Value specified here 
                            will replace the NULL/None values.
            -- subset: This is optional, when used it should be the subset of the column names where 
                       you want to replace NULL/None values.
- fill(value, subset=None)

"""


def exp6(df: DataFrame):
    # Replace null by 0 for all integer columns. So the type of value which we give must match the column type of
    # that contains null, otherwise null will not be replaced.
    print("Exp6 Impute all integer column null value with 0")
    df.na.fill(value=0).show()

    # notice, nothing happens to the city column, because wrong value type
    print("Exp6, with a subset column argument points to a string column")
    df.na.fill(value=0, subset=["city"]).show()

    print("Exp6 Impute all string column null value with empty string")
    df.na.fill(value=" ").show()

    print("Exp6, with a subset column argument points to a string column")
    df.na.fill(value="unknown", subset=["city"]).show()

    print("Exp6 fill multiple column with different types")
    # fill can also take a dict as value, the key is column name, the value is the value you want to fill with.
    df.na.fill(value={"city": "unknown", "population": 0}).show()
    # Below, we use two fill to archive the same result as above function
    df.na.fill("unknown", ["city"]).na.fill(0, ["population"]).show()


def main():
    spark = SparkSession.builder.master("local[2]").appName("DealWithNullValues").getOrCreate()
    data = [
        (1, 704, "STANDARD", None, "PR", 30100),
        (2, 704, None, "PASEO COSTA DEL SUR", "PR", None),
        (3, 709, None, "BDA SAN LUIS", "PR", 3700),
        (4, 76166, "UNIQUE", "CINGULAR WIRELESS", "TX", 84000),
        (5, 76177, "STANDARD", None, "TX", None)
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("zip_code", LongType(), False),
        StructField("type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("population", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)
    print("Source data frame: ")
    df.printSchema()
    df.show()

    # run exp1
    # exp1(spark, schema)

    # run exp2
    # exp2(df)

    # run exp3
    # exp3(df, spark)

    # run exp4
    # exp4(df)

    # run exp5
    # exp5(df)

    # run exp6
    exp6(df)


if __name__ == "__main__":
    main()
