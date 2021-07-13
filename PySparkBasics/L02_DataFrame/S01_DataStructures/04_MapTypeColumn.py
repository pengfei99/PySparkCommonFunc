from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, MapType, StringType
from pyspark.sql.functions import explode, map_keys, map_values

""" MapType Column
PySpark MapType is used to represent map key-value pair similar to python Dictionary (Dict), it extends DataType class 
which is a superclass of all types in PySpark 
MapType:
- keyType mandatory DataType argument
- valueType mandatory DataType argument
- valueContainsNull: optional boolean argument. 

keyType and valueType can be any type that extends the DataType class. for e.g StringType, IntegerType, ArrayType, 
MapType, StructType (struct) e.t.c. 

Key can not have null value, Value can have null value if valueContainsNUll is set to True (Default value)

1. Create MapType column in a data frame check main()
2. Access MapType column value. Exp1
3. explode MapType column Exp2
4. Get all keys list or values list of the map Exp3
"""


def exp1(df: DataFrame):
    # we can access map type column value by using getItem(key), key is the key of map key-value pair.
    print("Exp1 access map type column value by using getItem(key)")
    df.select("name", df.properties.getItem("eye").alias("eye"), df.properties.getItem("hair").alias("hair")).show()


"""Exp2 Explode map type column
- explode(column): It takes a map type column and generate two columns, the default column name is key, value.
"""


def exp2(df: DataFrame):
    # we can not use explode(MapType) in withColumn. Because, withColumn creates one column, explode(MapType) will
    # create two
    print("Exp2 explode map type with default generated column name")
    df.select("name", explode(df.properties)).show()

    print("Exp2 explode map type with specific column name")
    df.select("name", explode(df.properties).alias("property_key", "property_value")).show()

    print("Exp2 explode map type with specific column name")
    df.select(explode(df.properties).alias("property_key", "property_value")).show()


""" Exp3 Get map keys, and map values
- map_keys(column): It takes a map column, and returns a list of keys of the map
- map_values(column): It takes a map column, and returns a list of values of the map 

The type of column must be map. otherwise the function will failed with type mismatch error
"""


def exp3(df: DataFrame):
    # map_keys on string column will fail
    try:
        df.select(map_keys(df.name)).show()
    except Exception as e:
        print("failed, error message: {}".format(e))

    print("Exp3 Get key and value list of map type column")
    df.withColumn("property_keys", map_keys(df.properties)) \
        .withColumn("property_values", map_values(df.properties)) \
        .show()


def main():
    spark = SparkSession.builder.master("local[2]").appName("ArrayTypeColumn").getOrCreate()
    schema = StructType([
        StructField('name', StringType(), True),
        StructField('properties', MapType(StringType(), StringType()), True)
    ])
    # note the '' is not null, its an empty string.
    # We can notice that by default map allows null value, which means valueContainsNull is set to true by default
    data = [
        ('James', {'hair': 'black', 'eye': 'brown'}),
        ('Michael', {'hair': 'brown', 'eye': None}),
        ('Robert', {'hair': 'red', 'eye': 'black'}),
        ('Washington', {'hair': 'grey', 'eye': 'grey'}),
        ('Jefferson', {'hair': 'brown', 'eye': ''})
    ]
    df = spark.createDataFrame(data=data, schema=schema)
    print("Source data frame")
    df.printSchema()
    df.show(truncate=False)

    # run exp1
    # exp1(df)

    # run exp2
    # exp2(df)

    # run exp3
    exp3(df)


if __name__ == "__main__":
    main()
