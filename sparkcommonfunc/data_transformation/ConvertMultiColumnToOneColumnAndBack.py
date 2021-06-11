import itertools
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

""" Context:
Suppose we have a dataframe which contains multiple columns. and we want to convert these column into a single column
For example |A|B|C| -> |[A,B,C]| 
"""
"""
######### Scenario1. If you want to covert only two columns, you can just use map type in the converted column ######
"""


def scenario1(source_df):
    # In the following example, we convert two column key, value to one column called "target" which has map type
    # "key" column is the key of the map, "value" column is the value of the map
    target_df = source_df.withColumn("target", f.create_map("key", "value"))
    print("###################### Scenario_1: Convert two column into one ######################")
    target_df.show()
    print("###################### Note the target column has type map ######################")
    target_df.printSchema()

    # You can notice, we have many duplicate value in the name column,
    # we can do a groupby to group all value of one name.
    df_map = target_df.groupby("name").agg(f.collect_list("target").alias("target_list"))
    print("######################Group target column by name ######################")
    df_map.show(5, False)


"""
##### Scenario2. If you want to covert more than two columns, you need to use struct type in the converted column ######
"""


def scenario2(source_df):
    # we use pyspark.sql.functions.struct to build struct type in a column
    target_df = source_df.withColumn('target',
                                     f.struct(*[f.col('key').alias('t_key'), f.col("content").alias('t_content'),
                                                f.col("value").alias('t_value')]))
    print("###################### Scenario_2: Convert three column into one ######################")
    target_df.show()
    print("###################### Note the target column has type struct ######################")
    target_df.printSchema()

    df_new_collected = target_df.groupby("name").agg(f.collect_list("target").alias("target_list"))
    print("######################Group target column by name ######################")
    df_new_collected.show(5, False)
    print("###################### Note the target_list column has type array of struct ######################")
    df_new_collected.printSchema()


"""
##### Scenario3. If you want to covert more than two columns, and we can group the generated column by another column
We already see how to do in scenario2, but with two step. We can also do it in one step by combining collect_list and 
struct 
######
"""
# If we have a want to re
schema = ArrayType(
    StructType([
        StructField("t_key", StringType(), False),
        StructField("t_content", StringType(), False),
        StructField("t_value", LongType(), False)
    ]))


def scenario3(source_df):
    target_df = source_df.groupBy("name").agg(f.collect_list(
        f.struct(
            *[f.col("key").alias("t_key"), f.col("content").alias("t_content"),
              f.col("value").alias("t_value")])).alias("target_list"))
    print("######################Scenario_3: Group and convert in one step ######################")
    target_df.show(5, False)
    print("###################### Note the target_list column has type array of struct ######################")
    target_df.printSchema()
    return target_df


"""
######################## Scenario4. Get field values of nested struct type from column ###################
After groupby, the target_list column has type array(struct(key,content,value)). To access the value 
of columns with complex struct, we need to understand how a column is organized. It can be primitive(e.g. int, bool),
array, or struct. In the following example, we have an array of struct. 

"""


def show_content(var_list):
    res = []
    for var in var_list:
        # The struct type is represented as a list. The first field value can be accessed via index 0
        # we can't use filed name such as t_key which we defined in the dataframe.
        # Because this function is un aware of the schema of the input object.
        name = var[0]
        content = var[1]
        value = var[2]
        res.append(name + content + str(value))
    return res


Show_Content_UDF = f.udf(lambda var_list: show_content(var_list))


def scenario4(df):
    # The schema of df
    # root
    #  |-- name: string (nullable = true)
    #  |-- target_list: array (nullable = true)
    #  |    |-- element: struct (containsNull = false)
    #  |    |    |-- t_key: string (nullable = true)
    #  |    |    |-- t_content: string (nullable = true)
    #  |    |    |-- t_value: long (nullable = true)
    # access value of each row
    print("######################Scenario_4: Get field values of nested struct type from column ######################")
    print("In pyspark: A row of df acts as an dict. The column name is the key, the data is the value.")
    print("To return the value of a column, we just select the column name")
    df.select(df.target_list).show()
    # target_list is an array of struct, so to access the first element(a struct) of the array
    print("In our case, the column type is an array of object. To access each object, we can use its index")
    print("If there is no element in the list at the requested index, it returns null")
    df.select(df.target_list[2]).show()
    # to access the field of a struct
    print("To access the field value, we use obj.filed_name")
    df.select(df.target_list[0].t_key).show()
    # a udf which append three field as a single string
    print("Use a udf to concat all field value as a string")
    transform_df = df.withColumn("concat_field", Show_Content_UDF("target_list"))
    transform_df.show(5, False)


"""
######################## Scenario 5. Convert nested struct type column to multi primitive type col ###################
Very important Note: The field name should not contain ".". Because it will cause confusion when we want to access
them
"""


def scenario5(df):
    # The schema of df
    # root
    #  |-- name: string (nullable = true)
    #  |-- target_list: array (nullable = true)
    #  |    |-- element: struct (containsNull = false)
    #  |    |    |-- t_key: string (nullable = true)
    #  |    |    |-- t_content: string (nullable = true)
    #  |    |    |-- t_value: long (nullable = true)

    # Because we have a list of struct, the select returns a list
    print("Because we have a list of struct, the select returns a list")
    df.select(df.name.alias("name"), df.target_list.t_key.alias("key"),
              df.target_list.t_value.alias("value")).show()

    # if we want to primitive column, we need to explode the list first
    print("Because we explode the list of struct, the select returns primitive type")
    df.withColumn("target", f.explode("target_list")).select(f.col("name"), f.col("target.t_key").alias("key"),
                                                             f.col("target.t_value").alias("value")).show()


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("ConvertMultiColumnToOne") \
        .getOrCreate()

    source_data = [
        ("toto", "toto_1", "00", 1),
        ("titi", "titi_1", "00", 2),
        ("tata", "tata_1", "00", 3),
        ("toto", "toto_2", "00", 4),
        ("toto", "toto_3", "00", 5),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "key", "content", "value"])
    print("######################Source df######################")
    source_df.show()

    # run scenario 1
    # scenario1(source_df)

    # run scenario 2
    # scenario2(source_df)

    # run scenario 3
    df = scenario3(source_df)

    # run scenario 4
    # scenario4(df)

    # run scenario 5
    scenario5(df)


if __name__ == "__main__":
    main()

"""
scala code work with column struct type

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

val df = Seq(
  ("str", 1, 0.2)
).toDF("a", "b", "c").
  withColumn("struct", struct($"a", $"b", $"c"))

// UDF for struct
val func = udf((x: Any) => {
  x match {
    case Row(a: String, b: Int, c: Double) => 
      s"$a-$b-$c"
    case other => 
      sys.error(s"something else: $other")
  }
}, StringType)

df.withColumn("d", func($"struct")).show

"""
