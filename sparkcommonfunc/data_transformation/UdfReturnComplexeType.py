from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

""" ####### Scenario 1: UDF returns struct column
The function "converter1" returns three value, in the udf declaration, we use a schema which contains struct type 
"""


def converter1(i: int):
    return i, i * i, str(i)


schema1 = StructType([
    StructField("s_val", IntegerType(), False),
    StructField("s_square", IntegerType(), False),
    StructField("s_str", StringType(), False)
])
CONVERTER1_UDF = f.udf(lambda i: converter1(i), schema1)


def scenario1(df):
    gen_df = df.withColumn("generated_col", CONVERTER1_UDF("emp_id")).select("emp_id", "generated_col")
    gen_df.show()
    gen_df.printSchema()


""" ####### Scenario 2: UDF returns array(struct) column
The function "converter2" returns a list, in the udf declaration, we use a schema which contains struct type 
"""


def converter2(i: int):
    res = []
    for j in range(i):
        res.append((i, i * i, str(i)))
    return res


schema2 = ArrayType(
    StructType([
        StructField("s_val", IntegerType(), False),
        StructField("s_square", IntegerType(), False),
        StructField("s_str", StringType(), False)
    ]))

CONVERTER2_UDF = f.udf(lambda i: converter2(i), schema2)


def scenario2(df):
    gen_df = df.withColumn("generated_col", CONVERTER2_UDF("emp_id")).select("emp_id", "generated_col")
    gen_df.show()
    gen_df.printSchema()


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("UdfRetrunscomplexeType") \
        .getOrCreate()

    emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
           (2, "Rose", 1, "2010", "20", "M", 4000),
           (3, "Williams", 1, "2010", "10", "M", 1000),
           (4, "Jones", 2, "2005", "10", "F", 2000),
           (5, "Brown", 2, "2010", "40", "", -1),
           (6, "Brown", 2, "2010", "50", "", -1)
           ]
    emp_col_names = ["emp_id", "name", "superior_emp_id", "year_joined",
                     "emp_dept_id", "gender", "salary"]
    emp_df = spark.createDataFrame(data=emp, schema=emp_col_names)

    # scenario 1
    print("scenario 1: Udf create a struct column")
    scenario1(emp_df)

    # scenario 2
    print("scenario 2: Udf create a array(struct) column")
    scenario2(emp_df)


if __name__ == "__main__":
    main()
