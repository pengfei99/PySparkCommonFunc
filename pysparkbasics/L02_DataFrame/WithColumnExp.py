from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

""" withColumn
withColumn(new_col_name, func) is a transformation function of DataFrame which is used to
- change the column value
- convert the column datatype
- create new columns

new_col_name: is the name of the column that you will create, if it equals a column name which already exist in the
        data frame, then it changes the original column value or type based on the func.
func: is the function which we generate a new or update the old column.
"""

""" Exp1:
We cast type of salary column from long to integer. Note the new_col_name here is "salary" which exist already, so it
will update the old one.
"""


def exp1(df: DataFrame):
    # We cast type of salary from long to integer
    df1 = df.withColumn("salary", col("salary").cast("Integer"))
    print("exp1: schema after cast salary type from long to integer")
    df1.printSchema()


""" Exp2:
We update the value of column salary by multiply it with 100.

"""


def exp2(df: DataFrame):
    print("exp2: multiply salary by 100")
    df.withColumn("salary", col("salary") * 100).show()


""" Exp3:
We create a new column new_salary by using old salary column and column country by using lit.
"""


def exp3(df: DataFrame):
    df.withColumn("new_salary", col("salary") - 10).withColumn("Country", lit("FR")).show()


def main():
    spark = SparkSession.builder.master("local[2]").appName("WithColumnExp").getOrCreate()
    data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
            ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
            ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
            ]

    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

    df = spark.createDataFrame(data, schema=columns)
    print("Source data frame: ")
    df.printSchema()
    df.show()

    # run exp1
    # exp1(df)

    # run exp2
    # exp2(df)

    # run exp3
    exp3(df)


if __name__ == "__main__":
    main()
