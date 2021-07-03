from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

""" Union data frame.
We have seen how to join two dataframe which has common columns, which merge the other columns. Now if we have two
or more data frames which has the same schema or structure, we can use union() transformation
to merge two or more DataFrame’s rows. Because they have same columns.

Note the unionAll() transformation is deprecated since Spark “2.0.0” version. 

In exp1, we show how to union two dataframe of the same schema

In exp2, we show a failed union example, because two dataframe has different schema(e.g. column number)

In exp3, we tested on different column type, union works. How the output data frame choose column type is unclear. 
"""


def exp1(df1: DataFrame, df2: DataFrame):
    # note even the column name is not exactly the same. The union function is still successful
    # Because spark analyze column number and type. If those are identical, the difference between column names
    # are omit.

    # But you can notice, the union just merge the two dataframe without dealing with duplicates.
    # So you may combine a distinct() after you merge
    union_df = df1.union(df2)
    print("Exp1 : Union data row number {}".format(union_df.count()))
    union_df.show(truncate=False)

    distinct_df = union_df.distinct()
    print("Exp1 : Distinct union data row number {}".format(distinct_df.count()))
    distinct_df.show()


def exp2(df1: DataFrame, df2: DataFrame):
    df3 = df1.withColumn("msg", lit("hello"))
    try:
        df3.union(df2)
    except Exception as e:
        print("Exp 2 different column number union failed \n error message: {}".format(str(e)))


def exp3(df1: DataFrame, df2: DataFrame):
    # cast age to string, int, float, union still works
    df4 = df1.withColumn("age", df1.age.cast("string"))
    try:
        df5 = df2.union(df4)
        df5.printSchema()
        df5.show()
    except Exception as e:
        print("Exp 2 different column type union failed \n error message: {}".format(str(e)))


def main():
    spark = SparkSession.builder.master("local[*]").appName("UnionDataFrame").getOrCreate()
    data1 = [("James", "Sales", "NY", 90000, 34, 10000),
             ("Michael", "Sales", "NY", 86000, 56, 20000),
             ("Robert", "Sales", "CA", 81000, 30, 23000),
             ("Maria", "Finance", "CA", 90000, 24, 23000)
             ]
    columns1 = ["name", "department", "state", "salary", "age", "bonus"]
    df1 = spark.createDataFrame(data=data1, schema=columns1)
    print("Source data 1: row number {}".format(df1.count()))
    df1.printSchema()
    df1.show(truncate=False)

    data2 = [("James", "Sales", "NY", 90000, 34, 10000),
             ("Maria", "Finance", "CA", 90000, 24, 23000),
             ("Jen", "Finance", "NY", 79000, 53, 15000),
             ("Jeff", "Marketing", "CA", 80000, 25, 18000),
             ("Kumar", "Marketing", "NY", 91000, 50, 21000)
             ]
    columns2 = ["employee_name", "department", "state", "salary", "age", "bonus"]
    df2 = spark.createDataFrame(data=data2, schema=columns2)
    print("Source data 2: row number {}".format(df2.count()))
    df2.printSchema()
    df2.show(truncate=False)

    # run exp1
    # exp1(df1, df2)

    # run exp2
    # exp2(df1, df2)

    # run exp3
    exp3(df1, df2)


if __name__ == "__main__":
    main()
