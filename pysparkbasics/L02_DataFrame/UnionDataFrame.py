from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

""" Union data frame.
We have seen how to join two dataframe which has common columns, which merge the columns of dataframe. Now if we have 
two or more data frames which has the same (or almost the same) schema or structure, we have two solutions:
- union(other): It's called by a data frame, it takes another data frame as argument. It returns a new dataframe which
                is the union of the two.
- unionByName(other, allowMissingColumns). Idem to union, but since spark 3.1. A new argument allowMissingColumns which
                takes a bool value has been added. This allows us to merger data frame with different column numbers.

The difference between two transformations is that union() resolve column by its position. unionByName() resolve 
column by its name. 

In exp1, we show how to use union() to union two dataframe of the same column number

In exp2, we show a failed union example, because two dataframe has different schema(e.g. column number)

In exp3, we tested on different column type, union works. How the output data frame choose column type is unclear. 

Note there is another transformation called unionAll() which is deprecated since Spark “2.0.0” version. 




"""

"""Exp1 union example
Note in df1, the column name of the employee name is called name, in df2, the column name is called employee_name. The 
union is still successful. Because union() resolve column by position, not by name. In Exp4, we use unionByName() on the
same data frames, and it failed. Because unionByName() resolve column by using column name.

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


"""Exp2 union example
Union failed because df1 has one column "msg" more than df2

"""


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
        print("Exp 3 different column type union failed \n error message: {}".format(str(e)))


""" Exp4. In this example, we use unionByName() to union two data frame which has the same column number, but different
column name. It failed because unionByName() resolve columns of two data frame by using their name 

"""


def exp4(df1: DataFrame, df2: DataFrame):
    try:
        union_df = df1.unionByName(df2)
        union_df.printSchema()
        union_df.show()
    except Exception as e:
        print("Exp 4 unionByName failed with two data frame with different col name\n error message: {}".format(str(e)))


""" Exp 5: In this example, we use unionByName() to union two dataframe which have different column numbers. With the 
option allowMissingColumns=True, the absent columns will be filled with null after union.
"""


def exp5(df1: DataFrame, df2: DataFrame):
    df3 = df1.withColumn("msg", lit("hello"))
    df4 = df2.withColumn("mail", lit("world")).withColumnRenamed("employee_name", "name")

    print("Exp5 df3 schema: ")
    df3.printSchema()
    print("Exp5 df4 schema: ")
    df4.printSchema()

    df5 = df3.unionByName(df4, allowMissingColumns=True)
    print("Exp5, result after unionByName")
    df5.printSchema()
    df5.show(truncate=False)


""" Exp6: If you are using a spark version prior to 3.1. You don't have access of allowMissingColumns=True. You need
to creat the absent column and filled it with null by yourself.


"""


# This function finds all columns of df2 which does not exist in df1, then create then in df1 and fill it with None
def create_missing_column(df1, df2):
    for column in [column for column in df2.columns if column not in df1.columns]:
        df1 = df1.withColumn(column, lit(None))
    return df1


def exp6(df1: DataFrame, df2: DataFrame):
    # df3 has all the common columns and one extra column "msg"
    df3 = df1.withColumn("msg", lit("hello"))
    # df4 has all the common columns and one extra column "mail"
    df4 = df2.withColumn("mail", lit("world")).withColumnRenamed("employee_name", "name")

    # we need to create column "mail" in df3, and column "msg" in df4
    df3_after_fill = create_missing_column(df3, df4)
    print("Exp6 : df3_after_fill ")
    df3_after_fill.show()

    df4_after_fill = create_missing_column(df4, df3)
    print("Exp6 : df4_after_fill ")
    df4_after_fill.show()

    # after the fill, we can do the unionByName or union

    # Check the different result of union and unionByName, it confirms the union resolve column by positon, unionByName
    # resolve column by name
    union1_df = df3_after_fill.union(df4_after_fill)
    print("Exp6: result of applying union(): ")
    union1_df.show()

    union2_df = df3_after_fill.unionByName(df4_after_fill)
    print("Exp6: result of applying unionByName(): ")
    union2_df.show()


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
    # exp3(df1, df2)

    # run exp4
    # exp4(df1, df2)

    # exp5(df1, df2)

    exp6(df1, df2)


if __name__ == "__main__":
    main()
