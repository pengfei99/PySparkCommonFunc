from pyspark.sql import SparkSession, DataFrame

"""To eliminate duplicates rows in data frame, spark provides two methods:
- distinct(): is used to drop/remove the duplicate rows (all columns) from DataFrame. It takes no arg and return a 
              new data frame
- removeDuplicates(*colName): is used to drop rows based on selected (one or multiple) columns. It takes an array of
      column names and return a new data frame.

In exp1, we show how to use distinct() and removeDuplicates to drop duplicates rows on all columns.
In exp2, we show how to use removeDuplicates() to drop duplicates rows on all columns.
In exp3, we show how to use removeDuplicates() to drop duplicates rows on column "name", "department"
"""


def exp1(df: DataFrame):
    df_no_dup = df.distinct()
    print("Exp1 Distinct rows number: {}".format(df_no_dup.count()))
    df_no_dup.show()


def exp2(df: DataFrame):
    df_no_dup1 = df.dropDuplicates()
    print("Exp2 Distinct rows number with ropDuplicates(): {}".format(df_no_dup1.count()))
    df_no_dup1.show()

    # more python compatible function name, but it does exactly the same thing
    df_no_dup2 = df.drop_duplicates()
    print("Exp2 Distinct rows number with drop_duplicates(): {}".format(df_no_dup2.count()))
    df_no_dup2.show()


def exp3(df: DataFrame):
    df_no_dup1 = df.dropDuplicates(["department"])
    print("Exp3 Distinct rows number on col department: {}".format(df_no_dup1.count()))
    df_no_dup1.show()

    df_no_dup2 = df.dropDuplicates(["name", "department"])
    print("Exp3 Distinct rows number on col name, department: {}".format(df_no_dup2.count()))
    df_no_dup2.show()


def main():
    spark = SparkSession.builder.master("local[2]").appName("DealWithDuplicates").getOrCreate()
    data = [("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100),
            ("Maria", "Finance", 3000),
            ("James", "Sales", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Saif", "Sales", 4100)
            ]
    columns = ["name", "department", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    print("Source data frame: ")
    df.printSchema()
    df.show(truncate=False)

    # run exp1
    exp1(df)

    # run exp2
    exp2(df)

    # run exp3
    exp3(df)


if __name__ == "__main__":
    main()
