from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

""" In spark, we have two function (e.g. sort, orderBy) which can sort rows by ascending or descending 
order based on single or multiple columns.
sort(*col_name): It's called by a dataframe, takes a list of column names, sort it with required order, then return
                 a new data frame. The default order is ascending.
orderBy(*col_name): It's called by a dataframe, takes a list of column names, sort it with required order, then return
                 a new data frame. The default order is ascending.
                 
In exp1, we use sort() to sort data frames
In exp2. we use orderBy() to sort data frames

You can notice there is no difference between these two function from this example. If you run this example in a multi
executor cluster, you may see different results. That's because:
sort: only sort partitions inside one executor to avoid shuffle between different executor or even different workers.
       Thus sort is more efficient. But the total order is not guaranteed due to partition level sort.
       
groupBy: collects all the data into a single executor and then sorts them. This means that the total order of the 
        output data frame is guaranteed. But if you have a very large data frame, this will be a very costly 
        operation. In some case, you may even get an OutOfMemory error.
        
Conclusion : if you need to sort a DataFrame where the order is not so critical and at the same time you want it to 
             be as fast as possible then sort() is more suitable. In case the order is critical, make sure to use 
             orderBy()
"""


def exp1(df: DataFrame):
    print("Exp1: Sort data frame by using age in ascending order")
    df.sort("age").show(truncate=False)

    print("Exp1: Sort data frame by using age in descending order")
    df.sort(col("age").desc()).show(truncate=False)

    # note when sort by using two columns, the principal order is done by using first col, if first col has equal rows
    # then we use second column as secondary order.
    print("Exp1: Sort data frame by using salary, age in ascending order")
    df.sort(col("salary"), col("age").desc()).show(truncate=False)


def exp2(df: DataFrame):
    # When sort string, we use alphabet order
    print("Exp2: orderBy data frame by using age in ascending order")
    df.orderBy(df.department.asc(), df.state.desc()).show(truncate=False)


def main():
    if __name__ == '__main__':
        spark = SparkSession.builder.master("local[4]").appName("SortRows").getOrCreate()
        data = [("James", "Sales", "NY", 90000, 34, 10000),
                ("Michael", "Sales", "NY", 86000, 56, 20000),
                ("Robert", "Sales", "CA", 81000, 30, 23000),
                ("Maria", "Finance", "CA", 90000, 24, 23000),
                ("Raman", "Finance", "CA", 99000, 40, 24000),
                ("Scott", "Finance", "NY", 83000, 36, 19000),
                ("Jen", "Finance", "NY", 79000, 53, 15000),
                ("Jeff", "Marketing", "CA", 80000, 25, 18000),
                ("Kumar", "Marketing", "NY", 90000, 50, 21000)
                ]
        columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
        df = spark.createDataFrame(data=data, schema=columns)
        print("Source data frame: ")
        df.printSchema()
        df.show(truncate=False)
        print("Partition number: {}".format(df.rdd.getNumPartitions()))

        # run exp1
        # exp1(df)

        # run exp2
        exp2(df)


if __name__ == "__main__":
    main()
