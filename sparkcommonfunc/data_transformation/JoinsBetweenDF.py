from pyspark.sql import SparkSession


# Scenario 1: Inner join
def scenario1(emp_df, dept_df):
    inner_join = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "inner")
    inner_join.show(truncate=False)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("Joins between dataframe") \
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
    emp_df.printSchema()
    emp_df.show(truncate=False)
    dept = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("IT", 40)
            ]

    dept_col_name = ["dept_name", "dept_id"]
    dept_df = spark.createDataFrame(data=dept, schema=dept_col_name)
    dept_df.printSchema()
    dept_df.show(truncate=False)

    # run scenario 1 for inner join
    scenario1(emp_df, dept_df)


if __name__ == "__main__":
    main()
