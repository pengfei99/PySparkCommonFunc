from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, dense_rank
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder.master("local[2]").appName("top_3_department_salary").getOrCreate()
    d_path = "data/department.csv"
    e_path = "data/employee.csv"
    department_df = spark.read.option("header", "true").csv(d_path)
    employee_df = spark.read.option("header", "true").csv(e_path)
    department_df.show()
    department_df.printSchema()
    employee_df.show()
    employee_df.printSchema()


if __name__ == "__main__":
    main()
