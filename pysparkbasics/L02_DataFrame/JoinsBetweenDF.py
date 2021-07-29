from pyspark.sql import SparkSession
from pyspark.sql.functions import col

""" Introduction: pyspark.sql.DataFrame.join(otherDf, onCond=None,how=None) 
The join function joins one df with another.
It has three parameters:
- otherDf: a DataFrame which is on the right side of the join
- onCond: a condition of the join. It can be a str, list or Column, this parameter is optional
          If it's a string or a list of strings indicating the column name, the columns must exist 
          on both data frame, and it performs an equi-join.
          If the column name is different on two sides, we can use "df1.col1==df2.col2" 
          to match the columns. If multi columns are involved, we can use list such as
          [df1.age1==df2.age2, df1.name1==df2.name2] 
- how: is the join type, default value is "inner", it has string type and optional.
       The value must be one of: inner, cross, outer, full, fullouter, full_outer, 
       left, leftouter, left_outer, right, rightouter, right_outer, semi, leftsemi, 
       left_semi, anti, leftanti and left_anti.
"""

""" Scenario1: Inner join
 In this example, we inner join two dataframes on a column, note that the name of the column are different
 for the two dataframe. So no confusion 
"""


# Scenario 1: Inner join
def scenario1(emp_df, dept_df):
    inner_join = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "inner")
    print("Scenario1 print:")
    inner_join.show(truncate=False)


""" Scenario2: Inner join with the same column name
 In this example, we also inner join two dataframes on a column, but the name of the column are the same,
 "dept_id". Try the bad example, you will see the result has two column called "dept_id". If you try
 to select the "dept_id" column, you will receive an error message "Reference 'dept_id' is ambiguous,"

 To avoid this, we can ask spark to create only one column after inner join. In scenario2_good, we
 just use the column name which are shared by the two dataframes. 
"""


def scenario2_bad(emp_df, dept_df):
    emp_df_clean = emp_df.select("name", "emp_dept_id").withColumnRenamed("emp_dept_id", "dept_id")

    inner_join = emp_df_clean.join(dept_df, emp_df_clean.dept_id == dept_df.dept_id, "inner")
    print("Scenario2_bad print:")
    inner_join.show(truncate=False)

    # uncomment to see the error message
    # inner_join.select("dept_id").show()


def scenario2_good(emp_df, dept_df):
    emp_df_clean = emp_df.select("name", "emp_dept_id").withColumnRenamed("emp_dept_id", "dept_id")
    inner_join = emp_df_clean.join(dept_df, "dept_id", "inner")
    print("Scenario2_good print:")
    inner_join.show(truncate=False)


""" Scenario3: Inner join with multiple column which has same column name
In this example, we inner join two dataframes on multiple column, which are "dept_id" and "dept_creation_year"
I intentionally introduced three error in the emp_df, you can notice the last three row, the year and dept_id 
does not match with the dept_df. So the join only returns 3 rows.
Another important note, in the cond list, we separate two condition with "," and this is
considered as an "and". If you want to express "and" explicitly, use "&" instead of ",".
To express "or", use "|".  
"""


def scenario3(emp_df, dept_df):
    # an and multi cond join, you can switch between implicit and explicit cond expression,
    # it returns the same result
    and_implicit_cond = [emp_df.emp_dept_id == dept_df.dept_id,
                         emp_df.dept_creation_year == dept_df.dept_creation_year]
    and_explicit_cond = [
        (emp_df.emp_dept_id == dept_df.dept_id) & (emp_df.dept_creation_year == dept_df.dept_creation_year)]
    and_multi_cond_inner_join = emp_df.join(dept_df, and_implicit_cond, "inner")
    print("Scenario3 print and_multi_cond_inner_join:")
    and_multi_cond_inner_join.show(truncate=False)

    # an or multi cond join
    or_cond = [(emp_df.emp_dept_id == dept_df.dept_id) | (emp_df.dept_creation_year == dept_df.dept_creation_year)]
    or_multi_cond_inner_join = emp_df.join(dept_df, or_cond, "inner")
    print("Scenario3 print or_multi_cond_inner_join:")
    or_multi_cond_inner_join.show()


""" Scenario4: Inner join with multiple column which has same column name

In the output of scenario3, we noticed the duplicate column name, To avoid this, we need to use the method of scenario 2
But we have one column which has the same name, and one column which has different names.
So if we can mix the two in one cond list, it will be nice. For example, in and_implicit_cond_bad
we mix the two mode. But it does not work. And the error message 
"py4j.Py4JException: Method and ([class java.lang.String]) does not exist" does tell us that
py4j will translate the list to a logical and expression, when the first element is bool. So the second
element must be bool too.

The simplest solution is that we rename the column name, and a list of column name(string). 
"""


def scenario4(emp_df, dept_df):
    # this cond does not work
    cond_bad = [emp_df.emp_dept_id == dept_df.dept_id, "dept_creation_year"]
    cond = ["dept_id", "dept_creation_year"]
    emp_df_rename = emp_df.select("name", "emp_dept_id", "dept_creation_year").withColumnRenamed("emp_dept_id",
                                                                                                 "dept_id")
    print("Scenario 4 output: ")
    emp_df_rename.join(dept_df, cond, "inner").show(truncate=False)


""" Scenario5: outer join 
Outer (a.k.a full, fullouter join) returns all rows from both datasets, where join expression does not 
match it returns null on respective record columns.

You can notice that the following 3 mode returns the same result.
note : you will find the output df has two rows contains null,
        row 1: dept_id = 50, where the emp_dept_id does not have this value
        row 2: emp_dept_id = 150, where the dept_id does not have this value 
    

"""


def scenario5(emp_df, dept_df):
    mode1 = "outer"
    mode2 = "full"
    mode3 = "fullouter"
    print("Scenario 5 output outerJoin: ")
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, mode1).show()
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, mode2).show()
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, mode3).show()


""" Scenario6: left outer join 
Left (a.k.a Leftouter join) returns all rows from the left dataset, when match join expression does not 
match it assigns null on respective record columns.

You can notice that the following 2 mode returns the same result.
note : you will find the output df has one row contains null,
        row 1: emp_dept_id = 150, where the dept_id does not have this value 
Because in emp_df.join(), emp_df is consider as left side df, and dept_df is the right side
So all rows of left df are conserved, for those who does not have a match on right hand df, the right hand df columns 
are filled with null

For the rows of the right df who does not have a match on the left df, they are dropped (e.g. row with dept_id =50)
  
"""


def scenario6(emp_df, dept_df):
    mode1 = "left"
    mode2 = "leftouter"
    print("Scenario 6 output LeftOuterJoin: ")
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, mode1).show()
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, mode2).show()


""" Scenario7: Right outer join 
Right (a.k.a Rightouter join) returns all rows from the right dataset, when match join expression does not 
match it assigns null on respective record columns. For the rows of the left hand df who does not 
have a match on the right df, they are dropped (e.g. row with emp_dept_id =50)


You can notice that the following 2 mode returns the same result.
note : you will find the output df has one row contains null,
        row 1: dept_id = 50, where the emp_dept_id does not have this value 
Because in emp_df.join(), emp_df is consider as left side df, and dept_df is the right side
So all rows of right hand (i.e. dept_df) df are conserved, for those who does not have a 
match on left hand df, the left hand df columns are filled with null


"""


def scenario7(emp_df, dept_df):
    mode1 = "right"
    mode2 = "rightouter"
    print("Scenario 7 output RightOuterJoin: ")
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, mode1).show()
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, mode2).show()


""" Scenario8: Left Semi Join
leftsemi join equals to a inner join and a select of the columns of left hand side df. 
As a result, all columns from the right dataset are ignored. 

Note the result of below example, we only have the left hand side columns, and the row 
where emp_dep_id=150 is dropped, because there is no match no the right side.

"""


def scenario8(emp_df, dept_df):
    print("Scenario 8 output LeftSemiJoin: ")
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "leftsemi").show()


""" Scenario9: Left Anti Join
leftanti join does the opposite of the inner join and select of the columns of left hand side df. 
As a result, all columns from the right dataset are ignored. 

Note the result of below example, we only have the left hand side columns. We only had one
one row where emp_dep_id=150, because this row has no match no the right side.
"""


def scenario9(emp_df, dept_df):
    print("Scenario 9 output LeftAntiJoin: ")
    emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "leftanti").show()


""" Scenario10: SelfJoin
All above join type can be applied to the same dataframe 

Below example shows an inner join on the same dataframe which use join condition superior_emp_id= emp_id to generate 
a table with his superior name
"""


def scenario10(emp_df):
    print("Scenario 10 output self inner join: ")
    emp_df.alias("emp1").join(emp_df.alias("emp2"), col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
        .select(col("emp1.emp_id").alias("emp_id"), col("emp1.name").alias("name"),
                col("emp2.emp_id").alias("superior_emp_id"), col("emp2.name").alias("superior_name")) \
        .show(truncate=False)


""" Scenario 11: pure SQL"""


def scenario11(spark, emp_df, dept_df):
    emp_df.createOrReplaceTempView("EMP")
    dept_df.createOrReplaceTempView("DEPT")
    print("Scenario 11 output inner join with pure sql: ")
    joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
        .show(truncate=False)

    joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
        .show(truncate=False)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("Joins between dataframe") \
        .getOrCreate()

    emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
           (2, "Rose", 1, "2010", "20", "M", 4000),
           (3, "Williams", 1, "2018", "21", "M", 1000),
           (4, "Jones", 2, "2005", "31", "F", 2000),
           (5, "Brown", 2, "2010", "30", "", -1),
           (6, "Foobar", 2, "2010", "150", "", -1)
           ]
    emp_col_names = ["emp_id", "name", "superior_emp_id", "dept_creation_year",
                     "emp_dept_id", "gender", "salary"]
    emp_df = spark.createDataFrame(data=emp, schema=emp_col_names)
    emp_df.printSchema()
    emp_df.show(truncate=False)
    dept = [("Finance", 10, "2018"),
            ("Marketing_US", 20, "2010"),
            ("Marketing_FR", 21, "2018"),
            ("Sales_US", 30, "2005"),
            ("Sales_FR", 31, "2010"),
            ("IT", 50, "2005")
            ]

    dept_col_name = ["dept_name", "dept_id", "dept_creation_year"]
    dept_df = spark.createDataFrame(data=dept, schema=dept_col_name)
    dept_df.printSchema()
    dept_df.show(truncate=False)

    # run scenario 1 for inner join
    scenario1(emp_df, dept_df)

    # run scenario 2
    # scenario2_bad(emp_df, dept_df)
    # scenario2_good(emp_df, dept_df)

    # run scenario 3
    # scenario3(emp_df, dept_df)

    # run scenario 4
    # scenario4(emp_df, dept_df)

    # run scenario 5
    # scenario5(emp_df, dept_df)

    # run scenario 6
    # scenario6(emp_df, dept_df)

    # run scenario 7
    # scenario7(emp_df, dept_df)

    # run scenario 8
    # scenario8(emp_df, dept_df)

    # run scenario 9
    # scenario9(emp_df, dept_df)

    # run scenario 10
    # scenario10(emp_df)

    # run scenario 11
    scenario11(spark, emp_df, dept_df)

    # todo broadcast join https://mungingdata.com/apache-spark/broadcast-joins/


if __name__ == "__main__":
    main()
