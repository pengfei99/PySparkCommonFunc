"""
We can use select to filter which column we want to get. To filter which rows we want to get, we have two options
- filter(condition): condition is a boolean
- where()
"""
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.functions import concat, lit, array_contains

""" Exp1
We use simple equality condition filter the rows that has certain values
"""


def exp1(df: DataFrame):
    print("Filter by a simple column value")
    df.filter(df.department == "Finance").show()

    print("Filter by a field value of a struct column")
    df.filter(df.name.fname == "Maria").show()

    print("Filter by a key paire of a map column")
    df.filter(df.properties.getItem("hair") == "black").show()

    print("Filter by an item of a list column")
    df.filter(df.score.getItem(2) == 30).show()

    # we can also use pure sql expression
    # Using SQL Expression
    print("Filter by using pure sql expression")
    df.filter("department == 'Finance'").show()
    # For not equal
    df.filter("department != 'Finance'").show()
    df.filter("department <> 'Finance'").show()


""" Exp2
Multiple conditions. We can use the following operators to chain multiple conditions
- &: and condition, 
- |: or condition 
- ~: not conditional
Best practice, add () to all conditions. Because the condition evaluation is not very smart
For example ~(df.department == "Finance") is correct, ~df.department == "Finance" is interpreted as not department
which is wrong.
"""


def exp2(df: DataFrame):
    # hair black and work at finance
    print("Filter by hair black and work at finance")
    df.filter((df.properties.getItem("hair") == "black") & (df.department == "Finance")).show(truncate=False)

    print("Filter by hair black or work at finance")
    df.filter((df.properties.getItem("hair") == "black") | (df.department == "Finance")).show(truncate=False)

    print("Filter by hair black and not work at finance")
    df.filter((df.properties.getItem("hair") == "black") & ~(df.department == "Finance")).show(truncate=False)


""" Exp3
Use predefine column functions inside filter, note the function must returns boolean
- between(lowerBound, upperBound): Checks if the columns values are between lower and upper bound. Returns 
                                  boolean value.
- contains(arg): Check if String contains in another string. Returns boolean value.
- startswith(arg): Check if String starts with another string. Returns boolean value
- endswith(other): Check if String starts with another string. Returns boolean value
- isNotNull(): Returns True if the current expression is NOT null.
- isNull(): Returns True if the current expression is null.	
- isin(*cols):	A boolean expression that is evaluated to true if the value of this expression is contained by the 
                evaluated values of the arguments(a list of values).
- like():
- rlike(regex):

# not called by an column, thus not a column function,  
array_contains(): it can take an array column as argument, and check if an element is in this array
"""


def exp3(df: DataFrame):
    print("Filter by using isin() function")
    dept = ['Sales', 'IT']
    df.filter(df.department.isin(dept)).show(truncate=False)
    # negation of isin()
    print("Filter by using ~ isin() function")
    df.filter(~df.department.isin(dept)).show(truncate=False)

    print("Filter by using like() function")
    df1 = df.select(concat(df.name.fname, lit(" "), df.name.lname).alias("name"))
    df1.filter(df1.name.like("%Rose%")).show()
    print("Filter by using rlike() function and case insensitive regexp")
    df1.filter(df1.name.rlike("(?i)^*rose$")).show()

    print("Filter by using array_contains() function, where score contains 20")
    df.filter(array_contains(df.score,20)).show(truncate=False)



def main():
    spark = SparkSession.builder.master("local[2]").appName("FilterRows").getOrCreate()
    Person = Row("name", "department", "properties", "score")
    data = [Person(Row(fname="James", lname="Smith"), "Finance", {'hair': 'black', 'eye': 'black'}, [10, 20, 30]),
            Person(Row(fname="Michael", lname="Rose"), "Marketing", {'hair': 'bleu', 'eye': 'yellow'}, [20, 30, 40]),
            Person(Row(fname="Anne", lname="rose"), "Sales", {'hair': 'green', 'eye': 'yellow'}, [22, 33, 48]),
            Person(Row(fname="Robert", lname="Williams"), "Sales", {'hair': 'black', 'eye': 'bleu'}, [10, 30, 40]),
            Person(Row(fname="Maria", lname="Jones"), "IT", {'hair': 'yellow', 'eye': 'black'}, [10, 30, 40])
            ]
    df = spark.createDataFrame(data)
    print("Source data frame:")
    df.printSchema()
    df.show()

    # exp1
    # exp1(df)

    # exp2
    # exp2(df)

    # exp3
    exp3(df)


if __name__ == "__main__":
    main()
