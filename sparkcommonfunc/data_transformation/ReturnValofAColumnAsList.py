""" Context: We want to collect the value of a column in a dataframe, and return it as a list or an array """
from pyspark.sql import SparkSession

"""Solution 1: use toPandas"""


def solution1(df, col_name):
    # select the column and convert it to pandas data frame. then select the column of the pandas data frame
    pandas_df = df.select(col_name).toPandas()[col_name]
    # convert the pandas column to list, note list can have duplicate value
    return list(pandas_df)


"""Solution 2: use rdd and flatmap"""


def solution2(df, col_name):
    # note if we do .collect directly on a dataframe, it returns a list of rows. here row is struct type
    # rdd is a list of row, but row is a list of values, not struct (note rdd has no schema, so no column name)
    return df.select(col_name).rdd.flatMap(lambda x: x).collect()


"""Solution 3: use rdd and map"""


def solution3(df, col_name):
    # rdd is a list of row where is a list value. and map take the first row, and row[0] returns the first element
    # of the row
    return df.select(col_name).rdd.map(lambda row: row[0]).collect()


"""Solution 4:  the collect() list comprehension"""


def solution4(df, col_name):
    return [row[0] for row in df.select(col_name).collect()]


"""Solution 5: the toLocalIterator list comprehension"""


def solution5(df, col_name):
    return [row[0] for row in df.select(col_name).toLocalIterator()]


""" Performance: 
 
 
Method  | ONE THOUSAND	| HUNDRED THOUSAND	| HUNDRED MILLION
toPandas|	0.32	    |       0.35	    |   18.12
flatMap	|   0.55	    |       0.5	        |   57.5
map	    |   0.39	    |       0.77	    |   60.6
list comprehension|	0.85|   	5.18	
toLocalIterator| 1.44	|       1.34	

Note that the toPandas solution has the best performance, map and flatMap are almost equal.  
The list comprehension approach failed and the toLocalIterator took more than 800 seconds to complete on the dataset 
with a hundred million rows, so they are not suitable for big data.
 """


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("Return column val as a list") \
        .getOrCreate()
    source_data = [
        ("toto", "toto_1", "00", 1),
        ("titi", "titi_1", "00", 2),
        ("tata", "tata_1", "00", 3),
        ("toto", "toto_2", "00", 4),
        ("toto", "toto_3", "00", 5),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "key", "content", "value"])
    print("######################Source df######################")
    source_df.show()

    # run solution1
    key_list1 = solution1(source_df, "key")
    print(key_list1)

    content_list1 = solution1(source_df, "content")
    print(content_list1)

    # run solution2
    key_list2 = solution2(source_df, "key")
    print(key_list2)

    content_list2 = solution2(source_df, "content")
    print(content_list2)

    # run solution3
    key_list3 = solution3(source_df, "key")
    print(key_list3)

    content_list3 = solution3(source_df, "content")
    print(content_list3)

    # run solution4
    key_list4 = solution4(source_df, "key")
    print(key_list4)

    content_list4 = solution4(source_df, "content")
    print(content_list4)

    # run solution5
    key_list5 = solution5(source_df, "key")
    print(key_list5)

    content_list5 = solution5(source_df, "content")
    print(content_list5)


if __name__ == "__main__":
    main()
