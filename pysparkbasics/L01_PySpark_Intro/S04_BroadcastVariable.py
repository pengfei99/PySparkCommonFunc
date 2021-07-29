""" Broadcast variables
Broadcast variables are read-only shared variables that are cached and available on all nodes in a
cluster in-order to access or use by the tasks. Instead of sending this data along with every task,
Spark distributes broadcast variables to the workers using efficient broadcast algorithms to reduce
communication costs. It means all executor in the same worker can share the same broadcast variable

Note that broadcast variables are not sent to workers with sc.broadcast(variable) call instead, they will
be sent to executors when they are first used.

"""
from pyspark.sql import SparkSession

""" Exp1 : We have a rdd which contains the state code, we have a stats map which maps the code to a complete name
The result rdd must use the complete state name to replace the state code.

"""


def exp1(spark: SparkSession, data, broadcast_var):
    rdd = spark.sparkContext.parallelize(data)
    # get the broadcast state map
    states_map = broadcast_var.value
    result = rdd.map(lambda x: (x[0], x[1], x[2], states_map[x[3]])).collect()
    print("Exp1: after the rdd map on broadcast var ")
    print(result)


""" Exp2: Idem to exp1, we just do it with a data frame instead of an rdd"""


def exp2(spark: SparkSession, data, broadcast_var):
    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data, schema=columns)
    print("Exp2: Source data frame")
    df.show()
    # get the broadcast state map
    states_map = broadcast_var.value
    df1 = df.rdd.map(lambda x: (x[0], x[1], x[2], states_map[x[3]])).toDF(columns)
    print("Exp2: after the data frame map on broadcast var ")
    df1.show()

    # Once the variable is broadcast, we can use it in any dataframe operation which is impossible for local variable
    # Because executors does not have access on the local variables defines on spark drivers
    local_states_map = ["NY"]
    try:
        df2 = df.where(df.state.isin(local_states_map))
        df2.show()
    except:
        print("Can't use local variables in dataframe operations")

    # isin takes a list, so we need to get the keys of the dict and return it to a list, and I only take the first
    # element of the list.
    keys = list(states_map.keys())[0:1]
    df3 = df.where(df["state"].isin(keys))
    df3.show()


def main():
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName("Broadcast variables") \
        .getOrCreate()
    # here, we broadcast states_map
    states_map = {"NY": "New York", "CA": "California", "FL": "Florida"}
    broadcast_states = spark.sparkContext.broadcast(states_map)

    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]
    print("source data: \n {}".format(data))

    # run example 1
    # exp1(spark, data, broadcast_states)

    # run example 2
    exp2(spark, data, broadcast_states)


if __name__ == "__main__":
    main()
