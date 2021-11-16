from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def main():
    spark = SparkSession.builder.master("local[2]").appName("didomi_challenge").getOrCreate()
    trips_path = "data/trips.csv"
    users_path = "data/users.csv"

    print("Raw source data")
    df_trips = spark.read.option("header", True).csv(trips_path)

    df_trips.printSchema()
    df_trips.show(5, truncate=False)

    df_users = spark.read.option("header", True).csv(users_path)
    df_users.printSchema()
    df_users.show(5, truncate=False)

    ############################ Step1: remove all banned user's request #################################

    df_comp = df_trips.join(df_users, df_trips.client_id == df_users.users_id, "inner") \
        .withColumnRenamed("banned", "banned_user").drop("users_id").drop("role") \
        .join(df_users, df_trips.driver_id == df_users.users_id, "inner") \
        .withColumnRenamed("banned", "banned_driver").drop("users_id").drop("role")

    df_not_banned = df_comp.filter((col("banned_user") == "No") & (col("banned_driver") == "No"))
    df_not_banned.show(10)
    ############################ Step2:  get total request count groupby date #################################
    df_total_request = df_not_banned.groupBy("request_at").count().withColumnRenamed("count", "total")

    ############################ Step3:  get cancelled request count groupby date #################################

    df_cancel = df_not_banned.withColumn("cancel", when(col("status") == "cancelled_by_client", 1)
                                         .when(col("status") == "cancelled_by_driver", 1)
                                         .otherwise(0))
    df_cancel_request = df_cancel.groupBy("request_at").sum("cancel").withColumnRenamed("sum(cancel)", "cancel")
    df_total_request.show()
    df_cancel_request.show()

    ########################## Step4: join ##############################
    df1 = df_total_request.join(df_cancel_request, "request_at", "inner") \
        .withColumn("Cancellation_Rate", col("cancel") / col("total")) \
        .withColumnRenamed("request_at", "Day").select("Day", "Cancellation_Rate").orderBy("Day")
    df1.show()
    # df1


if __name__ == "__main__":
    main()
