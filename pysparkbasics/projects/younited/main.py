from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, unix_timestamp, min
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder.master("local[2]").appName("younited_challenge").getOrCreate()
    score_path = "data/score_date.csv"
    user_path = "data/user_event_date.csv"

    print("Raw source data")
    df_score = spark.read.option("header", True).csv(score_path)

    df_score.printSchema()
    df_score.show(5, truncate=False)

    df_user = spark.read.option("header", True).csv(user_path)
    df_user.printSchema()
    df_user.show(5, truncate=False)

    ############################ Step1: join the user with score and convert date #################################

    df_ts = df_score.join(df_user, "id", "left") \
        .withColumn("score_timestamp", unix_timestamp("score_date", "yyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("funnel_timestamp", unix_timestamp("funnel_date", "yyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("signature_timestamp", unix_timestamp("signature_date", "yyy-MM-dd'T'HH:mm:ss"))
    df_ts.show(10, truncate=False)

    ############################# Step2:  get date diff for each score #################################
    df_date_diff = df_ts.withColumn("funnel_s_diff", df_ts.funnel_timestamp - df_ts.score_timestamp) \
        .withColumn("signature_s_diff", df_ts.signature_timestamp - df_ts.score_timestamp)
    df_date_diff.show()

    # ############################ Step3:  get best score before funnel date #################################
    # can't use group by, because we lose score column after group by
    # so use window function
    win_spec = Window.partitionBy("id")
    df_funnel_min = df_date_diff.filter(df_date_diff.funnel_s_diff > 0).select("id", "score", "funnel_s_diff") \
        .withColumn("min_time", min(col("funnel_s_diff")).over(win_spec))

    df_funnel_score = df_funnel_min.select("id", "score").filter(col("min_time") == col("funnel_s_diff")) \
        .withColumnRenamed("score", "best_score_before_funnel")

    # ############################ Step4:  get best score before signature date #################################
    # can't use group by, because we lose score column after group by
    # so use window function
    win_spec = Window.partitionBy("id")
    df_sig_min = df_date_diff.filter(df_date_diff.signature_s_diff > 0).select("id", "score", "signature_s_diff") \
        .withColumn("min_time", min(col("signature_s_diff")).over(win_spec))

    df_sig_score = df_sig_min.select("id", "score") \
        .filter(col("min_time") == col("signature_s_diff")) \
        .withColumnRenamed("score", "best_score_before_signature")

    # ########################## Step4: join ##############################
    df_final = df_funnel_score.join(df_sig_score, "id", "left").fillna("-1", "best_score_before_signature").orderBy("id")
    df_final.show()


if __name__ == "__main__":
    main()
