from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp, col, from_json, date_format


def group_by_count(df: DataFrame, group_col_name: str, count_alias: str) -> DataFrame:
    return df.groupBy(group_col_name).count().withColumnRenamed("count", count_alias)


def get_group_distinct_user_number(df: DataFrame, group_col_name, spark: SparkSession) -> DataFrame:
    result = []
    group_distinct_val_list = list(df.select(group_col_name).distinct().toPandas()[group_col_name])
    for group_distinct_val in group_distinct_val_list:
        # get distinct user number of the specific col value
        user_number = df.filter(col(group_col_name) == group_distinct_val).select("user_id").distinct().count()
        # print("{}:{}".format(group_distinct_val, user_number))
        result.append((group_distinct_val, user_number))
    return spark.createDataFrame(result, schema=[group_col_name, 'distinct_user_number'])


# note the two df must are the result of the same groupBy condition (e.g. country, domain, date)
def get_avg_view_per_user(df_distinct_user_number: DataFrame, df_view_number: DataFrame,
                          join_cond_col_name) -> DataFrame:
    df_tmp = df_distinct_user_number.join(df_view_number, join_cond_col_name, "inner")
    return df_tmp.withColumn("avg_pageviews_per_user", col("pageviews") / col("distinct_user_number")) \
        .drop("distinct_user_number").drop("pageviews")


def main():
    spark = SparkSession.builder.master("local[2]").appName("didomi_challenge").getOrCreate()
    root_path = "/home/pliu/data_set/didomi_challenge_input"
    data_path1 = "{}/datehour=2021-01-23-10".format(root_path)
    data_path2 = "{}/datehour=2021-01-23-11".format(root_path)

    df = spark.read.json(
        ['{}/*.json'.format(data_path1), '{}/*.json'.format(data_path2)])
    print("Raw source data")
    df.printSchema()
    df.show(5, truncate=False)
    ############################ Step1: preprocess data #################################
    # row number before dedup
    print("Row number before dedup: {}".format(df.count()))

    # remove duplicate based on event id
    df_dedup = df.dropDuplicates(["id"])
    print("Row number after dedup: {}".format(df_dedup.count()))

    # convert string date to timestamp
    df_cleaned = df_dedup.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-DD HH:mm:ss")) \
        .withColumn("datetime", date_format("datetime", "yyyy-MM-DD-HH"))
    df_cleaned.printSchema()
    df_cleaned.show(5, truncate=False)

    # flat struct type
    df_flat = df_cleaned.withColumn("user_country", df_cleaned.user.country) \
        .withColumn("user_id", df_cleaned.user.id).withColumn("user_token", df_cleaned.user.token).drop('user')
    df_flat.show(5, truncate=False)

    # convert json token to flat column
    # get the json string column schema.
    json_schema = spark.read.json(df_flat.select("user_token").rdd.map(lambda row: row.user_token)).schema
    # print(json_schema)
    df_token_flat = df_flat.withColumn("token", from_json(col("user_token"), schema=json_schema)).drop("user_token")
    df_ready = df_token_flat.withColumn("consent", df_token_flat.token.purposes.enabled.getItem(0)).drop("token")
    # finish data preparation, ready for stats analyze
    df_ready.printSchema()
    df_ready.show(5, truncate=False)
    df_ready.cache()

    ############################ Step2: get metric #################################
    # total page view groupBy date, country, and domain
    df_page_view = df_ready.filter(col("type") == "pageview")
    df_page_view_groupby_date = df_page_view.groupBy("datetime").count().select("datetime",
                                                                                col("count").alias("pageviews"))
    df_page_view_groupby_country = group_by_count(df_page_view, "user_country", "pageviews")
    df_page_view_groupby_domain = group_by_count(df_page_view, "domain", "pageviews")
    # df_page_view_groupby_domain.show()

    # page view with consent groupby date, country domain
    df_page_view_with_consent = df_ready.filter(col("type") == "pageview").na.drop(subset=["consent"])
    df_page_view_consent_date = group_by_count(df_page_view_with_consent, "datetime", "pageviews_with_consent")
    df_page_view_consent_country = group_by_count(df_page_view_with_consent, "user_country", "pageviews_with_consent")
    df_page_view_consent_domain = group_by_count(df_page_view_with_consent, "domain", "pageviews_with_consent")

    # total consents_asked groupBy
    df_consents_asked = df_ready.filter(col("type") == "consent.asked")
    df_consents_asked_date = group_by_count(df_consents_asked, "datetime", "consents_asked")
    df_consents_asked_country = group_by_count(df_consents_asked, "user_country", "consents_asked")
    df_consents_asked_domain = group_by_count(df_consents_asked, "domain", "consents_asked")

    # total consents_given
    df_consents_given = df_ready.filter(col("type") == "consent.given")
    df_consents_given_date = group_by_count(df_consents_given, "datetime", "consents_given")
    df_consents_given_country = group_by_count(df_consents_given, "user_country", "consents_given")
    df_consents_given_domain = group_by_count(df_consents_given, "domain", "consents_given")
    # df_consents_given.show()

    # total consents_given with consent
    df_consents_given_with_consent = df_ready.filter(col("type") == "consent.given").na.drop(subset=["consent"])
    df_consents_given_with_consent_date = group_by_count(df_consents_given_with_consent, "datetime",
                                                         "consents_given_with_consent")
    df_consents_given_with_consent_country = group_by_count(df_consents_given_with_consent, "user_country",
                                                            "consents_given_with_consent")
    df_consents_given_with_consent_domain = group_by_count(df_consents_given_with_consent, "domain",
                                                           "consents_given_with_consent")

    # Average number of events of type pageviews per user
    df_distinct_user_by_country = get_group_distinct_user_number(df_page_view, "user_country", spark)
    df_avg_by_country = get_avg_view_per_user(df_distinct_user_by_country, df_page_view_groupby_country, "user_country")
    # df_avg_by_country.show()

    df_distinct_user_by_date = get_group_distinct_user_number(df_page_view, "datetime", spark)
    df_avg_by_date = get_avg_view_per_user(df_distinct_user_by_date, df_page_view_groupby_date, "datetime")
    # df_avg_by_date.show()

    df_distinct_user_by_domain = get_group_distinct_user_number(df_page_view, "domain", spark)
    df_avg_by_domain = get_avg_view_per_user(df_distinct_user_by_domain, df_page_view_groupby_domain, "domain")
    # df_avg_by_domain.show()

    ########################### Step3: join different metric to one final table ###########################
    # join page view with consent
    df_metric_by_date = df_page_view_groupby_date.join(df_page_view_consent_date, "datetime", "left")
    df_metric_by_country = df_page_view_groupby_country.join(df_page_view_consent_country, "user_country", "left")
    df_metric_by_domain = df_page_view_groupby_domain.join(df_page_view_consent_domain, "domain", "left")

    # join consents_asked
    df_metric_by_date = df_metric_by_date.join(df_consents_asked_date, "datetime", "left")
    df_metric_by_country = df_metric_by_country.join(df_consents_asked_country, "user_country", "left")
    df_metric_by_domain = df_metric_by_domain.join(df_consents_asked_domain, "domain", "left")

    # join consents_given
    df_metric_by_date = df_metric_by_date.join(df_consents_given_date, "datetime", "left")
    df_metric_by_country = df_metric_by_country.join(df_consents_given_country, "user_country", "left")
    df_metric_by_domain = df_metric_by_domain.join(df_consents_given_domain, "domain", "left")

    # join consents_given_with_consents
    df_metric_by_date = df_metric_by_date.join(df_consents_given_with_consent_date, "datetime", "left")
    df_metric_by_country = df_metric_by_country.join(df_consents_given_with_consent_country, "user_country", "left")
    df_metric_by_domain = df_metric_by_domain.join(df_consents_given_with_consent_domain, "domain", "left")

    # join avg page views per user
    df_metric_by_date = df_metric_by_date.join(df_avg_by_date, "datetime", "left")
    df_metric_by_country = df_metric_by_country.join(df_avg_by_country, "user_country", "left")
    df_metric_by_domain = df_metric_by_domain.join(df_avg_by_domain, "domain", "left")

    ############################ Step4: show final metric ############################################
    df_metric_by_date.show()
    df_metric_by_country.show()
    df_metric_by_domain.show()


if __name__ == "__main__":
    main()
