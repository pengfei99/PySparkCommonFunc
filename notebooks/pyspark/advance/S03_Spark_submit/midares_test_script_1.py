from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col, split, when

# Create a SparkSession
spark = SparkSession.builder \
    .master("yarn") \
    .appName("Midares test job") \
    .getOrCreate()

# prepare fna dataframes
parent_path = "hdfs:///dataset/MiDAS_v3"
fna_file_names = ["pjc", "odd", "dal", "cdt"]

fna_dfs = {name: spark.read.parquet(f"{parent_path}/FNA/{name}.parquet") for name in fna_file_names}

# load fhs dataframe
de_df = spark.read.parquet(f"{parent_path}/FHS/de.parquet")

empty_str_val = "Manquant"
other_str_val = "Autres"
unknown_str_val = "Inconnu"

# create new column (alloc_det, alloc, regime) in pjc
pjc_df_1 = fna_dfs["pjc"].withColumn("alloc_det", when(col("KCALF").isNull, empty_str_val)
                                     .when(col("KCALF") == "", empty_str_val)
                                     .when(col("KCALF").isin([]), "ARE")
                                     .when(col("KCALF").isin([]), "ARE-CG")
                                     .when(col("KCALF").isin([]), "AREF-CG")
                                     .when(col("KCALF").isin([]), "ARE-I")
                                     .when(col("KCALF").isin([]), "ARED")
                                     .when(col("KCALF").isin([]), "ARER")
                                     .when(col("KCALF").isin([]), "ARER-I")
                                     .when(col("KCALF").isin([]), "ARER-FI")
                                     .when(col("KCALF").isin([]), "ARERF")
                                     .when(col("KCALF").isin([]), "AREF")
                                     .when(col("KCALF").isin([]), "AREF-I")
                                     .when(col("KCALF").isin([]), "AREF-D")
                                     .otherwise(other_str_val)
                                     ) \
    .withColumn("alloc", when(col("alloc_det") == empty_str_val, empty_str_val)
                .when(col("alloc_det").isin([]), "ARE")
                .when(col("alloc_det").isin([]), "AREF")
                .otherwise(other_str_val)
                ) \
    .withColumn("regime", when(col("alloc_det").isin([]), "AC")
                .when(col("alloc_det").isin([]), "PE")
                .otherwise(unknown_str_val)
                )

# write df to hdfs
# pjc_out_file_path = "hdfs:///test_pengfei/pjc_converted"
# pjc_df_1.write.parquet(pjc_out_file_path)

# cache the dataframe
pjc_df_1.cache()
pjc_df_1.head()

# date format str
dateformat_str = "%Y-%m-%d"

seuil_debut = datetime.strptime("2017-01-01", dateformat_str)
seuil_fin = datetime.strptime("2023-08-31", dateformat_str)
seuil_raz = datetime.strptime("2021-07-01", dateformat_str)
seuil_fin_censure_17 = datetime.strptime("2018-08-31", dateformat_str)
seuil_fin_censure_18 = datetime.strptime("2019-08-31", dateformat_str)
seuil_fin_censure_19 = datetime.strptime("2020-08-31", dateformat_str)

# step 3: create temp dataframe
# 3.1 filter rows which KDDPJ date <= 2023-08-31
temp = pjc_df_1.select("id_midas", "KROD3", "KCPJC", "KQBPJP", "KDDPJ", "KDFPJ", "KCFPP", "alloc") \
    .filter(col("KDDPJ") <= seuil_fin) \
    .groupBy("id_midas", "KROD3")

# Stop the SparkSession
spark.stop()
