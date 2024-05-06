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


pjc_out_file_path = "hdfs:///test_pengfei/pjc_converted"
pjc_df_1.write.parquet(pjc_out_file_path)
# Stop the SparkSession
spark.stop()
