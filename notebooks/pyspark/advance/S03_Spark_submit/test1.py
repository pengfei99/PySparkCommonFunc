import os
import sys

from pyspark.sql import SparkSession, DataFrame
import requests
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col, split

ENV_KEY = "OSRM_HOST"

def read_code_list_from_file(filename):
    with open(filename, 'r') as file:
        lines = file.readlines()
        lines = [line.rstrip('\n') for line in lines]
        return lines


# required functions
def get_osrm_host(env_key, default: str = "127.0.0.1:5000") -> str:
    return os.getenv(env_key, default)


def get_route(lat_start: str, long_start: str, lat_end: str, long_end: str,
              show_steps: str = "false") -> dict:
    host = get_osrm_host(ENV_KEY)
    start_point = f"{long_start},{lat_start}"
    end_point = f"{long_end},{lat_end}"
    # Define the URL
    url = f"http://{host}/route/v1/driving/{start_point};{end_point}?steps={show_steps}"

    # Make the GET request
    response = requests.get(url, verify=False, timeout=10)
    json_response = None
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Print the response content
        json_response = response.json()
    else:
        print("Error:", response.status_code)
    return json_response


def parse_route_json(input_route: dict) -> (float, float):
    route = input_route['routes'][0]
    if route:
        # the raw distance is in meter
        distance = route["distance"]
        # the raw duration is in second
        # the returned duration is in minutes
        duration = round((route["duration"] / 60), 2)
    else:
        distance = 0
        duration = 0
    return distance, duration


def calculate_distance_duration(lat_start: str, long_start: str, lat_end: str, long_end: str) -> (float, float):
    route = get_route(lat_start, long_start, lat_end, long_end)
    return parse_route_json(route)


def calculate_distance_duration_str(lat_start: str, long_start: str, lat_end: str, long_end: str) -> str:
    distance, duration = calculate_distance_duration(lat_start, long_start, lat_end, long_end)
    return f"{distance};{duration}"


@udf(returnType=StringType())
def get_distance_duration(lat_start: str, long_start: str, lat_end: str, long_end: str):
    return calculate_distance_duration_str(lat_start, long_start, lat_end, long_end)


def calculate_distance_duration_matrix_in_patch(insee_code_list: list, centroid_df: DataFrame, output_file_path: str,
                                                patch_size: int = 4, partition_num: int = 4):
    # split the input insee code list into patch
    for i in range(0, len(insee_code_list), patch_size):
        patch_code_list = insee_code_list[i:i + patch_size]
        # 1. build the source commune df with the given insee code
        commune_df = centroid_df.filter(col("insee").isin(patch_code_list))
        # 2. build a matrix with given source commnue which joins all others commune
        commune_matrix_df = commune_df.alias("add1").join(centroid_df.alias("add2"),
                                                          col("add1.insee") != col("add2.insee"), "inner").select(
            col("add1.longitude").alias("source_long"), col("add1.latitude").alias("source_lat"),
            col("add1.insee").alias("source_insee"), col("add1.nom").alias("source_nom"),
            col("add2.longitude").alias("dest_long"), col("add2.latitude").alias("dest_lat"),
            col("add2.insee").alias("dest_insee"), col("add2.nom").alias("dest_nom"))
        commune_matrix_df = commune_matrix_df.repartition(partition_num)
        # 3. calculate the distance and duration
        distance_duration_df = commune_matrix_df.withColumn("distance_duration",
                                                            get_distance_duration(col("source_lat"), col("source_long"),
                                                                                  col("dest_lat"),
                                                                                  col("dest_long"))).select(
            "source_nom", "source_insee", "dest_nom", "dest_insee", "distance_duration").withColumn("distance(meter)",
                                                                                                    split(
                                                                                                        col("distance_duration"),
                                                                                                        ";")[
                                                                                                        0]).withColumn(
            "duration(minutes)", split(col("distance_duration"), ";")[1]).drop("distance_duration")
        # 4. write the result into a parquet file
        distance_duration_df.write.mode("append").partitionBy("source_insee").parquet(output_file_path)


def main():
    # get argument from command line
    if len(sys.argv) != 3:
        print("Usage: python calculate_distance_duration.py <osrm_host> <partition_number>")
        return

    # step1: set osrm host
    osrm_host = str(sys.argv[1])
    os.environ[ENV_KEY] = osrm_host
    partition = int(sys.argv[2])

    # input argument config
    # path of the code list split files parent dir
    code_list_parent_dir = "/home/pliu/fr_commune_distance/data/code_split_test"
    #
    start_part = 41
    end_part = 55
    # output result file path
    output_file_path = "/home/pliu/fr_commune_distance/data/duration_prod_part_41_55"

    # step2: Create a SparkSession
    spark = SparkSession.builder \
        .appName("Example of distance matrix") \
        .getOrCreate()

    # step3: read the converted french commune centroid parquet file
    fr_zone_file_path = "/home/pliu/data/converted_centroid_of_french_commune"
    converted_centroid_df = spark.read.parquet(fr_zone_file_path)
    converted_centroid_df.cache()
    converted_centroid_df.show(5)

    # step4: for each code list split part, calculate the distance and duration matrix
    for i in range(start_part, end_part+1):
        filename = f"{code_list_parent_dir}/part_{i}.txt"
        code_list = read_code_list_from_file(filename)
        calculate_distance_duration_matrix_in_patch(code_list, converted_centroid_df,
                                                    output_file_path,
                                                    partition_num=partition)

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
