# dataset explication page: https://github.com/huggingface/datasets/tree/master/datasets/allocine
# dataset download page: "https://github.com/TheophileBlard/french-sentiment-analysis-with-bert/raw/master/allocine_dataset/data.tar.bz2"
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


def exp1(spark: SparkSession):
    file_path = "/home/pliu/Downloads/dummy_data/json_data/allo_cine/train.jsonl"
    df = spark.read.json(file_path)
    df.printSchema()
    df.show()


""" Exp2 Read json file contains multiple lines.
A normal json file has the format
{record_1}
{record_2}
{record_3}
And each record shares the same schema.
A json file that has multiple lines has the following form:
[{record_1},
{record_2},
{record_3}
]
The records are located in a list.
 
PySpark JSON reader accept only the normal json format. To read JSON files scattered across multiple lines, we must 
active the "multiline" option. By default the "multiline" option, is set to false.

For example, below json file is a multiple line json file.
[{
  "RecordNumber": 2,
  "Zipcode": 704,
  "ZipCodeType": "STANDARD",
  "City": "PASEO COSTA DEL SUR",
  "State": "PR"
},
{
  "RecordNumber": 10,
  "Zipcode": 709,
  "ZipCodeType": "STANDARD",
  "City": "BDA SAN LUIS",
  "State": "PR"
}]
"""


def exp2(spark: SparkSession):
    file_path = "/home/pliu/Downloads/dummy_data/json_data/multiline.json"
    # try to remove .option() and see what happens
    df = spark.read.option("multiline", "true").json(file_path)
    df.printSchema()
    df.show()


""" Exp3: Read multiple json files
If we put multiple file path in a list, spark.read.json() is capable to read all these file and output their content
in the same dataframe. 
Note their content must have the same schema.
"""


def exp3(spark: SparkSession):
    file_path1 = "/home/pliu/Downloads/dummy_data/json_data/allo_cine/train.jsonl"
    file_path2 = "/home/pliu/Downloads/dummy_data/json_data/allo_cine/test.jsonl"
    file_path3 = "/home/pliu/Downloads/dummy_data/json_data/allo_cine/val.jsonl"

    df = spark.read.json([file_path1, file_path2, file_path3])
    df.printSchema()
    print(f"The number of rows is {df.count()}")
    df.show()


""" Exp4: Read all files under a directory
If we use *(wild card), we can read all files that satisfy the specific file format
"""


def exp4(spark: SparkSession):
    file_path_wild_card = "/home/pliu/Downloads/dummy_data/json_data/allo_cine/*.jsonl"
    df = spark.read.json(file_path_wild_card)
    df.printSchema()
    print(f"The number of rows is {df.count()}")
    df.show()


""" Exp5: Read json file with explicit schema
In the previous example, spark infer the schema automatically. We can also give a schema explicitly to avoid the 
inferred schema.
"""


def exp5(spark: SparkSession):
    file_path = "/home/pliu/Downloads/dummy_data/json_data/allo_cine/train.jsonl"
    # note as the json file already has the field name, if your schema does not match with these field name
    # you will get null value for the mis match column.
    schema = StructType([StructField("film-url", StringType(), True),
                         StructField("polarity", LongType(), True),
                         StructField("review", StringType(), True)
                         ])
    df = spark.read.schema(schema).json(file_path)
    df.printSchema()
    df.show()


""" Exp 6: Read json file with various options
You can find all possible options in 
https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.json.html

"""


def exp6(spark: SparkSession):
    root_path = "/home/pliu/data_set/nlp/allo_cine"
    train = f"{root_path}/train.jsonl"
    test = f"{root_path}/test.jsonl"
    val = f"{root_path}/val.jsonl"
    train_df = spark.read.json(train)
    test_df = spark.read.json(test)
    val_df = spark.read.json(val)
    # print(f"tain_df has row: {train_df.count()}")
    # print(f"test_df has row: {test_df.count()}")
    # print(f"val_df has row: {val_df.count()}")
    train_df.coalesce(1).write.options(header='True', delimiter="|").mode("overwrite").csv(f"{root_path}/train")
    # test_df.coalesce(1).write.options(header='True', delimiter="|").mode("overwrite").csv(f"{root_path}/test")
    val_df.coalesce(1).write.options(header='True', delimiter="|").mode("overwrite").csv(f"{root_path}/val")


""" 

"""


def main():
    spark = SparkSession.builder.master("local[2]").appName("read_json").getOrCreate()

    # run exp1
    # exp1(spark)

    # run exp2
    # exp2(spark)

    # run exp3
    # exp3(spark)

    # run exp4
    # exp4(spark)

    # run exp5
    # exp5(spark)
    exp6(spark)


if __name__ == "__main__":
    main()
