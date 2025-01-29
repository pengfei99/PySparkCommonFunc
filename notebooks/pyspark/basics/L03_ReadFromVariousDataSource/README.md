# 3. Read data from various data sources

In this lesson, we will learn how to use spark to read/write data from various data sources.


## schema determination

When the data source file format is not structured such as `csv`,`json`, the schema of the dataframe is not given by
the data source. You need to decide how to define the schema. You have three options:
1. Give explicitly a schema (with spark StructType, StructField)
2. Set infer schema = True
3. Set infer schema = False

### Give a schema

You can define a schema with StructType, and StructField. when read the data source, you can provide the schema

```python
schema = StructType([
StructField("customer_id”, IntegerType(), nullable=True),
StructField("customer_name”, StringType(), nullable=True),
StructField("salary”, DoubleType(), nullable=True)
])
```

This method ensure that the schema is enforced and the data types are correctly assigned to the columns when loading the data.

### Infer schema 

When inferSchema is set to true , Spark scans the dataset (either a sample or the entire dataset, depending on configuration) 
to automatically infer the data types of each column. For instance, if a column contains numeric values, Spark will infer it as IntegerType or DoubleType .

The `data scanning to infer the schema occurs lazily`. This means the scanning process **does not happen immediately** 
when you set inferSchema to true , but only when an action (such as show() , count () etc.) triggers the Dataframes evaluation.

#### Disadvantages of Using inferSchema :

- Performance Impact: Enabling inferSchema can significantly increase the time required to load large datasets, as Spark must scan the data to infer the types of each column. This process can be resource-intensive and slow, especially for large files.
- Schema Accuracy Issues: Spark may not always infer the correct data types for certain columns, leading to 
                        inaccuracies in data processing. Since Spark uses sampling to infer the schema, 
                        the sample may not be fully representative of the entire dataset, which could result in 
                        `incorrect data type assignments` or `runtime error`.
- Handling Complex Data: Columns with mixed data types or edge cases in the data may not be correctly inferred, 
                         leading to potential issues in downstream processing.

#### Sampling Ratio

When infer schema set to true, spark reads the **first 100 rows per partition** as sampling data to determine schema.

You can set a custom value to overwrite the default value. The below code is an example

```python
# setting samplingRatio to 0.1 means Spark will scan 10% of the data to infer the schema, rather than first 100 rows 
df = spark.read.option("inferSchema", "true").option("samplingRatio", 0.1).csv("data.csv")
```


> If data types are inconsistent across rows, you need to increase samplingRatio (e.g., 0.5 or 1.0)

### Infer schema disabled 

If you do not provide a schema and the infer-schema option is disabled. Spark will read all columns as string type

## Save mode

All spark data source connector can be configured with below save mode options:

|Scala/Java code|    Option config (Any Language) |    Meaning |
|---------|-------------------------------|----------|
|SaveMode.ErrorIfExists (default) |    "error" or "errorifexists" (default)    | When saving a DataFrame to a data source, if data already exists, an exception is expected to be thrown.|
|SaveMode.Append |    "append" |    When saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.|
|SaveMode.Overwrite    | "overwrite"    | Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.|
|SaveMode.Ignore |    "ignore" |    Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected not to save the contents of the DataFrame and not to change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.|


## Saving to Persistent Tables

DataFrames can also be saved as persistent tables into **Hive metastore** using the `saveAsTable` command. 
Notice that an existing Hive deployment is not necessary to use this feature. Spark will create a default local 
Hive metastore (using Derby) for you. Unlike the createOrReplaceTempView command, saveAsTable will materialize the 
contents of the DataFrame and create a pointer to the data in the Hive metastore. Persistent tables will still 
exist even after your Spark program has restarted, as long as you maintain your connection to the same metastore. 
A DataFrame for a persistent table can be created by calling the table method on a SparkSession with the name of the table.

For file-based data source, e.g. text, parquet, json, etc. you can specify a custom table path via the path option, 
e.g. df.write.option("path", "/some/path").saveAsTable("t"). When the table is dropped, the custom table path will 
not be removed and the table data is still there. If no custom table path is specified, Spark will write data to a 
default table path under the warehouse directory. When the table is dropped, the default table path will be removed too.

Starting from Spark 2.1, persistent datasource tables have per-partition metadata stored in the Hive metastore. 
This brings several benefits:

- Since the metastore can return only necessary partitions for a query, discovering all the partitions on the first 
query to the table is no longer needed.
- Hive DDLs such as ALTER TABLE PARTITION ... SET LOCATION are now available for tables created with the Datasource API.
- 
Note that partition information is not gathered by default when creating external datasource tables (those with a path option). 
To sync the partition information in the metastore, you can invoke MSCK REPAIR TABLE.

## Bucketing, sorting, partitioning

https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html