# 3. Read data from various data sources

In this lesson, we will learn how to use spark to read/write data from various data sources.

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