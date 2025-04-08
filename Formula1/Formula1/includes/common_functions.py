# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return (output_df)

# COMMAND ----------

def rearrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.columns:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return (output_df)

# COMMAND ----------

def incremental_write(input_df, database, table_name, partition_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{database}.{table_name}")):
        input_df.write.mode("overwrite").insertInto(f"{database}.{table_name}")
    else:
        input_df.write.mode("append").format("parquet").partitionBy(f"{partition_column}").saveAsTable(f"{database}.{table_name}")

# COMMAND ----------

def merge_delta_data(input_df, database, table_name, partition_column, folder_path, merge_condition):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")    
    
    from delta.tables import DeltaTable

    if (spark._jsparkSession.catalog().tableExists(f"{database}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(input_df.alias("src"), merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        input_df.write.mode("append").format("delta").partitionBy(f"{partition_column}").saveAsTable(f"{database}.{table_name}")