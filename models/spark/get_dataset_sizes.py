#!/usr/bin/env spark-submit
from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col, when
import pyspark.sql.functions as fn
import pyspark.sql.types as types
import schemas

outputfile="hdfs://analytix/user/ddavila/model/dataset_sizes.parquet"

print("===========================================================")
print("writing: "+outputfile)
print("===========================================================")

conf = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Get information from DBS about datatsets and Blocks
csvreader = spark.read.format("com.databricks.spark.csv").option("nullValue","null").option("mode", "FAILFAST")
dbs_datasets = csvreader.schema(schemas.schema_datasets()).load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/DATASETS/part-m-00000")
dbs_blocks = csvreader.schema(schemas.schema_blocks()).load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/BLOCKS/part-m-00000")


# We sum up the size of all the blocks that belong to a dataset to calculate the
# dataset size
datasets = (dbs_blocks
                .join(dbs_datasets, col('b_dataset_id') == col('d_dataset_id'))
                .groupBy('d_dataset_id', 'd_dataset','d_is_dataset_valid')
                .agg(
                    fn.sum('b_block_size').alias('dataset_size')
                )
           )
           
datasets.write.parquet(outputfile)

