#!/usr/bin/env spark-submit
from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col, when
import pyspark.sql.functions as fn
import pyspark.sql.types as types
import schemas
import sys

year=sys.argv[1]
month=sys.argv[2]

inputfile="/project/monitoring/archive/condor/raw/metric/"+year+"/"+month+"/*/*.json.gz"
outputfile="hdfs://analytix/user/ddavila/model/data_tier_days_"+year+month+".parquet"

print("===========================================================")
print("reading: "+inputfile)
print("writing: "+outputfile)
print("===========================================================")

conf = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Get information from DBS about datatsets IDs
csvreader = spark.read.format("com.databricks.spark.csv").option("nullValue","null").option("mode", "FAILFAST")
dbs_datasets = csvreader.schema(schemas.schema_datasets()).load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/DATASETS/part-m-00000")
dbs_data_tiers = csvreader.schema(schemas.schema_data_tiers()).load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/DATA_TIERS/part-m-00000")

schema = types.StructType([
            types.StructField("data", types.StructType([
                    types.StructField("Status", types.StringType(), True),
                    types.StructField("Type", types.StringType(), True),
                    types.StructField("JobUniverse", types.StringType(), True),
                    types.StructField("DESIRED_CMSDataset", types.StringType(), True),
                    types.StructField("RecordTime", types.LongType(), True),
                 ]), False),
            ])

jobreports = spark.read.schema(schema).json(inputfile)
working_set_day = (jobreports
        .filter(col('data.Status')=="Completed")
        .filter(col('data.Type')=="analysis")
        .filter(col('data.JobUniverse')==5)
        .filter(col('data.DESIRED_CMSDataset').isNotNull())
        .withColumn('day_ts', (col('data.RecordTime')-col('data.RecordTime')%fn.lit(86400000))/fn.lit(1000))
        .withColumn('week_ts', (col('data.RecordTime')-col('data.RecordTime')%fn.lit(604800000))/fn.lit(1000))
        .join(dbs_datasets, col('data.DESIRED_CMSDataset')==col('d_dataset'))
        .join(dbs_data_tiers, col('d_data_tier_id')==col('data_tier_id'))
        .withColumn('data_data_tier_name_class',when(col('data_tier_name').isin("MINIAODSIM", "MINIAOD"),0).when(col('data_tier_name').isin("NANOAOD"),1).when(col('data_tier_name').isin("AOD"),2).otherwise(3))
        .groupBy('day_ts', 'week_ts', 'data_data_tier_name_class')
        .agg(
            fn.collect_set('d_dataset_id').alias('datasets_set'),
        )
    )

working_set_day.write.parquet(outputfile)

