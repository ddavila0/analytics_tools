The algorithm uses 3 DataFrames: **weeks_df**, **days_df** and **dataset_sizes**: 

1) In **days_df** each record will have a week timestamp,
'week_ts', a day timestamp 'day_ts' and the set of dataset IDs that were accessed
that day 'datasets_set'. See "The origin of days_df" below for more information on how is this data obtained.


| week_ts    | day_ts     | datasets_set |
|------------|------------|--------------|
|1562803200  |1562803200  |{1,2}         |
|1562803200  |1562716800  |{1,5}         |
|1562198400  |1562112000  |{3}           |
|1562198400  |1562025600  |{4}           |
**Note:** This is just an example of how **days_df** looks, it doesn't contain real data
<br><br>



2) **weeks_df** is the result of grouping the **days_df**
table by 'week_ts' and making a 'union' operation to create a unique set of all
the datasets read by the group.

| week_ts    | datasets_set |
|------------|--------------|
|1562803200  |{1,2,5}       |
|1562198400  |{3,4}         |
**Note:** This is just an example of how **weeks_df** looks, it doesn't contain real data
<br><br>


3) **dataset_sizes** contains each of the datasets and its size in
Bytes. See "The origin of dataset_sizes" below for more details on where this data is
coming from.


| d_dataset_id | dataset_size |
|--------------|--------------|
|1             | 100000000    |
|2             | 150000000    |
|3             | 1800000      |
**Note:** This is just an example of how **dataset_sizes** looks, it doesn't contain real data
<br><br>


## The origin of days_df

These data is obtained from HDFS using the spark cluster at CERN. The spark code
can be found here:
https://github.com/ddavila0/analytics_tools/blob/master/models/spark/get_datasets_per_day.py

The data in HDFS comes from 2 sources:
 * Condor Job Monitoring. A monitoring systems that records the condor job classAds of the CMS jobs
 * DBS(Data Agregation System). Provides information about the datastes read by the jobs 


The most siginificant par of the code is the following and the different parts are explained below:
```
working_set_day = (jobreports
        .filter(col('data.Status')=="Completed")
        .filter(col('data.Type')=="analysis")
        .filter(col('data.JobUniverse')==5)
        .filter(col('data.DESIRED_CMSDataset').isNotNull())
        .withColumn('day_ts', (col('data.RecordTime')-col('data.RecordTime')%fn.lit(86400000))/fn.lit(1000))
        .withColumn('week_ts', (col('data.RecordTime')-col('data.RecordTime')%fn.lit(604800000))/fn.lit(1000))
        .join(dbs_datasets, col('data.DESIRED_CMSDataset')==col('d_dataset'))
        .join(dbs_data_tiers, col('d_data_tier_id')==col('data_tier_id'))
        .withColumn('data_data_tier_name_class',when(col('data_tier_name').isin("MINIAODSIM", "MINIAOD"),0)\
            .when(col('data_tier_name').isin("NANOAOD"),1)\
            .when(col('data_tier_name').isin("AOD"),2).otherwise(3))
        .groupBy('day_ts', 'week_ts', 'data_data_tier_name_class')
        .agg(
            fn.collect_set('d_dataset_id').alias('datasets_set'),
        )
    )
```

The first part filters out job records that are not interesting for this
study. We are only focusing on "analysis" jobs coming form CRAB that are "Completed"
and have "JobUniverse" 5 (there are certain utility CRAB jobs with JobUniverse 12
that we want to filter out). Also we are only interested in those jobs which read a dataset so that they must
have a not null value on the filed 'DESIRED_CMSDataset'. 

```
.filter(col('data.Status')=="Completed")
.filter(col('data.Type')=="analysis")
.filter(col('data.JobUniverse')==5)
.filter(col('data.DESIRED_CMSDataset').isNotNull())
``` 

The second part adds two aditional columns to the table:
 * 'day_ts' The day timestamp when the job was completed
 * 'week_ts' The week timestamp when the job was completed

This two aditional columns are created from the column 'RecordTime' which is the
job's completion timestamp in miliseconds. These new columns would be used later
on to group all jobs wich were completed on the same week and/or the same day


```
.withColumn('day_ts', (col('data.RecordTime')-col('data.RecordTime')%fn.lit(86400000))/fn.lit(1000))
.withColumn('week_ts', (col('data.RecordTime')-col('data.RecordTime')%fn.lit(604800000))/fn.lit(1000))
``` 

Then we will join the records from the job monitoring with the DBS records so
that we can obtain information about the datasets read by the jobs, more 
specifically we want to get the dataset Id(d_data_tier_id) and the datatier
name(data_tier_name). 

```
.join(dbs_datasets, col('data.DESIRED_CMSDataset')==col('d_dataset'))
.join(dbs_data_tiers, col('d_data_tier_id')==col('data_tier_id'))
```

The next step is to add another aditional column that will help us to
classify the records into 4 categories according to the datatier of 
the dataset read by the job. These are the 4 categories: 

 * NANO. All datasets that belong to the NANOAOD datatier
 * MINI. All datasets that belong to either the dataset MINIAOD or MINIAODSIM
 * ADO.  All datasets that belong to the AOD 
 * REST. All of the datasets that don't fall into any of the above's categories

```
.withColumn('data_data_tier_name_class',when(col('data_tier_name').isin("MINIAODSIM", "MINIAOD"),0)\
.when(col('data_tier_name').isin("NANOAOD"),1)\
.when(col('data_tier_name').isin("AOD"),2).otherwise(3))
```

Finally, jobs wich were completed within the same day and read a dataset within
the same datatier category will be group together. All the dataset IDs of a group
of jobs are put together in a set called 'datasets_set'. We keep the week timestamp
'week_ts' to perform another aggregation in a further step on the algorithm. 

```
.groupBy('day_ts', 'week_ts', 'data_data_tier_name_class')
 .agg(
     fn.collect_set('d_dataset_id').alias('datasets_set'),
 )
```

## The origin of dataset_sizes

These data is obtained from HDFS using the spark cluster at CERN. The spark code
can be found here:
https://github.com/ddavila0/analytics_tools/blob/master/models/spark/get_dataset_sizes.py

The most significant part of the code is this and it is explained below:

```
datasets = (dbs_blocks
                .join(dbs_datasets, col('b_dataset_id') == col('d_dataset_id'))
                .groupBy('d_dataset_id', 'd_dataset','d_is_dataset_valid')
                .agg(
                    fn.sum('b_block_size').alias('dataset_size')
                )
           )
```

We use two tables from the DBS database: dbs_blocks and dbs_datasets. Given that
the dbs_datasets table doesn't contain the size of each dataset, we need to get
this data from the dbs_blocks table which has the information about the size of
each block and the dataset it belongs to, this way we can sum up the size of all 
the blocks that belong to a dataset to get the size of a dataset.

Notice that the size of a dataset can vary with time. I've run the code described
in this section twice with separation of 2 weeks and found
slightly different sizes in few datasets (~0.5%). Looking at those cases I found
that the "d_last_modification_date' fell into the two week period between the
the executions.



