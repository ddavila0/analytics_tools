# Modeling disk space required and tape recall based on deletion policy

Commonly a CMS analysis job will read a dataset, apply some analysis on it and write
the results in a new dataset.These datasets are spread on the
sites that pledges resources to CMS and depending on the demand, these datasets can
be stored either in disk or tape. When a job requests a dataset that is not in disk, 
the dataset will be recalled from tape and written into disk.

Recalling a dataset from tape takes a long time so we want to avoid it as much as possible
on the other hand, having all the datasets on disk is very expensive, so we want to minimize
this cost as well.

Similarly to what happens in a cache, when the amount of disk space is reached, it becomes
necessary to make some space for new datasets being recalled from tape. The algorithm
used to decide what is removed is known as the "deletion policy"

In this work we are modeling how much disk would be necessary and how much is recalled
when different deletion policies are applied.

We look at the historical data of the analysis jobs to find when and which datasets were
accessed in the past and then apply an algorithm that will calculate the amount of disk
and Bytes recalled from tape, as well as other similar statistics, for a given policy.


## The data preparation


### Fetching from HDFS
### Building the DataFrames to be input into the algorithm


## The algorithm

The algorithm receives 3 DataFrames: **weeks_df**, **days_df** and **dataset_sizes** 
and a policy range as input and it will write a report with the workingset size
and the recalled size per week among other statistics as an output.
The 3 DataFrames contain the folowing information and format:


1) DataFrame **weeks_df**. This is the main Dataframe. Each record in this DataFrame
contains a week timestamp 'week_ts' and the set of dataset IDs accessed in that
week.

| week_ts    | datasets_set |
|------------|--------------|
|1562803200  |{1,2,5}       |
|1562198400  |{3,4}         |
<br><br>

2) DataFrame **days_df**. It is very similar to the above DataFrame but in this case
each record will have, apart from the week timestamp, the day timestamp 'day_ts'
and the set of datasets IDs that were accessed that day. The DataFrame **weeks_df**
is the result of grouping the **days_df** by 'week_ts'.


| week_ts    | day_ts     | datasets_set |
|------------|------------|--------------|
|1562803200  |1562803200  |{1,2}         |
|1562803200  |1562716800  |{1,5}         |
|1562198400  |1562112000  |{3}           |
|1562198400  |1562025600  |{4}           |
<br><br>


3) DataFrame **dataset_sizes**. It contains each of the datasets and its size in Bytes

| d_dataset_id | dataset_size |
|--------------|--------------|
|1             | 100000000    |
|2             | 150000000    |
|3             | 1800000      |
<br><br>


## Writing reports
