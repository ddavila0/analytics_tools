# Modeling disk space required and tape recall based on deletion policy

Commonly a CMS analysis job will read a dataset, apply some analysis on it and write
the results in a new dataset. These datasets are spread on the
sites that pledges resources to CMS and depending on the demand, these datasets can
be stored either in disk or tape. When a job requests a dataset that is not in disk, 
the dataset will be recalled from tape and written into disk.

Recalling a dataset from tape takes a long time so we want to avoid it as much as possible
on the other hand, having all the datasets on disk is very expensive, so we want to minimize
this cost as well.

Given that the disk space is not infinite, we need a mechanism in place that periodically
removes datasets in disk to make space for new datasets requested
that are not in disk. The way to decide what is removed and what is kept is known as the 
"deletion policy"

In this work we are modeling how much disk would be necessary and how much data is
recalled from tape when different deletion policies are applied.

We look at the historical data of the analysis jobs to find when and which datasets were
accessed in the past and then apply an algorithm that will calculate the disk space
necessary and amount of Bytes recalled from tape, as well as other similar statistics,
when different deletion policies are used.

