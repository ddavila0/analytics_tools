# The Algorithm

The algorithm receives 3 DataFrames: **weeks_df**, **days_df** and **dataset_sizes** 
and a **policy range** as inputs and it will write a report with the workingset size
and the recalled size per week among other statistics as an output.

The policy range will indicate which are the deletion policies we want to
model e.g. the range (1, 3) will mean that we want to model the following
deletion policies: "every dataset gets removed after 1,2 and 3 weeks of not
being used"

The algorithm will execute independent iterations, throught the policy range
and perform the following 3 steps at each iteration:

**Step 1**. For each week on **weeks_df**, calculate the *workingset size* and
the set of datasets *freed* from disk and *recalled* form tape.

**Step 2**. Gets the day(timestamp) with more Bytes recalled.
One of the statistics we are interested in is the maximum number of Bytes
recalled from tape on a single day. 

**Step 3**. Using the sets of *recalled* and *freed* datasets from Step 1, it will calculate
for each of the weeks, the amount of Bytes freed and recalled.
Similarly to what is done for current_working_set_size in the step 1, we will use
the **dataset_sizes** to get the size on Bytes of each dataset.


