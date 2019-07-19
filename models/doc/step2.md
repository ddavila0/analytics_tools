#Step 2 detailed

Once we have the set of datasets recalled per week from Step 1, we can use the
**days_df** and see in which day, each of the recalled datasets was accessed, then
get the size of each of the datasets usign **dataset_sizes**, add up all these
sizes for each of the days and get the day with the maximum size.

It is important to notice, that in the case that a given recalled dataset was
accessed in more than 1 day on the same week, we will only account for the first
access because that is the one the would trigger the recall, consecutive accesses
to the dame dataset posteriorly, will not trigger a recall.


