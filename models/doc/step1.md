# Step 1 detailed

At this step the algorithm will receive a specific deletion policy 
and will iterate through the list of weeks in
**weeks_df** on chunks on N weeks, where N=policy.

It will start at the (N)est week and move forward one week at a time. Then it
will calculate 3 intermediate datasets: *old_week*, *new-week* and *int_ws* 
that will help us to calculate: *current_working_set*, *to_recall* and *to_free*.
Using *current_working_set* and the DataFrame **datasets_size** it will calculate
*current_working_set_size*.

Finally at the end of the iteration, the 3 sets: 
**current_working_set_size**, **to_recall** and **to_free** will be appended to
3 independent lists.

Following we will describe all of the intermediate and final products and how
are they calculated.


## new_week, old week, int_ws

Given that we are looking at a week (i) from **weeks_df** and that the policy = N
 * **new_week**. would be the set of datasets accessed within the week  i  
 * **old_week**. would be the set of datasets accessed within the week N weeks before i
 * **int_ws**(intermediate workingset). would be the set of datasets accessed in
    all the weeks between **old_week** and **new_week** (not including either of them)

```
new_week: weeks[i] 
old_week: weeks[i-N]
int_ws: 
for j in range(i-N+1, i-1):
    int_ws= int_ws.union(weeks[j]
```
Once we have new_week, old_week and int_ws, we procees to calculate the
*current_working_set*, the *to_recall* and *to_free*.


## current_working_set, to_recall, to_free 

Given that we have a  week (i) from **weeks_df** and that the policy = N
 * **working_set** is the set of datsets kept in disk at the end of the week i
 * **to_recall** is the set of datasets that were recalled during the week i
 * **to_free** is the set of datasets that will be removed from disk, given that they
    haven't been used in the last N weeks

```
current_working_set = int_ws.union(new_week)
to_recall = (new_week - (int_ws.union(old_week)))
to_free = old_week - (int_ws.union(new_week))
```
Notice that when N=1, int_ws = null and the alogrithm continues to work as expected:

 * current_working_set = new_week
 * to_recall = new_week - old_week
 * to_free old_week - new_week


The above 3 sets represent the status, in a given week, of all the datasets: in
disk, removed from disk and recalled from tape. If now we want to get the amount
Bytes instead, we need to get the size of each of the datasets and sum them up
in their respective sets. 


##current_working_set_size

On this step (Step 1), we will calculate the amount of Bytes only for the 
**current_working_set**, in the case of **to_recall** and **to_free** this 
will be done at Step 3.

Using the **datasets_size** we will calculate the amount of Bytes in the current
working set.

```
current_working_set_size = get_size_of_datasets_set(current_working_set, datasets_size) 
```

## The lists

At this point we can forget of **current_working_set**, since it is not used in
in the next steps. Then up until now we have, for a given week (i),: 

 * current_working_set_size
 * to_recall
 * to free

Given that we are interested in keeping the above sets for each of the weeks in
**weeks_df**, we will create a list for each of these sets and append a new set
for every week as we iterate through **weeks_df**

At the end of the Step 1 we will have produced 3 lists:
 * working_set_size_per_week
 * recalled_per_week
 * freed_per_week

Each of these list have as many items as weeks on **weeks_df** and together they
represent the status of disk usage(in Bytes), the set of datasets recalled from
tape and the datasets removed from disk at each week for a given delete policy.


