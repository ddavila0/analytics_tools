#Step 2 detailed

One of the products of the Step 1 is **recalled_per_week**, which is a list
of the sets of datasets recalled from tape in each of the weeks, for example,
the first item on that list will have all of the datasets that were recalled on
the first week.

What we want to calculate in this step is the maximum amount of Bytes recalled
on a single day. For that we first need to know for every week in **recalled_per_week**
in which day every dataset was recalled. We will do that using the **days_df**.

Let's say that **recalled_per_week** has the following data:

```
recalled_per_week = [{1,2,5},{4}]
```

So first of all we see that our study is only looking at the data of 2 weeks.
Then, we know that in the first week the dataset recalled are: 1 and 5
whereas in the second week only the dataset 4 was recalled.

If we look at the **day_df** below, we can see as well that there are
Only 2 weeks on this table, the first two rows belong to the first week
and the last two rows belong to the second week.

Notice that **days_df** will show all of the datasets accessed per day
whereas **recalled_per_week** will only show those dataset that were
recalled from tape, for example, the dataset (2) is accessed in the second
week but it is not recalled because it was also accessed the week before,
wich means that the dataset was already in disk.


| week_ts    | day_ts     | datasets_set |
|------------|------------|--------------|
|1562803200  |1562803200  |{1,2}         |
|1562803200  |1562716800  |{1,5}         |
|1562198400  |1562112000  |{2}           |
|1562198400  |1562025600  |{4}           |


By looking at both **recalled_per_week** and **days_df** we can tell in wich day
each of the datasets in **recalled_per_week** was recalled, for example, we can
see that in the first week 1 and 2 were recalled on the day with timestamp 1562803200
and 5 was recalled on 1562716800. In this case we know that 1 could not be recalled
on day 1562716800 because it was already recalled the day before.

Now let's say we have this intermediate table representing the datasets recalled
per day

| day_ts     | recalled datasets_set |
|------------|-----------------------|
|1562803200  |{1,2}                  |
|1562716800  |{5}                    |
|1562112000  |                       |
|1562025600  |{4}                    |

We will need to go and get the size of each dataset from **dataset_sizes** and
calculate the amount of Bytes recalled per day, Finally we will lokk for the
day with the maximum amount of Bytes recalled.


## The actual implementation

The process that is described above is implemented in 3 functions: **get_datasets_recalled_per_day**,
**get_size_of_datasets_set** and **get_day_with_max_bytes_recalled**


### get_datasets_recalled_per_day.
This function will calculate for a given week, the datasets recalled per day.
The input parameters are:
 * recalled_set. The set of recalled datasets on a given week. Using the example
    above, that would be {1,2,5}
 * week_ts. The timestamp of the week we want to get the recalled set per day.
    Using the example above, that would be: 1562803200
 * days_df. The DataFrame **days_df**

It will return a dictionary that contains a timestamp of each day of the week
(only days that contain records in **days_df**) and the set of recalled datasets
of that day. Following the example of the section above that would be:

```
datasets_recalled_per_day = {
    1562803200:{1,2},
    1562716800:{5},
}
```
The code is available here: https://github.com/ddavila0/analytics_tools/blob/master/models/algorithmUtils.py
and explained in the comments.

```python
# Get the set of datasets recalled in every day of a given week
def get_datasets_recalled_per_day(recalled_set, week_ts, days_df):
    # Initialize the dictionary to be returned
    datasets_recalled_per_day = dict()

    # Fill the dictionary with empty sets, so that later we don't need to check
    # if a day exists before accessing it
    for day_ts in days_df[days_df['week_ts'] == week_ts]['day_ts'].values:
        datasets_recalled_per_day[day_ts]=set()

    # For each dataset that was recalled in the given week, let's find in which
    # day it was recalled
    for recalled_dataset in recalled_set:
        # For each of the days in the week
        for day_ts in days_df[days_df['week_ts'] == week_ts]['day_ts'].values:
            # Is the recalled dataset in the set of all the datasets accessed that day?
            if recalled_dataset in days_df[days_df['day_ts'] == day_ts]['datasets_set'].values[0]:
                # If yes, add it to the dictionary and stop looking for it in the same week
                datasets_recalled_per_day[day_ts].add(recalled_dataset)
                # If a dataset was accessed more than once within the same week
                # it was only recalled the first time
                break

    return datasets_recalled_per_day
```

### get_size_of_datasets_set.
This function calculates the summatory of the Bytes of all the datasets in a given set.
The input parameters are:
 * datasets_set. The set of datasets to calculate
 * datasets_size. The **datasets_size** DataFrame. This contains the size of each
    Of the datasets

The code is available here: https://github.com/ddavila0/analytics_tools/blob/master/models/algorithmUtils.py
and explained in the comments.

```python
# Calculate the added up size of a set of datasets
def get_size_of_datasets_set(datasets_set, datasets_size):
    size=0
    total_size=0
    # For each dataset in the set, find its size in the datasets_size DataFrame and add it up
    # to the 'total_size' variable
    for dataset_id in datasets_set:
        size = datasets_size[datasets_size['d_dataset_id'] == dataset_id].dataset_size.values[0]
        total_size = total_size + size

    return total_size
```


### get_day_with_max_bytes_recalled.
This function will call the above's functions for every week in
**recalled_per_week**, which was produced in Step 1, and find the day with the
maximum amount of bytes recalled.
The input parameters are:
 * datasets_recalled. A list where every item is a set of the datasets recalled
    in a week. Using the example above, that would be: [{1,2,5},{4}]
 * weeks_ts. A list with the week timestamps corresponding to the items in
    'datasets_recalled'.
 * days_df: The DataFrame **days_df**. It has the set of datasets accessed per day
 * datasets_size: The DataFrame **dataset_sizes**. It has the size of every dataset

It will return:
 * max_recalled: The maximum amount of Bytes recalled in a given day
 * max_recalled_day_ts: The day timestmap in which the maximum amount of Bytes
    were recalled

The code is available here: https://github.com/ddavila0/analytics_tools/blob/master/models/algorithmUtils.py
and explained in the comments.

```python

# Calculates the day with more Bytes recalled
def get_day_with_max_bytes_recalled(datasets_recalled, weeks_ts, days_df, datasets_size):
    max_recalled = 0
    max_recalled_day_ts = 0
    # For every week
    for i in range(0, len(weeks_ts)):
        # Get a dictionary with the datasets recalled per day
        recalled_datasets_per_day= get_datasets_recalled_per_day(datasets_recalled[i], weeks_ts[i], days_df)
        # For each day in the dictionary of recalled datasets per day
        for day in recalled_datasets_per_day:
            # Get the amount of Bytes recelled
            max_recalled_per_day = get_size_of_datasets_set(recalled_datasets_per_day[day], datasets_size)
            # If the amount of recalled Bytes is greater that the current maximum
            # make that the new current maximum and save the days timestamp of that day
            if max_recalled_per_day > max_recalled:
                max_recalled = max_recalled_per_day
                max_recalled_day_ts = day

    return max_recalled, max_recalled_day_ts
```

