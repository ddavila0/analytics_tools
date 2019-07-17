# Receives a weeks DataFrame ('weeks_df') and returns a sorted (by week_ts) list
# of datasets sets
# @weeks_ds: a pandas DataFrame of the form:
#    ------------------------------------------------
#    |weeks_ts      |datasets_set                   |
#    ------------------------------------------------
#    |1.561594e+09  |[12686513, 13766504, 14689984] |
#    |1.361594e+09  |[15686513, 16766504]           |
#    |1.761594e+09  |[17686513, 18766504, 13689984] |
#    ------------------------------------------------
#    where:
#     'weeks_ts' is a Linux timestamp that identifies the week and 
#     'datasets_set' is an array of datasets IDs that were accessed in that week
#    @return: [{15686513, 16766504},{12686513, 13766504, 14689984},{17686513, 18766504, 13689984}] 
#
def get_sorted_list_of_datasets_sets(weeks_df):
    # Sort the dataset in cronological order (by week_ts (week timestamp))
    # Reset the index once the dataFrame is sorted so that we can access it
    # in order using the indices
    weeks_df_sorted = weeks_df.sort_values('week_ts')
    weeks_df_sorted = weeks_df_sorted.reset_index(drop=True)

    # count() returns a series structure, get an integer 
    weeks_df_count = weeks_df_sorted.count()
    weeks_df_count = weeks_df_count.week_ts
    # Create a cronological ordered list of datasets sets(arrays are converted into sets)
    weeks_sorted_list= []
    for i in range(0, weeks_df_count):
        weeks_sorted_list.append(set(weeks_df_sorted.datasets_set[i]))
        
    return weeks_sorted_list

def get_freed_recalled_and_ws_sizes(weeks_list, policy, datasets_size):
    freed = set()
    recalled_per_week = []
    freed_per_week = []
    called_per_week = []
    working_set_size_per_week=[]
    ws_per_week = []
    to_free = set()
    to_recall = set()
    
    # Fill in the first 'policy' weeks with empty sets given that nothing could have
    # recalled nor freed during those weeks.
    # The working set size for these first 'policy' weeks will be accumulated set of
    # datasets accessed during those weeks
    current_working_set = set()
    current_working_set_size = 0
    for i in range(0, policy):
        recalled_per_week.append(to_recall)
        freed_per_week.append(to_free)
        current_working_set = current_working_set.union(weeks_list[i])
        current_working_set_size = get_size_of_datasets_set(current_working_set, datasets_size)
        working_set_size_per_week.append(current_working_set_size)
   
    # For each week in the list, starting on the first week
    # after the policy
    for i in range(policy, len(weeks_list)):
        # Calculate the intermediate working_set that includes the set of datasets
        # accesed between the week leaving the working set(old_week) and the
        # the current week(new_week)
        int_ws = set()
        int_ws_to = i - 1
        int_ws_from = i - (policy) + 1
 
        for j in range(int_ws_from, int_ws_to+1):
            int_ws.update(weeks_list[j])
        new_week = weeks_list[i]
        old_week = weeks_list[int_ws_from -1]
        
        current_working_set = int_ws.union(new_week)
        current_working_set_size = get_size_of_datasets_set(current_working_set, datasets_size)
        to_free = old_week - (int_ws.union(new_week))
        to_recall = (new_week - (int_ws.union(old_week))).intersection(freed)
       
        working_set_size_per_week.append(current_working_set_size)
        freed.update(to_free)
        recalled_per_week.append(to_recall)
        freed_per_week.append(to_free)

    return freed_per_week, recalled_per_week, working_set_size_per_week


# Get the set of datasets recalled in every day of a given week
def get_datasets_recalled_per_day(recalled_set, week_ts, days_df):
    datasets_recalled_per_day = dict()
    
    for day_ts in days_df[days_df['week_ts'] == week_ts]['day_ts'].values:
        datasets_recalled_per_day[day_ts]=set()
    
    for recalled_dataset in recalled_set:   
        # For each of the days in the week
        for day_ts in days_df[days_df['week_ts'] == week_ts]['day_ts'].values:
            a= days_df['week_ts'] == week_ts
            b= days_df['day_ts'] == day_ts
            # Is the recalled dataset in this day
            if recalled_dataset in days_df[a&b]['datasets_set'].values[0]:
                #print(recalled_dataset)
                datasets_recalled_per_day[day_ts].add(recalled_dataset)
                # If a dataset was accessed more than once within the same week
                # it was only recalled the first time since the minimum delete
                # policy is 1 week
                break
                
    return datasets_recalled_per_day

# Calculates the day with more Bytes recalled
def get_day_with_max_bytes_recalled(datasets_recalled, weeks_ts, days_df, datasets_size): 
    max_recalled = 0
    max_recalled_day_ts = 0
    max_recalled_week_ts = 0
    for i in range(0, len(weeks_ts)):
        recalled_datasets_per_day= get_datasets_recalled_per_day(datasets_recalled[i], weeks_ts[i], days_df)
        for day in recalled_datasets_per_day:
            max_recalled_per_week = get_size_of_datasets_set(recalled_datasets_per_day[day], datasets_size)
            if max_recalled_per_week > max_recalled:
                max_recalled = max_recalled_per_week
                max_recalled_day_ts = day
                max_recalled_week_ts = weeks_ts[i]
    return max_recalled, max_recalled_week_ts, max_recalled_day_ts 


# Calculates the amount of Bytes recalled and freed per week and in total
def get_recalled_and_freed_sizes(datasets_recalled, datasets_freed, datasets_size):
    # Get the size recalled size per week
    datasets_recalled_sizes = get_sizes_of_datasets_sets(datasets_recalled, datasets_size)

    # Get the size freed size per week
    datasets_freed_sizes = get_sizes_of_datasets_sets(datasets_freed, datasets_size)

    # Calculate totals
    total_recalled_bytes = 0
    for week in datasets_recalled_sizes:
        total_recalled_bytes = total_recalled_bytes + week   
    
    total_freed_bytes =0
    for week in datasets_freed_sizes:
        total_freed_bytes = total_freed_bytes + week

    return datasets_recalled_sizes, datasets_freed_sizes, total_recalled_bytes, total_freed_bytes

def to_petabytes(list_of_floats):
    return_list= []
    for i in list_of_floats:
        return_list.append(i/(10**15))
    
    return return_list
        
def format_bytes(size):
    # 2**10 = 1024
    #power = 2**10
    power = 1000
    n = 0
    power_labels = {0 : 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB', 5:'PB'}
    while size > power:
        size /= power
        n += 1
    formated = "{0:.2f}".format(size)
    formated = formated +" "+power_labels[n]
    return formated

def get_sizes_of_datasets_sets(datasets_sets, datasets_size):
    sizes = []
    for datasets_set in datasets_sets:
        total_size=get_size_of_datasets_set(datasets_set, datasets_size)
        sizes.append(total_size)
    return sizes

def get_size_of_datasets_set(datasets_set, datasets_size):
    size=0
    total_size=0
    for dataset_id in datasets_set:
        size = datasets_size[datasets_size['d_dataset_id'] == dataset_id].dataset_size.values[0]
        total_size = total_size + size
    
    return total_size
