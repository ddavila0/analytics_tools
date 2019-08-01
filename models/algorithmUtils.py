import pandas as pd
import glob

###############################################################################
#                               READ INPUT DATA
###############################################################################
# Takes a list of dataframes (one per month) and merge them into 
# a single dataframe making sure that records with the same week timestamp(week_ts)
# and day timestamp(day_ts) present in more than one month are merged together
def merge_days_dataframes(days_df_list):
    # Data is coming separated in months, let's concatenate all these months
    # to make a single DataFrame containing all the data
    all_days = pd.concat(days_df_list)
    all_days.reset_index(inplace=True)
    
    # Transform 'datasets_set' from array to list, so that we can group lists
    all_days['datasets_set']=all_days['datasets_set'].apply(list)
    
    # Make sure that the same week_ts + day_ts doesn't exist in more than 1 month
    # and if so, group it together
    all_days= all_days.groupby(['week_ts','day_ts']).agg({'datasets_set':sum})
    all_days.reset_index(inplace=True)
    
    return all_days

# group day records into weeks, making a union on the datasets sets
def group_days_into_weeks_df(all_days):
    # Create a new DataFrame 'all_weeks' where we group all days, within a week, together
    all_weeks = all_days.groupby(['week_ts']).agg({'datasets_set':sum})

    # Transform the 'datasets_set' from list to set, to remove duplicates
    all_weeks['datasets_set']=all_weeks['datasets_set'].apply(set)
    all_weeks.reset_index(inplace=True)
    
    # Transform the 'datasets_set' from list to set, to remove duplicates
    all_days['datasets_set']=all_days['datasets_set'].apply(set)
    all_days.reset_index(inplace=True)
    
    return all_weeks


# Receives 2 paths:
# @days_paths: from where to read input files of days datasets and 
# @datasets_path: where is the file that contains the datasets sizes 
# returns 3 DataFrames:
# 1. all_days: each record contains the set of datsets accessed in a given day 
# 2. all_weeks: each record contains the set of datsets accessed in a given day week
# 3. datasets_size: each record contains a dataset ID and its size on Bytes
def get_input_data(days_path, datasets_path):
    datasets_df = pd.read_parquet(datasets_path)
    datasets_size = datasets_df[['d_dataset_id', 'dataset_size']]
    
    days_df_list = [] 
    list_of_files = glob.glob(days_path)
    for file in list_of_files:
        print("Reading: "+file)
        day_df = pd.read_parquet(file)
        days_df_list.append(day_df)
        
    all_days = merge_days_dataframes(days_df_list)
    all_weeks = group_days_into_weeks_df(all_days)
    
    return all_days, all_weeks, datasets_size
            

###############################################################################



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

###############################################################################
#                       MAIN ALGORITHM
###############################################################################
def get_freed_recalled_and_ws_sizes(weeks_list, weeks_first_access_list, policy, datasets_size):
    freed = set()
    freed_per_week = []
    called_per_week = []
    working_set_size_per_week=[]
    ws_per_week = []
    to_free = set()
    to_recall = set()
    newly_called = set()
    really_recalled = set()
    really_recalled_per_week = []
    newly_called_per_week = []
 
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
        
        # Get the set of datasets that have been accessed for first time in the
        # N weeks prior to the current week which is pointed by (i)
        for j in range(i-policy-deltaT, i):
            newly_created.update(weeks_first_access_list[j])
 
        for j in range(int_ws_from, int_ws_to+1):
            int_ws.update(weeks_list[j])

        new_week = weeks_list[i]
        old_week = weeks_list[int_ws_from -1]
        
        current_working_set = int_ws.union(new_week)
        current_working_set_size = get_size_of_datasets_set(current_working_set, datasets_size)
        to_free = old_week - (int_ws.union(new_week))
        # According to minutes from 20190711, everything after the first 'policy' weeks
        # counts as recalled regardless whether it was freed or not 
        #to_recall = (new_week - (int_ws.union(old_week))).intersection(freed)
        to_recall = (new_week - (int_ws.union(old_week)))
        for dataset in to_recall:
            if dataset in newly_created:
                newly_called.update(dataset)
            else:
                really_recalled.update(dataset)
        
        working_set_size_per_week.append(current_working_set_size)
        freed.update(to_free)
        #recalled_per_week.append(to_recall)
        really_recalled_per_week.append(really_recalled)
        newly_called_per_week.append(newly_called)
        freed_per_week.append(to_free)

    return freed_per_week, recalled_per_week, working_set_size_per_week


###############################################################################

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
