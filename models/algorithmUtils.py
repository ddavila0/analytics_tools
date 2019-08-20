import pandas as pd
import glob

###############################################################################
#                               READ  DATA
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


# Receives 3 paths:
# @days_paths: from where to read input files of days datasets
# @datasets_creation_path: where is the file that contains the datasets creation date 
# @datasets_path: where is the file that contains the datasets sizes 
# returns 3 DataFrames:
# 1. all_days: each record contains the set of datsets accessed in a given day 
# 2. all_weeks_access: each record contains the set of datsets accessed in a given day week
# 3. all_weeks_creation: each record contains the set of datsets created in a given day week
# 4. datasets_size: each record contains a dataset ID and its size on Bytes
def get_input_data(days_path, datasets_creation_path, datasets_path):
    datasets_df = pd.read_parquet(datasets_path)
    datasets_size = datasets_df[['d_dataset_id', 'dataset_size']]
    datasets_creation_df = pd.read_parquet(datasets_creation_path)
    
    days_df_list = [] 
    list_of_files = glob.glob(days_path)
    for file in list_of_files:
        print("Reading: "+file)
        day_df = pd.read_parquet(file)
        days_df_list.append(day_df)
        
    all_days = merge_days_dataframes(days_df_list)
    all_weeks_access = group_days_into_weeks_df(all_days)
    all_weeks_creation = datasets_creation_df.groupby(['creation_week_ts']).agg({'d_dataset_id':list})
    all_weeks_creation.reset_index(inplace=True)

    return all_days, all_weeks_access, all_weeks_creation, datasets_size

def get_input_data_old(days_path, datasets_path):
    datasets_df = pd.read_parquet(datasets_path)
    datasets_size = datasets_df[['d_dataset_id', 'dataset_size']]
   
    days_df_list = [] 
    list_of_files = glob.glob(days_path)
    for file in list_of_files:
        print("Reading: "+file)
        day_df = pd.read_parquet(file)
        days_df_list.append(day_df)
        
    all_days = merge_days_dataframes(days_df_list)
    all_weeks_access = group_days_into_weeks_df(all_days)
   
    return all_days, all_weeks_access, datasets_size
            

###############################################################################



# Receives a weeks DataFrame ('weeks_df') and returns a sorted (by 'sortby') list
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
# @week_ts: the name of the field in @week_ds that identifies the week timestamp
#    and that will be used to sort the data. In the example above it would be "weeks_ts"
# @datasets_set: the name of the field in @week_ds that identifies the dataset list. In the
#   example above it would be "datasets_set"
# @init: the timestamp of the first week on the time fram being analyzed 
# @end: the timestamp of the last week on the time fram being analyzed
# @return: [{15686513, 16766504},{12686513, 13766504, 14689984},{17686513, 18766504, 13689984}] 
#
def get_sorted_list_of_datasets_setsX(weeks_df, week_ts, datasets_set, init, end):
    seconds_in_a_week=3600*24*7
    weeks_sorted_list = []
    # Sort the dataset in cronological order using @week_ts (week timestamp)
    weeks_df_sorted = weeks_df.sort_values(week_ts)
    #print(weeks_df_sorted.head(5))
    # Once sorted, create a list with the sets of datasets
    # this list has to be as long as weeks are in the time frame we are looking at
    # in case there is an empty week we will append an empty set on the list
    i = init
    for week_timestamp, dataset_list in weeks_df_sorted.values:
        
        # Drop any week outside the time frame we are analyzing
        if week_timestamp < init or week_timestamp > end:
            #print("dropping week: "+str(week_timestamp))
            continue
        
        if week_timestamp != i:
            # There is 1 or more weeks where no activity was registered
            # let's put an empty set on each of them
            for j in range(0, int((week_timestamp - i)/seconds_in_a_week) ):
                print("week: "+str(i)+"is missing)")
                weeks_sorted_list.append(set())
                i+=seconds_in_a_week
        weeks_sorted_list.append(set(dataset_list))
        last = week_timestamp
        i+=seconds_in_a_week

    # If there were some weeks with no activity at the end, fill the list
    # with empty sets
    for j in range(0, int((end - i)/seconds_in_a_week)+1):
                print("week: "+str(i)+"is missing at last)")
                weeks_sorted_list.append(set())
                i+=seconds_in_a_week

    return weeks_sorted_list
###############################################################################
#                       MAIN ALGORITHM
###############################################################################
def get_freed_recalled_and_ws_sizes(weeks_list_accessed, weeks_list_created, policy, deltaT, datasets_size):
    
    working_set_size_accessed_per_week =[]
    working_set_size_created_per_week =[]
    recalled_accessed_per_week = []
    recalled_created_per_week = []
    freed_per_week = []

    current_working_set_accessed = set()
    current_working_set_created = set()

    to_free_created = set()
    to_free_accessed = set()
    to_recall_accessed = set()
    

    # For the first deltaT weeks we would assume nothing happened, just the working set both
    # for created and accessed is initialized
    for i in range(0, deltaT):
        recalled_created_per_week.append(set())
        recalled_accessed_per_week.append(set())
        freed_per_week.append(set())

        current_working_set_created = current_working_set_created.union(weeks_list_created[i])
       
        # The accessed working set gets initialized only with the datasets accessed in the last
        # N weeks, where N = policy
        if i >= deltaT - policy:
            current_working_set_accessed = current_working_set_accessed .union(weeks_list_accessed[i])
            curr_ws_size  = get_size_of_datasets_set(current_working_set_accessed, datasets_size)
            working_set_size_accessed_per_week.append(curr_ws_size )
            # A dataset that is in the accessed working set cannot be at the same time in the created one
            current_working_set_created = current_working_set_created - current_working_set_accessed
        else:
            working_set_size_accessed_per_week.append(0)
        
        curr_ws_size = get_size_of_datasets_set(current_working_set_created, datasets_size)
        working_set_size_created_per_week.append(curr_ws_size)
   
    #return freed_created_per_week, freed_accessed_per_week, recalled_accessed_per_week, working_set_size_created_per_week, working_set_size_accessed_per_week
    # For each week in the list, starting on the first week
    # after deltaT
    for i in range(deltaT, len(weeks_list_accessed)):
        # Calculate the intermediate working_set that includes the set of datasets
        # accesed between the week leaving the working set(old_week) and the
        # the current week(new_week)
        int_ws_created = set()
        int_ws_created_from = i - (deltaT) + 1
        
        int_ws_accessed = set()
        int_ws_accessed_from = i - (policy) + 1
        
        # Get the set of datasets that have been accessed for first time in the
        # N weeks prior to the current week which is pointed by (i)
        for j in range(int_ws_created_from, i):
            int_ws_created.update(weeks_list_created[j])
 
        for j in range(int_ws_accessed_from, i):
            int_ws_accessed.update(weeks_list_accessed[j])

        new_week_created  = weeks_list_created[i]
        new_week_accessed = weeks_list_accessed[i]
        old_week_created  = weeks_list_created[int_ws_created_from -1]
        old_week_accessed = weeks_list_accessed[int_ws_accessed_from -1]
        
        current_working_set_created = int_ws_created.union(new_week_created)
        
        current_working_set_accessed = int_ws_accessed.union(new_week_accessed)
        curr_ws_size  = get_size_of_datasets_set(current_working_set_accessed, datasets_size)
        working_set_size_accessed_per_week.append(curr_ws_size )
        
        # A dataset that is in the accessed working set cannot be at the same time in the created one
        current_working_set_created = current_working_set_created - current_working_set_accessed
        curr_ws_size = get_size_of_datasets_set(current_working_set_created, datasets_size)
        working_set_size_created_per_week.append(curr_ws_size)

        
        to_free_created    = old_week_created - current_working_set_accessed
        #to_recall_accessed = new_week_accessed - int_ws_accessed.union(old_week_accessed) - current_working_set_created
        to_recall_accessed = new_week_accessed - int_ws_accessed.union(old_week_accessed) - (int_ws_created.union(old_week_created))
        to_free_accessed   = old_week_accessed - current_working_set_accessed - current_working_set_created       
        
        freed_per_week.append(to_free_created.union(to_free_accessed))
        recalled_accessed_per_week.append(to_recall_accessed)
        

    return freed_per_week, recalled_accessed_per_week, working_set_size_created_per_week, working_set_size_accessed_per_week


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
