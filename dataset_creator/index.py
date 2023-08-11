import os
import sys
import numpy as np
import pandas as pd

from rich.console import Console
from rich.progress import track
from pprint import pprint
from win10toast import ToastNotifier

console = Console()

console = Console()

import warnings
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

errors = []

def get_total_sub_df(df):
    sub_cols = [col for col in df.columns if "sub" in col]
    metrics = ['total_samples', 'samples_per_sec', 'mbps', 'lost_samples', 'lost_samples_percent']

    for metric in metrics:
        metric_cols = [col for col in sub_cols if metric in col]
        df[metric_cols] = df[metric_cols].apply(pd.to_numeric, errors='coerce')
        df = df.dropna(subset=metric_cols)
        
        df[f"total_{metric}"] = df[metric_cols].sum(axis=1, skipna=True)

    # ? Remove any rows that have string values
    df = df[df.applymap(lambda x: not isinstance(x, str)).all(1)]

    return df

def get_metric_stat(series, stat):
    try:
        stat = int(stat)
        return series.quantile(stat / 100)
    except ValueError:
        if "mean" in stat:
            return series.mean()
        elif "std" in stat:
            return series.std()
        elif "min" in stat:
            return series.min()
        elif "max" in stat:
            return series.max()
        else:
            raise ValueError(f"Unknown stat: {stat}")

def get_metric_stats(df, desired_stats):
    metric_stats = {}

    total_cols = [col for col in df.columns if "total" in col and "sub" not in col]
    latency_cols = [col for col in df.columns if "latency" in col]
    desired_cols = total_cols + latency_cols

    for col in desired_cols:
        for stat in desired_stats:
            metric_stats[f"{col}_{stat}"] = get_metric_stat(df[col], stat)
    
    for key, value in metric_stats.items():
        if not isinstance(value, (float, int, np.int64)):
            # errors.append(f"Value of {key} is not a float or int: {value}. It is a {type(value)}")
            raise ValueError(f"Value of {key} is not a float or int: {value}. It is a {type(value)}")

    return metric_stats

def get_setting_value_from_filename(filename):
    filename = filename.replace(".csv", "")

    filename_parts = filename.split("_")

    sec_part = [part for part in filename_parts if "SEC" in part][0]
    duration_secs = int(sec_part.replace("SEC", ""))
    datalen_part = [part for part in filename_parts if "B" in part][0]
    datalen_bytes = int(datalen_part.replace("B", ""))
    pub_count = int([part for part in filename_parts if "P" in part][0].replace("P", ""))
    sub_count = int([part for part in filename_parts if "S" in part and "SEC" not in part][0].replace("S", ""))
    durability_part = int([part for part in filename_parts if "DUR" in part][0].replace("DUR", ""))
    latency_count = int([part for part in filename_parts if "LC" in part][0].replace("LC", ""))
    
    if "REL" in filename:
        reliable = True
    else:
        reliable = False
    if "MC" in filename:
        multicast = True
    else:
        multicast = False

    return duration_secs, datalen_bytes, pub_count, sub_count, reliable, multicast, durability_part, latency_count

assert get_setting_value_from_filename('600SEC_1000B_5P_1S_REL_MC_0DUR_100LC.csv') == (600, 1000, 5, 1, True, True, 0, 100)

def create_dataset_from_summaries(summ_dir):
    test_csvs = os.listdir(summ_dir)

    dataset_df = pd.DataFrame()

    for test_csv in track(test_csvs, description="[4/4] Creating dataset..."):
        try:
            test_df = pd.read_csv(os.path.join(summ_dir, test_csv))
        except pd.errors.DtypeWarning as e:
            # pprint(e)
            # pprint(f"\n\n{test_csv}\n\n")
            errors.append(f"Error reading {test_csv}: {e}")
            raise e
        
        test_df = get_total_sub_df(test_df)

        duration_secs, datalen_bytes, pub_count, sub_count, reliable, multicast, durability, latency_count = get_setting_value_from_filename(test_csv)
        
        desired_stats = ['mean', 'std', 'min', 'max', '1', '2', '5', '10', '20', '25', '30', '40', '50', '60', '70', '75', '80', '90', '95', '99']

        metric_stats = get_metric_stats(test_df, desired_stats)
        metric_stats['duration_secs'] = duration_secs
        metric_stats['datalen_bytes'] = datalen_bytes
        metric_stats['pub_count'] = pub_count
        metric_stats['sub_count'] = sub_count
        metric_stats['reliable'] = reliable
        metric_stats['multicast'] = multicast
        metric_stats['durability'] = durability
        metric_stats['latency_count'] = latency_count

        dataset_df = pd.concat([dataset_df, pd.DataFrame(metric_stats, index=[0])], ignore_index=True)

    dataset_df['reliable'] = dataset_df['reliable'].astype(int)
    dataset_df['multicast'] = dataset_df['multicast'].astype(int)
    dataset_df = pd.get_dummies(dataset_df, columns=['durability'], prefix=['durability'])

    # ? Durability columns are booleans, convert them to ints
    durability_cols = [col for col in dataset_df.columns if "durability_" in col]
    for col in durability_cols:
        dataset_df[col] = dataset_df[col].astype(int)

    desired_columns = ['duration_secs', 'datalen_bytes', 'pub_count', 'sub_count', 'reliable', 'multicast', 'durability_0', 'durability_1', 'durability_2', 'durability_3', 'latency_count']
    other_columns = [col for col in dataset_df.columns if col not in desired_columns]

    dataset_df = dataset_df[desired_columns + other_columns]

    rows_before = len(dataset_df)
    dataset_df.dropna(inplace=True)
    rows_after = len(dataset_df)
    errors.append(f"Dropped {rows_before - rows_after} rows due to NaN values.")
    
    dataset_df.to_csv("dataset.csv", index=False)

def get_all_sub_metric(sub_files):
    test_df = pd.DataFrame()
    
    for file in sub_files:
        # ? Find out where to start parsing the file from 
        with open(file, "r") as file_obj:
            if os.stat(file).st_size == 0:
                continue
            file_obj.seek(0)
            pub_first_5_lines = [next(file_obj) for x in range(5)]
            
        start_index = 0    
        for i, line in enumerate(pub_first_5_lines):
            if "Length (Bytes)" in line:
                start_index = i
                break
        
        if start_index == 0:
            # print(f"Couldn't get start_index for header row from {file}.")
            errors.append(f"Couldn't get start_index for header row from {file}.")
            continue

        # ? Find out where to stop parsing the file from (ignore the summary stats at the end)
        with open(file, "r") as file_obj:
            file_contents = file_obj.readlines()
        pub_last_5_lines = file_contents[-5:]
        line_count = len(file_contents)
        
        end_index = 0
        for i, line in enumerate(pub_last_5_lines):
            if "throughput summary" in line.lower():
                end_index = line_count - 5 + i - 2
                break
            
        if end_index == 0:
            # print(f"Couldn't get end_index for summary row from {file}.")
            errors.append(f"Couldn't get end_index for summary row from {file}.")
            continue

        nrows = end_index - start_index
        nrows = 0 if nrows < 0 else nrows

        try:
            df = pd.read_csv(file, on_bad_lines="skip", skiprows=start_index, nrows=nrows)
        except pd.errors.ParserError as e:
            errors.append(f"Error when getting data from {file}:{e}")
            continue
        
        desired_metrics = ["total samples", "samples/s", "mbps", "lost samples"]
        
        sub_name = os.path.basename(file).replace(".csv", "")

        for col in df.columns:
            for desired_metric in desired_metrics:
                if desired_metric in col.lower() and "avg" not in col.lower():
                    col_name = col.strip().lower().replace(" ", "_")
                    if "samples/s" in col_name:
                        col_name = "samples_per_sec"
                    elif "%" in col_name:
                        col_name = "lost_samples_percent"
                    test_df[f"{sub_name}_{col_name}"] = df[col]

        # ? Remove rows with strings in them
        test_df = test_df[test_df.applymap(lambda x: not isinstance(x, str)).all(1)]

        test_df = test_df.astype(float, errors="ignore")

    return test_df

def get_lat_df_from_pub_file(pub_file):
    # ? Find out where to start parsing the file from 
    with open(pub_file, "r") as pub_file_obj:
        pub_first_5_lines = []
        for i in range(5):
            line = pub_file_obj.readline()
            if not line:
                break
            pub_first_5_lines.append(line)
    
    start_index = 0    
    for i, line in enumerate(pub_first_5_lines):
        if "Ave" in line and "Length (Bytes)" in line:
            start_index = i
            break

    if start_index == 0:
        # console.print(f"Couldn't find start index for {pub_file}.", style="bold red")
        errors.append(f"Couldn't find start index for header row for {pub_file}.")
        return None

    # ? Find out where to stop parsing the file from (ignore the summary stats at the end)
    with open(pub_file, "r") as pub_file_obj:
        pub_file_contents = pub_file_obj.readlines()

    pub_last_5_lines = pub_file_contents[-5:]
    line_count = len(pub_file_contents)
    
    end_index = 0
    for i, line in enumerate(pub_last_5_lines):
        if "latency summary" in line.lower():
            end_index = line_count - 5 + i - 2
            break
    
    if end_index == 0:
        # console.print(f"Couldn't find end index for {pub_file}.", style="bold red")
        errors.append(f"Couldn't find end index for summary row for {pub_file}.")
        return None

    try:
        lat_df = pd.read_csv(pub_file, skiprows=start_index, nrows=end_index-start_index, on_bad_lines="skip")
    except pd.errors.EmptyDataError:
        # console.print(f"EmptyDataError for {pub_file}.", style="bold red")
        errors.append(f"EmptyDataError for {pub_file}.")
        return None
    
        # ? Pick out the latency column ONLY
    latency_col = None
    for col in lat_df.columns:
        if "latency" in col.lower():
            latency_col = col
            break

    if latency_col is None:
        # console.print(f"Couldn't find latency column for {pub_file}.", style="bold red")
        errors.append(f"Couldn't find latency column for {pub_file}.")
        return None

    lat_df = lat_df[latency_col]
    lat_df = lat_df.rename("latency_us")
    
    return lat_df

def summarise_tests(test_dirs, camp_name):
    summ_dir = f"{camp_name}_summaries"
    if not os.path.exists(summ_dir):
        os.mkdir(summ_dir)

    for test in track(test_dirs, description="[3/4] Summarising tests..."):
        test_name = os.path.basename(test)
        summ_path = os.path.join(summ_dir, f"{test_name}.csv")
        
        if os.path.exists(summ_path):
            # console.print(f"[yellow]Skipping {test_name} as summary already exists[/yellow]")
            continue

        test_files = os.listdir(test)
        csv_files = [f for f in test_files if f.endswith('.csv')]
        pub_file = [os.path.join(test, f) for f in csv_files if "pub_" in f][0]
        sub_files = [os.path.join(test, f) for f in csv_files if "sub_" in f]

        lat_df = get_lat_df_from_pub_file(pub_file)
        if lat_df is None:
            continue
        subs_df = get_all_sub_metric(sub_files)
        if subs_df is None:
            continue

        df_list = [lat_df, subs_df]
        df = pd.concat(df_list, axis=1)
        df.to_csv(summ_path, index=False)

    return summ_dir

def get_expected_csv_count_from_testname(testname):
    split = testname.split("_")
    sub_split = [_ for _ in split if "S" in _ and "SEC" not in _]
    try:
        sub_value = sub_split[0].replace("S", "")
    except IndexError:
        errors.append(f"Could not get expected csv count from testname: {testname}")
        return 0
    sub_value = int(sub_value)
    
    return sub_value + 1

assert(get_expected_csv_count_from_testname("600SEC_6400B_25P_25S_BE_MC_2DUR_100LC") == 26)

def get_usable_tests(test_dirs):
    usable_tests = []
    for test_dir in track(test_dirs, description="[2/4] Getting usable tests..."):
        expected_csv_count = get_expected_csv_count_from_testname(os.path.basename(test_dir))
        actual_csv_count = len([f for f in os.listdir(test_dir) if f.endswith('.csv')])

        if expected_csv_count != actual_csv_count:
            errors.append(f"Expected {expected_csv_count} csv files, but found {actual_csv_count} in {test_dir}")
            continue
        else:
            usable_tests.append(test_dir)

    return usable_tests


def validate_test_dirs(test_dirs):
    for test_dir in track(test_dirs, description="[1/4] Validating test directories..."):
        if not os.path.isdir(test_dir):
            console.print(f"[red]Error: [/red]Invalid test directory path: {test_dir}")
            sys.exit(1)

        test_dir_contents = os.listdir(test_dir)
        if len(test_dir_contents) == 0:
            # console.print(f"[red]Error: [/red]Test directory is empty: {test_dir}")
            errors.append(f"{test_dir} is empty.")
            continue

def validate_command_line_args(args):
    if len(args) != 2:
        console.print("[red]Error: [/red]Invalid number of arguments")
        console.print(f"Usage: python index.py <dirpath>")
        sys.exit(1)

    dirpath = sys.argv[1]
    if not os.path.isdir(dirpath):
        console.print(f"[red]Error: [/red]Invalid directory path: {dirpath}")
        sys.exit(1)

    dir_contents = os.listdir(dirpath)
    if len(dir_contents) == 0:
        console.print(f"[red]Error: [/red]Directory is empty: {dirpath}")
        sys.exit(1)

if __name__ == "__main__":
    validate_command_line_args(sys.argv)
    
    dirpath = sys.argv[1]
    test_dirs = [os.path.join(dirpath, d) for d in os.listdir(dirpath)]
    test_dirs = [d for d in test_dirs if os.path.isdir(d)]

    camp_name = os.path.basename(dirpath)

    validate_test_dirs(test_dirs)

    usable_tests = get_usable_tests(test_dirs)
    
    summ_dir = summarise_tests(usable_tests, camp_name)

    create_dataset_from_summaries(summ_dir)

    console.print(f"Finished with {len(errors)} errors.", style="bold white")

    if len(errors) > 0:
        with open("error_log.txt", "w") as file:
            for error in errors:
                file.write(error + "\n")

    toaster = ToastNotifier()
    toaster.show_toast("Dataset Created", f"{dirpath}: {len(errors)} errors.", duration=5)