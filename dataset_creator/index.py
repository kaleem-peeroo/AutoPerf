import os
import sys
import numpy as np
import pandas as pd

from rich.console import Console
from rich.progress import track
from pprint import pprint

console = Console()

console = Console()

import warnings
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

errors = []

def create_dataset(summ_dir):
    test_csvs = os.listdir(summ_dir)

    pprint(test_csvs)

    asdf

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
            print(f"Couldn't get start_index for header row from {file}.")
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
            print(f"Couldn't get end_index for summary row from {file}.")
            errors.append(f"Couldn't get end_index for summary row from {file}.")
            continue

        nrows = end_index - start_index
        nrows = 0 if nrows < 0 else nrows

        try:
            df = pd.read_csv(file, on_bad_lines="skip", skiprows=start_index, nrows=nrows)
        except pd.errors.ParserError as e:
            print(f"Error when getting data from {file}:")
            print(f"\t{e}")
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

        test_df = test_df.astype(float)


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
        console.print(f"Couldn't find start index for {pub_file}.", style="bold red")
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
        console.print(f"Couldn't find end index for {pub_file}.", style="bold red")
        errors.append(f"Couldn't find end index for summary row for {pub_file}.")
        return None

    try:
        lat_df = pd.read_csv(pub_file, skiprows=start_index, nrows=end_index-start_index, on_bad_lines="skip")
    except pd.errors.EmptyDataError:
        console.print(f"EmptyDataError for {pub_file}.", style="bold red")
        errors.append(f"EmptyDataError for {pub_file}.")
        return None
    
        # ? Pick out the latency column ONLY
    latency_col = None
    for col in lat_df.columns:
        if "latency" in col.lower():
            latency_col = col
            break

    if latency_col is None:
        console.print(f"Couldn't find latency column for {pub_file}.", style="bold red")
        errors.append(f"Couldn't find latency column for {pub_file}.")
        return None

    lat_df = lat_df[latency_col]
    lat_df = lat_df.rename("latency_us")
    
    return lat_df

def summarise_tests(test_dirs, camp_name):
    summ_dir = f"{camp_name}_summaries"
    if not os.path.exists(summ_dir):
        os.mkdir(summ_dir)

    for test in track(test_dirs, description="Summarising tests..."):
        test_name = os.path.basename(test)
        summ_path = os.path.join(summ_dir, f"{test_name}.csv")
        
        if os.path.exists(summ_path):
            console.print(f"[yellow]Skipping {test_name} as summary already exists[/yellow]")
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
        df.to_csv(summ_path)

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
    for test_dir in track(test_dirs, description="Getting usable tests..."):
        expected_csv_count = get_expected_csv_count_from_testname(os.path.basename(test_dir))
        actual_csv_count = len([f for f in os.listdir(test_dir) if f.endswith('.csv')])

        if expected_csv_count != actual_csv_count:
            errors.append(f"Expected {expected_csv_count} csv files, but found {actual_csv_count} in {test_dir}")
            continue
        else:
            usable_tests.append(test_dir)

    return usable_tests


def validate_test_dirs(test_dirs):
    for test_dir in track(test_dirs, description="Validating test directories..."):
        if not os.path.isdir(test_dir):
            console.print(f"[red]Error: [/red]Invalid test directory path: {test_dir}")
            sys.exit(1)

        test_dir_contents = os.listdir(test_dir)
        if len(test_dir_contents) == 0:
            console.print(f"[red]Error: [/red]Test directory is empty: {test_dir}")
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

    camp_name = os.path.basename(dirpath)

    validate_test_dirs(test_dirs)

    usable_tests = get_usable_tests(test_dirs)
    
    summ_dir = summarise_tests(usable_tests, camp_name)

    create_dataset(summ_dir)

    if len(errors) > 0:
        with open("error_log.txt", "w") as file:
            for error in errors:
                file.write(error + "\n")