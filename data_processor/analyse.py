from pprint import pprint
from rich.console import Console
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import os
import pandas as pd
import numpy as np

console = Console()

def get_expected_csv_count_from_testname(testname):
    split = testname.split("_")
    sub_split = [_ for _ in split if "S" in _ and "SEC" not in _]
    sub_value = sub_split[0].replace("S", "")
    sub_value = int(sub_value)
    
    return sub_value + 1

def analyse_tests(tests_dir):
    tests_dirname = os.path.basename(tests_dir)
    camp_name = tests_dirname.replace("_raw", "")

    # ? Make the folder that will contain all the test analysis
    analysis_dirpath = os.path.join(os.path.dirname(tests_dir), f"{camp_name}_analysis")
    if not os.path.exists(analysis_dirpath):
        os.mkdir(analysis_dirpath)
        console.print(f"Created folder: {analysis_dirpath}", style="bold blue")
    else:
        console.print(f"{analysis_dirpath} already exists.", style="bold #ffa500")
    
    tests = [f.path for f in os.scandir(tests_dir) if f.is_dir()]

    # ? Get tests with expected csv files
    tests_with_expected_csv = []
    for test in tests:
        expected_csv_count = get_expected_csv_count_from_testname(os.path.basename(test))
        actual_csv_count = len([f for f in os.listdir(test) if f.endswith('.csv')])
        
        if expected_csv_count == actual_csv_count:
            tests_with_expected_csv.append(test)

    # ? Get tests that are missing csv files
    tests_without_expected_csv = []
    for test in tests:
        if test not in tests_with_expected_csv:
            tests_without_expected_csv.append(test)

    # ? Just get the names - not the full paths
    tests_with_expected_csv_names = [os.path.basename(test) for test in tests_with_expected_csv]
    tests_without_expected_csv_names = [os.path.basename(test) for test in tests_without_expected_csv]
    
    # ? Write test names to file for future custom test lists
    with open(os.path.join(analysis_dirpath, "tests_with_expected_csv_names.txt"), "w") as f:
        for test in tests_with_expected_csv_names:
            f.write(test + "\n")
    console.print(f"Created tests_with_expected_csv_names.txt for {camp_name.upper()}.", style="bold blue")
    with open(os.path.join(analysis_dirpath, "tests_without_expected_csv_names.txt"), "w") as f:
        for test in tests_without_expected_csv_names:
            f.write(test + "\n")
    console.print(f"Created tests_without_expected_csv_names.txt for {camp_name.upper()}.", style="bold blue")

    return tests_with_expected_csv

def get_lat_df_from_pub_file(pub_file):
    # ? Find out where to start parsing the file from 
    with open(pub_file, "r") as pub_file_obj:
        pub_first_5_lines = [next(pub_file_obj) for x in range(5)]
    
    start_index = 0    
    for i, line in enumerate(pub_first_5_lines):
        if "Ave" in line:
            start_index = i
            break
    
    # ? Find out where to stop parsing the file from (ignore the summary stats at the end)
    with open(pub_file, "r") as pub_file_obj:
        pub_file_contents = pub_file_obj.readlines()

    pub_last_5_lines = pub_file_contents[-5:]
    line_count = len(pub_file_contents)
    
    end_index = 0
    for i, line in enumerate(pub_last_5_lines):
        if "summary" in line.lower():
            end_index = line_count - 5 + i - 2
            break
    
    lat_df = pd.read_csv(pub_file, skiprows=start_index, nrows=end_index-start_index, on_bad_lines="skip")
    
    # ? Pick out the latency column ONLY
    latency_col = None
    for col in lat_df.columns:
        if "latency" in col.lower():
            latency_col = col
            break
    lat_df = lat_df[latency_col]
    lat_df = lat_df.rename("latency_us")
    
    return lat_df

def get_all_sub_metric(sub_files, metric):
    sub_series = []
    
    for file in sub_files:
        # ? Find out where to start parsing the file from 
        with open(file, "r") as file_obj:
            pub_first_5_lines = [next(file_obj) for x in range(5)]
        start_index = 0    
        for i, line in enumerate(pub_first_5_lines):
            if "Avg" in line:
                start_index = i
                break
        
        # ? Find out where to stop parsing the file from (ignore the summary stats at the end)
        with open(file, "r") as file_obj:
            file_contents = file_obj.readlines()

        pub_last_5_lines = file_contents[-5:]
        line_count = len(file_contents)
        
        end_index = 0
        for i, line in enumerate(pub_last_5_lines):
            if "summary" in line.lower():
                end_index = line_count - 5 + i - 2
                break
            
        try:
            df = pd.read_csv(file, on_bad_lines="skip", skiprows=start_index, nrows=end_index-start_index)
        except pd.errors.ParserError as e:
            print(f"Error when getting data from {file}:")
            print(f"\t{e}")
            continue
        
        sub_head = [x for x in df.columns if metric in x.lower()][0]
        series = df[sub_head]
        series.name = os.path.basename(file).split(".")[0] + "_" + metric
        series.name = series.name.replace(" ", "_")
        
        sub_series.append(series)
        
    if sub_series:
        # ? Concatenate all the series into a dataframe horiznotally
        sub_df = pd.concat(sub_series, axis=1)
        # ? Sort the columns alphabetically
        sub_df = sub_df.reindex(sorted(sub_df.columns, key=lambda x: int(x.split("_")[1])), axis=1)
        # ? Add a total column
        valid_cols = [col for col in sub_df.columns if col.startswith('sub_')]
        try:
            sub_df["total_" + metric] = sub_df[valid_cols].sum(axis=1)
        except FutureWarning:
            pass

        return sub_df
    else:
        print(f"Couldn't get any data from {sub_files}.")

def summarise_usable_tests(tests, camp_name):
    # ? Create summary dir for all the summaries
    summary_dir = f"{camp_name}_summaries"
    
    if not os.path.exists(summary_dir):
        os.mkdir(summary_dir)
    
    total_tests = len(tests)
    for i, test in enumerate(tests):
        with console.status(f"Summarising test {i+1}/{total_tests}..."):
            test_files = os.listdir(test)
            csv_files = [f for f in test_files if f.endswith('.csv')]

            pub_file = [os.path.join(test, f) for f in csv_files if "pub_" in f][0]
            sub_files = [os.path.join(test, f) for f in csv_files if "sub_" in f]
    
            lat_df = get_lat_df_from_pub_file(pub_file)

            throughput_df = get_all_sub_metric(sub_files, "mbps")
            sample_rate_df = get_all_sub_metric(sub_files, "samples/s")
            total_samples_received_df = get_all_sub_metric(sub_files, "total samples")
            lost_samples_df = get_all_sub_metric(sub_files, "lost samples")

            # ? Concatenate all the dataframes together horizontally
            df_list = [lat_df, throughput_df, sample_rate_df, total_samples_received_df, lost_samples_df]
            df = pd.concat(df_list, axis=1)

            test_name = os.path.basename(test)
            summary_filename = os.path.join(summary_dir, test_name + "_summary.csv")
            df.to_csv(summary_filename)