import numpy as np
import os
import pandas as pd
import re
from pprint import pprint
from rich.console import Console
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

console = Console()

def get_settings_from_testname(test):
    datalen_bytes = re.findall("\d*B", test)[0].replace("B", "")
    pub_count = re.findall("\d*P", test)[0].replace("P", "")
    sub_count = re.findall("\d*S_", test)[0].split("S")[0]
    best_effort = 1 if len(re.findall("_BE_", test)) > 0 else 0
    multicast = 1 if len(re.findall("_MC_", test)) > 0 else 0
    durability = re.findall("\dDUR", test)[0].replace("DUR", "")

    return datalen_bytes, pub_count, sub_count, best_effort, multicast, durability

def get_expected_csv_count_from_testname(testname):
    split = testname.split("_")
    sub_split = [_ for _ in split if "S" in _ and "SEC" not in _]
    try:
        sub_value = sub_split[0].replace("S", "")
    except IndexError:
        pprint(split)
        raise IndexError
    sub_value = int(sub_value)
    
    return sub_value + 1

def analyse_tests(tests_dir):
    tests_dir = os.path.normpath(tests_dir)

    if not os.path.exists(tests_dir):
        console.print(f"{tests_dir} does not exist.", style="bold red")
        return []

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
        pub_first_5_lines = []
        for i in range(5):
            line = pub_file_obj.readline()
            if not line:
                break
            pub_first_5_lines.append(line)
    
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
    
    try:
        lat_df = pd.read_csv(pub_file, skiprows=start_index, nrows=end_index-start_index, on_bad_lines="skip")
    except pd.errors.EmptyDataError:
        console.print(f"EmptyDataError for {pub_file}.", style="bold red")
        return None
    
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
            if os.stat(file).st_size == 0:
                continue
            file_obj.seek(0)
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
        nrows = end_index - start_index
        nrows = 0 if nrows < 0 else nrows
        try:
            df = pd.read_csv(file, on_bad_lines="skip", skiprows=start_index, nrows=nrows)
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
        
        # ? Remove any rows that contains strings
        sub_df = sub_df[sub_df.applymap(np.isreal).all(1)]
        
        # ? Sort the columns alphabetically
        sub_df = sub_df.reindex(sorted(sub_df.columns, key=lambda x: int(x.split("_")[1])), axis=1)
        
        # ? Add a total column
        valid_cols = [col for col in sub_df.columns if col.startswith('sub_')]
        try:
            sub_df["total_" + str(metric).replace(" ", "_")] = sub_df[valid_cols].sum(axis=1)
        except FutureWarning:
            pass
        except TypeError as e:
            print(metric)
            print(valid_cols)
            print(sub_df[valid_cols].head())
            asdf
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
        with console.status(f"[{i+1}/{total_tests}] Summarising test..."):
            test_name = os.path.basename(test)
            summary_filename = os.path.join(summary_dir, test_name + "_summary.csv")
    
            if os.path.exists(summary_filename):
                continue
            
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
            df.to_csv(summary_filename)

def get_metric_stats(df, metric):
    try:
        if df[metric].dtype == 'object':
            df = df[pd.to_numeric(df[metric], errors='coerce').notnull()]
    except KeyError:
        return None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
    
    metric_mean = df[metric].astype(float).mean()
    metric_std = df[metric].astype(float).std()
    metric_min = df[metric].astype(float).min()
    metric_max = df[metric].astype(float).max()
    metric_10 = df[metric].astype(float).quantile(0.1)
    metric_20 = df[metric].astype(float).quantile(0.2)
    metric_25 = df[metric].astype(float).quantile(0.25)
    metric_30 = df[metric].astype(float).quantile(0.3)
    metric_40 = df[metric].astype(float).quantile(0.4)
    metric_50 = df[metric].astype(float).quantile(0.5)
    metric_60 = df[metric].astype(float).quantile(0.6)
    metric_70 = df[metric].astype(float).quantile(0.7)
    metric_75 = df[metric].astype(float).quantile(0.75)
    metric_80 = df[metric].astype(float).quantile(0.8)
    metric_90 = df[metric].astype(float).quantile(0.9)
    metric_95 = df[metric].astype(float).quantile(0.95)
    metric_99 = df[metric].astype(float).quantile(0.99)
    
    return (metric_mean, metric_std, metric_min, metric_max, metric_10, metric_20, metric_25, metric_30, metric_40, metric_50, metric_60, metric_70, metric_75, metric_80, metric_90, metric_95, metric_99)

def generate_ml_summary(camp_path):
    ml_filename = f"{camp_path}_ml.csv"

    # ? If the file already exists, don't do anything
    if os.path.exists(ml_filename):
        return

    summary_dir = f"{camp_path}_summaries"
    summary_csvs = [os.path.join(summary_dir, f) for f in os.listdir(summary_dir) if f.endswith(".csv") and "summary" in f.lower()]
    
    if len(summary_csvs) == 0:
        return

    for i, file in enumerate(summary_csvs):
        with console.status(f"[{i+1}/{len(summary_csvs)}] Generating ML file from {file}..."):
            # ? Get the settings used from the test name
            filename = os.path.basename(file)
            datalen_bytes, pub_count, sub_count, best_effort, multicast, durability = get_settings_from_testname(filename)
            
            summ_df = pd.read_csv(file, index_col=0).astype(float, errors='ignore')
            summ_df = summ_df.apply(pd.to_numeric, errors='coerce').dropna()
            
            # ? Get stats for all metrics
            stats = [datalen_bytes, pub_count, sub_count, best_effort, multicast, durability]
            columns = ["datalen_bytes", "pub_count", "sub_count", "best_effort", "multicast", "durability"]
            
            for metric in ["latency_us", "total_mbps", "total_total_samples", "total_samples/s", "total_lost_samples"]:
                metric_stats = get_metric_stats(summ_df, metric)
                stats.extend(metric_stats)
                columns.extend([
                    f"{metric}_mean",
                    f"{metric}_std",
                    f"{metric}_min",
                    f"{metric}_max",
                    f"{metric}_10",
                    f"{metric}_20",
                    f"{metric}_25",
                    f"{metric}_30",
                    f"{metric}_40",
                    f"{metric}_50",
                    f"{metric}_60",
                    f"{metric}_70",
                    f"{metric}_75",
                    f"{metric}_80",
                    f"{metric}_90",
                    f"{metric}_95",
                    f"{metric}_99",
                ])

            # ? Put all stats into a row
            try:
                row_df = pd.DataFrame([stats], columns=columns)
            except ValueError:
                pprint(f"len(stats): {len(stats)}")
                pprint(f"len(columns): {len(columns)}")

                raise ValueError("Stats and columns are not the same length")
    
            if not 'df' in locals():
                df = pd.DataFrame(columns=columns)
            
            df = pd.concat([df, row_df], ignore_index=True)
    
    df.to_csv(ml_filename)
    console.print(f"ML file generated: {ml_filename}", style="bold green")