import sys
import os
import pandas as pd
import warnings

from typing import Optional, Tuple
from rich.pretty import pprint
from rich.console import Console
from rich.progress import track

from constants import *
from autoperf import get_qos_dict_from_test_name
from Timer import Timer

console = Console()
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

error_df = []

def parse_sub_files(
    sub_files: list[str] = []
) -> Tuple[
    Optional[pd.DataFrame | pd.Series],
    Optional[str]
]:
    global error_df
    if len(sub_files) == 0:
        return None, "No subscriber files found"

    subs_df = pd.DataFrame()
    for sub_file in sub_files:
        # ? Find out where to start parsing the file from 
        with open(sub_file, "r") as file_obj:
            if os.stat(sub_file).st_size == 0:
                continue
            file_obj.seek(0)
            sub_first_5_lines = [next(file_obj) for _ in range(5)]
            
        start_index = 0    
        for i, line in enumerate(sub_first_5_lines):
            if "Length (Bytes)" in line:
                start_index = i
                break
        
        if start_index == 0:
            console.print(
                f"Couldn't get start_index for header row from {os.path.basename(sub_file)}.",
                style="bold red"
            )
            error_df.append(
                {
                    "filepath": sub_file,
                    "filename": os.path.basename(sub_file),
                    "error": "Couldn't get start_index for header row"
                }
            )
            continue

        # ? Find out where to stop parsing the file from (ignore the summary stats at the end)
        with open(sub_file, "r") as file_obj:
            file_contents = file_obj.readlines()
        sub_last_5_lines = file_contents[-5:]
        line_count = len(file_contents)
        
        end_index = 0
        for i, line in enumerate(sub_last_5_lines):
            if "throughput summary" in line.lower():
                end_index = line_count - 5 + i - 2
                break
            
        if end_index == 0:
            # console.print(
            #     f"Couldn't get end_index for summary row from {os.path.basename(sub_file)}. Defaulting to end of file.",
            #     style="bold white"
            # )
            error_df.append(
                {
                    "filepath": sub_file,
                    "filename": os.path.basename(sub_file),
                    "error": "Couldn't get end_index for summary row. File writing might have been interrupted."
                }
            )
            end_index = line_count - 1

        nrows = end_index - start_index
        nrows = 0 if nrows < 0 else nrows

        try:
            df = pd.read_csv(
                sub_file, 
                on_bad_lines="skip", 
                skiprows=start_index, 
                nrows=nrows
            )
        except pd.errors.ParserError as e:
            console.log(
                f"Error when getting data from {os.path.basename(sub_file)}:{e}",
                style="bold red"
            )
            error_df.append(
                {
                    "filepath": sub_file,
                    "filename": os.path.basename(sub_file),
                    "error": e
                }
            )
            continue
        
        desired_metrics = ["total samples", "samples/s", "mbps", "lost samples"]
        
        sub_name = os.path.basename(sub_file).replace(".csv", "")

        for col in df.columns:
            for desired_metric in desired_metrics:
                if desired_metric in col.lower() and "avg" not in col.lower():
                    col_name = col.strip().lower().replace(" ", "_")

                    if "samples/s" in col_name:
                        col_name = "samples_per_sec"
                    elif "%" in col_name:
                        col_name = "lost_samples_percent"

                    subs_df[f"{sub_name}_{col_name}"] = df[col]
                    subs_df[
                        f"{sub_name}_{col_name}"
                    ] = subs_df[
                        f"{sub_name}_{col_name}"
                    ].astype(
                        float, 
                        errors="ignore"
                    )

    if subs_df.empty:
        error_df.append(
            {
                "filepath": sub_file,
                "filename": os.path.basename(sub_file),
                "error": "Subscriber data is empty"
            }
        )
        return None, "Subscriber data is empty"

    return subs_df, None

def get_colname(coltype: str = "", colnames: list[str] = []) -> Tuple[str, Optional[str]]:
    if coltype == "":
        return "", "Col name type not specified"

    if len(colnames) == 0:
        return "", "Col names not specified"

    for colname in colnames:
        if coltype in colname.lower():
            return colname, None

    return "", f"Couldn't find {coltype} colname"

def parse_pub_file(
    pub_file: str = ""
) -> Tuple[
    Optional[pd.Series],
    Optional[str]
]:
    global error_df
    if pub_file == "":
        return None, "Publisher file not specified"

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
        return None, f"Couldn't find start index for header row for {os.path.basename(pub_file)}."

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
        error_df.append(
            {
                "filepath": pub_file,
                "filename": os.path.basename(pub_file),
                "error": "Couldn't get end_index for summary row. File writing might have been interrupted."
            }
        )
        end_index = line_count - 1

    try:
        lat_df = pd.read_csv(
            pub_file, 
            skiprows=start_index, 
            nrows=end_index-start_index, 
            on_bad_lines="skip"
        )
    except pd.errors.EmptyDataError:
        error_df.append(
            {
                "filepath": pub_file,
                "filename": os.path.basename(pub_file),
                "error": "EmptyDataError"
            }
        )
        return None, f"EmptyDataError for {os.path.basename(pub_file)}."
    
    min_colname, error = get_colname('min', lat_df.columns)
    if error:
        console.print(
            f"Error getting min colname for {pub_file}: {error}",
            style="bold red"
        )

    max_colname, error = get_colname('max', lat_df.columns)
    if error:
        console.print(
            f"Error getting max colname for {pub_file}: {error}",
            style="bold red"
        )
    
    if len(lat_df) == 0:
        error_df.append(
            {
                "filepath": pub_file,
                "filename": os.path.basename(pub_file),
                "error": "Publisher data is empty"
            }
        )
        return None, f"Publisher data is empty for {os.path.basename(pub_file)}."

    first_row = lat_df.iloc[0]
    first_min = first_row[min_colname]
    first_max = first_row[max_colname]
    
    first_latency_values = list(set([first_min, first_max]))

    # ? Pick out the latency column ONLY
    latency_col = None
    for col in lat_df.columns:
        if "latency" in col.lower():
            latency_col = col
            break

    if latency_col is None:
        error_df.append(
            {
                "filepath": pub_file,
                "filename": os.path.basename(pub_file),
                "error": "Couldn't find latency column"
            }
        )
        return None, f"Couldn't find latency column for {os.path.basename(pub_file)}."

    lat_df = lat_df[latency_col]

    # ? Add the first latency values to the dataframe
    lat_df = pd.concat([
        pd.Series(first_latency_values),
        lat_df
    ], axis=0)

    lat_df = lat_df.reset_index(drop=True)
    lat_df = lat_df.rename("latency_us")
    lat_df = lat_df.dropna()
    lat_df = lat_df.astype(float, errors="ignore")
    
    return lat_df, None

def remove_strings(
    df: pd.DataFrame = pd.DataFrame()
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:

    # TODO: Validate parameters
    
    for col in df.columns:
        if df[col].dtype == "object":
            df.loc[:, col] = df[col].str.replace('[^0-9]', '')
            df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')

    return df, None

def summarise_test(
    test_dir: str = "",
    summ_path: str = ""
) -> Tuple[
    str,
    Optional[str]
]:
    global error_df
    if test_dir == "":
        return "", "Test directory not specified"

    if summ_path == "":
        return "", "Summary path not specified"

    if not os.path.exists(test_dir):
        return test_dir, f"Test directory does not exist: {test_dir}"

    if not os.path.exists(summ_path):
        return summ_path, f"Summary path does not exist: {summ_path}"

    if not os.path.isdir(test_dir):
        return test_dir, f"Test directory is not a directory: {test_dir}"

    test_dir = os.path.abspath(test_dir)
    summ_path = os.path.abspath(summ_path)
    test_name = os.path.basename(test_dir)

    test_summ_path = os.path.join(
        summ_path,
        test_name + ".csv"
    )

    csv_files = [f for f in os.listdir(test_dir) if f.endswith(".csv")] 
    pub_file = os.path.join(
        test_dir,
        "pub_0.csv"
    )
    sub_files = [f for f in csv_files if f.startswith("sub_")]
    sub_files = [os.path.join(test_dir, f) for f in sub_files]

    if not os.path.exists(pub_file):
        error_df.append(
            {
                "filepath": pub_file,
                "filename": os.path.basename(pub_file),
                "error": "Publisher file not found"
            }
        )
        return test_summ_path, f"Publisher file not found: {os.path.basename(pub_file)}"

    if len(sub_files) == 0:
        error_df.append(
            {
                "filepath": test_dir,
                "filename": os.path.basename(test_dir),
                "error": "No subscriber files found"
            }
        )
        return test_summ_path, f"No subscriber files found in {os.path.basename(test_dir)}"

    lat_df, error = parse_pub_file(pub_file)
    if lat_df is None or error:
        error_df.append(
            {
                "filepath": pub_file,
                "filename": os.path.basename(pub_file),
                "error": error
            }
        )
        return test_summ_path, f"Error parsing publisher file: {error}"

    subs_df, error = parse_sub_files(sub_files)
    if subs_df is None or error:
        return test_summ_path, f"Error parsing subscriber files: {error}"

    test_df = pd.concat([lat_df, subs_df], axis=1)
    
    # Calculate average and total for subs
    sub_cols = [col for col in test_df.columns if 'sub' in col.lower()]
    sub_cols_without_sub = ["_".join(col.split("_")[2:]) for col in sub_cols]
    sub_metrics = list(set(sub_cols_without_sub))

    for sub_metric in sub_metrics:
        sub_metric_cols = [col for col in sub_cols if sub_metric in col]
        sub_metric_df = test_df[sub_metric_cols]

        sub_metric_df, error = remove_strings(sub_metric_df)
        if error:
            error_df.append(
                {
                    "filepath": test_dir,
                    "filename": os.path.basename(test_dir),
                    "error": error
                }
            )
            continue

        test_df['avg_' + sub_metric + "_per_sub"] = sub_metric_df.mean(axis=1)
        test_df['total_' + sub_metric + "_over_subs"] = sub_metric_df.sum(axis=1)

    test_df.to_csv(test_summ_path, index=False)

    return test_summ_path, None

def summarise_tests(
    data_path: str = "",
    status: Console.status = None
) -> Tuple[
    str, 
    Optional[str]
]:
    global error_df
    if data_path == "":
        return "", "Data path not specified"

    if not os.path.exists(data_path):
        return data_path, f"Data path does not exist: {data_path}"

    data_path = os.path.abspath(data_path)

    summ_path = os.path.join(
        "./output/summarised_data/",
        os.path.basename(data_path)
    )

    SUMM_PATH = os.path.abspath(summ_path)
    os.makedirs(SUMM_PATH, exist_ok=True)

    test_dirs = [os.path.join(data_path, d) for d in os.listdir(data_path)]
    test_dirs = [d for d in test_dirs if os.path.isdir(d)]
    test_list = [os.path.basename(d) for d in test_dirs]
    
    for test_dir_count, test_dir in enumerate(test_dirs):
        count_string = f"[{test_dir_count + 1}/{len(test_dirs)}]"
        status.update(
            "{} Summarising {}".format(
                count_string, 
                test_dir
            )
        )
        test_summ_path, error = summarise_test(test_dir, SUMM_PATH)
        if error:
            console.print(
                f"Error summarising test {os.path.basename(test_dir)}: {error}",
                style="bold red"
            )
            error_df.append(
                {
                    "filepath": test_dir,
                    "filename": os.path.basename(test_dir),
                    "error": error
                }
            )
            continue

        console.print(
            f"{count_string} Summarised {os.path.basename(test_dir)}.",
            style="bold green"
        )

    # Check if all tests were summarised and if not list out which ones were not summarised
    summ_test_list = [os.path.basename(f).replace(".csv", "") for f in os.listdir(SUMM_PATH) if f.endswith(".csv")]

    if len(test_list) != len(summ_test_list):
        unsummarised_test_list = list(set(test_list) - set(summ_test_list))
        for unsummarised_test in unsummarised_test_list:
            error_df.append(
                {
                    "filepath": data_path,
                    "filename": unsummarised_test,
                    "error": "Test not summarised"
                }
            )

        console.print(
            f"{len(unsummarised_test_list)} tests were not summarised.",
            style="bold red"
        )
                    
    return SUMM_PATH, None

def generate_dataset(
    summ_path: str = "",
    truncation_percent: float = 0,
    status: Console.status = None
) -> Tuple[
    str,
    Optional[str]
]:
    if summ_path == "":
        return "", "Summary path not specified"

    if not os.path.exists(summ_path):
        return summ_path, f"Summary path does not exist: {summ_path}"

    summ_path = os.path.abspath(summ_path)
    test_files = os.listdir(summ_path)
    test_files = [os.path.join(summ_path, f) for f in test_files]
    test_files = [f for f in test_files if f.endswith(".csv")]

    truncation_integer = int(truncation_percent * 100)

    ds_path = summ_path.replace("summarised_data", "datasets")
    ds_filename = f"_{truncation_integer}_percent_dataset.parquet"
    ds_path = ds_path + ds_filename
    os.makedirs(os.path.dirname(ds_path), exist_ok=True)

    dataset_df = pd.DataFrame()
    for test_csv_count, test_csv in enumerate(test_files):
        count_string = f"[{test_csv_count + 1}/{len(test_files)}]"
        status.update(
            "{} Generating {} dataset from {}".format(
                count_string, 
                f"{truncation_integer}%",
                os.path.basename(test_csv)
            )
        )

        new_dataset_row = {}

        try:
            test_df = pd.read_csv(test_csv)
        except UnicodeDecodeError:
            console.print(
                f"UnicodeDecodeError reading {test_csv}.",
                style="bold red"
            )
            continue

        test_name = os.path.basename(test_csv)

        new_dataset_row = get_qos_dict_from_test_name(test_name)
        if new_dataset_row is None:
            console.print(
                f"Couldn't get qos dict for {test_name}.",
                style="bold red"
            )
            continue

        for key, value in new_dataset_row.items():
            if value == True:
                new_dataset_row[key] = 1
            elif value == False:
                new_dataset_row[key] = 0

        for column in test_df.columns:
            if "index" in column.lower():
                continue

            column_values = test_df[column]
            value_count = len(column_values)

            values_to_truncate = int(
                value_count * (truncation_percent / 100)
            )

            column_values = test_df[column].iloc[values_to_truncate:]
            column_df = pd.DataFrame(column_values)
            
            for PERCENTILE in PERCENTILES:
                new_dataset_row[
                    f"{column}_{PERCENTILE}%"
                ] = column_df.quantile(PERCENTILE / 100).values[0]

            for STAT in DISTRIBUTION_STATS:
                if STAT == "mean":
                    new_dataset_row[f"{column}_mean"] = column_df.mean().values[0]

                elif STAT == "std":
                    new_dataset_row[f"{column}_std"] = column_df.std().values[0]

                elif STAT == "min":
                    new_dataset_row[f"{column}_min"] = column_df.min().values[0]

                elif STAT == "max":
                    new_dataset_row[f"{column}_max"] = column_df.max().values[0]

        new_dataset_row_df = pd.DataFrame(
            [new_dataset_row]
        )

        dataset_df = pd.concat(
            [dataset_df, new_dataset_row_df],
            axis = 0,
            ignore_index = True
        )

    dataset_df.to_parquet(ds_path, index=False)
    console.print(
        f"Generated {truncation_integer}% dataset at {ds_path}.",
        style="bold green"
    )

    return ds_path, None

def main(sys_args: list[str]) -> None:
    global error_df
    with console.status("Running...") as status:
        if len(sys_args) < 2:
            console.print(
                "Filepath not specified. Usage:{}".format(
                    "\n\tpython data_summariser.py <path_to_test_folders>"
                ),
                style="bold red"
            )
            return

        DATA_PATH = sys_args[1]
        if not os.path.exists(DATA_PATH):
            console.print(
                f"Path does not exist: {DATA_PATH}",
                style="bold red"
            )
            return

        status.update("Summarising tests...")
        SUMM_PATH, error = summarise_tests(DATA_PATH, status)
        if error:
            console.print(
                "Error summarising tests: {}".format(error),
                style="bold red"
            )
            return
        
    error_df = pd.DataFrame(error_df)
    error_df.to_csv(
        "./output/summarised_data/data_summariser_errors.csv",
        index=False
    )
    console.print(
        "Errors saved to ./output/summarised_data/data_summariser_errors.csv",
        style="bold red"
    )

if __name__ == "__main__":
    main(sys.argv)
