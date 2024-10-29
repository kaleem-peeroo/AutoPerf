import pandas as pd
import os

from rich.console import Console
from rich.pretty import pprint

console = Console()

CAMP_NAME = "5Pi_Data_Collection_71Mbps_Missing_Reruns"
ESS_PATH = f"./output/ess/{CAMP_NAME}.parquet"

ess_df = pd.read_parquet(ESS_PATH, engine="fastparquet")

empty_ess_df = ess_df[ess_df['end_status'] == 'empty_file_found']

print(f"success Before: {len(ess_df[ess_df['end_status'] == 'success'])}")
print(f"empty_file_found Before: {len(ess_df[ess_df['end_status'] == 'empty_file_found'])}")

for index, row in empty_ess_df.iterrows():
    count_string = f"[{index}/{len(ess_df)}]"
    with console.status(f"{count_string} Processing {row['test_name']}...") as status:

        test_name = row['test_name']
        output_path = os.path.join(f"./output/data/{CAMP_NAME}/", test_name)

        if not os.path.exists(output_path):
            print(f"{output_path} does NOT exist.")
            continue

        test_files = [os.path.join(output_path, f) for f in os.listdir(output_path)]
        if len(test_files) == 0:
            print(f"No files found in {output_path}")
            continue

        csv_files = [f for f in test_files if f.endswith(".csv")]
        if len(csv_files) == 0:
            print(f"No csv files found in {output_path}")
            continue

        really_has_empty_file = False
        for csv_file in csv_files:
            try:
                file_df = pd.read_csv(csv_file, nrows=10)
            except pd.errors.EmptyDataError as e:
                really_has_empty_file = True
                filename = os.path.basename(csv_file)
                print(f"{filename} was empty for {test_name}")
                break

            except pd.errors.ParserError as e:
                continue

        if not really_has_empty_file:
            ess_df.at[index, 'end_status'] = 'success'

print(f"success After: {len(ess_df[ess_df['end_status'] == 'success'])}")
print(f"empty_file_found After: {len(ess_df[ess_df['end_status'] == 'empty_file_found'])}")

ess_df.to_parquet(ESS_PATH)
