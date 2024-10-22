import os
import sys
import unittest
import pandas as pd

from rich.console import Console
from pprint import pprint
from Timer import Timer

console = Console()
class TestDatasetMaker(unittest.TestCase):
    def test_dataset_maker(self):
        self.assertTrue(os.path.exists("./output/datasets/"))

class APExperiment:
    def __init__(self, path):
        self.path = path
        self.filename = os.path.basename(path)

    def get_qos(self):
        self.qos = {}

        split_filename = self.filename.replace(".csv", "").split("_")
        for item in split_filename:
            item = item.lower()

            if "sec" in item:
                self.qos["duration_secs"] = int(item.replace("sec", ""))

            elif "pub" in item:
                self.qos["pub_count"] = int(item.replace("pub", ""))

            elif "sub" in item:
                self.qos["sub_count"] = int(item.replace("sub", ""))

            elif "be" in item or "rel" in item:

                if "be" in item:
                    self.qos["use_reliable"] = False
                else:
                    self.qos["use_reliable"] = True

            elif "uc" in item or "mc" in item:

                if "uc" in item:
                    self.qos["use_multicast"] = False
                else:
                    self.qos["use_multicast"] = True

            elif "dur" in item:
                self.qos["durability_level"] = int(item.replace("dur", ""))

            elif "lc" in item:
                self.qos["latency_count"] = int(item.replace("lc", ""))

            elif item.endswith("b"):
                self.qos["datalen_bytes"] = int(item.replace("b", ""))

            else:
                print(f"Unknown item: {item}")

        return self.qos

class DatasetMaker:
    def __init__(self, path):
        self.path = path
        self.dirname = os.path.basename(os.path.dirname(path))
        self.output_dir = f"./output/datasets/{self.dirname}"

    def make_dataset(self):
        files = os.listdir(self.path)
        csv_files = [file for file in files if file.endswith(".csv")]
        csv_files = [os.path.join(self.path, file) for file in csv_files]

        df = pd.DataFrame()
        for index, file in enumerate(csv_files):
            file_count_str = f"[{index + 1}/{len(csv_files)}]"

            with console.status(
                f"{file_count_str} Processing {file}..."
            ) as status:
                ap_experiment = APExperiment(file)
                qos = ap_experiment.get_qos()

                file_df = pd.read_csv(file)

                df_obj = file_df.select_dtypes(["object"])
                file_df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
                file_df[df_obj.columns] = df_obj.apply(pd.to_numeric, errors="coerce")

                file_df["duration_secs"] = qos["duration_secs"]
                file_df["datalen_bytes"] = qos["datalen_bytes"]
                file_df["pub_count"] = qos["pub_count"]
                file_df["sub_count"] = qos["sub_count"]
                file_df["use_reliable"] = qos["use_reliable"]
                file_df["use_multicast"] = qos["use_multicast"]
                file_df["durability_level"] = qos["durability_level"]
                file_df["latency_count"] = qos["latency_count"]

                df = pd.concat([df, file_df])

                console.print(
                    f"{file_count_str} Processed {os.path.basename(file)}",
                    style="bold green",
                )

            if index % 50 == 0:
                try:
                    df.to_parquet(f"{self.output_dir}_dataset.parquet", index=False)
                    print(f"Dataset periodically saved to {self.output_dir}_dataset.parquet")
                except Exception as e:
                    pprint(file_df['sub_0_lost_samples'])

                    weird = (file_df.applymap(type) != file_df.iloc[0].apply(type)).any(axis=1)
                    pprint(file_df[weird])
                    # file_df[weird].to_csv(f"{self.output_dir}_weird.csv", index=False)
                    # print(f"Dataset saved to {self.output_dir}_weird.csv")

                    pprint(e)
                    return

        df.to_parquet(f"{self.output_dir}_dataset.parquet", index=False)
        print(f"Dataset saved to {self.output_dir}_dataset.parquet")

def main(args):
    SUMM_PATH = args[0]
    if not os.path.exists(SUMM_PATH):
        return "Path does not exist"

    dataset_maker = DatasetMaker(SUMM_PATH)
    dataset_maker.make_dataset()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        console.print(
            "Usage: python dataset_maker.py <path>",
            style="bold red",
        )
        sys.exit(1)

    unittest.main(argv=[""], exit=False)

    with Timer():
        error = main(sys.argv[1:])
        if error:
            console.print(
                error,
                style="bold red",
            )
            sys.exit(1)
