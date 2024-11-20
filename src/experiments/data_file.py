import os
import pandas as pd

from rich.pretty import pprint

class DataFile:
    def __init__(self, filepath):
        self.filepath = filepath

    def get_filepath(self):
        return self.filepath

    def get_filename(self):
        return os.path.basename(self.get_filepath())

    def set_filepath(self, filepath):
        if not isinstance(filepath, str):
            raise ValueError("Filepath must be a string")

        if not filepath:
            raise ValueError("Filepath cannot be empty")

        if not os.path.exists(filepath):
            raise FileNotFoundError("File does not exist")

        self.filepath = filepath

    def read(self):
        try:
            df = pd.read_csv(self.get_filepath(), nrows=10)

            if df.empty:
                raise ValueError(f"File is empty: {self.get_filepath()}")

        except Exception as e:
            raise ValueError(f"Error reading {self.get_filepath()}: {e}")

        return df

    def is_valid(self):
        try:
            df  = self.read()
        except Exception as e:
            return False, str(e)

        return True, None
