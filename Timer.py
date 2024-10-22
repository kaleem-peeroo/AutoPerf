from datetime import datetime

class Timer:
    def __enter__(self):
        self.start_time = datetime.now()
        return self

    def __exit__(self, *args):
        self.end_time = datetime.now()
        self.interval = (self.end_time - self.start_time).total_seconds()
        self.interval = round(self.interval, 2)

        now_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now_timestamp}] Ran in {self.interval} seconds.")
