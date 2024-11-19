import concurrent.futures

from datetime import datetime
from rich.pretty import pprint
from typing import Dict, Tuple

class ExperimentRunner:
    def __init__(self, experiment):
        self.experiment = experiment
        self.attempt = 0
        self.start_time = datetime.min
        self.end_time = datetime.min
        self.status = "pending"
        self.errors = []

    def __rich_repr__(self):
        yield "experiment", self.experiment
        yield "attempt", self.attempt
        yield "start_time", datetime.strftime(self.start_time, "%Y-%m-%d %H:%M:%S")
        yield "end_time", datetime.strftime(self.end_time, "%Y-%m-%d %H:%M:%S")
        yield "status", self.status
        yield "errors", self.errors

    def run(self):
        """
        1. Check connections to machine (ping + ssh).
        2. Restart machines.
        3. Check connections to machines (ping + ssh).
        4. Get QoS configuration.
        5. Generate test scripts from QoS config.
        6. Allocate scripts per machine.
        7. Delete any artifact csv files.
        8. Generate noise genertion scripts if needed and add to existing scripts. 
        9. Run scripts.
        10. Check for and download results.
        11. Confirm all files are downloaded.
        12. Updated ESS.
        13. Return ESS.
        """
        self.start_time = datetime.now()
        
        if not self.ping_machines():
            self.status = "failed to ping machines"
            self.end_time = datetime.now()
            return
                
    def ping_machines(self):
        machines = self.experiment.get_machines()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(lambda machine: machine.ping(), machines)
            results = list(results)

        for result in results:
            was_pinged, ping_errors = result

            # Specify the action to differentiate between errors.
            for ping_error in ping_errors:
                ping_error["action"] = "ping"

            self.errors.append(ping_errors)

            if not was_pinged:
                return False

        return True

    def save_results(self):
        if self.end_time == datetime.min:
            self.end_time = datetime.now()

        if self.status == "pending":
            self.status = "completed"

        pprint(self)
