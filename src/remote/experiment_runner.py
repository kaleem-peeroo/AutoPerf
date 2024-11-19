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
        yield "start_time", datetime.strftime(
            self.start_time, 
            "%Y-%m-%d %H:%M:%S"
        )
        yield "end_time", datetime.strftime(
            self.end_time, 
            "%Y-%m-%d %H:%M:%S"
        )
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

        if not self.ssh_machines():
            self.status = "failed to ssh to machines"
            self.end_time = datetime.now()
            return

        if not self.restart_machines():
            self.status = "failed to restart machines"
            self.end_time = datetime.now()
            return

    def execute_on_machines(self, action: str = "") -> bool:
        if action == "":
            raise ValueError("Action must be specified.")

        action_map = {
            "ping": lambda machine: machine.check_connection("ping"),
            "ssh": lambda machine: machine.check_connection("ssh"),
            "restart": lambda machine: machine.restart()
        }

        if action not in action_map:
            raise ValueError(f"Action not supported: {action}")

        lambda_function = action_map[action]
        
        machines = self.experiment.get_machines()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(
                executor.map(
                    lambda_function,
                    machines
                )
            )

        for result in results:
            was_executed, execute_errors = result

            if execute_errors:
                for execute_error in execute_errors:
                    execute_error["action"] = action
                self.errors.append(execute_errors)

            if not was_executed:
                return False

        return True
    
    def ping_machines(self) -> bool:
        return self.execute_on_machines("ping")
        
    def ssh_machines(self) -> bool:
        return self.execute_on_machines("ssh")

    def restart_machines(self) -> bool:
        return self.execute_on_machines("restart")
        
    def save_results(self):
        if self.end_time == datetime.min:
            self.end_time = datetime.now()

        if self.status == "pending":
            self.status = "completed"

        raise NotImplementedError("Saving results is not implemented yet.")
