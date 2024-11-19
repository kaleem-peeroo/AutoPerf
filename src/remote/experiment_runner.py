import concurrent.futures
import os

from src.logger import logger

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
        1. Check connections to machines (ping + ssh).
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

        # TODO: Uncomment the below:
        
        # if not self.ping_machines():
        #     self.status = "failed to ping machines"
        #     self.end_time = datetime.now()
        #     return
        #
        # if not self.ssh_machines():
        #     self.status = "failed to ssh to machines"
        #     self.end_time = datetime.now()
        #     return
        #
        # if not self.restart_machines():
        #     self.status = "failed to restart machines"
        #     self.end_time = datetime.now()
        #     return
        #
        # # Longer timeout to wait for machines to restart
        # if not self.ping_machines(attempts=3, timeout=20):
        #     self.status = "failed to ping machines after restart"
        #     self.end_time = datetime.now()
        #     return

        """
        - Generate scripts per participant
        - Allocate scripts per machine
        - Generate noise generation scripts
        - Allocate noise generation scripts to machines
        - Run scripts as commands per machine
        """
        qos = self.experiment.get_qos()
        qos_scripts = qos.generate_scripts()
        machines = self.experiment.get_machines()

        pub_machines = self.experiment.get_machines_by_type("pub")
        pub_machine_count = len(pub_machines)

        sub_machines = self.experiment.get_machines_by_type("sub")
        sub_machine_count = len(sub_machines)

        if pub_machine_count == 0 and sub_machine_count == 0:
            raise ValueError("No machines found for experiment.")

        if pub_machine_count == 0:
            raise ValueError("No publisher machines found for experiment.")

        if sub_machine_count == 0:
            raise ValueError("No subscriber machines found for experiment.")

        for machine in machines:
            perftest_path = machine.get_perftest_path()
            perftest_dir = os.path.dirname(perftest_path)

            machine.set_command(f"source ~/.bashrc; cd {perftest_dir};")

        if pub_machine_count == 1:
            pub_machines[0].set_scripts([script for script in qos_scripts if "-pub" in script])
            pub_machines[0].generate_command()

        if sub_machine_count == 1:
            sub_machines[0].set_scripts([script for script in qos_scripts if "-sub" in script])
            sub_machines[0].generate_command()

        if pub_machine_count > 1:
            pub_scripts = [script for script in qos_scripts if "-pub" in script]
            for pub_machine_i, pub_machine in enumerate(pub_machines):
                for script_i, script in enumerate(pub_scripts):
                    if script_i % pub_machine_count == pub_machine_i:
                        pub_machine.add_script(script)

                pub_machine.generate_command()

        if sub_machine_count > 1:
            sub_scripts = [script for script in qos_scripts if "-sub" in script]
            for sub_machine_i, sub_machine in enumerate(sub_machines):
                for script_i, script in enumerate(sub_scripts):
                    if (1 - (script_i % sub_machine_count )) == sub_machine_i:
                        sub_machine.add_script(script)

                sub_machine.generate_command()

        pprint(machines)
                                    
    def execute_on_machines(
        self, 
        action: str = "",
        attempts: int = 3,
        timeout: int = 10
    ) -> bool:
        if action == "":
            raise ValueError("Action must be specified.")

        action_map = {
            "ping": lambda machine: machine.check_connection(
                "ping",
                total_attempts=attempts,
                timeout=timeout
            ),
            "ssh": lambda machine: machine.check_connection(
                "ssh",
                total_attempts=attempts,
                timeout=timeout
            ),
            "restart": lambda machine: machine.restart(
                timeout=timeout
            )
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
    
    def ping_machines(
        self,
        attempts: int = 3,
        timeout: int = 10
    ) -> bool:
        return self.execute_on_machines(
            "ping",
            attempts=attempts,
            timeout=timeout
        )

    def ssh_machines(
        self,
        attempts: int = 3,
        timeout: int = 10
    ) -> bool:
        return self.execute_on_machines(
            "ssh",
            attempts=attempts,
            timeout=timeout
        )
    
    def restart_machines(self) -> bool:
        return self.execute_on_machines("restart", timeout=60)
        
    def save_results(self):
        if self.end_time == datetime.min:
            self.end_time = datetime.now()

        if self.status == "pending":
            self.status = "completed"

        raise NotImplementedError("Saving results is not implemented yet.")
