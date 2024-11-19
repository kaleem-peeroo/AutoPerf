import concurrent.futures
import os

from src.logger import logger

from datetime import datetime
from rich.pretty import pprint
from typing import Dict, Tuple
from multiprocessing import Process, Manager

class ExperimentRunner:
    def __init__(
        self, 
        experiment, 
        experiment_index,
        total_experiments_count
    ):
        self.experiment_index = experiment_index
        self.experiment = experiment
        self.total_experiments_count = total_experiments_count
        self.attempt = 0
        self.start_time = datetime.min
        self.end_time = datetime.min
        self.status = "pending"
        self.errors = []

    def __rich_repr__(self):
        yield "experiment_index", self.experiment_index
        yield "experiment", self.experiment
        yield "total_experiments_count", self.total_experiments_count
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

    def get_experiment_index(self):
        return self.experiment_index

    def get_experiment(self):
        return self.experiment

    def get_total_experiments_count(self):
        return self.total_experiments_count

    def get_attempt(self):
        return self.attempt

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def get_status(self):
        return self.status

    def get_errors(self):
        return self.errors

    def set_experiment_index(self, experiment_index):
        if not isinstance(experiment_index, int):
            raise ValueError("Experiment index must be an integer.")

        if experiment_index < 0:
            raise ValueError("Experiment index must be >= 0.")

        self.experiment_index = experiment_index

    def set_experiment(self, experiment):
        if not isinstance(experiment, Experiment):
            raise ValueError("Experiment must be an Experiment.")

        self.experiment = experiment

    def set_total_experiments_count(self, total_experiments_count):
        if not isinstance(total_experiments_count, int):
            raise ValueError("Total experiments count must be an integer.")

        if total_experiments_count < 0:
            raise ValueError("Total experiments count must be >= 0.")

        self.total_experiments_count = total_experiments_count

    def set_attempt(self, attempt):
        if not isinstance(attempt, int):
            raise ValueError("Attempt must be an integer.")

        if attempt < 0:
            raise ValueError("Attempt must be >= 0.")

        self.attempt = attempt

    def set_start_time(self, start_time):
        if not isinstance(start_time, datetime):
            raise ValueError("Start time must be a datetime.")

        self.start_time = start_time

    def set_end_time(self, end_time):
        if not isinstance(end_time, datetime):
            raise ValueError("End time must be a datetime.")

        self.end_time = end_time

    def set_status(self, status):
        if not isinstance(status, str):
            raise ValueError("Status must be a string.")

        self.status = status

    def set_errors(self, errors):
        if not isinstance(errors, list):
            raise ValueError("Errors must be a list.")

        self.errors = errors

    def add_error(self, error):
        if not isinstance(error, dict):
            raise ValueError("Error must be a dictionary.")

        self.errors.append(error)

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

        for machine in self.experiment.get_machines():
            perftest_path = machine.get_perftest_path()
            perftest_dir = os.path.dirname(perftest_path)

            machine.set_command(f"source ~/.bashrc; cd {perftest_dir};")

        self.generate_noise_scripts()
        self.generate_and_allocate_qos_scripts()

        for machine in self.experiment.get_machines():
            machine.generate_command()

            logger.debug(
                "[{}/{}] [{}] Removing artifact files...".format(
                    self.experiment_index + 1,
                    self.total_experiments_count,
                    self.experiment.get_name()
                )
            )
            if not machine.remove_artifact_files():
                self.status = "failed to remove artifact files"
                self.end_time = datetime.now()
                return

        self.run_scripts(timeout=self.experiment.get_timeout())

    def run_scripts(self, timeout: int = 600):
        logger.debug(
            "[{}/{}] [{}] Running scripts with timeout of {} seconds...".format(
                self.experiment_index + 1,
                self.total_experiments_count,
                self.experiment.get_name(),
                timeout
            )
        )

        machines = self.experiment.get_machines()
        with Manager() as manager:
            processes = []
            for machine in machines:
                process = Process(
                    target=machine.run,
                    name=f"{machine.get_hostname()}-process",
                )
                processes.append(process)
                process.start()

            for index, process in enumerate(processes):
                # Give the first process the actual timeout (test + buffer)
                if index == 0:
                    process.join(timeout)

                # Give everything else just the buffer
                else:
                    process.join(60)

                if process.is_alive():
                    logger.warning(
                        f"{process.name} is still alive after {timeout} seconds. Terminating..."
                    )
                    
                    machines[index].add_run_output(
                        "{} timed out after {} seconds".format(
                            machines[index].get_hostname(),
                            timeout
                        )
                    )

                    self.errors.append({
                        "hostname": machines[index].get_hostname(),
                        "ip": machines[index].get_ip(),
                        "command": machines[index].get_command(),
                        "error": f"{process.name} timed out after {timeout} seconds.",
                        "action": "run_scripts"
                    })

                    process.terminate()
                    process.join()
                    process.close()

                else:
                    machines[index].add_run_output(
                        "completed"
                    )
                    process.close()
                
        for machine in machines: 
            if "timed out" in machine.get_run_output():
                self.status = "timed out"
                self.end_time = datetime.now()
                return
                
    def generate_noise_scripts(self):
        logger.debug(
            "[{}/{}] [{}] Generating noise generation scripts...".format(
                self.experiment_index + 1,
                self.total_experiments_count,
                self.experiment.get_name()
            )
        )

        if self.experiment.get_noise_gen() is None:
            return

        noise_gen = self.experiment.get_noise_gen()
        if noise_gen == {}:
            return

        machines = self.experiment.get_machines()
        for machine in machines:
            qdisc_script = "sudo tc qdisc add dev eth0"
            netem_script = "root netem"
            bw_rate_script = f"rate {noise_gen['bandwidth_rate']}"

            noise_gen_script = "{} {} {}".format(
                qdisc_script,
                netem_script,
                bw_rate_script
            )

            machine.set_command(
                f"{machine.get_command()} {noise_gen_script};"
            )

    def generate_and_allocate_qos_scripts(self):
        logger.debug(
            "[{}/{}] [{}] Generating and allocating QoS scripts...".format(
                self.experiment_index + 1,
                self.total_experiments_count,
                self.experiment.get_name()
            )
        )

        qos = self.experiment.get_qos()
        qos_scripts = qos.generate_scripts()

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
