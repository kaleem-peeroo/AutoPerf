import concurrent.futures
import os
import time
import random

from src.logger import logger
from .experiment import Experiment
from .data_file import DataFile
from src.utils import generate_id

from datetime import datetime
from rich.pretty import pprint
from typing import Dict
from multiprocessing import Process, Manager

class ExperimentRunner:
    def __init__(
        self, 
        experiment, 
        experiment_index,
        total_experiments_count,
        attempt
    ):
        self.experiment_index           = experiment_index
        self.experiment                 = experiment
        self.total_experiments_count    = total_experiments_count
        self.output_dirpath             = experiment.get_output_dirpath()
        self.attempt                    = attempt
        self.start_time                 = datetime.min
        self.end_time                   = datetime.min
        self.errors                     = []
        self.data_files                 = []
        self.id                         = self.set_id()

    def __del__(self):
        # logger.debug(f"ExperimentRunner {self.get_id()} deleted.")
        self.experiment = None
        
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
        yield "errors", self.errors

    def get_id(self):
        return self.id

    def get_data_files(self):
        return self.data_files

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

    def get_errors(self):
        return self.errors

    def get_output_dirpath(self):
        return self.output_dirpath

    def set_id(self):
        return generate_id(
            "|".join([
                self.experiment.get_id(),
                str(self.attempt),
            ])
        )

    def set_data_files(self, data_files):
        if not isinstance(data_files, list):
            raise ValueError("Data files must be a list.")

        for data_file in data_files:
            if not isinstance(data_file, DataFile):
                raise ValueError("Data file must be a DataFile.")

        self.data_files = data_files

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

    def set_output_dirpath(self, output_dirpath):
        if not isinstance(output_dirpath, str):
            raise ValueError("Output dirpath must be a string.")

        if output_dirpath == "":
            raise ValueError("Output dirpath must not be empty.")

        if not os.path.exists(output_dirpath):
            raise ValueError(f"Output dirpath does not exist: {output_dirpath}")

        self.output_dirpath = output_dirpath
        
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

    def set_errors(self, errors):
        if not isinstance(errors, list):
            raise ValueError("Errors must be a list.")

        self.errors = errors

    def add_error(self, error):
        if not isinstance(error, dict):
            raise ValueError("Error must be a dictionary.")

        self.errors.append(error)

    def run_without_restart(self):
        """
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
            self.errors.append({"error": "failed to ping machines"})
            self.end_time = datetime.now()
            return
        
        if not self.ssh_machines():
            self.errors.append({"error": "failed to ssh to machines"})
            self.end_time = datetime.now()
            return
        
        for machine in self.experiment.get_machines():
            perftest_path = machine.get_perftest_path()
            perftest_dir = os.path.dirname(perftest_path)

            machine.set_command(f"source ~/.bashrc; cd {perftest_dir};")

        self.generate_noise_scripts()
        self.generate_and_allocate_qos_scripts()

        for machine in self.experiment.get_machines():
            machine.generate_command()

            logger.debug(
                "[{}/{}] [{}] [{}] Removing artifact files...".format(
                    self.experiment_index + 1,
                    self.total_experiments_count,
                    self.experiment.get_name(),
                    machine.get_hostname()
                )
            )

            if not machine.remove_artifact_files():
                self.errors.append({
                    "hostname": machine.get_hostname(),
                    "ip": machine.get_ip(),
                    "error": "failed to remove artifact files",
                })
                self.end_time = datetime.now()
                return

        self.run_scripts(timeout_secs=self.experiment.get_timeout())

        return len(self.get_errors()) == 0

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

        if not self.ping_machines():
            self.errors.append({"error": "failed to ping machines"})
            self.end_time = datetime.now()
            return
        
        if not self.ssh_machines():
            self.errors.append({"error": "failed to ssh to machines"})
            self.end_time = datetime.now()
            return
        
        if not self.restart_machines():
            self.errors.append({"error": "failed to restart machines"})
            self.end_time = datetime.now()
            return
        
        # Wait 10 seconds for restart
        logger.debug("Waiting 5 seconds for machines to restart...")
        time.sleep(5)
        
        # Longer timeout to wait for machines to restart
        if not self.ping_machines(attempts=3, timeout=20):
            self.errors.append({"error": "failed to ping machines after restart"})
            self.end_time = datetime.now()
            return

        # Longer timeout to wait for machines to restart
        if not self.ssh_machines(attempts=3, timeout=20):
            self.errors.append({"error": "failed to ssh to machines after restart"})
            self.end_time = datetime.now()
            return

        for machine in self.experiment.get_machines():
            perftest_path = machine.get_perftest_path()
            perftest_dir = os.path.dirname(perftest_path)

            machine.set_command(f"source ~/.bashrc; cd {perftest_dir};")

        self.generate_noise_scripts()
        self.generate_and_allocate_qos_scripts()

        for machine in self.experiment.get_machines():
            machine.generate_command()

            logger.debug(
                "[{}/{}] [{}] [{}] Removing artifact files...".format(
                    self.experiment_index + 1,
                    self.total_experiments_count,
                    self.experiment.get_name(),
                    machine.get_hostname()
                )
            )

            if not machine.remove_artifact_files():
                self.errors.append({
                    "hostname": machine.get_hostname(),
                    "ip": machine.get_ip(),
                    "error": "failed to remove artifact files",
                })
                self.end_time = datetime.now()
                return

        self.run_scripts(timeout_secs=self.experiment.get_timeout())

        return len(self.get_errors()) == 0
    
    def fake_run(self):
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

        for machine in self.experiment.get_machines():
            machine.generate_command()

            logger.debug(
                "[{}/{}] [{}] Removing artifact files...".format(
                    self.experiment_index + 1,
                    self.total_experiments_count,
                    self.experiment.get_name()
                )
            )

        # Randomly add error
        if random.randint(0, 1) == 1:

            # Sleep for random milliseconds between 0 and 1000
            # time.sleep(random.randint(0, 1000) / 1000)

            self.add_error({"error": "fake error"})

        self.end_time = datetime.now()

    def run_scripts(self, timeout_secs: int = 600):
        logger.debug(
            "[{}/{}] [{}] Running scripts with timeout of {} seconds...".format(
                self.experiment_index + 1,
                self.total_experiments_count,
                self.experiment.get_name(),
                timeout_secs
            )
        )

        machines = self.experiment.get_machines()
        with Manager() as manager:
            shared_dict = manager.dict()
            processes = []
            for machine in machines:
                process = Process(
                    target=run_script_on_machine,
                    args=(machine, timeout_secs, shared_dict),
                    name=f"{machine.get_hostname()}-process",
                )
                processes.append(process)
                process.start()

            for index, process in enumerate(processes):
                # Give the first process the actual timeout (test + buffer)
                if index == 0:
                    process.join(timeout_secs)

                # Give everything else just the buffer
                else:
                    process.join(60)

                if process.is_alive():
                    logger.warning(
                        f"{process.name} is still alive after {timeout_secs} seconds. Terminating..."
                    )
                    
                    machines[index].add_run_output(
                        "{} timed out after {} seconds".format(
                            machines[index].get_hostname(),
                            timeout_secs
                        )
                    )

                    self.errors.append({
                        "hostname": machines[index].get_hostname(),
                        "ip": machines[index].get_ip(),
                        "command": machines[index].get_command(),
                        "error": f"{process.name} timed out after {timeout_secs} seconds.",
                    })

                    process.terminate()
                    process.join()
                    process.close()

                else:
                    process.close()
                
                run_outputs = shared_dict.get(machines[index].get_hostname(), "")
                for run_output in run_outputs:
                    machines[index].add_run_output(run_output)
            
        for machine in machines: 
            if "timed out" in machine.get_run_output():
                self.errors.append({
                    "hostname": machine.get_hostname(),
                    "ip": machine.get_ip(),
                    "error": f"timed out after {timeout_secs} seconds",
                })
                self.end_time = datetime.now()
                return

        self.end_time = datetime.now()
                
    def generate_noise_scripts(self):
        logger.debug(
            "[{}/{}] [{}] Generating bandwidth limitting scripts...".format(
                self.experiment_index + 1,
                self.total_experiments_count,
                self.experiment.get_name()
            )
        )

        if self.experiment.get_bw_rate() is None:
            return

        bw_rate = self.experiment.get_bw_rate()
        if bw_rate == "":
            return

        machines = self.experiment.get_machines()
        for machine in machines:
            qdisc_script = "sudo tc qdisc add dev eth0"
            netem_script = "root netem"
            bw_rate_script = f"rate {bw_rate}"

            bw_rate_script = "{} {} {}".format(
                qdisc_script,
                netem_script,
                bw_rate_script
            )

            machine.set_command(
                f"{machine.get_command()} {bw_rate_script};"
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
                    self.errors.append(execute_error)

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
        
    def check_results(self):
        logger.debug(
            "[{}/{}] [{}] Checking results...".format(
                self.experiment_index + 1,
                self.total_experiments_count,
                self.experiment.get_name()
            )
        )

        output_dirpath = self.get_output_dirpath()

        files = os.listdir(output_dirpath)
        csv_files = [file for file in files if file.endswith(".csv")]
        expected_file_count = self.experiment.qos.get_expected_file_count()

        logger.debug(
            "[{}/{}] [{}] Found {} csv files (expected {})...".format(
                self.experiment_index + 1,
                self.total_experiments_count,
                self.experiment.get_name(),
                len(csv_files),
                expected_file_count
            )
        )

        if len(csv_files) != expected_file_count:
            self.add_error({
                "error": f"Expected {expected_file_count} csv files, but found {len(csv_files)}",
                "expected_file_count": expected_file_count,
                "actual_file_count": len(csv_files),
            })
            return

        for csv_file in csv_files:
            csv_file_path = os.path.join(output_dirpath, csv_file)
            data_file = DataFile(csv_file_path)
            self.data_files.append(data_file)

        logger.debug(
            "[{}/{}] [{}] Checking data files...".format(
                self.experiment_index + 1,
                self.total_experiments_count,
                self.experiment.get_name()
            )
        )

        for data_file in self.data_files:
            is_valid, error = data_file.is_valid()
            if not is_valid:
                logger.warning(
                    "[{}/{}] [{}] Data file {} is invalid: {}".format(
                        self.experiment_index + 1,
                        self.total_experiments_count,
                        self.experiment.get_name(),
                        data_file.get_filename(),
                        error
                    )
                )

                self.add_error({
                    "error": f"Data file {data_file.get_filename()} is not valid: {error}",
                })
                continue

    def download_results(self):
        machines = self.experiment.get_machines()
        for machine in machines:
            if not machine.download_results(self.get_output_dirpath()):
                self.add_error({
                    "hostname": machine.get_hostname(),
                    "ip": machine.get_ip(),
                    "error": "failed to download results",
                })
                continue

def run_script_on_machine(machine, timeout_secs: int = 600, shared_dict: Dict = {}):
    output = machine.run(timeout_secs)
    hostname = machine.get_hostname()
    shared_dict[hostname] = output
