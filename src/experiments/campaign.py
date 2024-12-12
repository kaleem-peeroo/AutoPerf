import os
import warnings
import random
import pandas as pd
import time
import sys

from typing import List
from .machine import Machine
from .experiment import Experiment
from .experiment_runner import ExperimentRunner
from .qos import QoS
from .smart_plug import SmartPlug
from src.utils import get_qos_from_experiment_name, generate_qos_permutations, machine_params_from_str
from src.logger import logger

from rich.pretty import pprint
from datetime import datetime

warnings.simplefilter("ignore", category=FutureWarning)

class Campaign:
    def __init__(self):
        self.name                           = ""
        self.output_dirpath                 = ""
        self.ess_path                       = ""
        self.gen_type                       = ""
        self.max_failures                   = 0
        self.max_retries                    = 0
        self.ping_attempts                  = 0
        self.ssh_attempts                   = 0
        self.machines                       = []
        self.qos_config                     = None
        self.bw_rate                        = ""
        self.start_time                     = None
        self.end_time                       = None
        self.experiments                    = []
        self.total_experiments              = 0         # For RCG
        self.experiment_names               = []        # Custom experiment list
        self.results                        = []        # List of ExperimentRunners
        self.expected_total_experiments     = 0

    def __rich_repr__(self):
        yield "name",                       self.name
        yield "output_dirpath",             self.output_dirpath
        yield "ess_path",                   self.ess_path
        yield "gen_type",                   self.gen_type
        yield "max_failures",               self.max_failures
        yield "max_retries",                self.max_retries
        yield "ping_attempts",              self.ping_attempts
        yield "ssh_attempts",               self.ssh_attempts
        yield "machines",                   self.machines
        yield "qos_config",                 self.qos_config
        yield "bw_rate",                    self.bw_rate
        yield "start_time",                 self.start_time
        yield "end_time",                   self.end_time
        yield "experiments",                self.experiments
        yield "total_experiments",          self.total_experiments
        yield "experiment_names",           self.experiment_names
        yield "results",                    self.results
        yield "expected_total_experiments", self.expected_total_experiments
        
    def get_name(self):
        return self.name

    def get_gen_type(self):
        return self.gen_type

    def get_max_failures(self):
        return self.max_failures

    def get_max_retries(self):
        return self.max_retries

    def get_ping_attempts(self):
        return self.ping_attempts

    def get_ssh_attempts(self):
        return self.ssh_attempts

    def get_machines(self):
        return self.machines

    def get_experiments(self):
        return self.experiments

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def get_qos_config(self):
        return self.qos_config

    def get_bw_rate(self):
        return self.bw_rate

    def get_experiment_names(self):
        return self.experiment_names

    def get_output_dirpath(self):
        return self.output_dirpath

    def get_total_experiments(self):
        if self.total_experiments == 0:
            if not self.experiments:
                self.generate_experiments()

        if not self.experiments:
            raise ValueError("Experiments must be set")

        return len(self.experiments)

    def get_results(self):
        return self.results

    def get_expected_total_experiments(self):
        return self.expected_total_experiments

    def get_machines_by_type(self, participant_type):
        machines = []
        for machine in self.machines:
            if machine.get_participant_type() == participant_type:
                machines.append(machine)

        return machines

    def get_experiment_runners_for_experiment(self, experiment):
        experiment_runners = []
        for result in self.results:
            if result.get_experiment().get_id() == experiment.get_id():
                experiment_runners.append(result)

        return experiment_runners

    def set_name(self, name):
        if not isinstance(name, str):
            raise ValueError(f"Name must be a string: {name}")

        if name == "":
            raise ValueError("Name must not be empty")

        self.name = name

    def set_gen_type(self, gen_type):
        if not isinstance(gen_type, str):
            raise ValueError(f"Gen type must be a string: {gen_type}")

        if gen_type == "":
            raise ValueError("Gen type must not be empty")

        if gen_type not in ['pcg', 'rcg']:
            raise ValueError(f"Gen type must be 'pcg' or 'rcg': {gen_type}")

        if gen_type == 'rcg' and self.total_experiments == 0:
            raise ValueError("RCG needs total experiments to be > 0")

        self.gen_type = gen_type

    def set_max_failures(self, max_failures):
        if not isinstance(max_failures, int):
            raise ValueError(f"Max failures must be an int: {max_failures}")

        if max_failures < 0:
            raise ValueError(f"Max failures must be >= 0: {max_failures}")

        self.max_failures = max_failures

    def set_ping_attempts(self, ping_attempts):
        if not isinstance(ping_attempts, int):
            raise ValueError(f"Ping attempts must be an int: {ping_attempts}")

        if ping_attempts < 0:
            raise ValueError(f"Ping attempts must be >= 0: {ping_attempts}")

        self.ping_attempts = ping_attempts

    def set_ssh_attempts(self, ssh_attempts):
        if not isinstance(ssh_attempts, int):
            raise ValueError(f"SSH attempts must be an int: {ssh_attempts}")

        if ssh_attempts < 0:
            raise ValueError(f"SSH attempts must be >= 0: {ssh_attempts}")

        self.ssh_attempts = ssh_attempts

    def set_max_retries(self, max_retries):
        if not isinstance(max_retries, int):
            raise ValueError(f"Max retries must be an int: {max_retries}")

        if max_retries < 0:
            raise ValueError(f"Max retries must be >= 0: {max_retries}")

        self.max_retries = max_retries
    
    def set_machines(self, machines):
        if not isinstance(machines, list):
            raise ValueError(f"Machines must be a list: {machines}")

        if len(machines) == 0:
            raise ValueError("Machines must not be empty")

        for machine in machines:
            required_keys = [
                'hostname',
                'participant_type',
                'ip',
                'ssh_key_path',
                'username',
                'perftest_path'
            ]

            for key in required_keys:
                if key not in machine:
                    raise ValueError(f"Machine must have {key}")

            new_machine = Machine(
                machine['hostname'],
                machine['participant_type'],
                machine['ip'],
                machine['ssh_key_path'],
                machine['username'],
                machine['perftest_path'],
                self.ping_attempts,
                self.ssh_attempts
            )

            if 'smart_plug_name' in machine and 'smart_plug_ip' in machine:
                smart_plug = SmartPlug(
                    machine['smart_plug_name'],
                    machine['smart_plug_ip']
                )
                new_machine.set_smart_plug(smart_plug)

            self.machines.append(new_machine)

    def set_experiments(self, experiments):
        if not isinstance(experiments, list):
            raise ValueError(f"Experiments must be a list: {experiments}")

        if len(experiments) == 0:
            raise ValueError("Experiments must not be empty")

        for experiment in experiments:
            if not isinstance(experiment, Experiment):
                raise ValueError(f"Experiment must be an Experiment: {experiment}")

        self.experiments = experiments

    def set_start_time(self, start_time):
        if not isinstance(start_time, datetime):
            raise ValueError(f"Start time must be a float: {start_time}")

        self.start_time = start_time

    def set_end_time(self, end_time):
        if not isinstance(end_time, datetime):
            raise ValueError(f"End time must be a float: {end_time}")

        self.end_time = end_time
    
    def set_total_experiments(self, total_experiments):
        if not isinstance(total_experiments, int):
            raise ValueError(f"Total experiments must be an int: {total_experiments}")

        if total_experiments < 0:
            raise ValueError(f"Total experiments must be >= 0: {total_experiments}")

        self.total_experiments = total_experiments

    def generate_experiments(self):
        if self.experiments is not None and len(self.experiments) > 0:
            logger.info("Experiments already generated. Skipping...")
            return

        if not self.machines:
            raise ValueError("Machines must be set")

        if len(self.experiment_names) > 0:
            experiments = []

            for index, experiment_name in enumerate(self.experiment_names):
                qos = get_qos_from_experiment_name(experiment_name)
                experiment = Experiment(
                    index,
                    experiment_name, 
                    qos,
                    self.machines,
                    self.bw_rate
                )

                experiment_dirname = experiment_name.replace(" ", "_")
                experiment_dirpath = os.path.join(
                    self.output_dirpath, 
                    experiment_dirname
                )
                experiment.set_output_dirpath(
                    experiment_dirpath
                )

                experiments.append(experiment)

            self.set_experiments(experiments)

        elif self.gen_type == "pcg":
            self.generate_pcg_experiments()

        elif self.gen_type == "rcg":
            self.generate_rcg_experiments()

        else:
            raise ValueError(f"Unknown gen type: {self.gen_type}")

    def generate_pcg_experiments(self):
        """
        Thought dump:
        - take the qos config dict
        - generate all permutations
        - for each permutation, create an experiment
        - add the experiment to the list of experiments
        """
        if not self.qos_config:
            raise ValueError("QoS config must be set")

        qos_permutations = generate_qos_permutations(self.qos_config)

        experiments = []
        for index, qos_permutation in enumerate(qos_permutations):
            qos = QoS(
                qos_permutation['duration_secs'],
                qos_permutation['datalen_bytes'],
                qos_permutation['pub_count'],
                qos_permutation['sub_count'],
                qos_permutation['use_reliable'],
                qos_permutation['use_multicast'],
                qos_permutation['durability'],
                qos_permutation['latency_count']
            )
            experiment = Experiment(
                index,
                qos.get_qos_name(),
                qos,
                self.machines,
                self.bw_rate
            )

            experiment_dirname = qos.get_qos_name().replace(" ", "_")
            experiment_dirpath = os.path.join(
                self.output_dirpath, 
                experiment_dirname
            )
            experiment.set_output_dirpath(
                experiment_dirpath
            )

            experiments.append(experiment)

        self.set_experiments(experiments)

    def generate_random_qos(self):
        if not self.qos_config:
            raise ValueError("QoS config must be set")
        
        # Make sure that each qos value has max 2 values
        for key, value in self.qos_config.items():
            if len(value) > 2:
                raise ValueError(f"QoS value {key} has more than 2 values: {value}")

        random_qos = {}
        for key, value in self.qos_config.items():
            if len(value) == 1:
                random_qos[key] = value[0]

            else:
                if isinstance(value[0], int):
                    random_qos[key] = random.choice(range(value[0], value[1]))
                else:
                    random_qos[key] = random.choice(value)

        qos = QoS(
            random_qos['duration_secs'],
            random_qos['datalen_bytes'],
            random_qos['pub_count'],
            random_qos['sub_count'],
            random_qos['use_reliable'],
            random_qos['use_multicast'],
            random_qos['durability'],
            random_qos['latency_count']
        )

        return qos
        
    def generate_rcg_experiments(self):
        """
        Thought dump:
        - take the qos config dict
        - get the total experiments count
        - for i in range(total_experiments)
        - generate random qos config
        - create an experiment
        - check if the experiment already exists
        - add the experiment to the list of experiments
        """
        if not self.qos_config:
            raise ValueError("QoS config must be set")

        if not self.total_experiments:
            raise ValueError("Total experiments must be set")

        index = 0
        while len(self.experiments) < self.total_experiments:
            qos = self.generate_random_qos()
            if not qos:
                continue

            experiment = Experiment(
                index,
                qos.get_qos_name(),
                qos,
                self.machines,
                self.bw_rate
            )

            experiment_dirname = qos.get_qos_name().replace(" ", "_")
            experiment_dirpath = os.path.join(
                self.output_dirpath, 
                experiment_dirname
            )
            experiment.set_output_dirpath(
                experiment_dirpath
            )

            if experiment in self.experiments:
                logger.debug(f"Experiment already exists: {experiment.get_name()}")
                continue

            self.experiments.append(experiment)
            index += 1
                
    def set_qos_config(self, qos_config):
        if not isinstance(qos_config, dict):
            raise ValueError(f"QoS config must be a dict: {qos_config}")

        if qos_config == {}:
            raise ValueError("QoS config must not be empty")

        required_keys = [
            'duration_secs',
            'datalen_bytes',
            'pub_count',
            'sub_count',
            'use_reliable',
            'use_multicast',
            'durability',
            'latency_count'
        ]

        for key in required_keys:
            if key not in qos_config:
                raise ValueError(f"QoS config must have {key}")

        self.qos_config = qos_config

    def set_bw_rate(self, bw_rate):
        if bw_rate is None:
            self.bw_rate = ""
            return

        if not isinstance(bw_rate, str):
            raise ValueError(f"Bandwidth Rate (bw_rate) must be a str: {bw_rate}")

        self.bw_rate = bw_rate

    def set_experiment_names(self, experiment_names):
        if not isinstance(experiment_names, list):
            raise ValueError(f"experiment names must be a list: {experiment_names}")
        
        for experiment_name in experiment_names:
            if not isinstance(experiment_name, str):
                raise ValueError(f"experiment name must be a string: {experiment_name}")

            self.experiment_names.append(experiment_name)

    def set_output_dirpath(self, output_dirpath):
        if not isinstance(output_dirpath, str):
            raise ValueError(f"Output dirpath must be a string: {output_dirpath}")

        if output_dirpath == "":
            raise ValueError("Output dirpath must not be empty")

        if not os.path.exists(output_dirpath):
            raise ValueError(f"Output dirpath does not exist: {output_dirpath}")

        self.output_dirpath = output_dirpath

    def set_results(self, results):
        if not isinstance(results, list):
            raise ValueError(f"Results must be a list: {results}")
        
        self.results = results

    def set_expected_total_experiments(self, expected_total_experiments):
        if not isinstance(expected_total_experiments, int):
            raise ValueError(f"Expected total experiments must be an int: {expected_total_experiments}")

        if expected_total_experiments < 0:
            raise ValueError(f"Expected total experiments must be >= 0: {expected_total_experiments}")

        self.expected_total_experiments = expected_total_experiments

    def get_last_n_experiment_names(self, n):
        if n < 0:
            raise ValueError(f"n must be >= 0: {n}")

        if n == 0:
            return []

        if not self.results:
            return []

        returned_experiment_names = []
        for runner in self.results:
            exp_name = runner.get_experiment().get_name()
            if exp_name not in returned_experiment_names:
                returned_experiment_names.append(exp_name)

        return returned_experiment_names[-n:]

    def get_last_n_experiments(self, n):
        if n < 0:
            raise ValueError(f"n must be >= 0: {n}")

        if n == 0:
            return []

        if not self.results:
            return []

        exp_names = self.get_last_n_experiment_names(n)
        return [runner for runner in self.results if runner.get_experiment().get_name() in exp_names]

    def get_true_failed_experiment_names(self, experiments):
        if not experiments:
            return []

        exp_names = [runner.get_experiment().get_name() for runner in experiments]

        failed_experiment_names = []
        for exp_name in exp_names:
            exp_runners = [runner for runner in experiments if runner.get_experiment().get_name() == exp_name]
            have_errors = [len(runner.get_errors()) > 0 for runner in exp_runners]
            if all(have_errors):
                failed_experiment_names.append(exp_name)

        return failed_experiment_names

    def have_last_n_experiments_failed(self, n):
        if n < 0:
            raise ValueError(f"n must be >= 0: {n}")

        if n == 0:
            return []

        if not self.results:
            return []

        if len(self.results) < n:
            return False

        experiments = self.get_last_n_experiments(n)
        
        experiment_statuses = []
        for exp in experiments:
            exp_name = exp.get_experiment().get_name()

            exp_runners = [runner for runner in experiments if runner.get_experiment().get_name() == exp_name]
            have_errors = [len(runner.get_errors()) > 0 for runner in exp_runners]

            if exp_name not in [status['experiment_name'] for status in experiment_statuses]:
                if all(have_errors):
                    experiment_statuses.append({
                        'experiment_name': exp_name,
                        'has_failed': True
                    })
                else:
                    experiment_statuses.append({
                        'experiment_name': exp_name,
                        'has_failed': False
                    })

        last_n_statuses = [status['has_failed'] for status in experiment_statuses]

        logger.debug(f"Last {n} statuses: {last_n_statuses}")

        if all(last_n_statuses):
            return True 
        else:
            return False
        
    def get_ran_statuses(self, experiment=None, type="fail"):
        if type not in ["fail", "success"]:
            raise ValueError(f"Type must be 'fail' or 'success': {type}")

        if not experiment:
            results = self.results
        else:
            results = self.get_experiment_runners_for_experiment(experiment)

        if type == "fail":
            return [len(result.get_errors()) > 0 for result in results]

        else:
            return [len(result.get_errors()) == 0 for result in results]

    def get_ran_attempts(self, experiment):
        results = self.get_experiment_runners_for_experiment(experiment)
        return len(results)

    def get_experiment_results_from_ess(self, ess_path):
        logger.debug(f"Getting experiment results from ESS at {ess_path}")

        df = pd.read_json(
            ess_path, 
            orient='records',
            lines=True,
            compression='gzip'
        )

        logger.debug("Generating experiments from the ESS")
        for index, row in df.iterrows():
            cols = list(row.keys())

            if 'experiment_name' not in cols:
                raise ValueError("experiment_name not found in ESS")

            if 'machines' not in cols:
                raise ValueError(f"machines not found in ESS. cols: {cols}")

            experiment = Experiment(
                index,
                row['experiment_name'],
                get_qos_from_experiment_name(row['experiment_name']), 
                row['machines'],
                self.bw_rate,
            )
            experiment.set_output_dirpath(
                os.path.join(
                    self.output_dirpath,
                    row['experiment_name']
                )
            )

            experiment_runner = ExperimentRunner( 
                experiment,
                index,
                self.expected_total_experiments,
                row['attempt']
            )

            experiment_runner.set_errors(list(row['errors']))
            experiment_runner.set_start_time(row['start_time'])
            experiment_runner.set_end_time(row['end_time'])

            logger.debug(
                "Read #{} {}".format(
                    row['attempt'],
                    row['experiment_name']
                )
            )
            self.add_results(experiment_runner)

    def get_ess(self):
        ess_name = self.get_name().replace(" ", "_")
        ess_path = os.path.join("./output/ess", f"{ess_name}.jsonl.gz") 

        if os.path.exists(ess_path):
            logger.info(f"Resuming campaign: {self.get_name()}...")
            self.ess_path = ess_path
            self.get_experiment_results_from_ess(ess_path)
            return
            
        os.makedirs("./output/ess", exist_ok=True)

        logger.debug("Creating ESS at {}".format(ess_path))

        if os.path.exists(ess_path):
            self.ess_path = ess_path
            self.experiments = self.get_experiment_results_from_ess(ess_path)
            return

        else:
            columns = [
                'experiment_name',
                'attempt',
                'machines',
                'errors',
                'start_time',
                'end_time'
            ]

            df = pd.DataFrame(columns=columns)

            df.to_json(
                ess_path, 
                orient='records', 
                lines=True, 
                compression='gzip'
            )

            self.ess_path = ess_path

    def create_output_folder(self):
        dirname = self.get_name().replace(" ", "_")
        dirpath = os.path.join("./output/data", dirname)

        if os.path.exists(dirpath):
            logger.info(f"Output folder already exists at {dirpath}")

        os.makedirs(dirpath, exist_ok=True)
        
        self.set_output_dirpath(dirpath)

    def add_results(self, experiment_runner):
        if not isinstance(experiment_runner, ExperimentRunner):
            raise ValueError(f"ExperimentRunner must be an ExperimentRunner: {experiment_runner}")

        logger.debug("Adding results to campaign...")
        self.results.append(experiment_runner)

    def write_results(self):
        if not self.ess_path:
            raise ValueError("ESS path must be set")

        logger.debug("Reading ESS...")
        df = pd.read_json(
            self.ess_path, 
            orient='records',
            lines=True,
            compression='gzip'
        )
        
        df_row_count = len(df)
        results_count = len(self.results)

        if df_row_count > 0:
            if df_row_count + 1 != results_count:
                raise ValueError(f"Dataframe row count {df_row_count} + 1 != results count {results_count}")

        latest_result = self.results[-1]
        new_row = {
            'experiment_name': latest_result.experiment.get_name(),
            'attempt': latest_result.attempt,
            'machines': latest_result.experiment.get_machines(),
            'errors': latest_result.errors,
            'start_time': latest_result.start_time,
            'end_time': latest_result.end_time
        }

        try:
            df = pd.concat([
                df, 
                pd.DataFrame([new_row])
            ], ignore_index=True)

            df.to_json(
                self.ess_path, 
                orient="records", 
                lines=True, 
                compression="gzip"
            )

            logger.debug("Latest Experiment written to ESS")

        except Exception as e:
            pprint(new_row)
            raise e
