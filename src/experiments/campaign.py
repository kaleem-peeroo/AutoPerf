import os
import pandas as pd

from typing import List
from .machine import Machine
from .experiment import Experiment
from .qos import QoS
from src.utils import get_qos_from_experiment_name, generate_qos_permutations
from src.logger import logger

from rich.pretty import pprint
from datetime import datetime

class Campaign:
    def __init__(self):
        self.name = ""
        self.output_dirpath = ""
        self.ess_path = ""
        self.gen_type = ""
        self.max_failures = 0
        self.max_retries = 0
        self.machines = []
        self.qos_config = None
        self.total_experiments = 0
        self.noise_gen = {}
        self.experiment_names = []
        self.start_time = None
        self.end_time = None
        self.experiments = []
        self.total_experiments = 0
        self.noise_gen = {}
        self.experiment_names = []
        self.results = []

    def __rich_repr__(self):
        yield "name", self.name
        yield "gen_type", self.gen_type
        yield "max_failures", self.max_failures
        yield "max_retries", self.max_retries
        yield "machines", self.machines
        yield "qos_config", self.qos_config
        yield "total_experiments", self.total_experiments
        yield "noise_gen", self.noise_gen
        yield "experiment_names", self.experiment_names
        yield "start_time", self.start_time
        yield "end_time", self.end_time
        yield "experiments", self.experiments
        yield "total_experiments", self.total_experiments
        yield "output_dirpath", self.output_dirpath
        yield "results", self.results
        
    def get_name(self):
        return self.name

    def get_gen_type(self):
        return self.gen_type

    def get_max_failures(self):
        return self.max_failures

    def get_max_retries(self):
        return self.max_retries

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

    def get_noise_gen(self):
        return self.noise_gen

    def get_experiment_names(self):
        return self.experiment_names

    def get_output_dirpath(self):
        return self.output_dirpath

    def get_total_experiments(self):
        if self.total_experiments == 0:
            if not self.experiments:
                self.generate_experiments()

        return len(self.experiments)

    def get_machines_by_type(self, participant_type):
        machines = []
        for machine in self.machines:
            if machine.get_participant_type() == participant_type:
                machines.append(machine)

        return machines

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

        if gen_type == 'rcg' and not self.total_experiments:
            raise ValueError("RCG needs total experiments")

        self.gen_type = gen_type

    def set_max_failures(self, max_failures):
        if not isinstance(max_failures, int):
            raise ValueError(f"Max failures must be an int: {max_failures}")

        if max_failures < 0:
            raise ValueError(f"Max failures must be >= 0: {max_failures}")

        self.max_failures = max_failures

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
                machine['perftest_path']
            )

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

        if len(self.experiment_names) > 0:
            experiments = []

            for experiment_name in self.experiment_names:
                qos = get_qos_from_experiment_name(experiment_name)
                experiment = Experiment(
                    experiment_name, 
                    qos,
                    self.machines,
                    self.noise_gen
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
        for qos_permutation in qos_permutations:
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
                qos.get_qos_name(),
                qos,
                self.machines,
                self.noise_gen
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

    def generate_rcg_experiments(self):
        raise NotImplementedError("RCG experiment generation not implemented")
                
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

    def set_noise_gen(self, noise_gen):
        if not isinstance(noise_gen, dict):
            raise ValueError(f"Noise gen must be a dict: {noise_gen}")

        self.noise_gen = noise_gen

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

    def create_ess(self):
        ess_path = os.path.join("./output/ess", f"{self.get_name()}.parquet") 

        if os.path.exists(ess_path):
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            logger.warning("ESS already exists at {}. Renaming the existing file to {}".format(
                ess_path,
                f"{ess_path.replace(".parquet", "")}_{timestamp}.parquet"
            ))

            os.rename(
                ess_path, 
                f"{ess_path.replace(".parquet", "")}_{timestamp}.parquet"
            )

        os.makedirs("./output/ess", exist_ok=True)

        logger.debug("Creating ESS at {}".format(ess_path))

        if os.path.exists(ess_path):
            self.ess_path = ess_path
            return
        else:
            columns = [
                'experiment_name',
                'attempt',
                'machine',
                'status',
                'errors',
                'start_time',
                'end_time'
            ]

            df = pd.DataFrame(columns=columns)

            df.to_parquet(ess_path)
            self.ess_path = ess_path

    def create_output_folder(self):
        dirname = self.get_name().replace(" ", "_")
        dirpath = os.path.join("./output/data", dirname)

        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        self.set_output_dirpath(dirpath)

    def add_results(self, experiment_runner):
        self.results.append(experiment_runner)

    def write_results(self):
        """
        Data that we have per ExperimentRunner (which is what is in self.results):
            - experiment
                - name
                - qos
                    - qos_name
                - machines
                    - hostname
                    - command
                    - run_output
                - output_dirpath
            - status
            - errors
            - attempt
            - start_time
            - end_time

        We can flatten into:
            - attempt
            - experiment_name
            - machine
                - dictionary string:
                    - hostname
                    - command
                    - run_output
            - status
            - errors
            - start_time
            - end_time
        """
        if not self.ess_path:
            raise ValueError("ESS path must be set")

        logger.info("Writing latest Experiment to ESS")

        df = pd.read_parquet(self.ess_path)
        
        df_row_count = len(df)
        results_count = len(self.results)

        if df_row_count > 0:
            if df_row_count + 1 != results_count:
                raise ValueError(f"Dataframe row count {df_row_count} + 1 != results count {results_count}")
