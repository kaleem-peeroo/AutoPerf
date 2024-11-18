from typing import List
from .machine import Machine
from .experiment import Experiment
from .qos import QoS

from src.utils import get_qos_from_testname

from rich.pretty import pprint

class Campaign:
    def __init__(
        self,
        name: str,
        gen_type: str,
        max_failures: int,
        max_retries: int,
        machines: List[Machine],
        qos_config: dict,
        total_tests: int = 0,
        noise_gen: dict = {},
        experiment_names: List[str] = [],
    ):
        self.name = name
        self.gen_type = gen_type
        self.max_failures = max_failures
        self.max_retries = max_retries
        self.machines = machines
        self.qos_config = qos_config
        self.total_tests = 0
        self.noise_gen = {}
        self.test_names = []
        self.start_time = None
        self.end_time = None
        self.experiments = []

    def __init__(self):
        self.name = ""
        self.gen_type = ""
        self.max_failures = 0
        self.max_retries = 0
        self.machines = []
        self.qos_config = {}
        self.total_tests = 0
        self.noise_gen = {}
        self.test_names = []
        self.start_time = None
        self.end_time = None
        self.experiments = []
    
    def get_name(self):
        return self.name

    def set_name(self, name):
        if not isinstance(name, str):
            raise ValueError(f"Name must be a string: {name}")

        if name == "":
            raise ValueError("Name must not be empty")

        self.name = name

    def get_gen_type(self):
        return self.gen_type

    def set_gen_type(self, gen_type):
        if not isinstance(gen_type, str):
            raise ValueError(f"Gen type must be a string: {gen_type}")

        if gen_type == "":
            raise ValueError("Gen type must not be empty")

        if gen_type not in ['pcg', 'rcg']:
            raise ValueError(f"Gen type must be 'pcg' or 'rcg': {gen_type}")

        if gen_type == 'rcg' and not self.total_tests:
            raise ValueError("RCG needs total tests")

        self.gen_type = gen_type

    def get_max_failures(self):
        return self.max_failures

    def set_max_failures(self, max_failures):
        if not isinstance(max_failures, int):
            raise ValueError(f"Max failures must be an int: {max_failures}")

        if max_failures < 0:
            raise ValueError(f"Max failures must be >= 0: {max_failures}")

        self.max_failures = max_failures

    def get_max_retries(self):
        return self.max_retries

    def set_max_retries(self, max_retries):
        if not isinstance(max_retries, int):
            raise ValueError(f"Max retries must be an int: {max_retries}")

        if max_retries < 0:
            raise ValueError(f"Max retries must be >= 0: {max_retries}")

        self.max_retries = max_retries

    def get_machines(self):
        return self.machines

    def get_machines_by_type(self, participant_type):
        machines = []
        for machine in self.machines:
            if machine.get_participant_type() == participant_type:
                machines.append(machine)

        return machines

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

    def get_experiments(self):
        return self.experiments

    def set_experiments(self, experiments):
        if not isinstance(experiments, list):
            raise ValueError(f"Experiments must be a list: {experiments}")

        if len(experiments) == 0:
            raise ValueError("Experiments must not be empty")

        for experiment in experiments:
            if not isinstance(experiment, Experiment):
                raise ValueError(f"Experiment must be an Experiment: {experiment}")

        self.experiments = experiments

    def get_start_time(self):
        return self.start_time

    def set_start_time(self, start_time):
        if not isinstance(start_time, float):
            raise ValueError(f"Start time must be a float: {start_time}")

        if start_time < 0:
            raise ValueError(f"Start time must be >= 0: {start_time}")

        self.start_time = start_time

    def get_end_time(self):
        return self.end_time

    def set_end_time(self, end_time):
        if not isinstance(end_time, float):
            raise ValueError(f"End time must be a float: {end_time}")

        if end_time < 0:
            raise ValueError(f"End time must be >= 0: {end_time}")

        self.end_time = end_time

    def get_total_tests(self):
        if self.total_tests == 0:
            if not self.experiments:
                self.experiments = self.generate_experiments()
                self.total_tests = len(self.experiments)

                return self.total_tests

        return self.total_tests

    def set_total_tests(self, total_tests):
        if not isinstance(total_tests, int):
            raise ValueError(f"Total tests must be an int: {total_tests}")

        if total_tests < 0:
            raise ValueError(f"Total tests must be >= 0: {total_tests}")

        self.total_tests = total_tests

    def generate_experiments(self):
        experiments = []

        if len(self.test_names) > 0:
            for test_name in self.test_names:
                qos = get_qos_from_testname(test_name)
                experiment = Experiment(
                    test_name, 
                    qos
                )
                experiments.append(experiment)

            return experiments

        if self.gen_type == 'pcg':
            
                
    def get_qos_config(self):
        return self.qos_config

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

    def get_noise_gen(self):
        return self.noise_gen

    def set_noise_gen(self, noise_gen):
        if not isinstance(noise_gen, dict):
            raise ValueError(f"Noise gen must be a dict: {noise_gen}")

        self.noise_gen = noise_gen

    def get_test_names(self):
        return self.test_names

    def set_test_names(self, test_names):
        if not isinstance(test_names, list):
            raise ValueError(f"Test names must be a list: {test_names}")
        
        for test_name in test_names:
            if not isinstance(test_name, str):
                raise ValueError(f"Test name must be a string: {test_name}")

            self.test_names.append(test_name)
