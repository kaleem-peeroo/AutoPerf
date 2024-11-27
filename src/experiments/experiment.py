import os
import hashlib

from typing import List
from rich.pretty import pprint

from .qos import QoS
from .machine import Machine

class Experiment:
    def __init__(
        self,
        index: int,
        name: str,
        qos: QoS,
        machines: List[Machine],
        noise_gen: dict = {}
    ):
        self.index = index
        self.name = name
        self.qos = qos
        self.machines = machines
        self.noise_gen = noise_gen
        self.output_dirpath = ""
        self.id = self.generate_id()

    def __rich_repr__(self):
        yield "index", self.index
        yield "name", self.name
        yield "qos", self.qos
        yield "machines", self.machines
        yield "noise_gen", self.noise_gen
        yield "output_dirpath", self.output_dirpath

    def get_id(self):
        return self.id

    def get_index(self):
        return self.index

    def get_name(self):
        return self.name

    def get_qos(self):
        return self.qos

    def get_machines(self):
        return self.machines

    def get_noise_gen(self):
        return self.noise_gen

    def get_timeout(self):
        return self.qos.duration_secs + 120
        
    def get_output_dirpath(self):
        return self.output_dirpath

    def get_machines_by_type(self, participant_type): 
        if not isinstance(participant_type, str):
            raise ValueError(f"Participant type must be a str: {participant_type}")

        if len(participant_type) == 0:
            raise ValueError(f"Participant type must not be empty: {participant_type}")

        if " " in participant_type:
            raise ValueError(f"Participant type must not contain spaces: {participant_type}")

        if participant_type not in ["pub", "sub", "all"]:
            raise ValueError(f"Participant type not supported: {participant_type}")

        machines_by_type = []
        for machine in self.machines:
            if machine.get_participant_type() == "all" or machine.get_participant_type() == participant_type:
                machines_by_type.append(machine)

        return machines_by_type

    def set_id(self, id):
        if not isinstance(id, str):
            raise ValueError(f"ID must be a str: {id}")

        self.id = id

    def set_index(self, index):
        if not isinstance(index, int):
            raise ValueError(f"Index must be an int: {index}")

        if index < 0:
            raise ValueError(f"Index must be >= 0: {index}")

        self.index = index

    def set_name(self, name):
        if not isinstance(name, str):
            raise ValueError(f"Name must be a str: {name}")

        if len(name) == 0:
            raise ValueError(f"Name must not be empty: {name}")

        # Are there spaces in the name?
        if " " in name:
            raise ValueError(f"Name must not contain spaces: {name}")

        self.name = name

    def set_qos(self, qos):
        if not isinstance(qos, QoS):
            raise ValueError(f"QoS must be a QoS: {qos}")

        self.qos = qos

    def set_machines(self, machines):
        if not isinstance(machines, list):
            raise ValueError(f"Machines must be a list: {machines}")

        for machine in machines:
            if not isinstance(machine, Machine):
                raise ValueError(f"Machine must be a Machine: {machine}")

        self.machines = machines

    def set_noise_gen(self, noise_gen):
        if not isinstance(noise_gen, dict):
            raise ValueError(f"Noise gen must be a dict: {noise_gen}")

        self.noise_gen = noise_gen

    def set_output_dirpath(self, output_dirpath):
        if not isinstance(output_dirpath, str):
            raise ValueError(f"Output dirpath must be a str: {output_dirpath}")

        if not os.path.exists(output_dirpath):
            os.makedirs(output_dirpath)

        self.output_dirpath = output_dirpath    

    def generate_id(self):
        info = "|".join([
            str(self.index),
            self.name,
        ])

        id = hashlib.sha256(info.encode()).hexdigest()

        return id

