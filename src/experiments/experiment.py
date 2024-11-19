from datetime import datetime

from .qos import QoS
from .machine import Machine

class Experiment:
    def __init__(
        self,
        name: str,
        qos: QoS,
        machines: list,
    ):
        self.name = name
        self.qos = qos
        self.machines = machines

    def __rich_repr__(self):
        yield "name", self.name
        yield "qos", self.qos
        yield "machines", self.machines

    def get_name(self):
        return self.name

    def get_qos(self):
        return self.qos

    def get_machines(self):
        return self.machines

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
