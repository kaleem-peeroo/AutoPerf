from .qos import QoS
from .machine import Machine

class Experiment:
    def __init__(
        self,
        name: str,
        qos: QoS,
        machines: list,
        noise_gen: dict = {}
    ):
        self.name = name
        self.qos = qos
        self.machines = machines
        self.noise_gen = noise_gen

    def __rich_repr__(self):
        yield "name", self.name
        yield "qos", self.qos
        yield "machines", self.machines
        yield "noise_gen", self.noise_gen

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
