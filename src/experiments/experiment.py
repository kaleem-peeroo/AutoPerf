import os
import asyncio

from typing import List
from rich.pretty import pprint

from .qos import QoS
from .machine import Machine
from src.utils import generate_id
from src.logger import logger

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
        self.id = self.set_id()

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

    def set_id(self):
        return generate_id("|".join([
            self.get_name()
        ]))

    def restart_smart_plugs(self):
        restart_statuses = []

        for machine in self.get_machines():
            restart_status = {
                "hostname": machine.get_hostname(),
                "was_restart_successful": True
            }

            try:
                if machine.get_smart_plug() is None:
                    logger.error(
                        f"Machine {machine.get_hostname()} does not have a smart plug to restart."
                    )
                    restart_status["was_restart_successful"] = False
                    restart_statuses.append(restart_status)
                    continue

                asyncio.run(
                    machine.get_smart_plug().restart()
                )

            except Exception as e:
                logger.error(
                    "Failed to restart plug on machine {}:\n\t{}".format(
                        machine.get_hostname(),
                        e
                    )
                )
                restart_status["was_restart_successful"] = False

            restart_statuses.append(restart_status)
            
        if all([status["was_restart_successful"] for status in restart_statuses]):
            pprint([status["was_restart_successful"] for status in restart_statuses])
            logger.info("Successfully restarted all plugs.")
            return True

        else:
            logger.error(
                "Failed to restart all plugs. Restart statuses: {}".format(
                    restart_statuses
                )
            )
            return False
        
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
