from datetime import datetime

from .qos import QoS

class Experiment:
    def __init__(
        self,
        name: str,
        qos: QoS,
    ):
        self.name = name
        self.qos = qos
