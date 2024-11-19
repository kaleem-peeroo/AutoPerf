from datetime import datetime
from rich.pretty import pprint

class ExperimentRunner:
    def __init__(self, experiment):
        self.experiment = experiment
        self.attempt = 0
        self.start_time = datetime.min
        self.end_time = datetime.min
        self.status = "pending"

    def __rich_repr__(self):
        yield "experiment", self.experiment
        yield "attempt", self.attempt
        yield "start_time", self.start_time
        yield "end_time", self.end_time
        yield "status", self.status

    def run(self):
        """
        1. Check connections to machine (ping + ssh).
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

        machines = self.experiment.get_machines()

        for machine in machines:
            was_pinged, ping_errors = machine.ping()
