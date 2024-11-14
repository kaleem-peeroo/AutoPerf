import toml

from src.logger import logger

from rich.pretty import pprint

class ConfigParser:
    REQUIRED_SETTINGS = [
        "campaign_name",
        "gen_type",
        "max_failures",
        "max_retries",
        "slave_machines",
        "qos_settings"
    ]

    OPTIONAL_SETTINGS = [
        "total_tests",
        "noise_gen",
        "test_names"
    ]

    QOS_SETTINGS = [
        "duration_secs",
        "datalen_bytes",
        "pub_count",
        "sub_count",
        "use_reliable",
        "use_multicast",
        "durability",
        "latency_count"
    ]

    def __init__(self, filename):
        self.filename = filename
        self.config = {}

    def __rich_repr__(self):
        yield "filename", self.filename
        yield "config", self.config

    def parse(self):
        try:
            with open(self.filename) as f:
                self.config = toml.load(f)

            if 'campaigns' not in self.config:
                logger.error(
                    "No campaigns found in config file"
                )
                self.config = {}
                raise ValueError

            else:
                self.config = self.config['campaigns']

        except FileNotFoundError as e:
            logger.error(
                f"Config file {self.filename} not found"
            )
            raise e

    def validate(self):
        if self.config == {}:
            self.parse()

        for campaign in self.config:
            if not isinstance(campaign, dict):
                logger.error(
                    f"Invalid campaign found in config file: {self.filename}"
                )
                raise ValueError

            keys = list(campaign.keys())

            for setting in self.REQUIRED_SETTINGS: 
                if setting not in campaign:
                    logger.error(
                        f"Setting {setting} not found in config file: {self.filename}"
                    )
                    raise ValueError

            # RCG needs total_tests
            if campaign['gen_type'] == 'rcg':
                if 'total_tests' not in keys:
                    logger.error(
                        "Gen = RCG but 'total_tests' setting not found in {}".format(
                            self.filename
                        )
                    )
                    raise ValueError

            if "test_names" in keys:
                if not isinstance(campaign["test_names"], list):
                    logger.error(
                        f"test_names must be a list in {self.filename}"
                    )
                    raise ValueError

            if "noise_gen" in keys:
                if not isinstance(campaign["noise_gen"], dict):
                    logger.error(
                        f"noise_gen must be a dictionary in {self.filename}"
                    )
                    raise ValueError

                self.validate_noise_gen(campaign)

            if "qos_settings" in keys:
                if not isinstance(campaign["qos_settings"], dict):
                    logger.error(
                        f"qos_settings must be a dictionary in {self.filename}"
                    )
                    raise ValueError
                
                self.validate_qos_settings(campaign)

            if "slave_machines" in keys:
                if not isinstance(campaign["slave_machines"], list):
                    logger.error(
                        f"slave_machines must be a list in {self.filename}"
                    )
                    raise ValueError

                self.validate_slave_machines(campaign)

    def validate_noise_gen(self, campaign):
        REQUIRED_KEYS = [
            'delay',
            'bandwidth_rate'
        ]

        OPTIONAL_KEYS = [
            'packet_loss',
            'packet_corruption',
            'packet_duplication',
        ]

        noise_gen = campaign["noise_gen"]
        if noise_gen == {}:
            return

        keys = list(noise_gen.keys())

        for key in REQUIRED_KEYS:
            if key not in keys:
                logger.error(
                    f"Setting {key} not found in noise_gen in {self.filename}"
                )
                raise ValueError

            if key == 'delay':
                if not isinstance(noise_gen[key], dict):
                    logger.error(
                        f"delay must be a dictionary in noise_gen in {self.filename}"
                    )
                    raise ValueError

            else:
                if not isinstance(noise_gen[key], str):
                    logger.error(
                        f"{key} must be a string in noise_gen in {self.filename}"
                    )
                    raise ValueError

        for key in OPTIONAL_KEYS:
            if key in keys:
                if not isinstance(noise_gen[key], str):
                    logger.error(
                        f"{key} must be a string in noise_gen in {self.filename}"
                    )
                    raise ValueError
                    
    def validate_qos_settings(self, campaign):
        qos_settings = campaign["qos_settings"]
        keys = list(qos_settings.keys())

        for setting in self.QOS_SETTINGS:
            if setting not in keys:
                logger.error(
                    f"Setting {setting} not found in qos_settings in {self.filename}"
                )
                raise ValueError

            if not isinstance(qos_settings[setting], list):
                logger.error(
                    f"Setting {setting} must be an list in {self.filename}"
                )
                raise ValueError

            if len(qos_settings[setting]) == 0:
                logger.error(
                    f"Setting {setting} must have at least one value in {self.filename}"
                )
                raise ValueError

            # Sort the lists from small to large
            qos_settings[setting].sort()

    def validate_slave_machines(self, campaign):
        REQUIRED_KEYS = [
            'hostname',
            'participant_type',
            'ip',
            'ssh_key_path',
            'username',
            'perftest_path'
        ]
        slave_machines = campaign["slave_machines"]

        for machine in slave_machines:
            if not isinstance(machine, dict):
                logger.error(
                    f"Slave machine must be a dict in {self.filename}"
                )
                raise ValueError

            if machine == {}:
                logger.error(
                    f"Slave machine must not be empty in {self.filename}"
                )
                raise ValueError

            keys = list(machine.keys())
            for key in REQUIRED_KEYS:
                if key not in keys:
                    logger.error(
                        f"Setting {key} not found in slave_machines in {self.filename}"
                    )
                    raise ValueError

                if not isinstance(machine[key], str):
                    logger.error(
                        f"Setting {key} must be a string in slave_machines in {self.filename}"
                    )
                    raise ValueError
