import toml

from src.logger import logger
from src.experiments import Campaign, Machine
from src.utils import generate_qos_permutations

from rich.pretty import pprint

class Config:
    REQUIRED_SETTINGS = [
        "campaign_name",
        "gen_type",
        "max_failures",
        "max_retries",
        "slave_machines",
        "qos_settings"
    ]

    OPTIONAL_SETTINGS = [
        "total_experiments",
        "bw_rate",
        "experiment_names"
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
        self.filename   = filename
        self.config     = self.parse()
        self.campaigns  = []

    def __rich_repr__(self):
        yield "filename",   self.filename
        yield "config",     self.config
        yield "campaigns",  self.campaigns

    def parse(self):
        logger.debug(f"Parsing config file: {self.filename}...")

        try:
            with open(self.filename) as f:
                self.config = toml.load(f)

            if 'campaigns' not in self.config:
                logger.error(
                    "No campaigns found in config file. Make sure the config file has a [[campaigns]]."
                )
                raise ValueError("No campaigns found in config file")

            else:
                self.config = self.config['campaigns']

                if len(self.config) == 0:
                    raise ValueError(f"No campaigns found in {self.filename}")

        except FileNotFoundError as e:
            logger.error(
                f"Config file {self.filename} not found"
            )
            raise e

        logger.debug(f"Parsed {self.filename} and found {len(self.config)} campaigns")

        return self.config

    def get_filename(self):
        return self.filename

    def set_filename(self, filename):
        if not isinstance(filename, str):
            raise ValueError(f"Filename must be a string: {filename}")

        if filename == "":
            raise ValueError("Filename must not be empty")

        self.filename = filename

    def get_config(self):
        return self.config

    def set_config(self, config):
        if not isinstance(config, dict):
            raise ValueError(f"Config must be a dict: {config}")

        if config == {}:
            raise ValueError("Config must not be empty")

        self.config = config

    def calculate_expected_total_experiments(self, campaign):
        if not isinstance(campaign, dict):
            raise ValueError(f"Campaign must be a dict: {campaign}")

        if campaign == {}:
            raise ValueError("Campaign must not be empty")

        campaign_keys = list(campaign.keys())

        if "gen_type" not in campaign_keys:
            raise ValueError("gen_type not found in campaign")

        if "total_experiments" not in campaign_keys:
            raise ValueError("total_experiments not found in campaign")

        if "qos_settings" not in campaign_keys:
            raise ValueError("qos_settings not found in campaign")

        if "experiment_names" not in campaign_keys:
            raise ValueError("experiment_names not found in campaign")

        if len(campaign["experiment_names"]) > 0:
            return len(campaign["experiment_names"])

        elif campaign['gen_type'] == "pcg":
            return len(generate_qos_permutations(campaign['qos_settings']))

        elif campaign['gen_type'] == "rcg":
            return campaign['total_experiments']

        else:
            raise ValueError(f"Unknown gen_type: {campaign['gen_type']}")
        
    def get_campaigns(self):
        logger.debug("Getting campaigns...")

        if not self.config:
            raise ValueError("Config is empty")
            
        if self.campaigns == []:

            for campaign in self.config:
                if not isinstance(campaign, dict):
                    logger.error(
                        f"Campaign must be a dictionary in {self.filename}"
                    )
                    raise ValueError

                self.validate_campaign_config(campaign)

                keys = list(campaign.keys())

                if "total_experiments" not in keys:
                    logger.debug("total_experiments not in keys. Setting total_experiments to 0.")
                    campaign["total_experiments"] = 0

                if "bw_rate" not in keys:
                    logger.debug("bw_rate not in keys. Setting bw_rate to nothing.")
                    campaign["bw_rate"] = ""

                if "experiment_names" not in keys:
                    logger.debug("experiment_names not in keys. Setting experiment_names to empty list.")
                    campaign["experiment_names"] = []

                new_campaign = Campaign()

                new_campaign.set_name(campaign["campaign_name"])
                new_campaign.set_total_experiments(campaign["total_experiments"])
                new_campaign.set_gen_type(campaign["gen_type"])
                new_campaign.set_max_failures(campaign["max_failures"])
                new_campaign.set_max_retries(campaign["max_retries"])
                new_campaign.set_machines(campaign["slave_machines"])
                new_campaign.set_qos_config(campaign["qos_settings"])
                new_campaign.set_bw_rate(campaign["bw_rate"])
                new_campaign.set_experiment_names(campaign["experiment_names"])
                new_campaign.set_expected_total_experiments(
                    self.calculate_expected_total_experiments(campaign)
                )

                self.campaigns.append(new_campaign)

        return self.campaigns

    def set_campaigns(self, campaigns):
        if not isinstance(campaigns, list):
            raise ValueError(f"Campaigns must be a list: {campaigns}")

        if campaigns == []:
            raise ValueError("Campaigns must not be empty")

        self.campaigns = campaigns

    def validate_bw_rate(self, campaign):
        bw_rate = campaign["bw_rate"]
        if not bw_rate:
            return

        if bw_rate == "":
            return

        if not isinstance(bw_rate, str):
            logger.error(
                f"bw_rate must be a string in {self.filename}"
            )
            raise ValueError

        if not bw_rate.endswith("bit"):
            logger.error(
                f"bw_rate must end with 'bit' in {self.filename}"
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

    def validate_config(self):
        if not self.config:
            raise ValueError("Config is empty")
            
        for campaign in self.config:
            campaign = self.validate_campaign_config(campaign)

    def validate_campaign_config(self, campaign):
        keys = list(campaign.keys())

        for setting in self.REQUIRED_SETTINGS:
            if setting not in keys:
                logger.error(
                    f"Setting {setting} not found in {self.filename}"
                )
                raise ValueError

        for setting in self.OPTIONAL_SETTINGS:
            if setting not in keys:
                campaign[setting] = None

        if campaign['max_retries'] < 1:
            logger.error(
                f"max_retries must be greater than 0 in {self.filename}"
            )
            raise ValueError
        
        self.validate_bw_rate(campaign)
        self.validate_qos_settings(campaign)

        return campaign

