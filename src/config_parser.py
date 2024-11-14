import toml

from src.logger import logger

from rich.pretty import pprint

class ConfigParser:
    REQUIRED_SETTINGS = [
        "campaign_name",
        "gen_type",
        "max_failures",
        "max_retries"
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
            for setting in self.REQUIRED_SETTINGS: 
                if setting not in campaign:
                    logger.error(
                        f"Setting {setting} not found in config file: {self.filename}"
                    )
                    raise ValueError

            for setting in campaign:
                pprint(setting)
