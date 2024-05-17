import pandas as pd
import sys
import pytest
import os
import logging
import json
from icecream import ic

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    filename="autoperf.log", 
    filemode="w",
    format='%(asctime)s \t%(levelname)s \t%(message)s'
)
logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s \t%(levelname)s \t%(message)s'
)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

SYS_ARGS = sys.argv

if len(SYS_ARGS) < 2:
    logger.error(
        f"Config filepath not specified."
    )
    sys.exit(0)

CONFIG_PATH = SYS_ARGS[1]

if not os.path.exists(CONFIG_PATH):
    logger.error(
        f"Config path {CONFIG_PATH} does NOT exist."
    )
    sys.exit(0)

def read_config(config_path: str = ""):
    if config_path == "":
        logger.error(
            f"No config path passed to read_config()"
        )
        return None

    with open(config_path, 'r') as f:
        try:
            config = json.load(f)
        except ValueError:
            logger.error(
                f"Error parsing JSON for config file: {CONFIG_PATH}"
            )
            return None

    return config

def main(sys_args: [str] = None) -> None:
    CONFIG = read_config(CONFIG_PATH)
    if CONFIG is None:
        logger.error(
            f"Couldn't read config of {CONFIG_PATH}."
        )
        return None

if __name__ == "__main__":
    if pytest.main(["-q", "./pytests", "--exitfirst"]) == 0:
        main(sys.argv[1:])
    else:
        logger.error("Tests failed.")
        sys.exit(1)
