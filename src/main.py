import sys

from src import Timer 
from src.logger import logger
from .config_parser import ConfigParser

from rich.console import Console
from rich.pretty import pprint

console = Console()

def main():
    if len(sys.argv) != 2:
        console.print(
            "Usage: python main.py <config_file>",
            style="bold red"
        )
        sys.exit(1)

    config_file = sys.argv[1]
    config = ConfigParser(config_file)
    config.parse()
    config.validate()

    # pprint(config.config)

if __name__ == "__main__":
    with Timer():
        main()
