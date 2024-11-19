import sys

from src import Timer, ExperimentRunner
from src.logger import logger
from .config import Config

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

    config = Config(config_file)

    campaigns = config.get_campaigns()

    for campaign in campaigns:
        campaign.generate_experiments()
        experiments = campaign.get_experiments()

        for experiment in experiments:
            experiment_runner = ExperimentRunner(experiment)
            experiment_runner.run()
            experiment_runner.save_results()
            asdf

        campaign.save_results()

if __name__ == "__main__":
    with Timer():
        main()
