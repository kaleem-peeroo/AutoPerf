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

    for campaign_index, campaign in enumerate(campaigns):
        logger.info("[{}/{}] Running campaign: {}".format(
            campaign_index + 1,
            len(campaigns),
            campaign.get_name()
        ))

        campaign.create_output_folder()
        campaign.generate_experiments()
        experiments = campaign.get_experiments()

        for index, experiment in enumerate(experiments):
            experiment_runner = ExperimentRunner(
                experiment, 
                index,
                len(experiments)
            )

            experiment_runner.run()
            experiment_runner.download_results()

        campaign.save_results()

if __name__ == "__main__":
    with Timer():
        main()
