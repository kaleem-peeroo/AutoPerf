import sys
import time

from src import Timer, ExperimentRunner
from src.logger import logger
from .config import Config
from src.experiments import Campaign
from src.utils import experiment_already_ran

from rich.console import Console
from rich.pretty import pprint
from datetime import datetime

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
        campaign.set_start_time(datetime.now())

        logger.info("[{}/{}] Running campaign: {}".format(
            campaign_index + 1,
            len(campaigns),
            campaign.get_name()
        ))

        campaign.create_output_folder()
        campaign.get_ess()
        campaign.generate_experiments()
        experiments = campaign.get_experiments()

        for experiment in experiments:
            max_retries = campaign.get_max_retries()
            current_attempt = 1

            if experiment_already_ran(experiment, campaign):
                successful_statuses = campaign.get_ran_statuses(experiment, "success")
                if any(successful_statuses):
                    logger.info("[{}/{}] Experiment: {} already ran successfully. Skipping.".format(
                        experiment.get_index() + 1,
                        len(experiments),
                        experiment.get_name()
                    ))
                    continue

            current_attempt = campaign.get_ran_attempts(experiment) + 1

            while current_attempt <= max_retries:
                experiment_runner = ExperimentRunner(
                    experiment, 
                    experiment.get_index(),
                    len(experiments),
                    current_attempt
                )
                            
                logger.info("[{}/{}] [Attempt #{}] Running experiment: {}".format(
                    experiment.get_index() + 1,
                    len(experiments),
                    current_attempt,
                    experiment.get_name()
                ))

                experiment_runner.run()
                experiment_runner.download_results()
                experiment_runner.check_results()

                logger.debug("{} status: {}".format(
                    experiment.get_name(),
                    experiment_runner.get_status()
                ))

                logger.info("[{}/{}] {} completed.".format(
                    experiment.get_index() + 1,
                    len(experiments),
                    experiment.get_name()
                ))

                campaign.add_results(experiment_runner)
                campaign.write_results()

                if len(experiment_runner.get_errors()) == 0:
                    break

                current_attempt += 1

        campaign.set_end_time(datetime.now())

if __name__ == "__main__":
    with Timer():
        main()
