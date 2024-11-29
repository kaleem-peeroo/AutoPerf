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
            message_header = "[{}/{}] [{}/{}]".format(
                campaign_index + 1,
                len(campaigns),
                experiment.get_index() + 1,
                len(experiments)
            )

            logger.info("\n{} {}".format(
                message_header,
                experiment.get_name()
            ))

            max_failures = campaign.get_max_failures()
            if max_failures > 0:
                logger.debug("max_failures = {}. Checking if last {} experiments failed.".format(
                    max_failures,
                    max_failures
                ))
                
                # Get last n statuses as list of booleans. True = success, False = failure.
                have_last_n_experiments_succeeded = campaign.have_last_n_experiments_failed(max_failures)
                if have_last_n_experiments_succeeded != []:
                    logger.debug(
                        "Failed tests for last {} experiments: {}".format(
                            max_failures,
                            have_last_n_experiments_succeeded
                        )
                    )

                    # Are there enough tests to check?
                    if len(have_last_n_experiments_succeeded) >= max_failures:
                        # If all tests failed, stop the campaign.
                        if all([has_failed for has_failed in have_last_n_experiments_succeeded]):
                            logger.info(
                                "{} Last {} tests failed. Stopping campaign.".format(
                                    message_header,
                                    max_failures
                                )
                            )
                            break
                
            max_retries = campaign.get_max_retries()
            current_attempt = campaign.get_ran_attempts(experiment) + 1

            if experiment_already_ran(experiment, campaign):
                successful_statuses = campaign.get_ran_statuses(experiment, "success")
                if any(successful_statuses):
                    logger.info(
                        "{} [#{}] Already ran successfully. Skipping.".format(
                            message_header,
                            current_attempt
                        )
                    )
                    continue

            if current_attempt > max_retries:
                logger.info(f"Reached max retries ({max_retries}). Skipping.")
                continue

            while current_attempt <= max_retries:
                experiment_runner = ExperimentRunner(
                    experiment, 
                    experiment.get_index(),
                    len(experiments),
                    current_attempt
                )
                            
                logger.info(
                    f"{message_header} [#{current_attempt}] Running..."
                )

                experiment_runner.fake_run()
                experiment_runner.download_results()
                experiment_runner.check_results()

                campaign.add_results(experiment_runner)
                campaign.write_results()

                if len(experiment_runner.get_errors()) == 0:
                    logger.info(
                        "{} [#{}] Experiment {} succeeded.".format(
                            message_header,
                            current_attempt,
                            experiment.get_name()
                        )
                    )
                    break

                current_attempt += 1

            print()

        campaign.set_end_time(datetime.now())

if __name__ == "__main__":
    with Timer():
        main()
