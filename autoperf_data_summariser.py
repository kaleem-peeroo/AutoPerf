import sys
from typing import Dict, List, Optional, Tuple

from rich.console import Console
console = Console()

from autoperf import *
from constants import *

def main(sys_args: List[str]) -> Optional[None]:
    if len(sys_args) < 2:
        logger.error(
            f"Config filepath not specified."
        )
        return 

    CONFIG_PATH = sys_args[1]
    console.print(f"Reading config: {CONFIG_PATH}", style="bold blue")
    CONFIG, config_error = read_config(CONFIG_PATH)
    if config_error:
        logger.error(f"Error reading config: {config_error}")
    console.print(f"Config read: {CONFIG_PATH}", style="bold green")

    for EXPERIMENT_INDEX, EXPERIMENT in enumerate(CONFIG):
        EXPERIMENT_NAME = EXPERIMENT['experiment_name']

        console.print(
            f"[{EXPERIMENT_INDEX + 1}/{len(CONFIG)}] Summarising {EXPERIMENT['experiment_name']}...",
            style="bold blue"
        )

        EXPERIMENT_DIRPATH, dirname_error = get_dirname_from_experiment(EXPERIMENT)
        if dirname_error:
            logger.error(f"Error getting experiment dirname: {dirname_error}")
            continue

        # Summarise tests
        summarise_tests(EXPERIMENT_DIRPATH) 
        if summarise_tests is None:
            logger.error(
                f"Error summarising tests for {EXPERIMENT['experiment_name']}"
            )
            continue

if __name__ == "__main__":
    main(sys.argv)
