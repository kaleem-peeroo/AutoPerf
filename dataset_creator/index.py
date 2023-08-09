import os
import sys

from rich.console import Console
from pprint import pprint

console = Console()

errors = []

def validate_test_dir(test_dir):
    if not os.path.isdir(test_dir):
        console.print(f"[red]Error: [/red]Invalid test directory path: {test_dir}")
        sys.exit(1)

    test_dir_contents = os.listdir(test_dir)
    if len(test_dir_contents) == 0:
        console.print(f"[red]Error: [/red]Test directory is empty: {test_dir}")
        errors.append(f"{test_dir} is empty.")
        return

def validate_command_line_args(args):
    if len(args) != 2:
        console.print("[red]Error: [/red]Invalid number of arguments")
        console.print(f"Usage: python index.py <dirpath>")
        sys.exit(1)

    dirpath = sys.argv[1]
    if not os.path.isdir(dirpath):
        console.print(f"[red]Error: [/red]Invalid directory path: {dirpath}")
        sys.exit(1)

    dir_contents = os.listdir(dirpath)
    if len(dir_contents) == 0:
        console.print(f"[red]Error: [/red]Directory is empty: {dirpath}")
        sys.exit(1)


if __name__ == "__main__":
    validate_command_line_args(sys.argv)
    
    dirpath = sys.argv[1]
    test_dirs = [os.path.join(dirpath, d) for d in os.listdir(dirpath)]

    for test_dir in test_dirs:
        validate_test_dir(test_dir)

        