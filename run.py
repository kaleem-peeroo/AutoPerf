import sys

from src.main import main

from rich.console import Console
console = Console()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        console.print(
            "Usage: python run.py <config_file>",
            style="bold red"
        )
        sys.exit(1)

    main()
