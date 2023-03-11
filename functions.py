import fnmatch
import shutil
import sys
import os
import json
import paramiko
import time
import concurrent.futures
import math
import threading
import re
import stat

from threading import Thread
from datetime import datetime, timedelta
from pprint import pprint
from itertools import product
from itertools import repeat
from rich.console import Console
from rich.markdown import Markdown
from rich.prompt import Prompt
from rich.prompt import Confirm
from rich.progress import track

# ? Uncomment for rich traceback formatting
# from rich.traceback import install
# install(show_locals=True)

console = Console()

DEBUG_MODE = "debug" in sys.argv

DEBUG = "[bold red]DEBUG: [/bold red]"

def validate_args(args):
    console.print(f"{DEBUG}Validating args...", style="bold white") if DEBUG_MODE else None

    # ? No args given.
    if len(args) == 0:
        console.print(f"No config file given.", style="bold red")
        console.print(f"Config file expected as:\n\tpython index.py <config_file>", style="bold green")
        sys.exit()

    # ? Validate config file
    config_path = args[0]
    if not ( os.path.isfile(config_path) and os.access(config_path, os.R_OK) ):
        console.print(f"Can't access {config_path}. Check it exists and is accessible and try again.", style="bold red")
        sys.exit()

    console.print(f"{DEBUG}args validated.", style="bold green") if DEBUG_MODE else None