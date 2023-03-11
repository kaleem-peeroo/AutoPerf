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

def create_dir(dirpath):
    dirpath_name = dirpath
    i = 0

    while os.path.exists(dirpath):
        i += 1
        dirpath = f"{dirpath_name}_{i}"

    os.mkdir(dirpath)

    return dirpath

def get_combinations(settings):
        return [dict( zip(settings, value)) for value in product(*settings.values()) ];