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

def validate_args(args):
    pprint(args)