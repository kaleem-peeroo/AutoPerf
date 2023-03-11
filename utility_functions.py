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

def get_test_title_from_combination(combination):
    title = ""
    for k, v in combination.items():
        if "bytes" in k:
            title = title + str(v) + "B_"
        elif "pub" in k:
            title = title + str(v) + "P_"
        elif "sub" in k:
            title = title + str(v) + "S_"
        elif "reliability" in k:
            if v:
                title = title + "rel_"
            else:
                title = title + "be_"
        elif "multicast" in k:
            if v:
                title = title + "mc_"
            else: 
                title = title + "uc_"
        elif "latency_count" in k:
            title = title + str(v) + "lc_"
        elif "duration" in k:
            title = title + str(v) + "s_"
        elif "durability" in k:
            title = title + str(v) + "dur_"
            
    return title[:-1]

def get_combination_from_title(title):
    # Example title: 600s_100B_75P_75S_rel_uc_1dur_100lc
    title = title.replace("\n", '') if '\n' in title else title

    combination = {
        "duration_s": None,
        "datalen_bytes": None,
        "pub_count": None,
        "sub_count": None,
        "reliability": None,
        "use_multicast": None,
        "durability": None,
        "latency_count": None
    }

    settings = title.split("_")

    for setting in settings:
        if "s" in setting:
            combination['duration_s'] = int(setting.replace("s", ""))
        elif "B" in setting:
            combination['datalen_bytes'] = int(setting.replace("B", ""))
        elif "P" in setting:
            combination['pub_count'] = int(setting.replace("P", ""))
        elif "S" in setting:
            combination['sub_count'] = int(setting.replace("S", ""))
        elif "uc" in setting or "mc" in setting:
            if "uc" in setting:
                combination["use_multicast"] = False
            else:
                combination["use_multicast"] = True
        elif "rel" in setting or "be" in setting:
            if "rel" in setting:
                combination["reliability"] = True
            else:
                combination["reliability"] = False
        elif "dur" in setting:
            combination['durability'] = int(setting.replace("dur", ""))
        elif "lc" in setting:
            combination['latency_count'] = int(setting.replace("lc", ""))

    return combination

def share(items, bins):
    if len(items) == 0 or bins == 0:
        return []

    if bins == 1:
        return items

    output = []
    
    for i in range(bins):
        output.append([])
    
    while len(items) > 0:
        for i in range(bins):
            try:
                output[i].append(items[0])
                items = items[1:]
            except Exception as e:
                None
            
    return output