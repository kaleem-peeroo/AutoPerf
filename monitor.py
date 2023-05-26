import warnings
from cryptography.utils import CryptographyDeprecationWarning
with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
    import paramiko
    
import json
import os
import re
import sys

from datetime import datetime
from pprint import pprint
from rich.bar import Bar
from rich.console import Console
from rich.progress import track
from rich.table import Table
from rich.markdown import Markdown

console = Console()

ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
# ? To use the above: ansi_escape.sub("", string)

args = sys.argv[1:]

if len(args) != 4:
    console.log(f"4 arguments should be given. Check the README to find out how to use this.", style="bold red")
    sys.exit()
else:
    host = args[0]
    name = args[1]
    ptstdir = args[2]
    key_path = args[3]

console.print(Markdown(f"# {name} Monitor"))

# ? Connect to the controller.
    
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

k = paramiko.RSAKey.from_private_key_file(key_path)
ssh.connect(host, username="acwh025", pkey = k, banner_timeout=120)

# ? Check if ptstdir is valid.
sftp = ssh.open_sftp()
try:
    remote_files = sftp.listdir(ptstdir)
except Exception as e:
    console.log(f"Exception when getting remote_files: \n\t{e}", style="bold red")
