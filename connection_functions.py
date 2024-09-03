import subprocess
import os
import socket

from typing import Dict, Optional, Tuple
from rich.console import Console
console = Console()


def check_connection(machine, connection_type="ping"):
    # TODO: Validate parameters 
    # connection type can be only ping or ssh

    name = machine['machine_name']
    username = machine['username']
    ip = machine['ip']
    
    if connection_type == "ping":
        command = ["ping", "-c", "5", "-W", "10", ip]
    else:
        command = [
            "ssh",
            "-o", "ConnectTimeout=10",
            f"{username}@{ip}",
            "hostname"
        ]

    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            return False, result.stderr.strip()
        else:
            return True, None

    except subprocess.TimeoutExpired:
        return False, "Timed out"
    except Exception as e:
        return False, e

def ping_machine(ip: str = "") -> Tuple[Optional[bool], Optional[str]]:
    """
    Ping a machine to check if it's online.

    Params:
        - ip (str): IP address of the machine to ping.

    Returns:
        - bool: True if machine is online, False if machine is offline.
        - error
    """
    if ip == "":
        return False, f"No IP passed for connection check."

    logger.info(
        f"Pinging {ip}"
    )

    # -W 60 means a timeout of 60 seconds
    response = os.system(f"ping -c 1 -W 60 {ip} > /dev/null 2>&1")

    if DEBUG_MODE:  
        logger.info(f"ping response: {response}")

    if response == 0:
       return True, None
    else:
        return False, f"Failed to ping {ip}"

def check_ssh_connection_with_socket(machine_config: Dict = {}) -> Tuple[Optional[bool], Optional[str]]:
    ssh_key_path = machine_config['ssh_key_path']
    username = machine_config['username']
    ip = machine_config['ip']

    logger.info(f"SSHing into {username}@{ip}")
    
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.connect((ip, 22))

    except Exception as e:
        return False, e
    else:
        return True, None
    
def check_ssh_connection(machine_config: Dict = {}) -> Tuple[Optional[bool], Optional[str]]:
    """
    Check if an SSH connection can be established to a machine.

    Params:
        - machine_config (Dict): Configuration for the machine to connect to.

    Returns:
        - bool: True if SSH connection is successful, False
        - error
    """
    if machine_config == {}:
        return False, f"No machine config passed to check_ssh_connection()."
    
    # ssh_key_path = machine_config['ssh_key_path']
    username = machine_config['username']
    ip = machine_config['ip']

    logger.info(f"SSHing into {username}@{ip}")

    try:
        response = os.system(
            f"ssh -o \"ConnectTimeout 60\" {username}@{ip} exit > /dev/null 2>&1"
        )

        if response != 0:
            return False, f"SSH check failed with exit code {response}"
        else:
            return True, None

    except subprocess.TimeoutExpired:
        return False, "SSH check timed out after 60 seconds"

    except Exception as e:
        return False, f"Exception occured when checking SSH: {e}"
