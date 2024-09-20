import paramiko
import os

from rich.pretty import pprint
from rich.console import Console

from constants import *

console = Console(record=True)

MACHINES = [
    {
        "name": "3pi",
        "ip": "100.114.252.94",
        "username": "acwh025",
        "ssh_key_path": "/Users/kaleem/.ssh/id_rsa",
        "ap_path": "/home/acwh025/AutoPerf"
    },
    {
        "name": "5pi",
        "ip": "100.103.95.99",
        "username": "acwh025",
        "ssh_key_path": "/Users/kaleem/.ssh/id_rsa",
        "ap_path": "/home/acwh025/AutoPerf"
    }
]

def main() -> None:
    for MACHINE in MACHINES:
        console.print(
            f"Downloading from {MACHINE['name']} ({MACHINE['ip']})..."
        )

        local_download_output_dir = f"./output/downloads/{MACHINE['name']}"
        os.makedirs(local_download_output_dir, exist_ok=True)

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            MACHINE["ip"],
            username=MACHINE["username"],
            key_filename=MACHINE["ssh_key_path"]
        )
        sftp = ssh.open_sftp()
        remote_output_path = f"{MACHINE['ap_path']}/output"

        # If the compressed output exists, remove it     
        with console.status(
            "[1/5] Checking if compressed output exists..."
        ):
            stdin, stdout, stderr = ssh.exec_command(
                f"if [ -f {remote_output_path}.tar.gz ]; then echo 1; else echo 0; fi"
            )
            stderr = stderr.read().decode()
            if stderr != "": 
                console.print(
                    f"Error checking if {remote_output_path}.tar.gz exists: {stderr}",
                    style="bold red"
                )
                continue

        # Compress the output directory
        with console.status("[2/5] Compressing output directory..."):
            stdin, stdout, stderr = ssh.exec_command(
                f"cd {MACHINE['ap_path']} && tar -czvf output.tar.gz output"
            )
            stderr = stderr.read().decode()
            if stderr != "":
                console.print(
                    f"Error compressing {remote_output_path}: {stderr}",
                    style="bold red"
                )
                continue

        # Download the compressed output directory
        with console.status("[3/5] Downloading compressed output directory..."):
            sftp.get(
                f"{remote_output_path}.tar.gz",
                f"{local_download_output_dir}/output.tar.gz"
            )

        # Remove the compressed output directory
        with console.status("[4/5] Removing compressed output directory..."):
            stdin, stdout, stderr = ssh.exec_command(
                f"rm {remote_output_path}.tar.gz"
            )
            stderr = stderr.read().decode()
            if stderr != "":
                console.print(
                    f"Error removing {remote_output_path}.tar.gz: {stderr}",
                    style="bold red"
                )
                continue

        # Extract the compressed output directory
        with console.status("[5/5] Extracting compressed output directory..."):
            os.system(
                f"tar -xzvf {local_download_output_dir}/output.tar.gz -C {local_download_output_dir} > /dev/null 2>&1"
            )

        # Remove the compressed output directory
        with console.status("Removing compressed output directory..."):
            os.remove(f"{local_download_output_dir}/output.tar.gz")

        sftp.close()
        ssh.close()
        
if __name__ == "__main__":
    main()
