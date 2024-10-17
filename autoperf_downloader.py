import paramiko
import os
import time

from rich.pretty import pprint
from rich.console import Console
from rich.progress import track
from rich.markdown import Markdown

from constants import *

console = Console(record=True)

SKIP_DOWNLOADED = True
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

def check_remote_file_exists(sftp, remote_file_path):
    try:
        sftp.stat(remote_file_path)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        raise e

def remove_zi_files(sftp, remote_data_path):
    # Look for files that start with zi in the remote_data_path and delete them
    remote_data_files = sftp.listdir(remote_data_path)
    zi_files = [file for file in remote_data_files if file.startswith("zi")]
    console.print(f"Found {len(zi_files)} files to delete.")
    for index, file in enumerate(zi_files):
        count_string = f"[{index + 1}/{len(zi_files)}]"
        sftp.remove(f"{remote_data_path}/{file}")
        console.print(f"{count_string} {file} deleted.", style="bold green")

def get_remote_hash(ssh, remote_file_path):
    stdin, stdout, stderr = ssh.exec_command(f"sha1sum {remote_file_path} | awk '{{print $1}}'")
    return stdout.read().decode().strip()

def format_bytes(bytes):
    # Return the number of bytes in a human-readable format
    if bytes == 0:
        return "0B"
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while bytes >= 1024 and i < len(suffixes) - 1:
        bytes /= 1024
        i += 1

    return f"{int(bytes)}{suffixes[i]}"

def zip_and_download(ssh, sftp, output_type, ap_path, local_download_output_dir):
    if output_type == "data":
        remote_data_path = f"{ap_path}/output/data"
    elif output_type == "summarised_data":
        remote_data_path = f"{ap_path}/output/summarised_data"
    else:
        raise ValueError(f"Invalid output type: {output_type}")

    data_dirs = sftp.listdir(remote_data_path)
    data_dirs = [data_dir for data_dir in data_dirs if not data_dir.endswith(".zip")]
    for index, data_dir in enumerate(data_dirs):
        count_string = f"[{index + 1}/{len(data_dirs)}]"

        remote_filesize_bytes = sftp.stat(f"{remote_data_path}/{data_dir}").st_size
        remote_filesize = format_bytes(remote_filesize_bytes)

        with console.status(f"{count_string} Downloading {data_dir} ({remote_filesize})...") as status:
            remote_data_zip = f"{remote_data_path}/{data_dir}.zip"

            # If the zip doesn't exist, create it
            if not check_remote_file_exists(sftp, remote_data_zip):
                status.update(f"{data_dir}.zip ({remote_filesize}) does NOT exist. Creating...")
                stdin, stdout, stderr = ssh.exec_command(
                    f"cd {remote_data_path} && zip -r {data_dir}.zip {data_dir}"
                ) 
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    console.print(
                        f"Couldn't create {data_dir}.zip:\n\t{stderr.read().decode()}",
                        style="bold red"
                    )
                    continue

                console.print(f"{data_dir}.zip created.", style="bold green")

            os.makedirs(f"{local_download_output_dir}/{output_type}", exist_ok=True)

            # console.print(
            #     "Downloading {}\n\t{}\n\t{}".format(
            #         data_dir, 
            #         f"remote: {remote_data_zip}",
            #         f"local: {local_download_output_dir}/{output_type}/{data_dir}.zip"
            #     )
            # )

            remote_hash = get_remote_hash(ssh, remote_data_zip)

            try:
                if SKIP_DOWNLOADED:
                    if os.path.exists(
                        f"{local_download_output_dir}/{output_type}/{data_dir}.zip"
                    ):
                        local_hash = os.popen(
                            f"sha1sum {local_download_output_dir}/{output_type}/{data_dir}.zip | awk '{{print $1}}'"
                        ).read().strip()

                        if remote_hash == local_hash:
                            console.print(
                                "{} {} already exists locally. Skipping...".format(
                                    count_string, 
                                    f"{data_dir}.zip"
                                ),
                                style="bold green"
                            )
                            continue

                sftp.get(
                    remote_data_zip, 
                    f"{local_download_output_dir}/{output_type}/{data_dir}.zip"
                )

                local_hash = os.popen(
                    f"sha1sum {local_download_output_dir}/{output_type}/{data_dir}.zip | awk '{{print $1}}'"
                ).read().strip()

                if remote_hash != local_hash:
                    console.print(
                        f"Hashes don't match for {data_dir}.zip. Deleting...",
                        style="bold red"
                    )
                    os.remove(f"{local_download_output_dir}/{output_type}/{data_dir}.zip")
                    continue

                console.print(
                    f"{count_string} {data_dir}.zip ({remote_filesize}) downloaded.", 
                    style="bold green"
                )
            except Exception as e:
                console.print(
                    f"Couldn't download {data_dir}:\n\t{e}",
                    style="bold red"
                )
                continue

    remove_zi_files(sftp, remote_data_path)
    remove_zi_files(sftp, os.path.dirname(remote_data_path))
    
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
        ap_path = MACHINE["ap_path"]

        console.print(Markdown(f"# /data"))
        zip_and_download(ssh, sftp, "data", ap_path, local_download_output_dir)
        console.print(Markdown(f"# /summarised_data"))
        zip_and_download(ssh, sftp, "summarised_data", ap_path, local_download_output_dir)

        remote_ds_dir = f"{ap_path}/output/datasets"

        if not check_remote_file_exists(sftp, f"{remote_ds_dir}.zip"):
            console.print(f"{remote_ds_dir}.zip does NOT exist. Creating...")
            ssh.exec_command(
                f"cd {ap_path}/output && zip -r datasets.zip datasets"
            )

        try:
            sftp.get(
                f"{remote_ds_dir}.zip",
                f"{local_download_output_dir}/datasets.zip"
            )
            console.print(f"datasets.zip downloaded.", style="bold green")
        except Exception as e:
            console.print(
                f"Couldn't download datasets.zip:\n\t{e}",
                style="bold red"
            )

        sftp.close()
        ssh.close()
        
if __name__ == "__main__":
    main()
