import re
import os
import subprocess

from typing import Tuple, Optional, List
from rich.pretty import pprint
from dataclasses import dataclass

from src.logger import logger
from .ssh_client import SSHClient

@dataclass
class Machine:
    def __init__(
        self,
        hostname: str,
        participant_type: str,
        ip: str,
        ssh_key_path: str,
        username: str,
        perftest_path: str
    ):
        self.hostname = hostname
        self.participant_type = participant_type
        self.ip = ip

        if "~" in ssh_key_path:
            ssh_key_path = ssh_key_path.replace(
                "~", 
                os.path.expanduser("~")
            )
        self.ssh_key_path = ssh_key_path


        self.perftest_path = perftest_path
        self.username = username
        self.scripts = []
        self.command = ""
        self.run_output = []
        
    def __rich_repr__(self):
        yield "hostname", self.hostname
        yield "participant_type", self.participant_type
        yield "ip", self.ip
        yield "ssh_key_path", self.ssh_key_path
        yield "username", self.username
        yield "perftest_path", self.perftest_path
        yield "scripts", self.scripts
        yield "command", self.command
        yield "run_output", self.run_output

    def to_str(self):
        return {
            "hostname": self.hostname,
            "participant_type": self.participant_type,
            "ip": self.ip,
            "ssh_key_path": self.ssh_key_path,
            "username": self.username,
            "perftest_path": self.perftest_path,
            "scripts": "\n".join(self.scripts),
            "command": self.command,
            "run_output": "\n".join(self.run_output),
        }.__str__()

    def get_hostname(self):
        return self.hostname

    def get_participant_type(self):
        return self.participant_type

    def get_ip(self):
        return self.ip

    def get_ssh_key_path(self):
        return self.ssh_key_path

    def get_username(self):
        return self.username

    def get_perftest_path(self):
        return self.perftest_path

    def get_scripts(self):
        return self.scripts

    def get_command(self):
        return self.command

    def get_run_output(self):
        return self.run_output

    def create_ssh_client(self):
        return SSHClient(
            self.ip, 
            self.username, 
            self.ssh_key_path
        )

    def set_run_output(self, run_output):
        if not isinstance(run_output, list):
            raise ValueError(f"Run output must be a list: {run_output}")

        self.run_output = run_output

    def add_run_output(self, run_output):
        if not isinstance(run_output, str):
            raise ValueError(f"Run output must be a string: {run_output}")

        self.run_output.append(run_output)

    def set_hostname(self, hostname):
        if not isinstance(hostname, str):
            raise ValueError(f"Hostname must be a string: {hostname}")

        if hostname == "":
            raise ValueError("Hostname must not be empty")

        self.hostname = hostname

    def set_participant_type(self, participant_type):
        if not isinstance(participant_type, str):
            raise ValueError(f"Participant type must be a string: {participant_type}")

        if participant_type == "":
            raise ValueError("Participant type must not be empty")

        self.participant_type = participant_type

    def set_ip(self, ip):
        if not isinstance(ip, str):
            raise ValueError(f"IP must be a string: {ip}")

        if ip == "":
            raise ValueError("IP must not be empty")

        # Check if IP is in valid format
        if not re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
            raise ValueError(f"IP format is invalid: {ip}")

        self.ip = ip

    def set_ssh_key_path(self, ssh_key_path):
        if not isinstance(ssh_key_path, str):
            raise ValueError(f"SSH key path must be a string: {ssh_key_path}")

        if ssh_key_path == "":
            raise ValueError("SSH key path must not be empty")

        if not os.path.exists(ssh_key_path):
            raise ValueError(f"SSH key path does not exist: {ssh_key_path}")

        self.ssh_key_path = ssh_key_path

    def set_username(self, username):
        if not isinstance(username, str):
            raise ValueError(f"Username must be a string: {username}")

        if username == "":
            raise ValueError("Username must not be empty")

        self.username = username

    def set_perftest_path(self, perftest_path):
        if not isinstance(perftest_path, str):
            raise ValueError(f"Perftest path must be a string: {perftest_path}")

        if perftest_path == "":
            raise ValueError("Perftest path must not be empty")

        self.perftest_path = perftest_path

    def check_connection(
        self, 
        type: str = "ping",
        total_attempts=3,
        timeout=10
    ) -> Tuple[bool, Optional[ List[str] ]]:
        logger.debug(
            "{}ing {} ({}) {} times (timeout={})...".format(
                type.capitalize(),
                self.hostname,
                self.ip,
                total_attempts,
                timeout
            )
        )

        attempts = total_attempts

        errors = []
        while attempts > 0:
            logger.debug(
                "[{} {}/{}] Attempting to {} {} ({})...".format(
                    type.upper(),
                    4 - attempts,
                    total_attempts,
                    type,
                    self.hostname,
                    self.ip
                )
            )

            if type == "ping":
                command = ["ping", "-c", "5", "-W", "10", self.ip]

            elif type == "ssh":
                command = [
                    "ssh",
                    "-o", 
                    f"ConnectTimeout={timeout}",
                    f"{self.username}@{self.ip}",
                    "echo", "Connected."
                ]

            else:
                raise ValueError(f"Invalid connection type: {type}")

            try:
                result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)

                if result.returncode != 0:
                    error = f"Return code: {result.returncode}.\nstderr: {result.stderr}"
                    
                else:

                    logger.debug(
                        "[{} {}/{}] {} ({}) Succeeded.".format(
                            type.upper(),
                            4 - attempts,
                            total_attempts,
                            self.hostname,
                            self.ip
                        )
                    )

                    return True, None

            except subprocess.TimeoutExpired:
                error = f"{type.capitalize()} timeout after {timeout} seconds."
                
            except Exception as e:
                error = str(e)
                
            logger.warning(
                "[{} {}/{}] {} ({}) Failed.".format(
                    type.upper(),
                    4 - attempts,
                    total_attempts,
                    self.hostname,
                    self.ip
                )
            )

            errors.append({
                "hostname": self.hostname,
                "ip": self.ip,
                "attempt": 4 - attempts,
                "command": " ".join(command),
                "error": error,
            })

            attempts -= 1

        logger.warning(
            "{} ({}) failed {} times.".format(
                self.hostname,
                self.ip,
                total_attempts
            )
        )
            
        return False, errors

    def restart(self, timeout=10):
        logger.debug(
            "Restarting {} ({}) with timeout of {} seconds...".format(
                self.hostname,
                self.ip,
                timeout
            )
        )

        command = [
            "ssh",
            "-i",
            self.ssh_key_path,
            f"{self.username}@{self.ip}",
            "sudo reboot"
        ]

        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)

            if result.returncode != 0:
                error = f"Return code: {result.returncode}.\nstderr: {result.stderr}"

            else:
                return True, None

        except subprocess.TimeoutExpired:
            error = "Restart timeout after 10 seconds."

        except Exception as e:
            error = str(e)

        return False, [{
            "hostname": self.hostname,
            "ip": self.ip,
            "command": " ".join(command),
            "error": error,
        }]

    def set_scripts(self, scripts):
        if not isinstance(scripts, list):
            raise ValueError(f"Scripts must be a list: {scripts}")

        if len(scripts) == 0:
            raise ValueError("Scripts must not be empty")

        self.scripts = scripts

    def set_command(self, command):
        if not isinstance(command, str):
            raise ValueError(f"Command must be a string: {command}")
        
        self.command = command

    def generate_command(self):
        machine_script = self.get_command()

        perftest_exec = f"./{os.path.basename(
            self.perftest_path
        )}"

        for script in self.scripts:
            script = f"{perftest_exec} {script}"
            machine_script = f"{machine_script} {script} &"

        self.set_command(machine_script)

    def add_script(self, script):
        if not isinstance(script, str):
            raise ValueError(f"Script must be a string: {script}")

        if script == "":
            raise ValueError("Script must not be empty")

        self.scripts.append(script)

    def run(self, timeout_secs=600):
        ip = self.get_ip()
        username = self.get_username()
        ssh_command = f"ssh {username}@{ip} '{self.get_command()}'"

        run_output = []

        process = subprocess.Popen(
            ssh_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        try:
            stdout, stderr = process.communicate(timeout=timeout_secs)
            stdout = stdout.decode('utf-8').strip()
            stderr = stderr.decode('utf-8').strip()

            if process.returncode != 0:
                logger.warning(
                    f"Error running script on {self.get_hostname()}."
                )
                logger.warning(
                    f"Return code: \t{process.returncode}"
                )

                run_output.append(f"Return code: {process.returncode}")
                        
        except subprocess.TimeoutExpired:
            process.kill()

            stdout, stderr = process.communicate()
            stdout = stdout.decode('utf-8').strip()
            stderr = stderr.decode('utf-8').strip()

            logger.error(
                f"Script on {self.get_hostname()} timed out."
            )

            run_output.append(f"Timeout after {timeout_secs} seconds.")

        run_output.append(f"stdout: {stdout}")
        run_output.append(f"stderr: {stderr}")

        return run_output

    def remove_artifact_files(self):
        perftest_path = self.get_perftest_path()
        perftest_dir = os.path.dirname(perftest_path)

        command = [
            "ssh",
            "-i",
            self.ssh_key_path,
            f"{self.username}@{self.ip}",
            f"cd {perftest_dir}; rm -f *.csv; ls -al;"
        ]

        try:
            result = subprocess.run(command, capture_output=True, text=True)

            if result.returncode != 0:
                error = f"Return code: {result.returncode}.\nstderr: {result.stderr}"

            else:
                return True, None

        except Exception as e:
            error = str(e)

        return False, [{
            "hostname": self.hostname,
            "ip": self.ip,
            "command": " ".join(command),
            "error": error,
        }]

    def download_results(self, output_dirpath):
        perftest_path = self.get_perftest_path()
        perftest_dir = os.path.dirname(perftest_path)

        logger.debug(
            "Downloading results from {} on {} ({}) to {}...".format(
                f"{perftest_dir}/*.csv",
                self.hostname,
                self.ip,
                output_dirpath
            )
        )

        try:
            ssh_client = self.create_ssh_client()
            sftp = ssh_client.get_sftp()

            if "~" in perftest_dir:
                perftest_dir = perftest_dir.replace(
                    "~", 
                    ssh_client.get_home_path()
                )
                logger.debug(
                    "Expanded perftest directory path: {}".format(
                        perftest_dir
                        )
                    )
                
            remote_files = sftp.listdir(perftest_dir)
            remote_csv_files = [
                f for f in remote_files if f.endswith(".csv")
            ]

            if len(remote_csv_files) == 0:
                raise ValueError(f"No CSV files found in {perftest_dir}")

            logger.debug("Found {} csv files to download on {}".format(
                len(remote_csv_files),
                self.hostname
            ))

            for file_index, remote_csv_file in enumerate(remote_csv_files):
                logger.debug(
                    "[{}/{}] Downloading {}...".format(
                        file_index + 1,
                        len(remote_csv_files),
                        remote_csv_file
                    )
                )

                sftp.get(
                    f"{perftest_dir}/{remote_csv_file}",
                    f"{output_dirpath}/{remote_csv_file}"
                )

            local_files = os.listdir(output_dirpath)
            local_csv_files = [
                f for f in local_files if f.endswith(".csv")
            ]

            logger.debug("Found {} csv files locally after download on {}".format(
                len(local_csv_files),
                self.hostname
             ))

            if len(local_csv_files) < len(remote_csv_files):
                logger.warning(
                    "Not all files downloaded. Expected: {}, Actual: {}.".format(
                        len(remote_csv_files),
                        len(local_csv_files)
                    )
                )

                return False, [{
                    "hostname": self.hostname,
                    "ip": self.ip,
                    "error": "Not all files downloaded. Expected: {}, Actual: {}.".format(
                        len(remote_csv_files),
                        len(local_csv_files)
                    )
                }]

            sftp.close()
            ssh_client.close()

            return True, None
                            
        except Exception as e:
            error = str(e)
            logger.warning(
                "Error downloading results from {} on {} ({}): {}".format(
                    f"{perftest_dir}/*.csv",
                    self.hostname,
                    self.ip,
                    error
                )
            )

        return False, [{
            "hostname": self.hostname,
            "ip": self.ip,
            "error": error,
        }]
