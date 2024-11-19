import re
import os
import subprocess

from typing import Tuple, Optional, List

from src.logger import logger

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
        self.ssh_key_path = ssh_key_path
        self.username = username
        self.perftest_path = perftest_path

    def __rich_repr__(self):
        yield "hostname", self.hostname
        yield "participant_type", self.participant_type
        yield "ip", self.ip
        yield "ssh_key_path", self.ssh_key_path
        yield "username", self.username
        yield "perftest_path", self.perftest_path

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

    def ping(
        self, 
        total_ping_attempts=3,
        ping_timeout=10
    ) -> Tuple[bool, Optional[ List[str] ]]:
        logger.info(
            "Pinging {} ({}) {} times (timeout={})...".format(
                self.hostname,
                self.ip,
                total_ping_attempts,
                ping_timeout
            )
        )

        ping_attempts = total_ping_attempts

        errors = []
        while ping_attempts > 0:
            logger.info(
                "[PING {}/{}] Attempting to ping {} ({})...".format(
                    4 - ping_attempts,
                    total_ping_attempts,
                    self.hostname,
                    self.ip
                )
            )

            command = ["ping", "-c", "5", "-W", "10", self.ip]

            try:
                result = subprocess.run(command, capture_output=True, text=True, timeout=ping_timeout)

                if result.returncode != 0:
                    error = f"Return code: {result.returncode}.\nstderr: {result.stderr}"
                    
                else:
                    return True, None

            except subprocess.TimeoutExpired:
                error = f"Ping timeout after {ping_timeout} seconds."
                
            except Exception as e:
                error = str(e)
                
            logger.warning(
                "[PING {}/{}] {} ({}) Failed.".format(
                    4 - ping_attempts,
                    total_ping_attempts,
                    self.hostname,
                    self.ip
                )
            )

            errors.append({
                "hostname": self.hostname,
                "ip": self.ip,
                "attempt": 4 - ping_attempts,
                "error": error
            })

            ping_attempts -= 1
            
        return False, errors
