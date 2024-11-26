import paramiko
import os

from rich.pretty import pprint

class SSHClient:
    def __init__(self, ip, username, ssh_key_path):
        self.ip = ip
        self.username = username

        if "~" in ssh_key_path:
            ssh_key_path = ssh_key_path.replace(
                "~", 
                os.path.expanduser("~")
            )

        self.ssh_key_path = ssh_key_path

        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(
            self.ip,
            username=self.username,
            key_filename=self.ssh_key_path,
        )

    def get_ssh(self):
        return self.ssh

    def get_sftp(self):
        return self.ssh.open_sftp()

    def get_home_path(self):
        _, stdout, _ = self.ssh.exec_command("echo $HOME")
        return stdout.read().decode().strip()

    def close(self):
        self.ssh.close()
