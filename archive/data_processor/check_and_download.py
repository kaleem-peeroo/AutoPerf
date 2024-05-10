import sys
import os
import json
import paramiko
from pprint import pprint
from rich.console import Console

console = Console()

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_key_path = "/Users/kaleem/.ssh/id_rsa"
if not os.path.exists(ssh_key_path):
    ssh_key_path = r"C:\Users\kalee\.ssh\id_rsa"
k = paramiko.RSAKey.from_private_key_file(ssh_key_path)

def ping_machine(ip):
    if ip is None:
        return False

    if os.name == 'nt':
        response = os.system("ping -n 1 " + ip + " > nul")
    else:
        response = os.system("ping -c 1 " + ip + " > /dev/null 2>&1")

    if response == 0:
        return True
    else:
        return False

def validate_machine(machine):
    
    if not ping_machine(machine['ip']):
        console.print(f"Could NOT ping {machine['name']} via {machine['ip']}", style="bold red")
        return False
    else:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        ssh_key_path = "/Users/kaleem/.ssh/id_rsa"

        if not os.path.exists(ssh_key_path):
            ssh_key_path = r"C:\Users\kalee\.ssh\id_rsa"

        k = paramiko.RSAKey.from_private_key_file(ssh_key_path)
        
        ssh.connect(machine['ip'], username='acwh025', pkey=k, banner_timeout=120)
        
        sftp = ssh.open_sftp()
        
        try:
            sftp.stat(machine['ptstdir'])

            # ? Check if outputdir is a valid path on this machine
            if not os.path.exists(machine['outputdir']):
                os.mkdir(machine['outputdir'])
                return True
            else:
                return True
            
        except IOError:
            console.print(f"Could NOT find {machine['ptstdir']} on {machine['name']}", style="bold red")
            return False
        finally:
            sftp.close()
            ssh.close()

def get_machines(config_path):
    if config_path is None:
        raise ValueError('config_path cannot be None')

    if not os.path.exists(config_path):
        raise ValueError('config_path does not exist')

    with open(config_path, 'r') as f:
        config = json.load(f)

    valid_machines = []
    for item in config:
        if len(item.keys()) != 0 and len(item.keys()) == 4:
            required_keys = ["name", "ip", "ptstdir", "outputdir"]
 
            if all(key in item.keys() for key in required_keys):
                valid_machines.append(item)
            else:
                console.print(f"Invalid item: {item}. It should have {required_keys} instead of {item.keys()}", style="bold red")
                sys.exit()
    
    for machine in valid_machines:
        if not validate_machine(machine):
            console.print(f"Invalid machine: {machine['name']}", style="bold red")
            valid_machines.remove(machine)

    return valid_machines

def get_zips_from_dir(outputdir):
    zips = []
    for file in os.listdir(outputdir):
        if file.endswith(".zip"):
            zips.append(os.path.join(outputdir, file))
    return zips

def download_new_zips(machines):
    for machine in machines:
        # ? Get all .zips from ptstdir
        ssh.connect(machine['ip'], username='acwh025', pkey=k, banner_timeout=120)
        stdin, stdout, stderr = ssh.exec_command(f"find {machine['ptstdir']} -name '*.zip'")
        remote_zips = stdout.readlines()
        
        # ? Get all zips from outputdir
        local_zip_paths = get_zips_from_dir(machine['outputdir'])
        
        # ? Get all remote zips that are NOT in local_zips
        remote_zip_paths = [zip.strip() for zip in remote_zips]
        remote_zips = set([os.path.basename(zip) for zip in remote_zip_paths])
        local_zips = set([os.path.basename(zip) for zip in local_zip_paths])
        new_zips = list(remote_zips - local_zips)
        
        new_zip_paths = []
        for zip in remote_zip_paths:
            if os.path.basename(zip.strip()) in new_zips:
                new_zip_paths.append(zip.strip())
        
        if len(new_zips) > 0:
            local_zip_paths = []
            console.print(f"New zips found on {machine['name']}:", style="bold green")
            
            for zip in new_zips:
                console.print(f"\t- {os.path.basename(zip)}", style="bold white")
            
            # ? Download new zips
            sftp = ssh.open_sftp()
            with console.status(f"Downloading new zips... (1/{len(new_zips)})") as status:
                for i, zip in enumerate(new_zip_paths):
                    remote_path = zip
                    local_path = f"{machine['outputdir']}/{zip.split('/')[-1]}"
                    if not os.path.exists(local_path):
                        try:
                            sftp.get(remote_path, local_path)
                            local_zip_paths.append(local_path)
                        except FileNotFoundError as e:
                            pprint(e)
                            console.print(f"Remote path: {remote_path}")
                            console.print(f"Local path: {local_path}")
                            
                        console.print(f"New zip has been downloaded: {os.path.basename(remote_path)}", style="bold green")
                    status.update(f"Downloading new zips... ({i+2}/{len(new_zips)})")

            sftp.close()
        else:
            console.print(f"No new zips found on {machine['name']}.", style="bold yellow")    

        machine['local_zips'] = local_zip_paths
        
    return machines