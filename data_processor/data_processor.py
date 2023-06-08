
import check_and_download as cad
from analyse import *
import sys
import os
import paramiko
from rich.progress import track
from rich.console import Console

from pprint import pprint

console = Console()

def get_args():
    if len(sys.argv) < 2:
        print('Usage: python data_processor.py <config_file>')
        sys.exit(1)
    
    return sys.argv[1]

def main():
    config_path = get_args()
    # ? Get machines from config
    machines = cad.get_machines(config_path)
    # ? Download any new zips
    machines = cad.download_new_zips(machines)
    
    for machine in machines:
        local_zips = machine['local_zips']
        
        if len(local_zips) > 0:
            # ? unzip the zip
            # ? generate stats file containing file types and counts
            # ? get the usable tests
            # ? summarise the usable tests into summaries
            # ? summarise the summaries into ML summary file
            
            for local_zip in local_zips:
                unzipped_dir = local_zip.replace(".zip", "")

                # ? unzip the file
                if not os.path.exists(unzipped_dir):
                    os.system(f'unzip {local_zip} -d {unzipped_dir} > /dev/null 2>&1')
                    console.print(f"Unzipped {local_zip} to {unzipped_dir}", style="bold green")
                
                tests_dirname = os.path.basename(unzipped_dir)
                camp_name = tests_dirname.replace("_raw", "")
                camp_name_path = os.path.join(os.path.dirname(unzipped_dir), camp_name)
                
                # ? generate stats files
                usable_tests = analyse_tests(unzipped_dir)
                
                # ? summarise usable tests
                summarise_usable_tests(usable_tests, camp_name_path)
                
                # ? create ML summary file
                
                
    

if __name__ == '__main__':
    main()