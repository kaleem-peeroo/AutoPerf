import os
import re
import time
import json
import paramiko
import zipfile
import pandas as pd
import shutil
import numpy as np

from pprint import pprint
from rich.console import Console
from rich.progress import track

console = Console()

# Define the remote server details
hostname = '10.210.58.126'
port = 22
username = 'acwh025'
ssh_key = "/Users/kaleem/.ssh/id_rsa"

# Define the remote directory to check for new zip files
remote_dir = '/home/acwh025/Documents/PTST/'

# Define the local directory to download and extract the zip files
local_dir = '/Volumes/kaleem_ssd/phd_data/5Pi/ML_DP_DATA/'
backup_local_dir = "./ML_DP_DATA/"

if not os.path.exists(local_dir):
    console.print(f"Couldn't access {local_dir}. Using backup {backup_local_dir}.", style="bold red")
    local_dir = backup_local_dir

# Define the time interval to check for new zip files (in seconds)
interval = 120

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
k = paramiko.RSAKey.from_private_key_file(ssh_key)
ssh.connect(hostname, port, username, pkey=k)

def get_settings_from_testname(test):
    datalen_bytes = re.findall("\d*B", test)[0].replace("B", "")
    pub_count = re.findall("\d*P", test)[0].replace("P", "")
    sub_count = re.findall("\d*S", test)[0].replace("S", "")
    best_effort = 1 if len(re.findall("_BE_", test)) > 0 else 0
    multicast = 1 if len(re.findall("_MC_", test)) > 0 else 0
    durability = re.findall("\dDUR", test)[0].replace("DUR", "")

    return datalen_bytes, pub_count, sub_count, best_effort, multicast, durability

def get_metric_per_sub(sub_file, metric):
    df = pd.read_csv(sub_file, on_bad_lines='skip', skiprows=2, skipfooter=3, engine='python')
    
    sub_head = [x for x in df.columns if metric in x.lower()][0]
    
    series = df[sub_head].iloc[:-2]
    
    series.rename(os.path.basename(sub_file).replace(".csv", ""), inplace=True)
    
    # Check if Series contains non-float values
    if not series.dtype == 'float64':
        # Filter out non-float values
        float_mask = series.apply(lambda x: np.isreal(x))
        series = series[float_mask]
        # Convert non-float values to float
        series = series.astype(float)
    
    return series

def get_participant_allocation_per_machine(type, test):
    config = os.path.join(test, 'config.json')
    
    if not os.path.exists(config):
        return []
    
    with open(config, 'r') as f:
        config = json.load(f)
        
    machines = config['machines']
    
    allocation_list = []
    
    for machine in machines:
        scripts = machine['scripts']
        allocation_list.append(scripts.count(f"-{type}"))
        
    return allocation_list

def get_total_sub_metric(sub_files, metric):
    sub_dfs = []
    
    for file in sub_files:
        try:
            df = pd.read_csv(file, on_bad_lines="skip", skiprows=2, skipfooter=3, engine="python")
        except Exception as e:
            console.print(f"Error when getting data from {file}:", style="bold red")
            console.print(f"\t{e}", style="bold red")
            continue
        sub_head = [x for x in df.columns if metric in x.lower()][0]
        df = df[sub_head]
        df.rename(os.path.basename(file).replace(".csv", ""), inplace=True)
        sub_dfs.append(df)
        
    if sub_dfs:
        sub_df = pd.concat(sub_dfs, axis=1)
    
        # ? Add up all columns to create total column
        sub_df["total_" + metric] = sub_df[list(sub_df.columns)].sum(axis=1)
        
        # ? Take off the last number because its an average produced by perftest
        sub_df = sub_df[:-2]
        
        return sub_df["total_" + metric][:-1]
    else:
        console.print(f"Couldn't get any data from {sub_files}.", style="bold red")

def get_latencies(pubfile):
    try:
        df = pd.read_csv(pubfile, on_bad_lines="skip", skiprows=2, skipfooter=5, engine="python")
    except Exception as e:
        console.print(f"Error looking at {pubfile}:", style="bold red")
        console.print(e, style="bold red")
        return
    
    try:
        lat_header = [_ for _ in df.columns if "latency" in _.lower()][0]
        df = df[lat_header]
    except Exception as e:
        print(e)
        return

    return df

def test_summary_exists(test, summaries_dir):
    testname = os.path.basename(test)
    summary_path = os.path.join(summaries_dir, f"{testname}_summary.csv")
    return os.path.exists(summary_path)

def get_expected_csv_count_from_testname(testname):
    split = testname.split("_")
    sub_split = [_ for _ in split if "S" in _ and "SEC" not in _]
    sub_value = sub_split[0].replace("S", "")
    sub_value = int(sub_value)
    
    return sub_value + 1

assert(get_expected_csv_count_from_testname("600s_32000B_25P_1S_rel_uc_1dur_100lc") == 2)
assert(get_expected_csv_count_from_testname("600s_32000B_25P_25S_rel_uc_1dur_100lc") == 26)

def get_actual_csv_count(testdir):
    csv_files = [_ for _ in os.listdir(testdir) if '.csv' in _]
    
    return len(csv_files)

while True:
    console.print("Checking for new zip files...")
    
    stdin, stdout, stderr = ssh.exec_command('ls ' + remote_dir + '/*.zip')
    remote_files = stdout.readlines()
    
    for remote_file in remote_files:
        remote_file = remote_file.strip()
        local_file = os.path.join(local_dir, os.path.basename(remote_file))
        extracted_dir = os.path.join(local_dir, os.path.splitext(os.path.basename(remote_file))[0])
        
        if not os.path.exists(local_file):
            with console.status("Downloading " + remote_file):
                sftp = ssh.open_sftp()
                sftp.get(remote_file, local_file)
                sftp.close()
        
        if not os.path.exists(extracted_dir):
            with console.status("Extracting " + local_file):
                with zipfile.ZipFile(local_file, 'r') as zip_ref:
                    zip_ref.extractall(local_file.replace(".zip", ""))
        

    camp_dirs = [os.path.join(local_dir, camp_dir) for camp_dir in os.listdir(local_dir) if os.path.isdir(os.path.join(local_dir, camp_dir)) and "usable" not in camp_dir and "summaries" not in camp_dir]
    
    for camp_dir in camp_dirs:
        usable_dir = camp_dir + "_usable"
        summaries_dir = camp_dir + "_summaries"
        
        # ? Get usable tests
        test_dirs = [os.path.join(camp_dir, test_dir) for test_dir in os.listdir(camp_dir) if os.path.isdir(os.path.join(camp_dir, test_dir))]
        
        if len(test_dirs) == 0:
            console.print(f"No tests found in {camp_dir}.", style="bold red")
            continue
        
        usable_test_dirs = []
        
        for test_dir in test_dirs:
            expected_csv_count = get_expected_csv_count_from_testname(os.path.basename(test_dir))
            actual_csv_count = get_actual_csv_count(test_dir)
            
            if expected_csv_count == actual_csv_count:
                usable_test_dirs.append(test_dir)

        usable_percentage = int(len(usable_test_dirs) / len(test_dirs) * 100)
        
        for i in track(range(len(usable_test_dirs)), description=f"Copying over {len(usable_test_dirs)} usable tests out of {len(test_dirs)} ({usable_percentage}%) total tests...\n"):
            usable_test_dir = usable_test_dirs[i]
            src = usable_test_dir
            dest = os.path.join(usable_dir, os.path.basename(usable_test_dir))
            
            try:
                if not os.path.exists(dest):
                    os.makedirs(dest)
                    shutil.copytree(src, dest, dirs_exist_ok=True)
            except FileExistsError as e:
                continue
    
        usable_tests = [os.path.join(usable_dir, test_dir) for test_dir in os.listdir(usable_dir) if os.path.isdir(os.path.join(usable_dir, test_dir))]
    
        for i in track( range( len(usable_tests) ), description="Summarising tests...", update_period=1 ):
            test = usable_tests[i]
            
            if test_summary_exists(test, summaries_dir):
                continue
            
            log_dir = os.path.join(test, "logs")
            
            if os.path.exists(log_dir):
                log_files = os.listdir(log_dir)
                logs = [os.path.join(log_dir, file) for file in log_files if '_cpu.log' in file or '_mem.log' in file or '_dev.log' in file or '_edev.log' in file]
            else:
                logs = [os.path.join(test, file) for file in os.listdir(test) if file.endswith(".log")]
                
            logs = sorted(logs)
            
            log_cols = []
            
            for log in logs:
                df = pd.read_csv(log, skiprows=1, delim_whitespace=True)
                
                log_name = os.path.basename(log).replace(".log", "")
                
                if "_cpu" in log:
                    user_df = pd.Series(df['%user']).rename(f"{log_name}_user").dropna()
                    system_df = pd.Series(df['%system']).rename(f"{log_name}_system").dropna()
                    iowait_df = pd.Series(df['%iowait']).rename(f"{log_name}_iowait").dropna()
                    idle_df = pd.Series(df['%idle']).rename(f"{log_name}_idle").dropna()
                    
                    log_cols.append(user_df)
                    log_cols.append(system_df)
                    log_cols.append(iowait_df)
                    log_cols.append(idle_df)
                
                elif "_mem" in log:
                    kbmemfree_df = pd.Series(df['kbmemfree']).rename(f"{log_name}_mem_kbmemfree").dropna()
                    kbmemused_df = pd.Series(df['kbmemused']).rename(f"{log_name}_mem_kbmemused").dropna()
                    percent_mem_used_df = pd.Series(df['%memused']).rename(f"{log_name}_mem_percentmemused").dropna()

                    log_cols.append(kbmemfree_df)         
                    log_cols.append(kbmemused_df)
                    log_cols.append(percent_mem_used_df)
            
                elif "_dev" in log:
                    df = df[df['IFACE'] == "eth0"].reset_index()
                    rxpck_df = pd.Series(df['rxpck/s']).rename(f"{log_name}_rxpck").dropna()
                    txpck_df = pd.Series(df['txpck/s']).rename(f"{log_name}_txpck").dropna()
                    rxkB_df = pd.Series(df['rxkB/s']).rename(f"{log_name}_rxkB").dropna()
                    txkB_df = pd.Series(df['txkB/s']).rename(f"{log_name}_txkB").dropna()
                    rxmcst_df = pd.Series(df['rxmcst/s']).rename(f"{log_name}_rxmcst").dropna()
                    
                    log_cols.append(rxpck_df)
                    log_cols.append(txpck_df)
                    log_cols.append(rxkB_df)
                    log_cols.append(txkB_df)
                    log_cols.append(rxmcst_df)
                    
                elif "_edev" in log:
                    rxerr_df = pd.Series(df['rxerr/s']).rename(f"{log_name}_rxerr").dropna()
                    txerr_df = pd.Series(df['txerr/s']).rename(f"{log_name}_txerr").dropna()
                    coll_df = pd.Series(df['coll/s']).rename(f"{log_name}_coll").dropna()
                    
                    log_cols.append(rxerr_df)
                    log_cols.append(txerr_df)
                    log_cols.append(coll_df)
            
            pub_files = [(os.path.join( test, _ )) for _ in os.listdir(test) if "pub" in _]
            
            if len(pub_files) == 0:
                console.print(f"{test} has no pub files.", style="bold red")
                continue

            pub0_csv = pub_files[0]
            
            sub_files = [(os.path.join( test, _ )) for _ in os.listdir(test) if "sub" in _]

            test_df = pd.DataFrame()

            # ? Add the metrics for the entire test
            latencies = get_latencies(pub0_csv)
            if latencies is None:
                continue    

            latencies = latencies.rename("latency_us")
            total_throughput_mbps = get_total_sub_metric(sub_files, "mbps").rename("total_throughput_mbps")
            total_sample_rate = get_total_sub_metric(sub_files, "samples/s").rename("total_sample_rate")
            total_samples_received = pd.Series([get_total_sub_metric(sub_files, "total samples").max()]).rename("total_samples_received")
            total_samples_lost = pd.Series([get_total_sub_metric(sub_files, "lost samples").max()]).rename("total_samples_lost")
            
            pub_allocation_per_machine = get_participant_allocation_per_machine('pub', test)
            if pub_allocation_per_machine is not []:
                pub_allocation_per_machine = pd.Series(get_participant_allocation_per_machine('pub', test)).rename("pub_allocation_per_machine")
            
            sub_allocation_per_machine = get_participant_allocation_per_machine('sub', test)
            if sub_allocation_per_machine is not []:
                sub_allocation_per_machine = pd.Series(get_participant_allocation_per_machine('sub', test)).rename("sub_allocation_per_machine")

            test_df = pd.concat([
                latencies,
                total_throughput_mbps,
                total_sample_rate,
                total_samples_received,    
                total_samples_lost,
                pub_allocation_per_machine,
                sub_allocation_per_machine
            ] + [col for col in log_cols], axis=1)
            
            # ? Add the metrics for each sub
            for sub_file in sub_files:
                sub_i = sub_files.index(sub_file)
                
                throughput_mbps = get_metric_per_sub(sub_file, "mbps").rename(f"sub_{sub_i}_throughput_mbps")
                
                sample_rate = get_metric_per_sub(sub_file, "samples/s").rename(f"sub_{sub_i}_sample_rate")
                
                total_samples_received = pd.Series([get_metric_per_sub(sub_file, "total samples").max()])
                total_samples_received = total_samples_received.rename(f"sub_{sub_i}_total_samples_received")
                
                total_samples_lost = pd.Series([get_metric_per_sub(sub_file, "lost samples").max()])
                total_samples_lost = total_samples_lost.rename(f"sub_{sub_i}_total_samples_lost")
                
                test_df = pd.concat([
                    test_df, 
                    throughput_mbps,
                    sample_rate,
                    total_samples_received,
                    total_samples_lost    
                ], axis=1)

            # ? Replace NaN with ""
            test_df = test_df.fillna("")

            if not os.path.exists(summaries_dir):
                os.mkdir(summaries_dir)

            summary_csv_path = os.path.join(summaries_dir, f"{os.path.basename(test)}_summary.csv")
            
            if not os.path.exists(summary_csv_path):
                test_df.to_csv(summary_csv_path, sep=",")
    
    summaries_dirs = [os.path.join(local_dir, camp_dir) for camp_dir in os.listdir(local_dir) if os.path.isdir(os.path.join(local_dir, camp_dir)) and "summaries" in camp_dir]
    
    output_filename = os.path.basename(summaries_dir).replace("_summaries", "_df.csv")
    output_filename = os.path.join(local_dir, output_filename)
    
    if not os.path.exists(output_filename):
    
        camp_dfs = []

        for summaries_dir in summaries_dirs:
            
            summaries_dir_index = summaries_dirs.index(summaries_dir)
            
            console.print(f"[{summaries_dir_index + 1}/{len(summaries_dirs)}] Summarising {summaries_dir}...", style="bold white")
            
            camp_df = pd.DataFrame({
                'datalen_bytes': [], 
                'pub_count': [], 
                'sub_count': [], 
                'best_effort': [], 
                'multicast': [], 
                'durability': [], 
                
                'total_latency_us_mean': [], 
                'total_latency_us_std': [],
                'total_throughput_mbps_mean': [],
                'total_throughput_mbps_std': [],
                'total_sample_rate_mean': [],
                'total_sample_rate_std': [],
                
                # ? Pubs per machine
                'k2_pub_count': [],
                'k3_pub_count': [],
                'k4_pub_count': [],
                'k5_pub_count': [],
                
                # ? Subs per machine
                'k2_sub_count': [],
                'k3_sub_count': [],
                'k4_sub_count': [],
                'k5_sub_count': [],
                
                # ? User CPU usage per machine
                'k2_cpu_user_mean': [],
                'k3_cpu_user_mean': [],
                'k4_cpu_user_mean': [],
                'k5_cpu_user_mean': [],
                
                # ? System CPU usage per machine
                'k2_cpu_system_mean': [],
                'k3_cpu_system_mean': [],
                'k4_cpu_system_mean': [],
                'k5_cpu_system_mean': [],
                
                # ? System CPU idle per machine
                'k2_cpu_idle_mean': [],
                'k3_cpu_idle_mean': [],
                'k4_cpu_idle_mean': [],
                'k5_cpu_idle_mean': [],
                
                # ? Incoming packets per machine
                'k2_packets_in_mean': [],
                'k3_packets_in_mean': [],
                'k4_packets_in_mean': [],
                'k5_packets_in_mean': [],
                
                # ? Outgoing packets per machine
                'k2_packets_out_mean': [],
                'k3_packets_out_mean': [],
                'k4_packets_out_mean': [],
                'k5_packets_out_mean': [],
                
                # ? Incoming kilobytes per machine
                'k2_kb_in_mean': [],
                'k3_kb_in_mean': [],
                'k4_kb_in_mean': [],
                'k5_kb_in_mean': [],
                
                # ? Outgoing kilobytes per machine
                'k2_kb_out_mean': [],
                'k3_kb_out_mean': [],
                'k4_kb_out_mean': [],
                'k5_kb_out_mean': [],
                
                # ? RAM kilobytes free per machine
                'k2_kb_memfree_mean': [],
                'k3_kb_memfree_mean': [],
                'k4_kb_memfree_mean': [],
                'k5_kb_memfree_mean': [],
                
                # ? RAM kilobytes used per machine
                'k2_kb_memused_mean': [],
                'k3_kb_memused_mean': [],
                'k4_kb_memused_mean': [],
                'k5_kb_memused_mean': [],
                
                # ? RAM % memory used per machine
                'k2_percent_memused_mean': [],
                'k3_percent_memused_mean': [],
                'k4_percent_memused_mean': [],
                'k5_percent_memused_mean': [],
            })

            files = os.listdir(summaries_dir)

            for i in track(range(len(files)), description=f"Summarising files in {summaries_dir}..."):
                file = files[i]
                filename = file.replace("_summary.csv", '')
                datalen_bytes, pub_count, sub_count, best_effort, multicast, durability = get_settings_from_testname(filename)
                
                try:
                    file_df = pd.read_csv(os.path.join(summaries_dir, file))
                except Exception as e:
                    console.print(f"Exception for {file}:", style="bold red")
                    print(e)
                    print(os.path.join(summaries_dir, file))
                    continue

                try:
                    lat_mean = file_df['latency_us'].mean()
                    lat_std = file_df['latency_us'].std()
                    
                    tp_mean = file_df['total_throughput_mbps'].mean()
                    tp_std = file_df['total_throughput_mbps'].std()
                    
                    sr_mean = file_df['total_sample_rate'].mean()
                    sr_std = file_df['total_sample_rate'].std()
                    
                    total_samples_received = file_df['total_samples_received'].max()
                    total_samples_lost = file_df['total_samples_lost'].max()
                    
                    new_row = {
                        'datalen_bytes': [datalen_bytes],
                        'pub_count': [pub_count],
                        'sub_count': [sub_count],
                        'best_effort': [best_effort],
                        'multicast': [multicast],
                        'durability': [durability],
                        'total_latency_us_mean': [lat_mean],
                        'total_latency_us_std': [lat_std],
                        'total_throughput_mbps_mean': [tp_mean],
                        'total_throughput_mbps_std': [tp_std],
                        'total_sample_rate_mean': [sr_mean],
                        'total_sample_rate_std': [sr_std],
                        
                        # ? Pubs per machine
                        'k2_pub_count': [file_df['pub_allocation_per_machine'].iloc[0]],
                        'k3_pub_count': [file_df['pub_allocation_per_machine'].iloc[1]],
                        'k4_pub_count': [file_df['pub_allocation_per_machine'].iloc[2]],
                        'k5_pub_count': [file_df['pub_allocation_per_machine'].iloc[3]],
                        
                        # ? Subs per machine
                        'k2_sub_count': [file_df['sub_allocation_per_machine'].iloc[0]],
                        'k3_sub_count': [file_df['sub_allocation_per_machine'].iloc[1]],
                        'k4_sub_count': [file_df['sub_allocation_per_machine'].iloc[2]],
                        'k5_sub_count': [file_df['sub_allocation_per_machine'].iloc[3]],
                        
                        # ? User CPU usage per machine
                        'k2_cpu_user_mean': [file_df['k2_cpu_user'].mean()],
                        'k3_cpu_user_mean': [file_df['k3_cpu_user'].mean()],
                        'k4_cpu_user_mean': [file_df['k4_cpu_user'].mean()],
                        'k5_cpu_user_mean': [file_df['k5_cpu_user'].mean()],
                        
                        # ? System CPU usage per machine
                        'k2_cpu_system_mean': [file_df['k2_cpu_system'].mean()],
                        'k3_cpu_system_mean': [file_df['k3_cpu_system'].mean()],
                        'k4_cpu_system_mean': [file_df['k4_cpu_system'].mean()],
                        'k5_cpu_system_mean': [file_df['k5_cpu_system'].mean()],
                        
                        # ? System CPU idle per machine
                        'k2_cpu_idle_mean': [file_df['k2_cpu_idle'].mean()],
                        'k3_cpu_idle_mean': [file_df['k3_cpu_idle'].mean()],
                        'k4_cpu_idle_mean': [file_df['k4_cpu_idle'].mean()],
                        'k5_cpu_idle_mean': [file_df['k5_cpu_idle'].mean()],
                        
                        # ? Incoming packets per machine
                        'k2_packets_in_mean': [file_df['k2_dev_rxpck'].mean()],
                        'k3_packets_in_mean': [file_df['k3_dev_rxpck'].mean()],
                        'k4_packets_in_mean': [file_df['k4_dev_rxpck'].mean()],
                        'k5_packets_in_mean': [file_df['k5_dev_rxpck'].mean()],
                        
                        # ? Outgoing packets per machine
                        'k2_packets_out_mean': [file_df['k2_dev_txpck'].mean()],
                        'k3_packets_out_mean': [file_df['k3_dev_txpck'].mean()],
                        'k4_packets_out_mean': [file_df['k4_dev_txpck'].mean()],
                        'k5_packets_out_mean': [file_df['k5_dev_txpck'].mean()],
                        
                        # ? Incoming kilobytes per machine
                        'k2_kb_in_mean': [file_df['k2_dev_rxkB'].mean()],
                        'k3_kb_in_mean': [file_df['k3_dev_rxkB'].mean()],
                        'k4_kb_in_mean': [file_df['k4_dev_rxkB'].mean()],
                        'k5_kb_in_mean': [file_df['k5_dev_rxkB'].mean()],
                        
                        # ? Outgoing kilobytes per machine
                        'k2_kb_out_mean': [file_df['k2_dev_txkB'].mean()],
                        'k3_kb_out_mean': [file_df['k3_dev_txkB'].mean()],
                        'k4_kb_out_mean': [file_df['k4_dev_txkB'].mean()],
                        'k5_kb_out_mean': [file_df['k5_dev_txkB'].mean()],
                        
                        # ? RAM kilobytes free per machine
                        'k2_kb_memfree_mean': [file_df['k2_mem_mem_kbmemfree'].mean()],
                        'k3_kb_memfree_mean': [file_df['k3_mem_mem_kbmemfree'].mean()],
                        'k4_kb_memfree_mean': [file_df['k4_mem_mem_kbmemfree'].mean()],
                        'k5_kb_memfree_mean': [file_df['k5_mem_mem_kbmemfree'].mean()],
                        
                        # ? RAM kilobytes used per machine
                        'k2_kb_memused_mean': [file_df['k2_mem_mem_kbmemused'].mean()],
                        'k3_kb_memused_mean': [file_df['k3_mem_mem_kbmemused'].mean()],
                        'k4_kb_memused_mean': [file_df['k4_mem_mem_kbmemused'].mean()],
                        'k5_kb_memused_mean': [file_df['k5_mem_mem_kbmemused'].mean()],
                        
                        # ? RAM % memory used per machine
                        'k2_percent_memused_mean': [file_df['k2_mem_mem_percentmemused'].mean()],
                        'k3_percent_memused_mean': [file_df['k3_mem_mem_percentmemused'].mean()],
                        'k4_percent_memused_mean': [file_df['k4_mem_mem_percentmemused'].mean()],
                        'k5_percent_memused_mean': [file_df['k5_mem_mem_percentmemused'].mean()],
                    }
                except Exception as e:
                    console.print(f"\nException for file: {file} \n{e}", style="bold red")
                    
                new_df = pd.DataFrame(new_row)

                camp_df = pd.concat([camp_df, new_df], ignore_index=True)

                camp_dfs.append(camp_df)
                
        
        df = pd.concat(camp_dfs, ignore_index=True)
        df.drop_duplicates(inplace=True)
        print(f"Has NaNs: {df.isnull().values.any()}")
        with console.status("Writin df to file..."):
            df.to_csv(output_filename, index=False)
        console.print(f"New df printed to {output_filename}", style="bold green")
    
    console.print(f"Waiting for {interval} seconds...", style="bold blue")
    time.sleep(interval)

ssh.close()