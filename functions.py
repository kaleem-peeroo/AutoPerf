from utility_functions import *

def validate_args(args):
    # ? No args given.
    if len(args) == 0:
        console.print(f"No config file given.", style="bold red")
        console.print(f"Config file expected as:\n\tpython index.py <config_file>", style="bold green")
        sys.exit()

    # ? Validate config file
    config_path = args[0]
    if not ( os.path.isfile(config_path) and os.access(config_path, os.R_OK) ):
        console.print(f"Can't access {config_path}. Check it exists and is accessible and try again.", style="bold red")
        sys.exit()

    # ? Validate buffer multiple
    try:
        buffer_multiple = float(args[1])
        if buffer_multiple <= 1:
            console.print(f"<buffer_multiple> has to be greater than 1 instead of {buffer_multiple}.", style="bold red")
            sys.exit()
    except ValueError:
        console.print(f"<buffer_multiple> has to be a float value greater than 1 instead of {args[1]}", style="bold red")
        sys.exit()

def read_config(confpath):
    try:
        with open(confpath) as f:
            config = json.load(f)

        return config
    except Exception as e:
        console.print(f"Error when reading config: \n\t{e}", style="bold red")
        sys.exit()

def get_combinations_count_from_settings(settings):
    product = 1
    
    for key in settings:
        product *= len(settings[key])
    
    return product

def write_combinations_to_file(config):
    dir_name = create_dir("test_combinations")
    
    for campaign in config['campaigns']:
        comb_filename = campaign['name'].replace(" ", '_')
        comb_filename = os.path.join(dir_name, comb_filename + ".txt")
        combinations = get_combinations(campaign['settings'])
        comb_titles = []

        for combination in combinations:
            comb_titles.append(get_test_title_from_combination(combination))

        with open(comb_filename, "w") as f:
            f.writelines(f"{title}\n" for title in comb_titles)

        log_debug(f"Written test combinations to {comb_filename}.")

    return dir_name

def get_combinations_from_file(dirpath, config):
    comb_files = os.listdir(dirpath)
    comb_files = [os.path.join(dirpath, file) for file in comb_files]

    combs = []
    for file in comb_files:
        camp_name = os.path.basename(file).replace(".json", "").replace(".txt", "").replace("_", " ")
        
        with open(file, 'r') as f:
            titles = f.readlines()

        combinations = [get_combination_from_title(title) for title in titles]

        combs.append({
            "name": camp_name,
            "combinations": combinations
        })

    return combs

def get_combinations_from_config(config):
    combs = []

    for campaign in config['campaigns']:
        comb = get_combinations(campaign['settings'])
        combs.append({
            "name": campaign['name'],
            "combinations": comb
        })

    return combs

def generate_scripts(combination):
    script_base = ""
    
    pub_count = 0
    sub_count = 0
    
    duration_output = None
    
    for k, v in combination.items():
        if "bytes" in k:
            script_base = script_base + "-dataLen " + str(v) + " "
        elif "pub" in k:
            pub_count = v
        elif "sub" in k:
            sub_count = v
        elif "reliability" in k:
            if not v:
                script_base = script_base + "-bestEffort "
        elif "multicast" in k:
            if v:
                script_base = script_base + "-multicast "
        elif "durability" in k:
            script_base = script_base + "-durability " + str(v) + " "
        elif "latency_count" in k:
            latency_count_output = "-LatencyCount " + str(v) + " "
        elif "duration" in k:
            duration_output = "-executionTime " + str(v)
    
    scripts = []
    
    if pub_count == 1:
        scripts.append( script_base + "-pub -outputFile pub_0.csv -numSubscribers " + str(sub_count))
    elif pub_count == 0:
        console.print("Publisher count can't be 0.", style="bold red")
        sys.exit(0)
    else:
        sub_count_string = "-numSubscribers " + str(sub_count) + " "
        for i in range(pub_count):
            if i == 0:
                scripts.append(script_base + "-pub -pidMultiPubTest " + str(i) + " -outputFile pub_" +str(i)+ ".csv " + sub_count_string)
            else:
                scripts.append(script_base + "-pub -pidMultiPubTest " + str(i) + " " + sub_count_string)
    
    if sub_count == 1:
        scripts.append( script_base + "-sub -outputFile sub_0.csv -numPublishers " + str(pub_count))
    elif sub_count == 0:
        console.print("Subscriber count can't be 0.", style="bold red")
        sys.exit(0)
    else:
        pub_count_string = "-numPublishers " + str(pub_count) + " "
        for i in range(sub_count):
            scripts.append(script_base + "-sub -sidMultiSubTest " + str(i) + " -outputFile sub_" + str(i) + ".csv " + pub_count_string)
    
    updated_scripts = []
    for script in scripts:
        if "-pub" in script:
            script = script + " " + duration_output + " " + latency_count_output + " -batchSize 0 "
        
        script = script + " -transport UDPv4 "
            
        updated_scripts.append(script)
           
    return updated_scripts

def convert_seconds(seconds):
    days = seconds // (24 * 3600)
    seconds = seconds % (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return f"{days} Days, {hours} Hours, {minutes} Minutes, {seconds} Seconds"

def calculate_total_duration(camp_scripts):
    total_duration_s = 0
    for camp in camp_scripts:
        for test in camp['tests']:
            duration_s = int(test['combination']['duration_s'])
            total_duration_s += duration_s

    return total_duration_s

def validate_scripts(combination, scripts):

    if not validate_setting(int(combination['datalen_bytes']), 'dataLen', scripts, combination):
        return False

    if not validate_setting(int(combination['duration_s']), 'executionTime', scripts, combination):
        return False

    if not validate_setting(int(combination['pub_count']), 'numPublishers', scripts, combination):
        return False
    
    if not validate_setting(int(combination['sub_count']), 'numSubscribers', scripts, combination):
        return False
    
    if not validate_setting(int(combination['durability']), 'durability', scripts, combination):
        return False
    
    if not validate_setting(int(combination['latency_count']), 'LatencyCount', scripts, combination):
        return False

    return True

def allocate_scripts_per_machine(scripts, machine_count):
    
    shared_pub_scripts = []
    shared_sub_scripts = []
    
    if machine_count == 1:
        return [scripts]
    else:
        for i in range(machine_count):
            shared_pub_scripts.append([])
            shared_sub_scripts.append([])
    
    pub_scripts = [x for x in scripts if '-pub' in x]
    sub_scripts = [x for x in scripts if '-sub' in x]
    
    shared_pub_scripts = share(pub_scripts, machine_count)
    shared_sub_scripts = share(sub_scripts, machine_count)

    return shared_pub_scripts, shared_sub_scripts

def machine_thread_func(machine, testdir, buffer_multiple):
    """
    - Check that the machine is online.
    - Check for, download, and then delete existing csv files.
    - Restart the machine.
    - Check its online.
    - Start the logging.
    - Run the scripts.
    - Wait for scripts to finish.
    - Write the stderr to a file (if it has content).
    - Check that all .csv files were generated and dowload them.
    - Download the system logs.
    """
    name = machine['name']
    host = machine['host']
    username = machine['username']
    ssh_key = machine['ssh_key']
    scripts = machine['scripts']

    NAME = f"[bold green]{name.upper()}:[/bold green]"

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # ? Check machine is online.
    log_debug(f"{NAME} Checking if online...")
    check_machine_online(ssh, host, username, ssh_key, 60)
    log_debug(f"{NAME} Is online.")

    # ? Check for, download and delete existing csv files.
    download_leftovers(machine, ssh, testdir)

    # ? Restart machine.
    log_debug(f"{NAME} Restarting machine...")
    restart_machine(ssh, host, username, ssh_key) if not SKIP_RESTART else None
    log_debug(f"{NAME} Machine restarted.")

    # ? Check machine is online again.
    log_debug(f"{NAME} Checking if online again...")
    check_machine_online(ssh, host, username, ssh_key, 60)
    log_debug(f"{NAME} Machine is online and ready for testing.")

    # ? Start system logging.
    if start_system_logging(machine, os.path.basename(testdir), buffer_multiple):
        log_debug(f"{machine['name']} Logging started.")
    else:
        log_debug(f"{machine['name']} Something went wrong when starting the system log. The sar_logs file wasn't found after running the log command.")
        return

    # ? Run the scripts.
    log_debug(f"{machine['name']} Running scripts...")
    if scripts:
        stdout, stderr = run_scripts(ssh, machine)
    else:
        log_debug(f"{machine['name']} No scripts to run.")
        stdout = None
        stderr = None
    log_debug(f"{machine['name']} Scripts finished running.")

    # ? Check that all expected files have been generated.
    log_debug(f"{machine['name']} Checking generated csv files...")
    k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])
    ssh.connect(machine['host'], username=machine['username'], pkey = k)

    # ? Check how many csv files exist in the home_dir now that the test has finished.
    stdin, stdout, stderr = ssh.exec_command(f"ls {machine['home_dir']}/*.csv")

    csv_file_count = len(stdout.readlines())

    log_debug(f"{machine['name']} {csv_file_count} csv files found.")

    if csv_file_count > 0:
        log_debug(f"{machine['name']} Downloading csv files...")
        downloaded_files_count = download_csv_files(machine, ssh, testdir)
        log_debug(f"{machine['name']} {downloaded_files_count} csv files downloaded.")
    else:
        console.print(f"{ERROR} {machine['name']} No csv files found after the test has finished...", style="bold white")
        downloaded_files_count = 0

    # ? Create folder for the system logs.
    logs_dir = os.path.join(testdir, "logs")
    if not os.path.exists(logs_dir):
        log_debug(f"{machine['name']} Creating logs folder...")
        os.makedirs(logs_dir)
        log_debug(f"{machine['name']} logs folder created.")

    # ? Download system logs.
    log_debug(f"{machine['name']} Parsing and downloading system logs...")
    downloaded_logs_count = download_logs(machine, ssh, logs_dir)
    log_debug(f"{machine['name']} {downloaded_logs_count} system logs downloaded.")

def add_seconds_to_now(seconds_amount):
    now = datetime.now()

    new_time = now + timedelta(seconds=seconds_amount)

    return new_time.strftime("%Y-%m-%d %H:%M:%S")

def format_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def output_test_progress(progress_json):

    with open(progress_json, 'r') as f:
        progress = json.load(f)

    # ? Get total number of tests run.
    total_test_count = len(progress)

    # ? Get number of failed tests.
    failed_test_count = len([test for test in progress if 'fail' in test['status']])
    failed_percent = (failed_test_count / total_test_count) * 100
    failed_percent = "{:.2f}".format(failed_percent)

    # ? Get number of good tests.
    good_test_count = len([test for test in progress if 'success' in test['status']])
    good_percent = (good_test_count / total_test_count) * 100
    good_percent = "{:.2f}".format(good_percent)

    # ? Good + bad = total
    assert(total_test_count == failed_test_count + good_test_count)

    console.print(f"[{format_now()}] {total_test_count} tests run. {failed_test_count} ({failed_percent}%) tests failed. {good_test_count} ({good_percent}%) tests succeeded.")