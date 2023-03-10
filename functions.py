import fnmatch
import shutil
import sys
import os
import json
import paramiko
import time
import concurrent.futures
import math
import threading
import re
import stat

from threading import Thread
from datetime import datetime, timedelta
from pprint import pprint
from itertools import product
from itertools import repeat
from rich.console import Console
from rich.markdown import Markdown
from rich.prompt import Prompt
from rich.prompt import Confirm
from rich.progress import track

# ? Uncomment for rich traceback formatting
# from rich.traceback import install
# install(show_locals=True)

console = Console()

DEBUG_MODE = True
# ? Don't restart the VM when running the tests - normally used to speed up debugging process.s
SKIP_RESTARTS = True
# ? Buffer duration to wait for after expected test duration before skipping the test.
FAIL_DURATION_S = 300
IS_TEST_OVER_ELAPSED = False

# ? Styling for debug output:
PING = "[bold white][PING][/bold white]"
SETUP = "[bold blue][SETUP][/bold blue]"
EXEC = "[bold green][EXEC][/bold green]"
LOG = "[bold bright_magenta][LOG][/bold bright_magenta]"
ERROR = "[bold red][ERROR][/bold red]"

"""
? get_config(args) returns the config filepath from the args.

:param      args:           list of arguments passed in when executing index.py

:returns    config_path:    string of the filepath of the config file
"""
def get_config(args):
    # The argument isn't passed in.
    if len(args) == 0:
        console.print("No config file path provided. \n\tUsing [bold white]config.json[/bold white] by default.", style="red")
        return os.path.join("config.json")
    # More than 1 argument is passed in.
    elif len(args) > 1:
        console.print("Multiple values for the config file provided. \n\tUsing the first value: [bold white]" + args[0] + "[/bold white].", style="bold red")
    # The argument is not a valid file path.
    else:
        config_path = args[0]
        if not os.path.exists(config_path):
            console.print("The config file can't be found: \n\t[bold white]" +config_path+ "[/bold white]", style="bold red")
            sys.exit(0)
        else:
            return config_path

"""
? read_config(config_path) reads and returns the contents of the config file.

:param      config_path:    string: filepath of the config file.

:returns    config:         contents of the config file
"""
def read_config(config_path):
    with open(config_path, 'r') as f:
        config = json.load(f)

    return config
    
"""
? parse_config(config) parses the config to produce the scripts per campaign.

:param      config:             contents of the config file

:returns    campaign_scripts:   list of scripts
"""
def parse_config(config):
    camps = config["campaigns"]
    campaigns_scripts = []
    for camp in camps:
        camp_name = camp["campaign"]
        camp_name = camp_name.lower().replace(" ", "_")
        
        if os.path.isdir(camp_name):    # if campaign dir already exists
            if Confirm.ask("The folder " + camp_name + " already exists. Would you like to overwrite it?", default=True):
                # ? Delete old campaign dir
                try:
                    shutil.rmtree(camp_name)
                    if os.path.exists(camp_name):
                        console.print(camp_name + " failed to be deleted.", style="bold red")
                    else:
                        console.print(camp_name + " deleted.", style="bold orange3")
                except Exception as e:
                    console.print("Couldn't delete " + camp_name, style="bold red")
                    console.print(e, style="bold red")
            else:
                console.print("Exiting program.", style="bold red")
                sys.exit(0)
        
        # ? Get the scripts for all test sets
        """
        ? parse_sets(camp) structure:
        [{
            name: <campaign_name>,
            repetitions: <repetitions>,
            scripts: [
                [{
                    host: <hostname>,
                    name: <machine_name>,
                    ssh_key: <ssh_key_location_on_client>
                    perftest: <perftest_on_machine>,
                    test: <test_name>,
                    username: <username>,
                    pub_scripts: [],
                    sub_scripts: []
                }]
            ]
        }]
        """
        campaigns_scripts.append(parse_sets(camp))
        
    return campaigns_scripts
    
"""
? parse_sets(camp) parses the sets to produce the scripts needed to run the tests.

:param      camp:                   contents of the campaign configuration from the 
                                    config file.

:returns    {campaign, scripts}:    dictionary containing the test campaign and the 
                                    scripts associated with that campaign
"""    
def parse_sets(camp):
    sets = camp["sets"] 
    machines = camp["machines"]
    
    campaign_scripts  = []
    
    for set in sets:
        settings = set["settings"]
        campaign_scripts.append({
            "name": set["set"],
            "repetitions": set["repetitions"],
            "scripts": get_scripts_for_set(settings, machines, set["set"])
        })
        
    return {"campaign": camp["campaign"], "scripts": campaign_scripts}

"""
? get_scripts_for_set(settings, machines, set_name) generates the scripts per machine depending on the combination of variables and how many are required per machine.

:param      settings:       settings of the test set from the config file.  
:param      machines:       machines used in the test from the config file.
:param      set_name:       name of the test set.

:returns    total_scripts:  the scripts allocated per machine.

? How it works:
1. Get all the combinations.
2. Ask user which combinations to run (by default its all).
3. For each combination:
    3.1. Generate all the pub and sub scripts.
    3.2. Allocate the pub and sub scripts per machine.
4. Return the scripts per machine.

"""
def get_scripts_for_set(settings, machines, set_name):
    combinations = get_combinations(settings)
    
    console.print("Here are all of the tests for [bold blue]" +set_name+ "[/bold blue]:", style="bold white")
    
    comb_titles = []
    
    for comb in combinations:
        test_title = get_test_title(comb)
        comb_titles.append(test_title)
        print("\t" + test_title)
        
    # ? Check for duplicate names
    if len(comb_titles) != len(set(comb_titles)):
        console.print("Duplicate test titles found!", style="bold red")
        sys.exit(0)
        
    total_duration = 0
    for comb in combinations:
        total_duration += comb["duration"]
    expected_end_time = datetime.now() + timedelta(minutes=total_duration)
    expected_end_time = "{:%d/%m/%Y - %H:%M:%S}".format(expected_end_time)
    
    console.print("[bold green]" + str(len(combinations)) + " total tests[/bold green] to run for [bold green]" + str(format_duration(total_duration)) + ".[/bold green]", style="bold white")
    console.print(f"Estimated to end on:\n\t[bold green]{expected_end_time}[/bold green]", style="bold white")

    if Confirm.ask("Would you like to remove some tests?", default=False):
        with open("tests.txt", "w") as f:
            for title in comb_titles:
                f.write(title + "\n")
        console.print("All of the tests have been listed in [bold white]tests.txt[/bold white]. Remove the ones you don't want and then come back here when you are ready.", style="bold white")
        
        is_ready = Confirm.ask("Are you ready?", default=True)
        while not is_ready:
            is_ready = Confirm.ask("Are you ready?", default=True)
        
        console.print("Here is the [bold green]new[/bold green] list of the tests for [bold blue]" +set_name+ "[/bold blue]:", style="bold white")
        
        for comb in combinations:
            test_title = get_test_title(comb)
            comb_titles.append(test_title)
            print("\t" + test_title)
            
        total_duration = 0
        for comb in combinations:
            total_duration += comb["duration"]
        console.print("[bold green]" + str(len(combinations)) + " total tests[/bold green] to run for [bold green]" + str(format_duration(total_duration)) + ".[/bold green]", style="bold white")
        console.print(f"Estimated to end on:\n\t[bold green]{expected_end_time}[/bold green]", style="bold white")
        
        with open("tests.txt", "r") as f:
            content = f.read()
            
        new_test_titles = [x for x in content.split("\n") if len(x) > 0]
        
        combinations = [comb for comb in combinations if get_test_title(comb) in new_test_titles]
                
    total_scripts = []
    
    for comb in combinations:
        machine_scripts = []
        
        test_title = get_test_title(comb)
        scripts = generate_scripts(comb)
        
        pub_scripts, sub_scripts = allocate_scripts_per_machine(scripts, machines)
        
        for i in range(len(machines)):            
            perftest = machines[i]["perftest"]
            
            if len(pub_scripts) > 1:
                machine_pub_scripts = [perftest + " " + x for x in pub_scripts[i]]
            else:
                machine_pub_scripts = pub_scripts
            
            if len(sub_scripts) > 1:
                machine_sub_scripts = [perftest + " " + x for x in sub_scripts[i]]
            else:
                machine_sub_scripts = sub_scripts
            
            machine_script = {
                "test": test_title,
                "name": machines[i]["name"],
                "host": machines[i]["host"],
                "ssh_key": machines[i]["ssh_key"],
                "username": machines[i]["username"],
                "perftest": machines[i]["perftest"],
                "perftest_publisher": machines[i]["perftest_publisher"],
                "perftest_subscriber": machines[i]["perftest_subscriber"],
                "home_dir": machines[i]["home_dir"],
                "pub_scripts": machine_pub_scripts,
                "sub_scripts": machine_sub_scripts
            }
            
            
            machine_scripts.append(machine_script)
            
        total_scripts.append(machine_scripts)
        
    return total_scripts
    
"""
? my_round(x) rounds a number down from .4 down and up from .5 up.

:param      x:  A number - preferably a float.

:returns    x: The rounded version of the number - now an integer.     

"""
def my_round(x):
    if (float(x) % 1) >= 0.5:
        return math.ceil(x)
    else:
        return round(x)
        
"""
? allocate_scripts_per_machine(scripts, machines) equally shares the pub and sub scripts per machine.

:param      scripts:            all of the scripts (both pub and subs)
:param      machines:           the machines from the config file

:returns    shared_pub_scripts: the pub scripts spread per machine
:returns    shared_sub_scripts: the sub scripts spread per machine
"""        
def allocate_scripts_per_machine(scripts, machines):
    shared_pub_scripts = []
    shared_sub_scripts = []
    
    if len(machines) == 1:
        return [scripts]
    else:
        for i in range(len(machines)):
            shared_pub_scripts.append([])
            shared_sub_scripts.append([])
    
    pub_scripts = [x for x in scripts if '-pub' in x]
    sub_scripts = [x for x in scripts if '-sub' in x]
    
    shared_pub_scripts = share(pub_scripts, len(machines))
    shared_sub_scripts = share(sub_scripts, len(machines))
    
    return shared_pub_scripts, shared_sub_scripts
        
"""
? vprint(string_val, variable_val) is used when debugging to output variables names and values.

:param      string_val:     string of the variable name
:param      variable_val:   the variable it self

:returns    none
"""
def vprint(string_val, variable_val):
    print(string_val + ": " + str(variable_val))

"""
? share(items, bins) equally shares a list of items into the provided bins.

:param      items:  list of things to share
:param      bins:   buckets to share the things into

:returns    output: list of sublists with the items shared (sublists are the bins)
"""
def share(items, bins):
    if len(items) == 0 or bins == 0:
        return []

    if bins == 1:
        return items

    output = []
    
    for i in range(bins):
        output.append([])
    
    while len(items) > 0:
        for i in range(bins):
            try:
                output[i].append(items[0])
                items = items[1:]
            except Exception as e:
                None
            
    return output
    
"""
? generate_scripts(combination) generates the scripts from the provided combination of settings.

:param      combination:        combination of the settings as a dictionary where the 
                                keys are the settings and the values are the setting 
                                values

:returns    updated_scripts:    list of all pub and sub scripts generated from the
                                provided combination of settings.
"""    
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
        elif "reliable" in k:
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

"""
? get_test_title(combination) creates a title for the test by stringing together abbreviations of the setting combination.

:param      combination:    The settings combination.

:returns    title:          The string of the title generated.
"""  
def get_test_title(combination):
    title = ""
    for k, v in combination.items():
        if "bytes" in k:
            title = title + str(v) + "B_"
        elif "pub" in k:
            title = title + str(v) + "P_"
        elif "sub" in k:
            title = title + str(v) + "S_"
        elif "reliable" in k:
            if v:
                title = title + "rel_"
            else:
                title = title + "be_"
        elif "multicast" in k:
            if v:
                title = title + "mc_"
            else: 
                title = title + "uc_"
        elif "latency_count" in k:
            title = title + str(v) + "lc_"
        elif "duration" in k:
            title = title + str(v) + "s_"
        elif "durability" in k:
            title = title + str(v) + "dur_"
            
    return title[:-1]
   
"""
? get_combinations(settings) generates a dictionary where the keys are the settings and the values are the setting values.

:param      settings:               The settings from the config file.

:returns    [{settings, values}]:   The dictionary generated.
"""
def get_combinations(settings):
    return [dict( zip(settings, value)) for value in product(*settings.values()) ];

"""
? restart_vm_thread(machine) is the function responsible for restarting the VM.

:param      machine     Object containing information about the machine including hostname, username, and ssh key location.

:returns    None    
"""
def restart_vm_thread(machine):
    host = machine["host"]
    username = machine["username"]
    ssh_key = machine["ssh_key"]
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    # ? Check the machine is online
    check_machine_online(ssh, host, username, ssh_key, 30)
    
    console.print(hostname + ": " + ERROR + " Taking longer than expected. Force restarting.", style="bold white") if DEBUG_MODE else None
    
    # ? Restart the machine
    restart_machine(ssh, host, username, ssh_key)
    
    console.print(hostname + ": " + ERROR + " Force restart finished.", style="bold white") if DEBUG_MODE else None

"""
? process_elapsed_test(test, expected_duration, run_dir) logs a test that is taking longer than unexpected and attempts to diagnose it.

:param      test:               the current test     
:param      expected_duration:  how long the test should have taken
:param      run_dir:            the run_n dir path of the test

:returns    none
"""
def process_elapsed_test(test, expected_duration, run_dir):
    console.print("Processing over elapsed test.", style="bold red") if DEBUG_MODE else None
    global IS_TEST_OVER_ELAPSED
    IS_TEST_OVER_ELAPSED = True
    
    run_index = os.path.basename(run_dir).split("_")[1]
    
    # ? Create test failure log
    # test_fail_log_path = os.path.join(run_dir, "test_fail.log")
    # if not os.path.exists(test_fail_log_path):
    #     with open(test_fail_log_path, "w") as f:
    #         f.write("TEST: " + test[0]["test"] + "\n")
    #         f.write("RUN: " + str(run_index) + "\n")
    
    # ? Add expected duration in test fail log
    # with open(test_fail_log_path, "r") as f:
    #     contents = f.readlines()
    # is_fail_reason_logged = len([x for x in contents if "FAIL REASON" in x]) > 0
    # if not is_fail_reason_logged:
    #     with open(test_fail_log_path, "a") as f:
    #         f.write("FAIL REASON: Exceeded expected duration of " + format_duration(expected_duration) + "\n")

    # ? Add warning to progress log
    with open("progress.log", "r") as f:
        content = f.readlines()
    is_elapsed_test_logged = len([x for x in content[-2:] if "taking longer than expected" in x]) > 0
    if not is_elapsed_test_logged:
        log_elapsed_test_warning(expected_duration)

    # ? Check which error logs are generated
    # logs = os.listdir(run_dir)
    # err_logs = [log for log in logs if "error.log" in log]
    # if len(err_logs) == 0:
    #     with open(test_fail_log_path, "r") as f:
    #         contents = f.readlines()
            
    #     is_diagnosis_logged = len([x for x in contents if "No error logs" in x]) > 0
            
    #     if not is_diagnosis_logged:
    #         with open(test_fail_log_path, "a") as f:
    #             f.write("\tDIAGNOSIS: No error logs generated. Test possibly still running.\n")
    
    # ? Restart all VMs
    console.print("Restarting VMs.", style="bold red")
    vm_threads = []
    for vm in test:
        vm_thread = Thread(target=restart_vm_thread, args=(vm, ))
        vm_thread.start()
        vm_threads.append(vm_thread)
        
    console.print("Waiting on VMs to restart.", style="bold red")
    for vm_thread in vm_threads:
        vm_thread.join()
    console.print("All VMs have restarted.", style="bold red")
    IS_TEST_OVER_ELAPSED = False
    
def exit():
    os._exit(1)
    
"""
? log_elapsed_test_warning(expected_duration) logs the test duration warning.

:param      expected_duration:  how long the test was expected to run for.

:returns    none
"""
def log_elapsed_test_warning(expected_duration):
    try:
        with open("progress.log", "r") as f:
            content = f.readlines()[:-1]
    
        content.append("\t\t\tWARNING: Test is taking longer than expected (" +format_duration(expected_duration)+ ").\n")
        
        update_progress_log(
            content,
            True
        )
    except OSError as e:
        return
        
"""
? elapsed_duration_thread(start_time, test, run_dir) is the function that tracks how long the test is taking.

:param      start_time:             the start time of the test
:param      test:                   the test that is running
:param      run_dir:                the run_n dir path of the test
:param      timer_finish_event      thread event to control the finish of this thread function - otherwise it'll run forever

:returns    none
"""
def elapsed_duration_thread(start_time, test, run_dir, timer_finish_event):
    exec_time = int(test[0]["test"].split("_")[0].replace("s", ""))
    
    while True:
        now = time.time() - start_time
        
        try:
            with open("progress.log", "r") as f:
                log_content = f.readlines()
        
            last_line = log_content[-1]
            new_log_content = log_content[:-1]
            
            if "Elapsed Duration: " in last_line:
                new_line = "\t\t\tElapsed Duration: " + format_duration("{:.0f}".format(now)) + "\n"
                new_log_content.append(new_line)
                update_progress_log(new_log_content, True)
            else:
                duration_output = "\t\t\tElapsed Duration: " + format_duration("{:.0f}".format(now))
                update_progress_log(duration_output)
        except OSError as e:
            continue
        
        # ? Check if test is taking longer than expected
        if int(now) > exec_time + FAIL_DURATION_S:
            process_elapsed_test(test, exec_time + FAIL_DURATION_S, run_dir)
            break
        
        if timer_finish_event.is_set():
            break
        
        time.sleep(1)
    
"""
? update_progress_log(content, overwrite) adds content to the test log which is progress.log.

:param      content:    the content to write into the log
:param      overwrite:  whether to overwrite everything in the log entirely or to 
                        append

:returns    none
""" 
def update_progress_log(content, overwrite=False):
    if overwrite:
        with open("progress.log", "w") as f:
            f.writelines(content)    
    else:
        with open("progress.log", "a") as f:
            f.write(content + "\n")
       
"""
? log_test_config(log_dirpath, conf) saves the test config in a test_config.log file.

:param      log_dirpath:    path to the test directory where the log is to be saved
:param      conf:           the test configuration

:returns    none
"""
def log_test_config(log_dirpath, conf):
    logpath = os.path.join(log_dirpath, "test_config.log")
    with open(logpath, "w") as f:
        f.write(str(conf))
        
"""
? run_tests(scripts) runs the tests.

:param      scripts:    the scripts produced from the combination for this campaign.

:returns    none

? How does it work:
1. For each campaign.
    1.1. For each test set.
        1.1.1. Write the set name to progress.log
        1.1.2. For each test
            1.1.2.1. Write the test name to progress.log
            1.1.2.2. For each repetition
                1.1.2.2.1. Start the timer thread
                1.1.2.2.2. Run scripts per machine
                1.1.2.2.3. End the timer thread and record test duration
"""
def run_tests(scripts):
    global IS_TEST_OVER_ELAPSED
    
    for camp in scripts:
        
        camp_name = camp["campaign"]
        sets = camp["scripts"]
        
        safe_camp_name = camp_name.replace(" ", "_").lower()
        
        # ? Create the campaign folder
        mkdir(safe_camp_name)
        
        # ? Delete progress log if it already exists
        progress_log_path = os.path.join(safe_camp_name, "progress.log")
        
        if os.path.exists( progress_log_path ):
            if DEBUG_MODE:
                console.print(f"{LOG}: progress.log already exists. Deleting it.", style="bold white")
            os.remove( progress_log_path )

        with open("progress.log", "w") as f:
            pass
            
        if DEBUG_MODE:
            console.print(camp_name + ": progress.log created.", style="bold white")
        
        for set in sets:
            
            set_name = set["name"]
            reps = set["repetitions"]
            safe_set_name = set_name.replace(" ", "_").lower()
            
            # ? Update progress.log
            update_progress_log("SET: " + set_name)
            
            # ? Create the set folder if it doesn't already exist
            mkdir(os.path.join(safe_camp_name, safe_set_name))

            for test in set["scripts"]:
                test_name = test[0]["test"]
                total_tests = len(set["scripts"])
                current_test_index = set["scripts"].index(test) + 1 
                
                # ? Create the test folder if it doesn't already exist
                mkdir(os.path.join(safe_camp_name, safe_set_name, test_name))

                # ? Create test log saving the settings
                log_test_config( os.path.join(safe_camp_name, safe_set_name, test_name), test)

                # ? Update progress.log
                update_progress_log("\tTEST #" +str(current_test_index)+ ": " + test_name)

                console.print("[bold blue]⏳  [" +str(current_test_index)+ "/" +str(total_tests)+ "] " +test_name+ " started.[/bold blue]") if DEBUG_MODE else None
                
                with console.status("[bold blue][" +str(current_test_index)+ "/" +str(total_tests)+ "] " +test_name+ " in progress.[/bold blue]"):
                    
                    for rep_count in range(reps):
                
                        # ? Create the run folders if it doesn't already exist
                        run_path = os.path.join(
                            safe_camp_name, 
                            safe_set_name, 
                            test_name, 
                            "run_" + str(rep_count + 1)
                        )
                        mkdir(run_path)
                        
                        start_time = time.perf_counter()
                        # ? Update progress.log with rep start time
                        update_progress_log(
                            "\t\t[" +str(rep_count + 1)+ "/" +str(reps)+ "]: Started at " + str(datetime.now())
                        )

                        expected_test_duration_s = int(test[0]["pub_scripts"][0].split("-executionTime")[1].split(" ")[1])

                        # ? Thread for measuring elapsed duration
                        timer_finish_event = threading.Event()
                        thread = Thread(target=elapsed_duration_thread, args=(time.time(), test, run_path, timer_finish_event))
                        thread.start()
                        
                        vm_threads = []
                        
                        for vm in test:
                            vm_thread = Thread(target=ssh_thread, args=(vm, set_name, camp_name, rep_count + 1, reps))
                            vm_thread.start()
                            vm_threads.append(vm_thread)
                            
                        for vm_thread in vm_threads:
                            vm_thread.join(expected_test_duration_s + FAIL_DURATION_S)
                        
                        timer_finish_event.set()
                        thread.join()
                        
                        end_time = time.perf_counter()

                        try:
                            # ? Update progress.log with rep end time
                            with open("progress.log", "r") as f:
                                content = f.readlines()[:-1]
                                
                            content.append("\t\t[" +str(rep_count + 1)+ "/" +str(reps)+ "]: Finished at " + str(datetime.now()) + "\n")
                                
                            update_progress_log(content, True)
                        except OSError as e:
                            console.print(f"{ERROR}: Couldn't write end time to progress.log.\n\t{e}", style="bold white")
                        
                        duration = get_duration(start_time, end_time)

                        console.print("✅ [" +str(rep_count + 1)+ "/" +str(reps)+ "] " + test_name + ": " + duration + ".", style="bold white")
                        
                        # ? Update progress log
                        update_progress_log("\t\t[" +str(rep_count + 1)+ "/" +str(reps)+ "]: " + duration)
                        
                        if rep_count + 1 == reps:
                            console.print("\n", style="bold white")

"""
? mkdir(dirpath) creates the dirpath if it doesn't already exist.

:param      dirpath:    path of the directory to make

:returns    none
"""
def mkdir(dirpath):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
                
"""
? format_duration(seconds_value) generates a nice looking string of the duration in days, hours, minutes, seconds.

:param      seconds_value:  duration in seconds.

:returns    string:         string formatted in days, hours, mins, seconds.
"""
def format_duration(seconds_value):
    duration_mins, seconds_value = divmod(int(seconds_value), 60)
    duration_hrs, duration_mins = divmod(duration_mins, 60)
    duration_days, duration_hrs = divmod(duration_hrs, 24)
    
    return f'{duration_days:02d} Days: {duration_hrs:02d} Hours: {duration_mins:02d} Minutes: {seconds_value:02d} Seconds'
         
"""
? get_duration(start_time, end_time) calculates the difference (formatted into days, hours, minutes, seconds) between two time stamps.

:param      start_time: start time stamp
:param      end_time:   end time stamp

:returns    string:     difference between the two time stamps formatted into days, 
                        hours, minutes, seconds.
"""       
def get_duration(start_time, end_time):
    duration_secs = end_time - start_time
    return format_duration(duration_secs)
                
"""
? ssh_thread(machine, set_name, camp_name, current_repetition, total_repetitions) is the function responsible for handling the ssh thread which connects to the machine and runs the tests.

:param      machine:            machine details from the config file
:param      set_name:           name of the current test set
:param      camp_name:          name of the current campaign
:param      current_repetition: index of the current repetition being run
:param      total_repetitions:  number of total repetitions being run

:returns    none

? How does it work
1. Join up the pub and sub scripts into a single command.
2. Check that the machine is online.
3. Restart the machine.
4. Check that the machine is online again.
5. Log the scripts.
6. Check for, download and delete existing .csv files.
7. Run the scripts.
8. Log the stdout (currently commented out because the logs were gigabytes in length).
9. Log the stderr.
10. Download the test results .csv files.
"""
def ssh_thread(machine, set_name, camp_name, current_repetition, total_repetitions):
    global IS_TEST_OVER_ELAPSED
    home_dir = machine["home_dir"]
    test_name = machine["test"]
    hostname = machine["name"]
    host = machine["host"]
    username = machine["username"]
    ssh_key = machine["ssh_key"]
    
    # safe_ variables are variables without spaces and formatted using _
    # e.g. Base Case = base_case
    safe_set_name = set_name.replace(" ", "_").lower()
    safe_camp_name = camp_name.replace(" ", "_").lower()
    
    test_dir_path = os.path.join("./", safe_camp_name, safe_set_name, test_name)
    
    pub_scripts = machine["pub_scripts"]
    sub_scripts = machine["sub_scripts"]
    
    pub_scripts = " & ".join(pub_scripts) if len(pub_scripts) > 0 else pub_scripts
    sub_scripts = " & ".join(sub_scripts) if len(sub_scripts) > 0 else sub_scripts
    
    if len(pub_scripts) > 0 and len(sub_scripts) > 0:
        scripts = pub_scripts + " & " + sub_scripts
    elif len(pub_scripts) > 0 and len(sub_scripts) == 0:
        scripts = pub_scripts
    elif len(pub_scripts) == 0 and len(sub_scripts) > 0:
        scripts = sub_scripts
    else:
        scripts = None
      
    if IS_TEST_OVER_ELAPSED:
        console.print(hostname + ": " + ERROR + " Test has over elapsed. Quitting this thread.", style="bold white")
        return
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    # ? Check the machine is online
    check_machine_online(ssh, host, username, ssh_key, 5)
    
    # ? Restart the machine
    console.print(hostname + ": " + EXEC + " Restarting.", style="bold white") if DEBUG_MODE else None
    restart_machine(ssh, host, username, ssh_key) if not SKIP_RESTARTS else None
    
    # ? Check the machine is online
    check_machine_online(ssh, host, username, ssh_key, 30)
    console.print(hostname + ": " +PING+ " Online.", style="bold white") if DEBUG_MODE else None

    # ? Check for, download and delete existing .csv files
    if has_leftovers(ssh, hostname, home_dir):
        leftovers_path = os.path.join(
            test_dir_path,
            "run_" + str(current_repetition),
            "leftovers"
        )
        if not os.path.exists(leftovers_path):
            os.makedirs(leftovers_path)
            console.print(hostname + ": " +SETUP+ " Making leftovers dir.", style="bold white") if DEBUG_MODE else None
    
        download_test_results(
            ssh, 
            hostname,
            host,
            home_dir, 
            os.path.join(
                "./", 
                safe_camp_name, 
                safe_set_name, 
                test_name, 
                "run_" + str(current_repetition), 
                "leftovers"
            )
        )

    # ? Create logs folders
    logs_dir = os.path.join(
        test_dir_path,
        "run_" + str(current_repetition),
        "logs"
    )
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
        console.print(hostname + ": " +SETUP+ " Created logs folder.", style="bold white") if DEBUG_MODE else None

    # ? Create logs of the scripts
    scripts_filepath = os.path.join(
        test_dir_path,
        "run_" + str(current_repetition),
        "logs",
        host + "_scripts.log"
    )
    if scripts:
        with open(scripts_filepath, "w") as f:
            f.writelines(scripts)
    console.print(hostname + ": " +LOG+ " Created script logs", style="bold white") if DEBUG_MODE else None
    
    # ? Log system info
    log_system(hostname, ssh, scripts, test_name, home_dir)
    
    # ? Run the scripts
    if scripts is not None:
        console.print(hostname + ": " +EXEC+ " Running scripts.", style="bold white") if DEBUG_MODE else None
        stdout, stderr = run_scripts(ssh, machine, username, scripts, ssh_key, test_name)
    else:
        console.print(hostname + ": " +EXEC+ " Scripts are empty.", style="bold orange1") if DEBUG_MODE else None
        stdout = None
        stderr = None
        
    # ? Create logs of the test output
    # ? Currently commented out because it was producing logs around 10GB in size
    # stdout_log_filepath = os.path.join(
    #     test_dir_path,
    #     "run_" + str(current_repetition), 
    #     host + "_output.log"
    # )
    # if stdout is not None:
    #     with open(stdout_log_filepath, "w") as f:
    #         f.writelines(stdout)
    #     if DEBUG_MODE:
    #         console.print(hostname + ": Created stdout logs", style="bold white")
        
    # ? Create logs of the test error output
    stderr_log_filepath = os.path.join(
        test_dir_path,
        "run_" + str(current_repetition),
        "logs",
        host + "_error.log"
    )
    if stderr is not None:
        with open(stderr_log_filepath, "w") as f:
            f.writelines(stderr)
        if DEBUG_MODE:
            console.print(hostname + ": " +LOG+ " Created stderr logs", style="bold white")
        
    # ? Download the result files
    console.print(hostname + ": " +EXEC+ " Downloading csv files.", style="bold white") if DEBUG_MODE else None
    download_test_results(
        ssh, 
        hostname,
        host, 
        home_dir, 
        os.path.join(
            test_dir_path, 
            "run_" + str(current_repetition)
        )
    )
    
    # ? Download the log files
    try:
        download_logs(hostname, host, ssh, home_dir, os.path.join(test_dir_path, "run_" + str(current_repetition), "logs"))
    except paramiko.SSHException as e:
        console.print(hostname + ": " + ERROR + " Error when downloading the logs. Exception:\n\t[bold red]" + str(e) + "[/bold red]", style="bold white")
    
    console.print(hostname + ": " +EXEC+ " Finished running.", style="bold white") if DEBUG_MODE else None
 
def download_logs(hostname, host, ssh, home_dir, target_dir):
    console.print(hostname + ": " +LOG+ " Downloading logs.", style="bold white") if DEBUG_MODE else None
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        console.print(hostname + ": " +SETUP+ " Making logs dir.", style="bold white") if DEBUG_MODE else None

    with ssh.open_sftp() as sftp:
        all_files = sftp.listdir(home_dir)
        sar_logs = [file for file in all_files if "sar_logs" in file and "log" in file]
        
        if len(sar_logs) == 0:
            console.print(hostname + ": " +LOG+ " No logs found.", style="bold red") if DEBUG_MODE else None
        elif len(sar_logs) > 1:
            console.print(hostname + ": " +LOG+ " Multiple log files found.", style="bold red") if DEBUG_MODE else None
        else:
            sar_log = sar_logs[0]
            # ? Parse CPU stats
            stdin, stdout, stderr = ssh.exec_command("sar -f " + sar_log + " > cpu.log")
            if stdout.channel.recv_exit_status() == 0:
                console.print(hostname + ": " +LOG+ " CPU logs parsed.", style="bold white") if DEBUG_MODE else None
            else:
                console.print(hostname + ": " + LOG + " Error parsing the CPU logs.", style="bold red") if DEBUG_MODE else None
            
            # ? Parse memory stats
            stdin, stdout, stderr = ssh.exec_command("sar -r -f " + sar_log + " > mem.log")
            if stdout.channel.recv_exit_status() == 0:
                console.print(hostname + ": " +LOG+ " MEMORY logs parsed.", style="bold white") if DEBUG_MODE else None
            else:
                console.print(hostname + ": " + LOG + " Error parsing the MEMORY logs.", style="bold red") if DEBUG_MODE else None
            
            # ? Parse network stats
            network_options = ["DEV", "EDEV", "NFS", "NFSD", "SOCK", "IP", "EIP", "ICMP", "EICMP", "TCP", "ETCP", "UDP", "SOCK6", "IP6", "EIP6", "ICMP6", "EICMP6", "UDP6"]
            for option in network_options:
                stdin, stdout, stderr = ssh.exec_command("sar -n " +option+ " -f " + sar_log + " > " +option.lower()+ ".log")
                if stdout.channel.recv_exit_status() != 0:
                    console.print(hostname + ": " + LOG + " Error parsing the " +option+ " logs.", style="bold red") if DEBUG_MODE else None
            console.print(hostname + ": " + LOG + " NETWORK logs parsed.", style="bold white") if DEBUG_MODE else None
            
            # ? Delete the original unparsed sar log file - we no longer have a use for it
            sftp.remove(sar_log)
            
            expected_logs = ["cpu.log", "mem.log"] + [x.lower() + ".log" for x in network_options]
            found_logs = [x for x in sftp.listdir(home_dir) if '.log' in x]
            
            if len(expected_logs) != len(found_logs):
                console.print(hostname + ": " + LOG + " Mismatch found between expected and found logs.\n\t" + str(len(expected_logs)) + " expected logs.\n\t" + str(len(found_logs)) + " logs founds.", style="bold red")
                
                if len(expected_logs) > len(found_logs):
                    console.print(hostname + ": " + "You are missing the following logs:", style="bold red") if DEBUG_MODE else None
                    for log in list(set(expected_logs) - set(found_logs)):
                        console.print("\t" + log, style="bold red")
                else:
                    console.print(hostname + ": " + "You have the following extra logs:", style="bold red") if DEBUG_MODE else None
                    for log in list(set(found_logs) - set(expected_logs)):
                        console.print("\t" + log, style="bold red")
            else:
                for log in expected_logs:
                    sftp.get(log, os.path.join(target_dir, host + "_" + log))
                    sftp.remove(log)
                    
            leftover_logs = [x for x in sftp.listdir(home_dir) if '.log' in x]
            if len(leftover_logs) > 0:
                console.print(hostname + ": " + LOG + " Some logs were leftover.", style="bold red")
            
    console.print(hostname + ": " +LOG+ " End of log download.", style="bold white") if DEBUG_MODE else None
            
"""
? log_system(host, ssh, scripts) runs the sar command to log the system information throughout the duration of the test.

:param      host:       hostname of machine just for console output purposes
:param      ssh:        ssh object representing the ssh connection to eh machine
:param      scripts:    test scripts to extract the test duration from

:returns    none
"""
def log_system(hostname, ssh, scripts, testname, home_dir):
    if scripts:
        duration = get_duration_from_test_scripts(scripts)
    else:
        duration = 0
    
    if duration == 0:
        duration = get_duration_from_test_name(testname, hostname)
    
    duration = duration + FAIL_DURATION_S
    


    # ? Check for any leftover sar_logs files
    with ssh.open_sftp() as sftp:
        
        # ? Check if home_dir exists
        try:
            attrs = sftp.stat(home_dir)
            if not stat.S_ISDIR(attrs.st_mode):
                console.print(f"{hostname}: {home_dir} is not a directory.", style="bold red")
                sys.exit()
        except FileNotFoundError:
            console.print(f"{hostname}: {home_dir} does not exist.", style="bold red")
            sys.exit()

        all_files = sftp.listdir(home_dir)
        sar_logs = [file for file in all_files if "sar_logs" in file and "log" in file]
        
        if len(sar_logs) > 0:
            console.print(hostname + ": " +ERROR+ " Leftover sar_logs found.", style="bold red") if DEBUG_MODE else None
    
    # ? Start logging and output to a file
    try:
        # console.print(f"{host}: {EXEC} SAR COMMAND: sar -A -o sar_logs 1 {str(duration)}", style="bold green")
        stdin, stdout, stderr = ssh.exec_command("sar -A -o sar_logs 1 " + str(duration) + " >/dev/null 2>&1 &")
    except Exception as e:
        console.print(hostname + ": " + ERROR + " Error when logging system stats. Exception:\n\t[bold red]" + str(e) + "[/bold red]", style="bold white")
    
    console.print(hostname + ": " +LOG+ " Logs started.", style="bold white") if DEBUG_MODE else None

def get_duration_from_test_name(testname, hostname):
    # ? Look for x numeric digits followed by "s_"
    durations_from_name = re.findall(r'\d*s_', testname)
    
    if len(durations_from_name) == 0:
        console.print(f"{hostname}: {ERROR} Couldn't get duration from scripts or test name for {testname}", style="bold white")
        return 0
    
    duration_from_name = durations_from_name[0]

    duration_from_name = duration_from_name.replace("s_", "")
    
    duration = int(duration_from_name)
    
    return duration

"""
? get_duration_from_test_scripts(scripts) reads the scripts and extracts the -executionTime value

:param      scripts:    The test scripts.

:returns    duration:   The test duration.
"""
def get_duration_from_test_scripts(scripts):
    if "-executionTime" in scripts:
        return int(scripts.split("-executionTime")[1].split("-")[0])
    else:
        return 0

def has_leftovers(ssh, hostname, home_dir):
    with ssh.open_sftp() as sftp:

        # ? Check if home_dir exists
        try:
            attrs = sftp.stat(home_dir)
            if not stat.S_ISDIR(attrs.st_mode):
                console.print(f"{hostname}: {home_dir} is not a directory.", style="bold red")
                sys.exit()
        except FileNotFoundError:
            console.print(f"{hostname}: {home_dir} does not exist.", style="bold red")
            sys.exit()
        
        csv_files = [file for file in sftp.listdir(home_dir) if fnmatch.fnmatch(file, "*.csv")]
        log_files = [file for file in sftp.listdir(home_dir) if fnmatch.fnmatch(file, "*.log")]
        if len(csv_files) > 0 or len(log_files) > 0:
            return True
        else:
            return False
 
"""
? download_test_results(ssh, host, home_dir, target_dir) downloads the files from the server to the client.

:param      ssh:        ssh object representing the ssh connection to the machine
:param      host:       host name of the machine
:param      home_dir:   destination dir of the server
:param      target_dir: destination dir of the client

:returns    none
"""       
def download_test_results(ssh, hostname, host, home_dir, target_dir):
    try:
        with ssh.open_sftp() as sftp:
            for filename in sftp.listdir(home_dir):
                if fnmatch.fnmatch(filename, "*.csv") or fnmatch.fnmatch(filename, "*log"):
                    sftp.get(filename, os.path.join(target_dir, filename))
                    sftp.remove(filename)
    
                    console.print(hostname + ": " +EXEC+ " Downloaded " + filename, style="bold white") if DEBUG_MODE else None
    except Exception as e:
            console.print(hostname + ": " + ERROR + " Error while downloading the test results. Exception:\n\t[bold red]" + str(e) + "[/bold red]", style="bold white")
        
"""
? check_machine_online(ssh, host, username, ssh_key) sends periodic pings to the machine until it responds.

:param      ssh:        ssh object representing the ssh connection
:param      host:       host name of the machine
:param      username:   username used to connect to machine
:param      ssh_key:    ssh_key location to connect to machine

:returns    none
"""
def check_machine_online(ssh, host, username, ssh_key, timeout):
    timer = 0
    while timer < timeout:
        try:
            k = paramiko.RSAKey.from_private_key_file(ssh_key)
            ssh.connect(host, username=username, pkey = k)
            break
        except Exception as e:
            # console.print("[red]Error connecting to " + host + ". Reconnecting...[/red]", style=output_colour)
            # console.print(e, style="bold yellow")
            time.sleep(1)
            timer += 1

    if timer == timeout:
        console.print(f"{PING}: Timeout after {timeout} seconds when pinging {host}.")
        sys.exit(0)
            
"""
? restart_machine(ssh, host, username, ssh_key) restarts the machine.

:param      ssh:        ssh connection object
:param      host:       host name of machine
:param      username:   username to connect to machine
:param      ssh_key:    ssh_key location to connect to machine

:returns    none
"""
def restart_machine(ssh, host, username, ssh_key):
    while True:
        try:
            k = paramiko.RSAKey.from_private_key_file(ssh_key)
            ssh.connect(host, username=username, pkey = k)
            ssh.exec_command("sudo reboot")
            time.sleep(3)
            break
        except Exception as e:
            # console.print("[red]Error connecting to " + host + ". Reconnecting...[/red]", style=output_colour)
            # console.print(e, style="bold red")
            time.sleep(1)
            
"""
? run_scripts(ssh, machine, username, scripts, ssh_key) executes the scripts on the machine.

:param      ssh:        ssh communication object
:param      machine:    machine details from config file
:param      username:   username to connect to machine
:param      scripts:    scripts to run on the machine
:param      ssh_key:    ssh_key location to connect to machine

:returns    stdout:     standard output resulting from the script execution
:returns    stderr:     standard error output from the script execution
"""
def run_scripts(ssh, machine, username, scripts, ssh_key, test_name):
    try:
        k = paramiko.RSAKey.from_private_key_file(ssh_key)
        ssh.connect(machine["host"], username=username, pkey = k)
        stdin, stdout, stderr = ssh.exec_command("source ~/.bash_profile;" + scripts)
    except Exception as e:
        console.print(machine["host"] + ": " + ERROR + " Error trying to execute the scripts. Exception:\n\t[bold red]" + str(e) + "[/bold red]", style="bold white")
        return None, str(e)

    return stdout, stderr