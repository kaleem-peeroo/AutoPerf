import warnings
from cryptography.utils import CryptographyDeprecationWarning
with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
    import paramiko
    
import json
import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
import time
import os
import re
import subprocess
import sys

from datetime import datetime
from pprint import pprint
from rich.bar import Bar
from rich.console import Console
from rich.progress import track
from rich.table import Table
from rich.markdown import Markdown

console = Console()

ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
# ? To use the above: ansi_escape.sub("", string)

machines = [
    {
        "name": "3Pi",
        "ip": "10.210.35.27",
        "ptstdir": "/home/acwh025/Documents/PTST",
        "ssh_key": "/Users/kaleem/.ssh/id_rsa",
        "errors": [],
        "current_campaign": {
            "tests": [],
            "last_test": {
                "name": "",
                "duration": "",
                "time_elapsed": "",
            },
            "name": "",
            "total_tests": 0,
            "completed_tests": 0,
            "punctual_tests": 0,
            "usable_tests": 0,
            "prolonged_tests": 0,
            "unreachable_tests": 0,
        }
    },
    {
        "name": "5Pi",
        "ip": "10.210.58.126",
        "ptstdir": "/home/acwh025/Documents/PTST",
        "ssh_key": "/Users/kaleem/.ssh/id_rsa",
        "errors": [],
        "current_campaign": {
            "tests": [],
            "last_test": {
                "name": "",
                "duration": "",
                "time_elapsed": "",
            },
            "name": "",
            "total_tests": 0,
            "completed_tests": 0,
            "punctual_tests": 0,
            "usable_tests": 0,
            "prolonged_tests": 0,
            "unreachable_tests": 0,
        }
    }
]

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
k = paramiko.RSAKey.from_private_key_file("/Users/kaleem/.ssh/id_rsa")

def format_duration(duration, units):
    days = duration // (24 * 3600)
    duration = duration % (24 * 3600)
    hours = duration // 3600
    duration %= 3600
    minutes = duration // 60
    duration %= 60
    seconds = duration

    days = int(days)
    hours = int(hours)
    minutes = int(minutes)
    seconds = int(seconds)

    days_str = f"{days} Days, " if days > 0 else ""
    hours_str = f"{hours} Hrs, " if hours > 0 else ""
    minutes_str = f"{minutes} Mins, " if minutes > 0 else ""
    seconds_str = f"{seconds} Secs" if seconds > 0 else ""
    
    output_str = ""
    if "d" in units:
        output_str += days_str
    if "h" in units:
        output_str += hours_str
    if "m" in units:
        output_str += minutes_str
    if "s" in units:
        output_str += seconds_str

    return output_str

def get_punctual_tests(tests):
    punctual_tests = []
    for test in tests:
        all_punctual = True
        for machine_status in test['machine_statuses']:
            if machine_status['status'] != 'punctual':
                all_punctual = False
                break
        if all_punctual:
            punctual_tests.append(test)
    return punctual_tests

def get_prolonged_tests(tests):
    prolonged_tests = []
    for test in tests:
        has_prolonged = False
        for machine_status in test['machine_statuses']:
            if machine_status['status'] == 'prolonged':
                has_prolonged = True
                break
        if has_prolonged:
            prolonged_tests.append(test)
    return prolonged_tests

def get_unreachable_tests(tests):
    unreachable_tests = []
    for test in tests:
        has_unreachable = False
        for machine_status in test['machine_statuses']:
            if machine_status['status'] == 'unreachable':
                has_unreachable = True
                break
        if has_unreachable:
            unreachable_tests.append(test)
    return unreachable_tests

def get_missing_files_tests(tests):
    missing_files_tests = []
    for test in tests:
        has_missing_files = False
        for machine_status in test['machine_statuses']:
            if "no csv files" in machine_status['status']:
                has_missing_files = True
                break
        if has_missing_files:
            missing_files_tests.append(test)
    return missing_files_tests

def check(machine):
    host = machine['ip']
    key_path = machine['ssh_key']
    ptstdir = machine['ptstdir']
    
    
    ssh.connect(host, username="acwh025", pkey = k, banner_timeout=120)

    # ? Check if ptstdir is valid.
    sftp = ssh.open_sftp()
    try:
        remote_files = sftp.listdir(ptstdir)
    except Exception as e:
        console.log(f"Exception when getting remote_files: \n\t{e}", style="bold red")
        machine['errors'].append(f"Exception when getting remote_files: {e}")
        return machine

    remote_jsons = [file for file in remote_files if file.endswith(".json")]

    if len(remote_jsons) == 0:
        console.print(Markdown("# No tests in progress."), style="bold red")
        machine["errors"].append("No tests in progress.")
        return machine
        
    # ? Get the latest json file that was last edited.
    latest_mtime = None
    latest_json = None
    for file in remote_jsons:
        file_path = os.path.join(ptstdir, file)
        mtime = sftp.stat(file_path).st_mtime
        if latest_mtime is None or mtime > latest_mtime:
            latest_mtime = mtime
            latest_file = file_path

    if latest_file is None:
        console.print(Markdown("# No tests in progress."), style="bold red")
        machine["errors"].append("No tests in progress.")
        return machine

    current_campaign_name = os.path.basename(latest_file).replace(".json", "").replace("_", " ").replace("statuses", "").strip()
    machine['current_campaign']['name'] = current_campaign_name
    
    # ? Get the contents of the latest json file.
    with sftp.open(latest_file, "r") as f:
        latest_json = f.read().decode("utf-8")
    latest_json = ", ".join(latest_json.split(",")[:-1]) + "]"
    try:
        latest_json = json.loads(latest_json)
    except json.decoder.JSONDecodeError as e:
        console.print(f"# The first test of {current_campaign_name} is still running. Please wait until it finishes.", style="bold red")
        machine["errors"].append(f"The first test of {current_campaign_name} is still running. Please wait until it finishes.")
        return machine
    
    tests = latest_json
    
    for test in tests:
        test['index'] = tests.index(test)

    machine['current_campaign']['tests'] = tests
    
    machine['current_campaign']['completed_tests'] = len(tests)
    
    punctual_test_count = len(get_punctual_tests(tests))
    prolonged_test_count = len(get_prolonged_tests(tests))
    unreachable_test_count = len(get_unreachable_tests(tests))
    missing_files_test_count = len(get_missing_files_tests(tests))
    
    machine['current_campaign']['punctual_tests'] = punctual_test_count
    machine['current_campaign']['prolonged_tests'] = prolonged_test_count
    machine['current_campaign']['unreachable_tests'] = unreachable_test_count
    machine['current_campaign']['missing_files_tests'] = missing_files_test_count
    
    # ? Check how long ago the last test ended.
    test_name = tests[-1]['permutation_name']
    end_time = tests[-1]['end_time']
    last_end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    current_time = datetime.now()
    time_elapsed = current_time - last_end_time
    time_elapsed_secs = time_elapsed.total_seconds()
    time_elapsed = format_duration(time_elapsed_secs, "dhms")

    # ? Get the duration from the test name
    match = re.search(r'(\d+)SEC', test_name)
    if match:
        test_duration_secs = int(match.group(1))
    else:
        test_duration_secs = 86400 / 2
    
    machine['current_campaign']['last_test']['name'] = test_name
    machine['current_campaign']['last_test']['duration'] = test_duration_secs
    machine['current_campaign']['last_test']['time_elapsed_seconds'] = time_elapsed_secs
    
    ssh.close()
    
    return machine
    
def get_tr_for_test(test, tests):
    index = tests.index(test) + 1
    test_name = test["permutation_name"]
    start_time = test["start_time"]
    split_start_time = start_time.split(" ")
    end_time = test["end_time"]
    split_end_time = end_time.split(" ")
    duration = format_duration(test["duration_s"], "hms")

    raw_statuses = sorted([f"{machine['name']}: {machine['status']}" for machine in test['machine_statuses']], key=lambda x: x.split(":")[0])
    raw_pings = sorted([f"{machine['name']}: {machine['pings']}" for machine in test['machine_statuses']], key=lambda x: x.split(":")[0])
    raw_ssh_pings = sorted([f"{machine['name']}: {machine['ssh_pings']}" for machine in test['machine_statuses']], key=lambda x: x.split(":")[0])
    
    status_elements = []
    for status in raw_statuses:
        if "punctual" in status:
            color = "#28a745"
        elif "prolonged" in status or "no csv file" in status:
            color = "#ffc107"
        elif "unreachable" in status:
            color = "#dc3545"
        elif "no csv files" in status:
            color = "#6c757d"

        status_elements.append(html.P(status, style={"font-family": "monospace", "color": color}))
    
    color = "#28a745"  # default to green

    for status in raw_statuses:
        if "unreachable" in status:
            color = "#dc3545"  # set to red
            break  # no need to check further
        elif "prolonged" in status or "no csv files" in status:
            color = "#ffc107"  # set to orange
    
    ping_elements = [html.P(ping, style={"font-family": "monospace"}) for ping in raw_pings]
    ssh_ping_elements = [html.P(ssh_ping, style={"font-family": "monospace"}) for ssh_ping in raw_ssh_pings]
    
    tr = html.Tr([
        html.Td(index),
        html.Td(test_name[:25] + "...", id=test_name, style={"color": color}),
        dbc.Tooltip(test_name, target=test_name),
        html.Td([html.P(split_start_time[0]), html.P(split_start_time[1])], style={"font-family": "monospace"}),
        html.Td([html.P(split_end_time[0]), html.P(split_end_time[1])], style={"font-family": "monospace"}),
        html.Td(duration),
        html.Td(ping_elements),
        html.Td(ssh_ping_elements),
        html.Td(status_elements)
    ], style={'font-size': "0.8rem"})
    
    return tr
  
def get_col(machine):
    time_since_last_test = machine['current_campaign']['last_test']['time_elapsed_seconds']
    last_test_duration = machine['current_campaign']['last_test']['duration']
    
    if time_since_last_test > last_test_duration * 2:
        last_test_color = "danger"
    else:
        last_test_color = "success"
    
    time_since_last_test = format_duration(machine['current_campaign']['last_test']['time_elapsed_seconds'], "dhms")
    
    recent_tests = machine['current_campaign']['tests'][-100:]
    recent_tests = sorted(recent_tests, key=lambda x: x['index'], reverse=True)
    recent_tests = [get_tr_for_test(test, machine['current_campaign']['tests']) for test in recent_tests]
    
    last_test_alert = dbc.Alert([
        html.Div([
            "Last test finished ", 
            html.Strong(f"{time_since_last_test} ago."),
        ]),
    ], color=last_test_color, style={"margin-bottom": "0.5vh", "display": "flex", "justify-content": "space-between", "align-items": "center"})
    
    return dbc.Col([
        dbc.Card([
            dbc.CardHeader([
                html.H5(machine['name'], style={'font-family': 'monospace'}),
                html.Span(id="campaign-name-target", children=[machine['current_campaign']['name']], style={'font-family': "monospace"}),
                dbc.Tooltip("Current Campaign Running", target="campaign-name-target"),
                dcc.Loading(
                    id="loading-icon",
                    children=[
                        html.Div([
                            html.I(className="fa fa-cog fa-spin", style={"margin-right": "0.5vw"}),
                            html.Span(machine['current_campaign']['completed_tests'])
                        ], style={"text-align": "center", "color": "#28a745"})
                    ],
                    type="circle",
                ),
            ], style={"display": "flex", "justify-content": "space-between", "align-items": "center"}),
            dbc.CardBody([
                dbc.Alert("".join(f"{machine['name']}: {machine['errors']}"), color="danger", dismissable=True, style={"margin-bottom": "0.5vh"}) if len(machine['errors']) > 0 else None,
                last_test_alert,
                html.Div([
                    html.Div([
                        html.I(className="far fa-clock", style={"margin-right": "0.5vw"}),
                        html.Span(machine['current_campaign']["punctual_tests"], id="punctual-target"),
                        dbc.Tooltip("Punctual tests", target="punctual-target")
                    ], style={"color": "#007bff"}),
                    html.Div([
                        html.I(className="fas fa-hourglass-end", style={"margin-right": "0.5vw"}),
                        html.Span(machine['current_campaign']["missing_files_tests"], id="missing-files-target"),
                        dbc.Tooltip("Missing Files tests", target="missing-files-target")
                    ], style={"color": "#6c757d"}),
                    html.Div([
                        html.I(className="fas fa-hourglass-end", style={"margin-right": "0.5vw"}),
                        html.Span(machine['current_campaign']["prolonged_tests"], id="prolonged-target"),
                        dbc.Tooltip("Prolonged tests", target="prolonged-target")
                    ], style={"color": "#ffc107"}),
                    html.Div([
                        html.I(className="fas fa-times", style={"margin-right": "0.5vw"}),
                        html.Span(machine['current_campaign']["unreachable_tests"], id="unreachable-target"),
                        dbc.Tooltip("Unreachable tests", target="unreachable-target")
                    ], style={"color": "#dc3545"})
                ], style={"display": "flex", "justify-content": "space-between", "align-items": "center", "margin": "1vh 0"}),
                html.Div([
                    # html.H3("Last 25 Tests"),
                    dbc.Table([
                        html.Thead([
                            html.Th("#"),
                            html.Th("Test"),
                            html.Th("Start"),
                            html.Th("End"),
                            html.Th("Duration"),
                            html.Th("Pings"),
                            html.Th("SSH Pings"),
                            html.Th("Statuses"),
                        ]),
                        html.Tbody(recent_tests if recent_tests else html.Tr([html.Td("No tests yet")]))
                    ], bordered=True, hover=True, responsive=False)
                ])
            ], style={"overflow-x": "scroll"})
        ])
    ], width=6)
    
for machine in machines:
    machine = check(machine)
    
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME])

@app.callback(dash.dependencies.Output('root', 'children'),
              dash.dependencies.Input('interval-component', 'n_intervals'))
def update_metrics(n):
    output = []
    for machine in machines:
        machine = check(machine)
        col = get_col(machine)
        output.append(col)
    
    return output

@app.callback(
    dash.dependencies.Output('restart-loading-output', 'children'),
    dash.dependencies.Input('force-restart-dropdown', 'value')
)
def click_force_restart(machine_name):
    if machine_name not in [machine['name'] for machine in machines]:
        return None
    
    success_output = [
        html.I(className="fas fa-check", style={"margin": "0 0.5vw 0 0 ", "color": "#f0fff0"}), 
        html.Span(f"{machine_name} restarted.", style={"color": "#f0fff0"})
    ]
    output = success_output
    
    # ? Get machine with same name
    machine = [machine for machine in machines if machine['name'] == machine_name][0]
    ip = machine['ip']
    
    try:
        ssh.connect(ip, username="acwh025", pkey = k, banner_timeout=120)
    
        # Resume screen
        stdin, stdout, stderr = ssh.exec_command('screen -r', get_pty=True)

        time.sleep(1)
        print("terminating current process")
        # Terminate current process
        stdin.write('\x03')  # Send ctrl+c
        stdin.flush()
        time.sleep(1)
        stdin.write('exit\n')  # Send exit command
        stdin.flush()
        time.sleep(1)
        print("rerunning last command")
        # Rerun last command
        stdin.write('!!\n')
        stdin.flush()
        time.sleep(1)

        terminal_output = stdout.read().decode('utf-8')

        print("leaving screen")
        # Leave screen
        stdin.write('\x01\x04')  # Send ctrl+a+d
        stdin.flush()
        time.sleep(1)
        
        print("all done")
        ssh.close()
    except Exception as e:
        output = [
            html.I(className="fas fa-times", style={"margin": "0 0.5vw 0 0 ", "color": "#f0fff0"}),
            html.Span(f"{machine_name} failed to restart: {e}", style={"color": "#f0fff0"})
        ]
    
    return output

if __name__ == '__main__':
    app.layout = dbc.Container([
        html.Div([
            html.P("PTST Monitor", style={"font-size": "2rem", "font-weight": "bold", "color": "white"}),
            html.Div([
                html.Span("Force Restart: ", style={"margin-right": "1vw"}),
                dcc.Dropdown(
                    options=[machine['name'] for machine in machines], 
                    placeholder='Force Restart',
                    id="force-restart-dropdown", 
                    style={"max-width": "25vw", "min-width": "15vw", "color": "black", "margin-right": "3vw"}
                ),
                dcc.Loading(
                    id="restart-loading", 
                    children=[html.Div(id="restart-loading-output")], 
                    style={"width": "fit-content", "margin-right": "3vw"},
                    # graph, cube, circle, dot
                    type="dot",
                    color="white"
                ),
            ], style={"display": "flex", "justify-content": "space-between", "align-items": "center"}),
        ], style={
            "width": "100vw", 
            "height": "10vh", 
            "display": "flex", 
            "justify-content": "space-between", 
            "align-items": "center",
            "padding": "0 2vw",
            "background-color": "#1E90FF",
            "color": "white"
        }
        ),
        dbc.Row(id="root", style={"padding": "2vh 2vw"}),
        dcc.Interval(
            id='interval-component',
            interval=30*1000, # in milliseconds
            n_intervals=0
        )
    ], fluid=True, style={"padding": "0"})
    
    app.run_server(debug=True, port=8051, host="127.0.0.1")