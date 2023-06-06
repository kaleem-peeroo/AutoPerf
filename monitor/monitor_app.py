from pprint import pprint
from rich.console import Console
import time
import sys
import paramiko
import json
import os
import dash
import dash_bootstrap_components as dbc
from dash import dcc
from datetime import datetime
from dash import html
from dash.dependencies import Output, Input

console = Console()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
k = paramiko.RSAKey.from_private_key_file("/Users/kaleem/.ssh/id_rsa")

def validate_args():
    erorr_message = ""
    args = sys.argv[1:]
    
    if len(args) == 0:
        error_message = "No arguments were given. You need to pass in the path to the config file containing the controllers you want to monitor."
        console.print(error_message, style="bold red")
        return html.Div(
            [
                dbc.Alert(
                    error_message ,
                    color="danger",
                    dismissable=True,
                    is_open=True,
                )
            ]
        )
    elif not os.path.isfile(args[0]):
        error_message = f"The argument passed in ({args[0]}) is not a file or does not exist."
        console.print(error_message, style="bold red")
        return html.Div(
            [
                dbc.Alert(
                    error_message ,
                    color="danger",
                    dismissable=True,
                    is_open=True,
                )
            ]
        )
    else:
        return ""

def get_controllers():
    config = sys.argv[1]
    
    if not os.path.isfile(config):
        console.print(f"The argument passed in ({config}) is not a file or does not exist.", style="bold red")
        sys.exit()
    
    with open(config, "r") as f:
        try:
            controllers = json.load(f)
        except json.JSONDecodeError as e:
            console.print(f"There was an error decoding the JSON file: {e}", style="bold red")
            sys.exit()
    
    return controllers

def generate_error_message(error):
    return html.Div(dbc.Alert(
        error,
        color="danger",
        dismissable=True,
        is_open=True,
    ))

@app.callback(
    Output('root', 'children'),
    Input('controllers-dropdown', 'value')
)
def get_controller_status(controller_ip):
    status = {}
    # ? Get the controller with the given IP.
    try:
        controller = [controller for controller in get_controllers() if controller['ip'] == controller_ip][0]
    except IndexError:
        return generate_error_message(f"Couldn't find any controller with the IP {controller_ip}.")
        
    ip = controller['ip']
    name = controller['name']
    ptstdir = controller['ptstdir']
    
    ssh.connect(ip, username="acwh025", pkey = k, banner_timeout=120)
    
    # ? Check if ptstdir is valid.
    sftp = ssh.open_sftp()
    try:
        remote_files = sftp.listdir(ptstdir)
    except Exception as e:
        console.log(f"Exception when getting remote_files: \n\t{e}", style="bold red")
        return generate_error_message(f"Couldn't find the directory {ptstdir} on the controller {name} ({ip}): {e}")
    
    remote_jsons = [file for file in remote_files if file.endswith(".json")]

    if len(remote_jsons) == 0:
        console.print(Markdown("# No tests in progress."), style="bold red")
        return generate_error_message(f"No tests in progress on the controller {name} ({ip}).")
    
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
        return generate_error_message(f"No tests in progress on the controller {name} ({ip}).")
    
    current_campaign_name = os.path.basename(latest_file).replace(".json", "").replace("_", " ").replace("statuses", "").strip()
    status['current_campaign_name'] = current_campaign_name
    
    # ? Get the contents of the latest json file.
    with sftp.open(latest_file, "r") as f:
        latest_json = f.read().decode("utf-8")
    latest_json = ", ".join(latest_json.split(",")[:-1]) + "]"
    try:
        latest_json = json.loads(latest_json)
    except json.decoder.JSONDecodeError as e:
        console.print(f"# The first test of {current_campaign_name} is still running. Please wait until it finishes.", style="bold red")
        return generate_error_message(f"The first test of {current_campaign_name} is still running. Please wait until it finishes.")
    
    tests = latest_json
    sorted_tests = sorted(tests, key=lambda x: datetime.strptime(x['end_time'], '%Y-%m-%d %H:%M:%S'), reverse=True)
    
    for test in tests:
        test['index'] = tests.index(test)
        
    status['tests'] = tests
    
    test_trs = []
    
    for test in sorted_tests:
        # ? Get all statuses for this test.
        statuses = []
        for machine_status in test['machine_statuses']:
            statuses.append(machine_status['status'])
        statuses = ", ".join(statuses)
        
        if "prolonged" in statuses.lower():
            row_color = "#FFA500"
        elif "unreachable" in statuses.lower():
            row_color = "#dc3545"
        elif "punctual" in statuses.lower():
            row_color = "#28a745"
        else:
            row_color = "#6c757d"
        
        test_tr = html.Tr([
            html.Td(test['index'], style={"color": row_color}),
            html.Td(test['permutation_name'], style={"color": row_color}),
            html.Td(test['start_time'], style={"color": row_color}),
            html.Td(test['end_time'], style={"color": row_color}),
            html.Td(test['duration_s'], style={"color": row_color}),
            html.Td(
                html.Ul([
                    html.Li(
                        [
                            html.Span(
                                machine_status['name'],
                                style={"font-weight": "bold"}
                            ),
                            html.Span(
                                f"- {machine_status['status']}",
                                style={"margin-left": "1vw"}
                            )
                        ]
                    )
                    for machine_status in test['machine_statuses']
                ]), style={"color": row_color}
            )
        ])
        test_trs.append(test_tr)
    
    table = dbc.Table(striped=True, bordered=True, hover=True, responsive=True, 
        children=[
            html.Thead(
                html.Tr([
                    html.Th("#"),
                    html.Th("Test"),
                    html.Th("Start"),
                    html.Th("End"),
                    html.Th("Duration (s)"),
                    html.Th("Statuses")
                ])
            ),
            html.Tbody(test_trs)
        ]
    )

    return html.Div(
        [
            table
        ],
        style={
            "padding": "2vw 2vh"
        }
    )

dropdown_options = [{'label': controller['name'], 'value': controller['ip']} for controller in get_controllers()]

navbar = dbc.NavbarSimple(
    children=[
        dcc.Dropdown(
            options=dropdown_options,
            placeholder="Controllers",
            id='controllers-dropdown', 
            style={"min-width": "10vw"},
            # Temporarily set the value to the first controller in the list.
            # TODO: Delete when done testing.
            value="10.210.35.27"
        ),
    ],
    brand="PTST Monitor",
    brand_href="#",
    color="primary",
    dark=True,
    sticky="top",
)

app.layout = html.Div(children=[
    dbc.Row([
       dbc.Col(
            navbar
       )
    ]),
    html.Div(children=validate_args()),
    dbc.Row(dbc.Col(
        dcc.Loading(
            id="root", 
            type="default", 
            color="black",
            style={
                "display": "flex",
                "justify-content": "center",
                "align-items": "center",
                "height": "100vh",
                "width": "100vw"
            }
        )
    ), style={"display": "flex", "justify-content": "center", "align-items": "center", "height": "100vh", "width": "100vw"})
], style={"padding": "0", "margin": "0"})

if __name__ == '__main__':
    app.run_server(debug=True)