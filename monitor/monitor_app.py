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

app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME])

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

def generate_machine_status_cards(statuses):
    """
    csv_count, log_count, time_to_script_s, script_exec_s, post_script_time_s, status, name, pings, ssh_pings
    """
    
    cards = []
    
    statuses = sorted(statuses, key=lambda x: x['name'])
    
    for status in statuses:
        if not isinstance(status, dict):
            console.print(f"Status is not a dict: {status}", style="bold red")
            return ""
        
        machine_name = status['name']
        machine_status = status['status']
        pings = status['pings']
        ssh_pings = status['ssh_pings']
        try:
            log_count = status['log_count']
        except KeyError:
            log_count = 0
        try:
            csv_count = status['csv_count']
        except KeyError:
            csv_count = 0
        try:
            time_to_scripts_s = int(status['time_to_scripts_s'])
        except KeyError:
            time_to_scripts_s = 0
        try:
            script_exec_s = int(status['script_exec_s'])
        except KeyError:
            script_exec_s = 0
        try:
            post_script_time_s = int(status['post_script_time_s'])
        except KeyError:
            post_script_time_s = 0
    
        if "no csv" in machine_status.lower():
            status_color = "#ffc107"
            bootstrap_color = "#ffc107"
        elif "punctual" in machine_status.lower():
            status_color = "#28a745"
            bootstrap_color = "#28a745"
        elif "prolonged" in machine_status.lower():
            status_color = "#ffc107"
            bootstrap_color = "#ffc107"
        elif "unreachable" in machine_status.lower():
            status_color = "#dc3545"
            bootstrap_color = "#dc3545"
        else:
            status_color = "white"
            bootstrap_color = "white"
    
        total_duration_s = time_to_scripts_s + script_exec_s + post_script_time_s
        
        if total_duration_s == 0:
            duration_bar = None
        else:
            duration_bar = dbc.Progress(
                [
                    dbc.Progress(value=int(time_to_scripts_s), label=int(time_to_scripts_s), color="info", striped=True, bar=True, max=total_duration_s, min=0),
                    dbc.Progress(value=int(script_exec_s), label=int(script_exec_s), color="warning", striped=True, bar=True, max=total_duration_s, min=0),
                    dbc.Progress(value=int(post_script_time_s), label=int(post_script_time_s), color="success", striped=True, bar=True, max=total_duration_s, min=0),
                ]
            )

        file_count = html.Div([
            html.Span(f"CSVs: {csv_count}"),
            html.Span(f"LOGs: {log_count}"),
            html.Span(f"Pings: {pings}"),
            html.Span(f"SSH Pings: {ssh_pings}"),
        ], style={"margin-top": "1vh", "display": "flex", "justify-content": "space-between", "align-items": "center", "color": "white"})

        card = dbc.Card([
            dbc.CardHeader(
                [
                  html.Span(f"{machine_name} Status", style={"max-width": "50%", "color": "white"}),
                  html.Span(f"{machine_status}", style={"max-width": "50%", "font-family": "monospace", "color": status_color, "background-color": "#444", "padding": "0 1vw"}),
                ],
                style={"font-family": "monospace", "display": "flex", "justify-content": "space-between", "align-items": "center"}
            ),
            dbc.CardBody([
                duration_bar, 
                file_count
            ])
        ], color=bootstrap_color, outline=True, style={"width": "100%", "margin-bottom": "1vh"})
    
        cards.append(card)

    cards_container = html.Div(cards, style={"display": "flex", "justify-content": "space-between", "flex-wrap": "wrap"})
    
    return cards_container

@app.callback(
    Output('root', 'children'),
    Input('controllers-dropdown', 'value')
)
def get_controller_status(controller_ip):
    if controller_ip is None:
        return html.Div([
            html.Img(
                src="https://img.freepik.com/free-vector/thoughtful-woman-with-laptop-looking-big-question-mark_1150-39362.jpg?w=1380&t=st=1686135280~exp=1686135880~hmac=1c6f47e3cc0639fa06b4dcd83623296665f722d1ec800ac751e8906e950d650d",
                style={"width": "25vw", "height": "auto"}
            ),
            html.P("Select a controller from the dropdown above to get started.", style={"color": "#666"})
        ], style={"display": "flex", "justify-content": "center", "align-items": "center", "height": "100vh", "flex-direction": "column"})
    
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
    
    for test in sorted_tests:
        test['index'] = tests.index(test)
        
    status['tests'] = sorted_tests
    punctual_tests = []
    prolonged_tests = []
    unreachable_tests = []
    
    for test in sorted_tests:
        statuses = []
        for machine_status in test['machine_statuses']:
            statuses.append(machine_status['status'])
        statuses = ", ".join(statuses)
        
        if "prolonged" in statuses.lower():
            prolonged_tests.append(test['permutation_name'])
        elif "unreachable" in statuses.lower():
            unreachable_tests.append(test['permutation_name'])
        elif "punctual" in statuses.lower():
            punctual_tests.append(test['permutation_name'])

    punctual_tests.reverse()
    prolonged_tests.reverse()
    unreachable_tests.reverse()

    test_types_container = dbc.CardBody([
        html.Span(f"Total: {len(sorted_tests)}", style={"color": "#007bff"}),
        html.Span(f"Punctual: {punctual_tests}", style={"color": "#28a745"}),
        html.Span(f"Prolonged: {prolonged_tests}", style={"color": "#ffc107"}),
        html.Span(f"Unreachable: {unreachable_tests}", style={"color": "#dc3545"})
    ], style={"display": "flex", "justify-content": "space-between", "width": "100%", "margin-top": "-3vh"})

    test_types_container = html.Div([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dbc.Accordion([
                            dbc.AccordionItem([
                                html.Div([
                                    dcc.Textarea(
                                        id="textarea",
                                        value="\n".join([test['permutation_name'] for test in tests]),
                                        style={"font-size": "0.8em", "min-height": "70vh", "min-width": "90%"}
                                    ),
                                    dcc.Clipboard(
                                        target_id="textarea",
                                        title="Copy to clipboard",
                                        style={"display": "block", "margin-left": "1vw"}
                                    )
                                ], style={"display": "flex", "justify-content": "space-between", "align-items": "top", "width": "100%"})
                            ], title=f"{len(tests)} Total Tests", style={"color": "#007bff"})
                        ], start_collapsed=True, style={"border": "2px solid #007bff", "border-radius": "3px"})
                    ])
                ], style={"border": "none", "box-shadow": "none"})
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dbc.Accordion([
                            dbc.AccordionItem([
                                html.Div([
                                    dcc.Textarea(
                                        id="textarea",
                                        value="\n".join(punctual_tests),
                                        style={"font-size": "0.8em", "min-height": "70vh", "min-width": "90%"}
                                    ),
                                    dcc.Clipboard(
                                        target_id="textarea",
                                        title="Copy to clipboard",
                                        style={"display": "block", "margin-left": "1vw"}
                                    )
                                ], style={"display": "flex", "justify-content": "space-between", "align-items": "top", "width": "100%"})
                            ], title=f"{len(punctual_tests)} Punctual Tests", style={"color": "#28a745"})
                        ], start_collapsed=True, style={"border": "2px solid #28a745", "border-radius": "3px"})
                    ])
                ], style={"border": "none", "box-shadow": "none"})
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dbc.Accordion([
                            dbc.AccordionItem([
                                html.Div([
                                    dcc.Textarea(
                                        id="textarea",
                                        value="\n".join(prolonged_tests),
                                        style={"font-size": "0.8em", "min-height": "70vh", "min-width": "90%"}
                                    ),
                                    dcc.Clipboard(
                                        target_id="textarea",
                                        title="Copy to clipboard",
                                        style={"display": "block", "margin-left": "1vw"}
                                    )
                                ], style={"display": "flex", "justify-content": "space-between", "align-items": "top", "width": "100%"})
                            ], title=f"{len(prolonged_tests)} Prolonged Tests", style={"color": "#ffc107"})
                        ], start_collapsed=True, style={"border": "2px solid #ffc107", "border-radius": "3px"})
                    ])
                ], style={"border": "none", "box-shadow": "none"})
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dbc.Accordion([
                            dbc.AccordionItem([
                                html.Div([
                                    dcc.Textarea(
                                        id="textarea",
                                        value="\n".join(unreachable_tests),
                                        style={"font-size": "0.8em", "min-height": "70vh", "min-width": "90%"}
                                    ),
                                    dcc.Clipboard(
                                        target_id="textarea",
                                        title="Copy to clipboard",
                                        style={"display": "block", "margin-left": "1vw"}
                                    )
                                ], style={"display": "flex", "justify-content": "space-between", "align-items": "top", "width": "100%"})
                            ], title=f"{len(unreachable_tests)} Unreachable Tests", style={"color": "#dc3545"})
                        ], start_collapsed=True, style={"border": "2px solid #dc3545", "border-radius": "3px"})
                    ])
                ], style={"border": "none", "box-shadow": "none"})
            ], width=3)
        ], style={"margin-top": "-3vh"})
    ])

    # ? Get how long its been since the last test.    
    last_test_time = datetime.strptime(sorted_tests[0]['end_time'], '%Y-%m-%d %H:%M:%S')
    last_test_duration_s = sorted_tests[0]['duration_s']
    time_since_last_test = datetime.now() - last_test_time
    time_since_last_test_s = time_since_last_test.total_seconds()
    if time_since_last_test_s < 60:
        time_since_last_test = f"{int(time_since_last_test_s)} second" if int(time_since_last_test_s) == 1 else f"{int(time_since_last_test_s)} seconds"
    elif time_since_last_test_s < 3600:
        time_since_last_test = f"{int(time_since_last_test_s/60)} minute" if int(time_since_last_test_s/60) == 1 else f"{int(time_since_last_test_s/60)} minutes"
    elif time_since_last_test_s < 86400:
        time_since_last_test = f"{int(time_since_last_test_s/3600)} hour" if int(time_since_last_test_s/3600) == 1 else f"{int(time_since_last_test_s/3600)} hours"
    else:
        time_since_last_test = f"{int(time_since_last_test_s/86400)} day" if int(time_since_last_test_s/86400) == 1 else f"{int(time_since_last_test_s/86400)} days"

    last_test_color = "#dc3545" if time_since_last_test_s > last_test_duration_s * 2 else "#28a745"

    last_test_alert = dbc.Toast([
        "Last test finished ", 
        html.Strong(f"{time_since_last_test} ago."),
    ], header=current_campaign_name, style={"border": f"2px solid {last_test_color}", "color": last_test_color, "position": "fixed", "bottom": "0", "left": "0", "margin": "1vh", "z-index": "9999", "max-width": "350px"})

    test_trs = []
    
    for test in sorted_tests:
        start_time = datetime.strptime(test['start_time'], '%Y-%m-%d %H:%M:%S').strftime('%a %d %b %y %H:%M')
        end_time = datetime.strptime(test['end_time'], '%Y-%m-%d %H:%M:%S').strftime('%a %d %b %y %H:%M')

        # ? Get all statuses for this test.
        statuses = []
        for machine_status in test['machine_statuses']:
            statuses.append(machine_status['status'])
        statuses = ", ".join(statuses)
        
        if "prolonged" in statuses.lower() or "no csv" in statuses.lower():
            row_color = "#FFA500"
        elif "unreachable" in statuses.lower():
            row_color = "#dc3545"
        elif "punctual" in statuses.lower():
            row_color = "#28a745"
        else:
            row_color = "#6c757d"
        
        duration_s = test['duration_s']
        
        duration = ""
        if duration_s < 60:
            duration = f"{duration_s} seconds"
        elif duration_s < 3600:
            minutes = int(duration_s/60)
            seconds = duration_s%60
            duration = f"{minutes} minutes, {seconds} seconds"
        elif duration_s < 86400:
            hours = int(duration_s/3600)
            minutes = int((duration_s%3600)/60)
            seconds = duration_s%60
            duration = f"{hours} hours, {minutes} minutes, {seconds} seconds"
        else:
            days = int(duration_s/86400)
            hours = int((duration_s%86400)/3600)
            minutes = int((duration_s%3600)/60)
            seconds = duration_s%60
            duration = f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"
        
        test_tr = html.Tr([
            html.Td(test['index'], style={"color": row_color, "font-family": "monospace"}),
            html.Td(test['permutation_name'], style={"color": row_color}),
            html.Td(start_time, style={"color": row_color}),
            html.Td(end_time, style={"color": row_color}),
            html.Td(duration, style={"color": row_color}),
            html.Td(generate_machine_status_cards(test['machine_statuses']), style={"color": row_color})
        ])
        test_trs.append(test_tr)
    
    table = dbc.Table(bordered=True, hover=True, responsive=True, 
        children=[
            html.Thead(
                html.Tr([
                    html.Th("#", style={"width": "5%"}),
                    html.Th("Test", style={"width": "20%"}),
                    html.Th("Start", style={"width": "10%"}),
                    html.Th("End", style={"width": "10%"}),
                    html.Th("Duration", style={"width": "10%"}),
                    html.Th("Statuses")
                ])
            ),
            html.Tbody(test_trs)
        ]
    )

    ssh.close()

    return html.Div(
        [
            last_test_alert,
            test_types_container,
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