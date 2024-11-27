import itertools
import json

from rich.pretty import pprint

def get_qos_from_experiment_name(experiment_name):
    from src.experiments import QoS

    duration_secs = 0
    datalen_bytes = 0
    pub_count = 0
    sub_count = 0
    use_reliable = False
    use_multicast = False
    durability = 0
    latency_count = 0

    if experiment_name == "":
        raise ValueError("experiment name must not be empty")

    if not isinstance(experiment_name, str):
        raise ValueError(f"experiment name must be a string: {experiment_name}")

    experiment_name_parts = experiment_name.split("_")
    if len(experiment_name_parts) != 8:
        raise ValueError("{} must have 8 parts but has {}".format(
            experiment_name, len(experiment_name_parts)
        ))

    for part in experiment_name_parts:
        if part == "":
            raise ValueError("experiment name part must not be empty")

        if part.endswith("SEC"):
            duration_secs = int(part[:-3])

        elif part.endswith("B"):
            datalen_bytes = int(part[:-1])

        elif part.endswith("LC"):
            latency_count = int(part[:-2])

        elif part.endswith("DUR"):
            durability = int(part[:-3])

        elif (part == "UC") or (part == "MC"):

            if part == "UC":
                use_multicast = False
            else:
                use_multicast = True

        elif (part == "REL") or (part == "BE"):

            if part == "REL":
                use_reliable = True
            else:
                use_reliable = False

        elif part.endswith("P"):
            pub_count = int(part[:-1])

        elif part.endswith("S"):
            sub_count = int(part[:-1])

        else:
            raise ValueError(f"Unknown experiment name part: {part}")

    qos = QoS(
        duration_secs,
        datalen_bytes,
        pub_count,
        sub_count,
        use_reliable,
        use_multicast,
        durability,
        latency_count
    )

    return qos

def generate_qos_permutations(qos_config):
    if not isinstance(qos_config, dict):
        raise ValueError(f"QoS config must be a dict: {qos_config}")

    if qos_config == {}:
        raise ValueError("QoS config must not be empty")

    required_keys = [
        'duration_secs',
        'datalen_bytes',
        'pub_count',
        'sub_count',
        'use_reliable',
        'use_multicast',
        'durability',
        'latency_count'
    ]

    keys = qos_config.keys()
    if len(keys) == 0:
        raise ValueError("No options found for qos")

    for key in required_keys:
        if key not in qos_config:
            raise ValueError(f"QoS config must have {key}")

    values = qos_config.values()
    if len(values) == 0:
        raise ValueError("No values found for QoS")

    for value in values:
        if len(value) == 0:
            raise ValueError("One of the settings has no values.")

    combinations = list(itertools.product(*values))
    combination_dicts = [dict(zip(keys, combination)) for combination in combinations]

    if len(combination_dicts) == 0:
        raise ValueError(f"No combinations were generated fro mthe QoS values:\n\t {qos_config}")

    return combination_dicts

def machine_params_from_str(machine_str):
    machine_str = machine_str.replace("{", "")
    machine_str = machine_str.replace("}", "")

    machine_str_parts = machine_str.split(", ")
    
    machine_params = {
        'hostname': "",
        'participant_type': "",
        'ip': "",
        'ssh_key_path': "",
        'username': "",
        'perftest_path': "",
        'scripts': "",
        'command': "",
        'run_output': "",
    }
    for part in machine_str_parts:
        if 'run_output' in part:
            part = part.replace("'", "")

        key = part.split(":")[0].replace("'", "")
        value = ":".join(part.split(":")[1:])
        
        if key not in machine_params.keys():
            continue

        machine_params[key] = value.replace("'", "").strip()
        
    return machine_params

def experiment_already_ran(experiment, campaign):
    ran_exps = [runner.get_experiment().get_id() for runner in campaign.get_results()]
    if experiment.get_id() in ran_exps:
        return True
    else:
        return False
