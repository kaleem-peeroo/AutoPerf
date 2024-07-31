DATA_DIR = 'output/data/'
LOG_DIR = 'output/logs/'
SUMMARISED_DIR = 'output/summarised_data/'
DATASET_DIR = 'output/datasets/'

REQUIRED_EXPERIMENT_KEYS = [
    'experiment_name',
    'combination_generation_type',
    'qos_settings',
    'slave_machines',
    'rcg_target_test_count',
    'quit_after_n_failed_tests',
    'noise_generation'
]

REQUIRED_QOS_KEYS = [
    "datalen_bytes",
    'durability_level',
    'duration_secs',
    'latency_count',
    'pub_count',
    'sub_count',
    'use_multicast',
    'use_reliable'
]

REQUIRED_MONITOR_MACHINE_KEYS = [
    'name',
    'ip',
    'username',
    'ssh_key_path',
    'config_path'
]

REQUIRED_SLAVE_MACHINE_KEYS = [
    'ip',
    'machine_name',
    'participant_allocation',
    'perftest_exec_path',
    'ssh_key_path',
    'username'
]

REQUIRED_NOISE_GENERATION_KEYS = [
    'delay',
    'packet_loss',
    'packet_corruption',
    'packet_duplication',
    'bandwidth_limit'
]

REQUIRED_DELAY_KEYS = [
    'value',
    'variation',
    'correlation',
    'distribution'
]

REQUIRED_BANDWIDTH_LIMIT_KEYS = [
    'rate',
    'max_burst',
    'latency_cut_off'
]

PERCENTILES = [
    0, 1, 2, 3, 4, 5, 10,
    20, 30, 40, 60, 70, 80, 90,
    95, 96, 97, 98, 99, 100,
    25, 50, 75
]
