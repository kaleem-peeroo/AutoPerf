import unittest
import warnings
import os
import random
import autoperf as ap
import pandas as pd
from icecream import ic
from typing import Dict, Optional
from datetime import datetime, timedelta

def generate_random_timestamp(start: datetime, end: datetime) -> datetime:
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

def generate_random_ess_row():
    random_start_timestamp = generate_random_timestamp(
        datetime(2024, 1, 1, 10, 0, 0),
        datetime(2024, 1, 1, 10, 10, 0)
    )
    random_start_timestamp = random_start_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    random_end_timestamp = generate_random_timestamp(
        datetime(2024, 1, 1, 10, 10, 0),
        datetime(2024, 1, 1, 10, 20, 0)
    )
    random_end_timestamp = random_end_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    duration_secs = random.randint(60, 120)
    datalen_bytes = random.randint(100, 1_000)
    pub_count = random.randint(10, 50)
    sub_count = random.randint(10, 50)
    use_reliable = random.choice([True, False])
    use_multicast = random.choice([True, False])
    durability_level = random.randint(0, 3)
    latency_count = random.randint(100, 1_000)

    random_test_name = f"{duration_secs}SEC_{datalen_bytes}B_{pub_count}PUB_{sub_count}SUB_"
    random_test_name += f"{'REL' if use_reliable else 'BE'}_{'MC' if use_multicast else 'UC'}_"
    random_test_name += f"{durability_level}DUR_{latency_count}LC"

    random_pings_count = random.randint(0, 3)
    random_ssh_check_count = random.randint(0, 3)
    random_end_status = random.choice(["success", "fail", "timeout"])
    random_attempt_number = random.randint(0, 3)
    random_qos_settings = {
        'duration_secs': duration_secs,
        'datalen_bytes': datalen_bytes,
        'pub_count': pub_count,
        'sub_count': sub_count,
        'use_reliable': use_reliable,
        'use_multicast': use_multicast,
        'durability_level': durability_level,
        'latency_count': latency_count
    }

    row = pd.DataFrame({
        'start_timestamp': [random_start_timestamp],
        'end_timestamp': [random_end_timestamp],
        'test_name': [random_test_name],
        'pings_count': [random_pings_count],
        'ssh_check_count': [random_ssh_check_count],
        'end_status': [random_end_status],
        'attempt_number': [random_attempt_number],
        'qos_settings': [random_qos_settings],
        'comments': ""
    })

    return row

def generate_random_ess(row_count: int = 10):
    ess_filepath = "./pytests/ess/random_ess.csv"
    column_headings = [
        'start_timestamp',
        'end_timestamp',
        'test_name',
        'pings_count',
        'ssh_check_count',
        'end_status',
        'attempt_number',
        'qos_settings',
        'comments'
    ]

    ess_df = pd.DataFrame(columns=column_headings)
    for _ in range(row_count):
        row = generate_random_ess_row()
        ess_df = pd.concat([ess_df, row], ignore_index=True)

    ess_df.to_csv(ess_filepath, index=False)

def generate_ess_from_config(config: Dict = {}) -> Optional[pd.DataFrame]:
    """
    if pcg:
        1. generate all combinations
        2. pick a random cut off point in combinations
        3. for each combination, generate a row
        4. if the combination did not succeed, generate new row with attempt_number + 1
        5. do this up to 3 times

    if rcg:
        1. generate a random config
        2. generate a row
        3. if the row did not succeed, generate new row with attempt_number + 1
        4. do this x times
    """

    if config == {}:
        return None

    if ap.get_if_pcg(config) == True:
        combinations = ap.generate_combinations_from_qos(config['qos_settings'])
        random_cut_off = random.randint(0, len(combinations))
        ess_df = pd.DataFrame(columns=[
            'start_timestamp',
            'end_timestamp',
            'test_name',
            'pings_count',
            'ssh_check_count',
            'end_status',
            'attempt_number',
            'qos_settings',
            'comments'
        ])

        for index, combination in enumerate(combinations):
            if index >= random_cut_off:
                break

            row = pd.DataFrame({
                'start_timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                'end_timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                'test_name': [ap.get_test_name_from_combination_dict(combination)],
                'pings_count': [random.randint(0, 3)],
                'ssh_check_count': [random.randint(0, 3)],
                'end_status': [random.choice(["success", "fail", "timeout"])],
                'attempt_number': [0],
                'qos_settings': [combination],
                'comments': ""
            })

            ess_df = pd.concat([ess_df, row], ignore_index=True)

        return ess_df

    else:
        random_cut_off = random.randint(0, 10)
        ess_df = pd.DataFrame(columns=[
            'start_timestamp',
            'end_timestamp',
            'test_name',
            'pings_count',
            'ssh_check_count',
            'end_status',
            'attempt_number',
            'qos_settings',
            'comments'
        ])

        for _ in range(10):
            row = pd.DataFrame({
                'start_timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                'end_timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                'test_name': [ap.get_test_name_from_combination_dict({
                    'duration_secs': random.randint(60, 120),
                    'datalen_bytes': random.randint(100, 1_000),
                    'pub_count': random.randint(10, 50),
                    'sub_count': random.randint(10, 50),
                    'use_reliable': random.choice([True, False]),
                    'use_multicast': random.choice([True, False]),
                    'durability_level': random.randint(0, 3),
                    'latency_count': random.randint(100, 1_000)
                })],
                'pings_count': [random.randint(0, 3)],
                'ssh_check_count': [random.randint(0, 3)],
                'end_status': [random.choice(["success", "fail", "timeout"])],
                'attempt_number': [0],
                'qos_settings': [{
                    'duration_secs': random.randint(60, 120),
                    'datalen_bytes': random.randint(100, 1_000),
                    'pub_count': random.randint(10, 50),
                    'sub_count': random.randint(10, 50),
                    'use_reliable': random.choice([True, False]),
                    'use_multicast': random.choice([True, False]),
                    'durability_level': random.randint(0, 3),
                    'latency_count': random.randint(100, 1_000)
                }],
                'comments': ""
            })

            ess_df = pd.concat([ess_df, row], ignore_index=True)

        return ess_df
    
class TestAutoPerf(unittest.TestCase):
    def setUp(self):
        generate_random_ess(50)

    def tearDown(self):
        pass

    def test_read_config(self):
        config = ap.read_config("./pytests/configs/good_config_1.json")
        self.assertNotEqual(config, None)

        config_paths_that_return_none = [
            './pytests/configs/bad_config_1.json',
            './pytests/configs/bad_config_2.json',
            './pytests/configs/bad_config_3.json',
            './pytests/configs/bad_config_4.json',
            './pytests/configs/bad_config_5.json',
        ]
        
        for config_path in config_paths_that_return_none:
            self.assertEqual(
                ap.read_config(config_path),
                None
            )

    def test_get_ess_df(self):
        ess_df = ap.get_ess_df("./pytests/ess/random_ess.csv")
        required_columns = [
            'start_timestamp',
            'end_timestamp',
            'test_name',
            'pings_count',
            'ssh_check_count',
            'end_status',
            'attempt_number',
            'qos_settings',
            'comments'
        ]

        self.assertEqual(
            len(ess_df.columns),
            len(required_columns)
        )

        self.assertEqual(
            list(ess_df.columns),
            required_columns
        )

    def test_get_valid_dirname(self):
        test_inputs = [
            'valid_folder_name',
            'invalid<name>',
            'name:with|invalid*chars?',
            '   leading and trailing spaces    ',
            'multiple     spaces ',
            'a' * 256,
            'Mixed CASE and Numbers 123',
            'special_!@#$%^&*()'
        ]
        test_outputs = [
            'valid_folder_name',
            'invalid_name_',
            'name_with_invalid_chars_',
            'leading_and_trailing_spaces',
            'multiple_spaces',
            None,
            'Mixed_CASE_and_Numbers_123',
            'special_!@#$%^&_()'
        ]

        for index, _ in enumerate(test_inputs):
            test_input = test_inputs[index]
            test_output = test_outputs[index]
            self.assertEqual(
                ap.get_valid_dirname(test_input),
                test_output
            )

    def test_get_test_name_from_combination_dict(self):
        test_dict = {
            'duration_secs': 10,
            'datalen_bytes': 100,
            'pub_count': 10,
            'sub_count': 10,
            'use_reliable': True,
            'use_multicast': False,
            'durability_level': 0,
            'latency_count': 100
        }
        test_name = ap.get_test_name_from_combination_dict(test_dict)
        self.assertEqual(
            test_name,
            "10SEC_100B_10PUB_10SUB_REL_UC_0DUR_100LC"
        )

        test_dict = {
            'duration_secs': 10,
            'datalen_bytes': 100,
            'pub_count': 10,
            'sub_count': 10,
            'use_reliable': 0,
            'use_multicast': 0,
            'durability_level': 0,
            'latency_count': 100
        }
        test_name = ap.get_test_name_from_combination_dict(test_dict)
        self.assertEqual(
            test_name,
            "10SEC_100B_10PUB_10SUB_BE_UC_0DUR_100LC"
        )

        test_dict = {
            'datalen_bytes': 100,
            'pub_count': 10,
            'sub_count': 10,
            'use_reliable': 0,
            'use_multicast': 0,
            'durability_level': 0,
            'latency_count': 100
        }
        test_name = ap.get_test_name_from_combination_dict(test_dict)
        self.assertEqual(
            test_name,
            None
        )

    def test_get_next_test_from_ess(self):
        ess_df = ap.get_ess_df("./pytests/ess/good_ess_1.csv")
        next_test = ap.get_next_test_from_ess(ess_df)
        self.assertEqual(
            next_test,
            {
                'duration_secs': 107, 
                'datalen_bytes': 947, 
                'pub_count': 27, 
                'sub_count': 31, 
                'use_reliable': False, 
                'use_multicast': False, 
                'durability_level': 3, 
                'latency_count': 524
            }
        )

        self.assertEqual(
            ap.get_next_test_from_ess(None),
            None
        )

        self.assertEqual(
            ap.get_next_test_from_ess(pd.DataFrame()),
            {}
        )

    def test_have_last_n_tests_failed(self):
        ess_df = ap.get_ess_df("./pytests/ess/good_ess_1.csv")
        self.assertEqual(
            ap.have_last_n_tests_failed(ess_df, 3),
            True
        )

        self.assertEqual(
            ap.have_last_n_tests_failed(ess_df, 5),
            False
        )

        self.assertEqual(
            ap.have_last_n_tests_failed(ess_df, 0),
            False
        )

        self.assertEqual(
            ap.have_last_n_tests_failed(ess_df, -1),
            None
        )

        self.assertEqual(
            ap.have_last_n_tests_failed(ess_df, 100),
            False
        )

        self.assertEqual(
            ap.have_last_n_tests_failed(ess_df, 50),
            False
        )

        self.assertEqual(
            ap.have_last_n_tests_failed(ess_df, 49),
            False
        )

    def test_get_buffer_duration_secs_from_test_duration_secs(self):
        self.assertEqual(
            ap.get_buffer_duration_secs_from_test_duration_secs(10),
            30
        )

        self.assertEqual(
            ap.get_buffer_duration_secs_from_test_duration_secs(60),
            30
        )

        self.assertEqual(
            ap.get_buffer_duration_secs_from_test_duration_secs(120),
            30
        )

        self.assertEqual(
            ap.get_buffer_duration_secs_from_test_duration_secs(0),
            None
        )

        self.assertEqual(
            ap.get_buffer_duration_secs_from_test_duration_secs(-1),
            None
        )

        self.assertEqual(
            ap.get_buffer_duration_secs_from_test_duration_secs(1200),
            60
        )

    def test_generate_combinations_from_qos(self):
        qos = {
            "duration_secs": [10, 20],
            "pub_count": [10, 20],
        }
        combinations = ap.generate_combinations_from_qos(qos)
        self.assertEqual(
            combinations,
            [
                {"duration_secs": 10, "pub_count": 10},
                {"duration_secs": 10, "pub_count": 20},
                {"duration_secs": 20, "pub_count": 10},
                {"duration_secs": 20, "pub_count": 20},
            ]
        )

        qos = {
            "duration_secs": [20],
            "pub_count": [10, 20],
        }
        combinations = ap.generate_combinations_from_qos(qos)
        self.assertEqual(
            combinations,
            [
                {"duration_secs": 20, "pub_count": 10},
                {"duration_secs": 20, "pub_count": 20},
            ]
        )

        qos = {
            "duration_secs": [],
            "pub_count": [10, 20],
        }
        combinations = ap.generate_combinations_from_qos(qos)
        self.assertEqual(
            combinations,
            None
        )

        qos = {
            "duration_secs": [True, False],
            "pub_count": [10, 20],
        }
        combinations = ap.generate_combinations_from_qos(qos)
        self.assertEqual(
            combinations,
            [
                {"duration_secs": True, "pub_count": 10},
                {"duration_secs": True, "pub_count": 20},
                {"duration_secs": False, "pub_count": 10},
                {"duration_secs": False, "pub_count": 20},
            ]
        )

    def test_get_dirname_from_experiment(self):
        CONFIG = ap.read_config('./pytests/configs/good_config_1.json') 
        for EXPERIMENT in CONFIG:
            experiment_name = ap.get_dirname_from_experiment(EXPERIMENT)
            self.assertEqual(experiment_name, "PCG_#1")

    def test_get_if_pcg(self):
        self.assertEqual(
                ap.get_if_pcg(None), 
                None
            )

        CONFIG = ap.read_config('./pytests/configs/good_config_1.json') 
        for EXPERIMENT in CONFIG:
            is_pcg = ap.get_if_pcg(EXPERIMENT)
            self.assertEqual(is_pcg, True)

        CONFIG = ap.read_config('./pytests/configs/good_config_2.json')
        for EXPERIMENT in CONFIG:
            is_pcg = ap.get_if_pcg(EXPERIMENT)
            self.assertEqual(is_pcg, False)

        CONFIG = ap.read_config('./pytests/configs/bad_config_6.json')
        for EXPERIMENT in CONFIG:
            is_pcg = ap.get_if_pcg(EXPERIMENT)
            self.assertEqual(is_pcg, None)

        CONFIG = ap.read_config('./pytests/configs/bad_config_7.json')
        for EXPERIMENT in CONFIG:
            is_pcg = ap.get_if_pcg(EXPERIMENT)
            self.assertEqual(is_pcg, None)

    def test_validate_dict_using_keys(self):
        self.assertEqual(
            ap.validate_dict_using_keys(
                ['one', 'two', 'three'],
                ['four', 'five', 'six']
            ),
            False
        )

        self.assertEqual(
            ap.validate_dict_using_keys(
                ['one', 'two', 'three'],
                ['one', 'two', 'three']
            ),
            True
        )

        self.assertEqual(
            ap.validate_dict_using_keys(
                ['one', 'two', 'three'],
                ['one', 'two']
            ),
            False
        )

        self.assertEqual(
            ap.validate_dict_using_keys(
                ['one'],
                ['one', 'two']
            ),
            False
        )

    def test_get_difference_between_lists(self):
        self.assertEqual(
           ap.get_difference_between_lists(
            [1, 2, 3],
            [1, 2, 3]
           ),
           []
        )

        self.assertEqual(
           ap.get_difference_between_lists(
            [1, 3],
            [1, 2, 3]
           ),
           [2]
        )

        self.assertEqual(
           ap.get_difference_between_lists(
            [],
            [1, 2, 3]
           ),
           [1, 2, 3]
        )

    def test_get_longer_list(self):
        # TODO
        pass

    def test_get_shorter_list(self):
        # TODO
        pass

    def test_check_ssh_connection(self):
        # TODO
        pass

    def test_ping_machine(self):
        # TODO
        pass

    def test_generate_scripts_from_qos_config(self):
        qos_config = {
            'datalen_bytes': 100,
            'durability_level': 0,
            'duration_secs': 30,
            'latency_count': 100,
            'pub_count': 1,
            'sub_count': 1,
            'use_multicast': True,
            'use_reliable': True
        }
        scripts = ap.generate_scripts_from_qos_config(qos_config)
        self.assertEqual(
            len(scripts),
            2
        )
        self.assertEqual(
            scripts,
            [
                '-dataLen 100 -multicast -durability 0 -pub -outputFile pub_0.csv -numSubscribers 1 -executionTime 30 -latencyCount 100 -batchSize 0 -transport UDPv4',
                '-dataLen 100 -multicast -durability 0 -sub -outputFile sub_0.csv -numPublishers 1 -transport UDPv4'
            ]
        )

        qos_config = {
            'datalen_bytes': 100,
            'durability_level': 0,
            'duration_secs': 30,
            'latency_count': 100,
            'pub_count': 1,
            'sub_count': 1,
            'use_multicast': True,
            'use_reliable': False
        }
        scripts = ap.generate_scripts_from_qos_config(qos_config)
        self.assertEqual(
            len(scripts),
            2
        )
        self.assertEqual(
            scripts,
            [
                '-dataLen 100 -bestEffort -multicast -durability 0 -pub -outputFile pub_0.csv -numSubscribers 1 -executionTime 30 -latencyCount 100 -batchSize 0 -transport UDPv4',
                '-dataLen 100 -bestEffort -multicast -durability 0 -sub -outputFile sub_0.csv -numPublishers 1 -transport UDPv4'
            ]
        )

        qos_config = {
            'datalen_bytes': 100,
            'durability_level': 0,
            'duration_secs': 30,
            'latency_count': 100,
            'pub_count': 2,
            'sub_count': 2,
            'use_multicast': False,
            'use_reliable': True
        }
        scripts = ap.generate_scripts_from_qos_config(qos_config)
        self.assertEqual(
            len(scripts),
            4
        )
        self.assertEqual(
            scripts,
            [
                '-dataLen 100 -durability 0 -pub -pidMultiPubTest 0 -outputFile pub_0.csv -numSubscribers 2 -executionTime 30 -latencyCount 100 -batchSize 0 -transport UDPv4',
                '-dataLen 100 -durability 0 -pub -pidMultiPubTest 1 -numSubscribers 2 -executionTime 30 -latencyCount 100 -batchSize 0 -transport UDPv4',
                '-dataLen 100 -durability 0 -sub -sidMultiSubTest 0 -outputFile sub_0.csv -numPublishers 2 -transport UDPv4',
                '-dataLen 100 -durability 0 -sub -sidMultiSubTest 1 -outputFile sub_1.csv -numPublishers 2 -transport UDPv4'
           ]
        )

    def test_distribute_scripts_to_machines(self):
        distributed_scripts = ap.distribute_scripts_to_machines(
            [],
            []
        )
        self.assertEqual(
            distributed_scripts,
            None
        )

        self.assertEqual(
            ap.distribute_scripts_to_machines(
                [
                    '-pub 1', 
                    '-pub 2',
                    '-pub 3',
                    '-sub 1',
                    '-sub 2',
                    '-sub 3',
                ],
                [
                    {
                        'machine_name': 'p1',
                        'ip': '129.123.123.123',
                        'participant_allocation': 'pub',
                        'perftest_exec_path': "~/path/to/perftest",
                    },
                    {
                        'machine_name': 'p2',
                        'ip': '129.123.123.123',
                        'participant_allocation': 'sub',
                        'perftest_exec_path': "~/path/to/perftest",
                    }

                ]
            ),
            [
                {
                    'machine_name': 'p1',
                    'ip': '129.123.123.123',
                    'participant_allocation': 'pub',
                    'perftest_exec_path': "~/path/to/perftest",
                    'script': 'source ~/.bashrc; cd ~/path/to; ./perftest -pub 1 & ./perftest -pub 2 & ./perftest -pub 3 &'
                },
                {
                    'machine_name': 'p2',
                    'ip': '129.123.123.123',
                    'participant_allocation': 'sub',
                    'perftest_exec_path': "~/path/to/perftest",
                    'script': 'source ~/.bashrc; cd ~/path/to; ./perftest -sub 1 & ./perftest -sub 2 & ./perftest -sub 3 &'
                }
            ]
        )

        self.assertEqual(
            ap.distribute_scripts_to_machines(
                [
                    '-pub 1', 
                    '-sub 1',
                ],
                [
                    {
                        'machine_name': 'p1',
                        'ip': '129.123.123.123',
                        'participant_allocation': 'pub',
                        'perftest_exec_path': "~/path/to/perftest",
                    },
                    {
                        'machine_name': 'p2',
                        'ip': '129.123.123.123',
                        'participant_allocation': 'sub',
                        'perftest_exec_path': "~/path/to/perftest",
                    }

                ]
            ),
            [
                {
                    'machine_name': 'p1',
                    'ip': '129.123.123.123',
                    'participant_allocation': 'pub',
                    'perftest_exec_path': "~/path/to/perftest",
                    'script': 'source ~/.bashrc; cd ~/path/to; ./perftest -pub 1 &'
                },
                {
                    'machine_name': 'p2',
                    'ip': '129.123.123.123',
                    'participant_allocation': 'sub',
                    'perftest_exec_path': "~/path/to/perftest",
                    'script': 'source ~/.bashrc; cd ~/path/to; ./perftest -sub 1 &'
                }
            ]
        )

        self.assertEqual(
            ap.distribute_scripts_to_machines(
                [
                    '-pub 1', 
                    '-pub 2',
                    '-pub 3',
                    '-sub 1',
                    '-sub 2',
                    '-sub 3',
                ],
                [
                    {
                        'machine_name': 'p1',
                        'ip': '129.123.123.123',
                        'participant_allocation': 'all',
                        'perftest_exec_path': "~/path/to/perftest",
                    },
                    {
                        'machine_name': 'p2',
                        'ip': '129.123.123.123',
                        'participant_allocation': 'sub',
                        'perftest_exec_path': "~/path/to/perftest",
                    }

                ]
            ),
            [
                {
                    'machine_name': 'p1',
                    'ip': '129.123.123.123',
                    'participant_allocation': 'all',
                    'perftest_exec_path': "~/path/to/perftest",
                    'script': 'source ~/.bashrc; cd ~/path/to; ./perftest -pub 1 & ./perftest -pub 2 & ./perftest -pub 3 & ./perftest -sub 2 &'
                },
                {
                    'machine_name': 'p2',
                    'ip': '129.123.123.123',
                    'participant_allocation': 'sub',
                    'perftest_exec_path': "~/path/to/perftest",
                    'script': 'source ~/.bashrc; cd ~/path/to; ./perftest -sub 1 & ./perftest -sub 3 &'
                }
            ]
        )

        self.assertEqual(
            ap.distribute_scripts_to_machines(
                [
                    '-pub 1', 
                    '-pub 2',
                    '-pub 3',
                    '-sub 1',
                    '-sub 2',
                    '-sub 3',
                ],
                [
                    {
                        'machine_name': 'p1',
                        'ip': '129.123.123.123',
                        'participant_allocation': 'all',
                        'perftest_exec_path': "~/path/to/perftest",
                    },
                    {
                        'machine_name': 'p2',
                        'ip': '129.123.123.123',
                        'participant_allocation': 'all',
                        'perftest_exec_path': "~/path/to/perftest",
                    }

                ]
            ),
            [
                {
                    'machine_name': 'p1',
                    'ip': '129.123.123.123',
                    'participant_allocation': 'all',
                    'perftest_exec_path': "~/path/to/perftest",
                    'script': 'source ~/.bashrc; cd ~/path/to; ./perftest -pub 1 & ./perftest -pub 3 & ./perftest -sub 2 &'
                },
                {
                    'machine_name': 'p2',
                    'ip': '129.123.123.123',
                    'participant_allocation': 'all',
                    'perftest_exec_path': "~/path/to/perftest",
                    'script': 'source ~/.bashrc; cd ~/path/to; ./perftest -pub 2 & ./perftest -sub 1 & ./perftest -sub 3 &'
                }
            ]
        )

    def test_get_machines_by_type(self):
        config = ap.read_config('./pytests/configs/good_config_1.json')
        machines = config[0]['slave_machines']
        pub_machines = ap.get_machines_by_type(machines, 'pub')
        sub_machines = ap.get_machines_by_type(machines, 'sub')
        self.assertEqual(
            len(pub_machines),
            1
        )
        self.assertEqual(
            len(sub_machines),
            1
        )

        self.assertEqual(
            pub_machines[0]['machine_name'],
            'p1'
        )
        self.assertEqual(
            sub_machines[0]['machine_name'],
            'p2'
        )

if __name__ == '__main__':
    warnings.filterwarnings("ignore", category=FutureWarning)
    unittest.main()
