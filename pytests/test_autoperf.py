import unittest
import warnings
import sys
import autoperf as ap
from icecream import ic

class TestAutoPerf(unittest.TestCase):
    def test_read_config(self):
        config = ap.read_config("./pytests/good_config_1.json")
        self.assertNotEqual(config, None)

        config_paths_that_return_none = [
            './pytests/bad_config_1.json',
            './pytests/bad_config_2.json',
            './pytests/bad_config_3.json',
            './pytests/bad_config_4.json',
            './pytests/bad_config_5.json',
        ]
        
        for config_path in config_paths_that_return_none:
            self.assertEqual(
                ap.read_config(config_path),
                None
            )

    def test_get_ess_df(self):
        # TODO
        pass

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
        # TODO
        pass

    def test_get_next_test_from_ess(self):
        # TODO
        pass

    def test_have_last_n_tests_failed(self):
        # TODO
        pass

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
        CONFIG = ap.read_config('./pytests/good_config_1.json') 
        for EXPERIMENT in CONFIG:
            experiment_name = ap.get_dirname_from_experiment(EXPERIMENT)
            self.assertEqual(experiment_name, "PCG_#1")

    def test_get_if_pcg(self):
        self.assertEqual(
                ap.get_if_pcg(None), 
                None
            )

        CONFIG = ap.read_config('./pytests/good_config_1.json') 
        for EXPERIMENT in CONFIG:
            is_pcg = ap.get_if_pcg(EXPERIMENT)
            self.assertEqual(is_pcg, True)

        CONFIG = ap.read_config('./pytests/good_config_2.json')
        for EXPERIMENT in CONFIG:
            is_pcg = ap.get_if_pcg(EXPERIMENT)
            self.assertEqual(is_pcg, False)

        CONFIG = ap.read_config('./pytests/bad_config_6.json')
        for EXPERIMENT in CONFIG:
            is_pcg = ap.get_if_pcg(EXPERIMENT)
            self.assertEqual(is_pcg, None)

        CONFIG = ap.read_config('./pytests/bad_config_7.json')
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


if __name__ == '__main__':
    warnings.filterwarnings("ignore", category=FutureWarning)
    unittest.main()
