import unittest

from data_summariser import *


class TestDataSummariser(unittest.TestCase):
    def test_parse_pub_file(self):
        pub_file = os.path.abspath(
            "./pytests/data/{}/{}".format(
                "1P3S_Multicast_Exploration",
                "600SEC_512B_1PUB_1SUB_REL_MC_0DUR_100LC/pub_0.csv",
            )
        )

        pub_df = parse_pub_file(pub_file)
        self.assertGreater(len(pub_df), 0)

    def test_summarise_test(self):
        test_summ_path, error = summarise_test("", "")
        self.assertEqual(test_summ_path, "")
        self.assertEqual(error, "Test directory not specified")

        test_summ_path, error = summarise_test(
            os.path.abspath(
                "./pytests/data/{}/{}".format(
                    "1P3S_Multicast_Exploration",
                    "600SEC_512B_1PUB_1SUB_REL_MC_0DUR_100LC",
                )
            ),
            os.path.abspath(
                "./output/summarised_data/{}".format("1P3S_Multicast_Exploration")
            ),
        )
        self.assertEqual(
            test_summ_path,
            os.path.abspath(
                "./{}/{}/{}/{}".format(
                    "output",
                    "summarised_data",
                    "1P3S_Multicast_Exploration",
                    "600SEC_512B_1PUB_1SUB_REL_MC_0DUR_100LC.csv",
                )
            ),
        )

    def test_summarise_tests(self):
        summ_path, error = summarise_tests("")

        self.assertEqual(summ_path, "")
        self.assertEqual(error, "Data path not specified")

        summ_path, error = summarise_tests("./pytests/data/1P3S_Multicast_Exploration/")
        self.assertEqual(
            summ_path,
            "/{}/{}/{}/{}/{}/{}".format(
                "Users",
                "kaleem",
                "AutoPerf",
                "output",
                "summarised_data",
                "1P3S_Multicast_Exploration",
            ),
        )
        self.assertEqual(error, None)

        self.assertEqual(os.path.exists(summ_path), True)

        summ_files = os.listdir(summ_path)
        self.assertGreater(len(summ_files), 0)


if __name__ == "__main__":
    unittest.main()
