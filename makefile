# test_data_summariser:
test_ds:
	python data_summariser_tests.py

# run_data_summariser:
run_ds:
	python data_summariser.py $(DATA_PATH)

devrun_ds:
	python data_summariser.py "./output/data/devtest/1P3S_Multicast_Exploration/"

# test_and_run_data_summariser:
test_and_run_ds:
	python data_summariser_tests.py
	python data_summariser.py $(DATA_PATH)
