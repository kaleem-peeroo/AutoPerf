# PTST
Welcome to PhD Test Script Tool. The tool to automate the DDS performance testing.

## How to use it?
Set up the config.json file. Here is an example:

```json
{
    "campaigns": [
        {
            "name": "Example Campaign",
            "settings": {
                "duration_s": [600],
                "datalen_bytes": [100],
                "pub_count": [1, 2, 5, 10],
                "sub_count": [1, 2, 5, 10],
                "reliability": [true, false],
                "use_multicast": [true, false],
                "durability": [0, 1, 2, 3],
                "latency_count": [100]
            },
            "custom_tests_file": "",
            "machines": [
                {
                    "name": "p1",
                    "host": "192.168.3.3",
                    "ssh_key": "path/to/id_rsa",
                    "username": "admin",
                    "home_dir": "/path/to/home",
                    "perftest": "",
                    "perftest_publisher": "$perftest_publisher",
                    "perftest_subscriber": "$perftest_subscriber",
                    "participant_allocation": "all"
                },
                {
                    "name": "p2",
                    "host": "192.168.3.45",
                    "ssh_key": "path/to/id_rsa",
                    "username": "admin",
                    "home_dir": "/path/to/home",
                    "perftest": "",
                    "perftest_publisher": "$perftest_publisher",
                    "perftest_subscriber": "$perftest_subscriber",
                    "participant_allocation": "pub"
                }
            ]
        }
    ]
}
```

Then run the following:
```bash
python index.py path/to/config.json <buffer_multiple>
```

Where `<buffer_multiple>` is a decimal value multiplied to the test duration to add a buffer to wait after a test. For example:
If:
```
test_duration = 60 seconds
buffer_multiple = 1.5
```
Then PTST will wait up to 90 seconds (60 * 1.5) for the test to finish before interrupting it and recording it as a failure.

For debug mode just add `debug`:
```bash
python index.py path/to/config.json debug
```

For test mode just add `test_mode`:
```bash
python index.py path/to/config.json test_mode
```

`test_mode` is when you want to pretend like you are running tests without actually sshing into machines and running scripts.

- It'll randomly produced punctual and prolonged tests

## Features

### Prolonged Tests Monitor

**What do I need to do?**

I need to implement the following functionality:

1. Monitor the percentage of prolonged tests throughout the campaign.
2. If 5 tests have finished and they have all been prolonged and the prolonged percentage doesn’t decrease then do the following:
    1. Ping all machines and find out which ones don’t respond.
        1. If all machines respond try sshing into all machines and find out which ones don’t respond.
    2. Finalise the campaign
        1. Move the output.txt and zip the results.
        2. Create file explaining which machines didn’t respond.

- How should I keep track of the test progresses?
    - Read the progress.json file or keep track in a dictionary?
- Where do I implement the check for lack of response?
    - After each test.
        - Do the numbered steps above.

**How can I test this?**

I need to somehow simulate a series of x punctual tests and y prolonged tests.

- I’ll put a randomiser in PTST to skip running the test and instead just record it as a random punctual/prolonged test.

I then need to simulate 10 prolonged tests in a row.

- I need to add a counter and once it reaches a certain number I need code to skip the next 10 tests and record them as prolonged.

### Random Combination Generation

**What do I need to do?**

1. Confirm the campaign is using this feature.
2. Print out the setting boundaries.
3. Create a log of all tests combinations run - either a file or in a list - most likely a file.
4. Generate a random combination
5. Check if its already been generated - if so then generate a new one.
6. Print out the test title and its combination.
7. Run the test.
8. Do the usual stuff.
9. Then generate a new test.

TODO: 
- [x] Zip final results
- [x] Show to user what the buffer duration is.
- [x] Show how long all tests will take in total and expected end date + time.
- [x] Record if test failed or not.
- [x] Why are empty leftovers folders being made?
- [x] Show test progress.
- [ ] Handle unresponsive machines.

# Monitor
Script used to check up on the controllers. The controllers are running PTST. `monitor.py` will basically SSH into the controllers and return the status of the campaign. It'll basically show you the following in a table format:
- Campaign Start + Expected End
- Total Test Count
- Punctual Test Count + %
- Prolonged Test Count + %
- Usable Tests Count + %
    - Where usable = has all data that is expected of it

## How to Use
```bash
python monitor.py <controller_ip> <controller_name> <path_to_ptst_on_controller> <path_to_private_ssh_key>
```

Example:
```bash
python monitor.py 10.210.35.27 3Pi "/home/acwh025/Documents/PTST" "/Users/kaleem/.ssh/id_rsa"
```
