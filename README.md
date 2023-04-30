# PTST
Welcome to PhD Test Script Tool. The tool to automate the DDS performance testing.

## How to use it?
Set up the config.json file. Here is an example:

```json
{
    "campaigns": [
        {
            "name": "Example Campaign",
            "repetitions": 1,
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
            "machines": [
                {
                    "name": "p1",
                    "host": "192.168.3.3",
                    "ssh_key": "path/to/id_rsa",
                    "username": "admin",
                    "home_dir": "/path/to/home",
                    "perftest": "",
                    "perftest_publisher": "$perftest_publisher",
                    "perftest_subscriber": "$perftest_subscriber"
                },
                {
                    "name": "p2",
                    "host": "192.168.3.45",
                    "ssh_key": "path/to/id_rsa",
                    "username": "admin",
                    "home_dir": "/path/to/home",
                    "perftest": "",
                    "perftest_publisher": "$perftest_publisher",
                    "perftest_subscriber": "$perftest_subscriber"
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

You can skip the restarts using `skip_restart`:
```bash
python index.py path/to/config.json debug skip_restart
```

If you want to save the output to a file:
```bash
python index.py path/to/config.json |& tee output.txt
```

TODO: 
<!-- - [ ] Periodically zip then upload files from k1 to cloud -->
<!-- - [ ] Show to user what the buffer duration is. -->
- [x] Show how long all tests will take in total and expected end date + time.
- [x] Record if test failed or not.
- [x] Why are empty leftovers folders being made?
- [x] Show test progress.

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
python monitor.py 10.210.35.27 3pk1 "/home/acwh025/Documents/PTST" "/Users/kaleem/.ssh/id_rsa"
```

TODO:
- [ ] Show current test running, when it started, and when its expected to finish