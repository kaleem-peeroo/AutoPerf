# PTST
Welcome to PhD Test Script Tool. The tool to automate DDS performance testing.

## How to use it?

There are 2 modes:
- Preset Combination Generation
- Random Combination Generation

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
