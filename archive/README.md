![PTST Logo](./images/ptst-logo.png)

Welcome to PhD Test Script Tool. The tool to automate Data Distribution Service (DDS) performance testing using [RTI Perftest](https://github.com/rticommunity/rtiperftest).

# Table of Contents
- [Table of Contents](#table-of-contents)
- [Command Usage](#command-usage)
- [Introduction](#introduction)
  - [JSON Config File](#json-config-file)
    - [Config Settings](#config-settings)
- [Preset Combination Generation (PCG)](#preset-combination-generation-pcg)
  - [Example](#example)
- [Random Combination Generation (RCG)](#random-combination-generation-rcg)
  - [Example](#example-1)


# Command Usage
```shell
python ptst.py <config> <buffer_duration_in_seconds>
```
Example:
```shell
python ptst.py configs/3Pi/qos_capture.json 120
```

# Introduction

You pretty much define all your settings in the config file and then run the command to get it working.

## JSON Config File
You need to first configure the `<config_file_name>.json` with the **following** settings:
```json
[
    {
        "name": ...,
        "random_combination_generation": ...,
        "random_combination_count": ...,
        "custom_tests_file": ...,
        "settings": {...},
        "machines": [...]
    }
]
```

### Config Settings

**`name`: `string`**
The name of the campaign - whatever you want it to be e.g. `"My First Campaign"`.

**`random_combination_generation`: `boolean`**
Whether to use random combination generation mode or not e.g. `true`.
`true`: Use random combination generation.
`false`: Use preset combination generation.

**`random_combination_count`: `int`**
Used when `random_combination_generation` is `true`.
Defines the number of tests to run using random combination generation e.g. `100`.

**`custom_tests_file`: `string`**
Path to a file containing a list of test names e.g. `/custom_lists/my_first_custom_list.txt`. A list of all test names is usually generated after running `PTST` at least once. You can then take that list and remove any tests you don't want and use that as the `custom_test_files` path.

**`settings`: `dict`**
The QoS settings and their corresponding ranges of values.
Each setting can consist of 1 or more values.
```json
"settings": {
    "duration_s": [...],
    "datalen_bytes": [...],
    "pub_count": [...],
    "sub_count": [...],
    "reliability": [...],
    "use_multicast": [...],
    "durability": [...],
    "latency_count": [...]
}
```

**`duration_s`: `int`**
How long to run the test for in seconds.

**`datalen_bytes`: `int`**
The length of the data payload in bytes.

**`pub_count`: `int`**
The number of publishers.

**`sub_count`: `int`**
The number of subscribers.

**`reliability`: `boolean`**
The reliability setting for the DDS communication.
`true`: Communication is reliable.
`false`: Communication uses best effort.

**`use_multicast`: `boolean`**
Whether to use multicast for communication or not.
`true`: Use multicast.
`false`: Don't use multicast (use unicast).

**`durability`: `int`**
The durability setting for the DDS communication.
`0`: `VOLATILE` - DDS does not store messages that have already been published.
`1`: `TRANSIENT_LOCAL`: Publishers store data locally so taht late joining subscribers get last published message if publisher is still alive.
`2`: `TRANSIENT`: DDS stores the previously sent messages in main memory.
`3`: `PERSISTENT`: DDS stores previously sent messages in non-volatile memory (disk).

**`latency_count`: `int`**
Between how many packets is a latency measurement taken e.g. `1000` - take a latency measurement after every `1000` packets.

Example `settings`:
```json
"settings": {
    "duration_s": [600],
    "datalen_bytes": [1, 100, 1000, 10000],
    "pub_count": [1, 5, 10],
    "sub_count": [1, 5, 10],
    "reliability": [true, false],
    "use_multicast": [true, false],
    "durability": [0, 1, 2, 3],
    "latency_count": [1000]
}
```
`duration_s` will only take the value of 600 seconds - all tests will have this duration.

`datalen_bytes` will take the values of `1, 100, 1000, 10000` where each test may have one of these values (depending on RCG or PCG mode).

`pub_count` will take values of `1, 5, 10` where each test will have only one of these values (value choice depends on which mode is being used: RCG or PCG).

`sub_count` will take values of `1, 5, 10` where each test will have only one of these values (value choice depends on which mode is being used: RCG or PCG).

`reliability` will take values of `true, false` where each test will have only one of these values (value choice depends on which mode is being used: RCG or PCG).

`use_multicast` will take values of `true, false` where each test will have only one of these values (value choice depends on which mode is being used: RCG or PCG).

`durability` will take values of `0, 1, 2, 3` where each test will have only one of these values (value choice depends on which mode is being used: RCG or PCG).

`latency_count` will take values of `1000` where each test will have only one of these values (value choice depends on which mode is being used: RCG or PCG).

**`machines`: `list`**
List of the machines to interact with and control to run tests on.
Each `machine` will have the following items:
```json
{
    "name": ...,
    "host": ...,
    "ssh_key": ...,
    "username": ...,
    "home_dir": ...,
    "perftest": ...,
    "participant_allocation": ...
}
```

**`machine` - `name`: `string`**
The name of the machine to interact with and control to run tests on - can be whatever you want.

**`machine` - `host`: `string`**
The host IP address of the machine.

**`machine` - `ssh_key`: `string`**
The filepath to the private RSA SSH key to use for authentication.

**`machine` - `username`: `string`**
The username to use for SSH authentication.

**`machine` - `home_dir`: `string`**
The home directory of the user on the machine.

**`machine` - `perftest`: `string`**
The path to the perftest executable on the machine.

**`machine` - `participant_allocation`: `string`**
The participant allocation for the machine which can be `all`, `pub`, or `sub`.
`all`: Both publishers and subscribers can be deployed on this machine.
`pub`: Only publishers can be deployed on this machine.
`sub`: Only subscribers can be deployed on this machine.

Example `machines`:
```json
"machines": [
    {
        "name": "p1",
        "fake_ip": "203.0.113.1",
        "ssh_key": "/home/my_username/.ssh/my_rsa_key_file",
        "username": "my_username",
        "home_dir": "/home/my_username/",
        "perftest": "/home/my_username/srcCpp/objs/armv7Linux4gcc7.5.0/perftest_publisher",
        "participant_allocation": "pub"
    },
    {
        "name": "p2",
        "host": "122.13.12.243",
        "ssh_key": "/home/my_username/.ssh/my_rsa_key_file",
        "username": "my_username",
        "home_dir": "/home/my_username/",
        "perftest": "/home/my_username/srcCpp/objs/armv7Linux4gcc7.5.0/perftest_publisher",
        "participant_allocation": "sub"
    }
]
```

# Preset Combination Generation (PCG)
This works by applying the cartesian product to all the setting values and generating these combinations as separate tests. 

## Example
Let's say we have the following settings:
```json
"settings": {
    "duration_s": [600],
    "datalen_bytes": [100],
    "pub_count": [1, 25],
    "sub_count": [1, 25],
    "reliability": [true],
    "use_multicast": [true],
    "durability": [0],
    "latency_count": [100]
}
```
If we apply the cartesian product we get the following combinations:
- `600SEC_100B_1P_1S_REL_MC_0DUR_100LC`
- `600SEC_100B_1P_25S_REL_MC_0DUR_100LC`
- `600SEC_100B_25P_1S_REL_MC_0DUR_100LC`
- `600SEC_100B_25P_25S_REL_MC_0DUR_100LC`

# Random Combination Generation (RCG)
This works by taking settings with 2 values defined and picking a random value between the two values as the chosen value for the setting.

## Example
Let's say we have the following settings:
```json
"settings": {
    "duration_s": [600],
    "datalen_bytes": [100, 128_000],
    "pub_count": [1, 25],
    "sub_count": [1, 25],
    "reliability": [true, false],
    "use_multicast": [true, false],
    "durability": [0, 3],
    "latency_count": [100]
}
```
For `datalen_bytes` we pick a random integer between `100` and `128,000`, let's say `34,353`.
For `pub_count` we pick a random integer between `1` and `10`, let's say `7`.
For `sub_count` we pick a random integer between `1` and `10`, let's say `3`.
For `reliability` we pick either one of the `true`, `false` values let's say `false`.
For `use_multicast` we pick either one of the `true`, `false` values let's say `true`.
For `durability` we pick a random integer between `0` and `3`, let's say `2`.

We then generate the test using those settings: `600SEC_34353B_7P_3S_BE_MC_2DUR_100LC`.

We generate random values for the tests up to `random_combination_count` tests.