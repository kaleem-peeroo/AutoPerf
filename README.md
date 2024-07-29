# Table of contents
1. [TODO](#todo)
2. [Introduction](#introduction)
3. [Features](#features)
4. [Terminology](#terminology)
5. [Examples](#examples)
   1. [QoS Config Dict](#qos-config-dict)
6. [System Story](#system-story)
7. [Configuration](#configuration)
8. [Experiment Status Spreadsheet (ESS)](#experiment-status-spreadsheet-(ess))
9. [Deprecated Content](#deprecated-content)
10. [User Story](#user-story)
11. [How does the current system work?](#how-does-the-current-system-work?)

# TODO
- [ ] Rewrite entire readme

# Introduction

I want to improve and revamp Autoperf (AP). So I’m going to plan out everything here and record things as they go along.

Let’s start off with the purpose of AP:

- [ ]  Run lots of perftest tests automatically without minimal (if any) human input
- [ ]  Gather lots of juicy performance data

What are the extra things that AP should be able to do?

- [ ]  Record problematic tests (tests that don’t produce data for any reason)
- [ ]  Automatically deal with cases where the machines don’t respond for a while
- [ ]  Provide an interface to keep track of what tests are happening and which tests have been successful so far and which ones have failed
- [ ]  Deal with the situation where several consecutive tests have failed - the machines could be off
- [ ]  Notify remotely when something has gone wrong
- [ ]  Continue a previous test campaign if it was interrupted
- [ ]  Rerun tests up to 3 times just in case something went wrong that isn"t related to the test itself (e.g. can"t access the slave machines)


# Features

- [ ] 🔃 Retry failed tests x times before moving on to next test.
- [ ] 🗂️ Automatically compress test data after each test.
- [ ] 💿 Store test statuses in a spreadsheet for easy monitoring.

# Terminology

Tests refer to Perftest tests.

Experiments (formerly campaigns) refer to AP experiments where 1 AP experiment can contain many Perftest tests.

ESS stands for Experiment Status Spreadsheet and is a csv file containing details about the run of each test. More details [here](#experiment-status-spreadsheet-ess).

# Examples

## QoS Config Dict
```python
{
    "duration_secs": [30],
    "datalen_bytes": [100],
    "pub_count": [1, 50, 100],
    "sub_count": [1, 50, 100],
    "use_reliable": [true, false],
    "use_multicast": [true, false],
    "durability_level": [0, 1, 2, 3],
    "latency_count": [100]
}
```

## Machine Config Dict
```python
{
    "ip": "169.254.248.55",
    "machine_name": "p1",
    "participant_allocation": "pub",
    "perftest_exec_path": "~/Documents/rtiperftest/srcCpp/objs/armv7Linux4gcc7.5.0/perftest_publisher",
    "ssh_key_path": "~/.ssh/id_rsa",
    "username": "acwh025"
}
```

# System Story

This is an overview of how the system will work from start to finish.

1. Validate connections to machines in config.
2. For each experiment:
    1. If PCG:
        1. Generate all possible combinations.
        2. Order them.
    2. Check for ESS.
    3. If ESS does exist:
        1. Find last successful test.
        2. Set PCG next test to be the next combination.
        3. Match tests that have run with test folders that exist.
            - Make sure that successful tests that have run, have existing data.
    4. If ESS does NOT exist:
        1. Make one.
        2. Set PCG next test to be first combination.
    5. If RCG: 
        1. Generate new combination.
        2. Check if combination already exists in ESS.
        3. If combination exists:
            1. Go back to step 4.5.1.
    6. Start timer.
    7. Record start time, test name, pings count, ssh check count, and attempt # into ESS.
    8. Start executing test.
    9. Finish running test.
    10. Get end timestamp.
    11. Find row in ESS for that test.
    12. Record end timestamp into ESS.
    13. Create directory for test.
    14. Move pub.csv and sub_n.csv files to directory.
    15. If last 15 tests have failed:
        - Stop program.
3. Compress experiment folder.

# Configuration

What do we need to store?

- [ ] Experiment Details
    - [ ] Experiment Name
    - [ ] QoS Configuration
    - [ ] PCG or RCG
    - [ ] Slave machine details
        - [ ] IP
        - [ ] SSH key filepath
        - [ ] Username
        - [ ] perftest executable filepath
        - [ ] Participant Allocation

RCG Example:
```json
[{
    "experiment_name": "RCG #1",
    "combination_generation_type": "rcg",
    "qos_settings": {
        "duration_secs": [30],
        "datalen_bytes": [100],
        "pub_count": [1, 100],
        "sub_count": [1, 100],
        "use_reliable": [true, false],
        "use_multicast": [true, false],
        "durability_level": [0, 1, 2, 3],
        "latency_count": [100]
    },
    "slave_machines": [
        {
            "machine_name": "p1",
            "participant_allocation": "pub",
            "ip": "169.254.248.55",
            "ssh_key_path": "~/.ssh/id_rsa",
            "username": "acwh025",
            "perftest_exec_path": "~/Documents/rtiperftest/srcCpp/objs/armv7Linux4gcc7.5.0/perftest_publisher"
        },
        {
            "machine_name": "p2",
            "participant_allocation": "sub",
            "ip": "169.254.201.141",
            "ssh_key_path": "~/.ssh/id_rsa",
            "username": "acwh025",
            "perftest_exec_path": "~/Documents/rtiperftest/srcCpp/objs/armv7Linux4gcc7.5.0/perftest_publisher"
        }
    ]
}]
```
    
PCG Example:
```json
[{
    "experiment_name": "PCG #1",
    "combination_generation_type": "pcg",
    "qos_settings": {
        "duration_secs": [30],
        "datalen_bytes": [100],
        "pub_count": [1, 50, 100],
        "sub_count": [1, 50, 100],
        "use_reliable": [true, false],
        "use_multicast": [true, false],
        "durability_level": [],
        "latency_count": [100]
    },
    "slave_machines": [
        {
            "machine_name": "p1",
            "participant_allocation": "pub",
            "ip": "169.254.248.55",
            "ssh_key_path": "~/.ssh/id_rsa",
            "username": "acwh025",
            "perftest_exec_path": "~/Documents/rtiperftest/srcCpp/objs/armv7Linux4gcc7.5.0/perftest_publisher"
        },
        {
            "machine_name": "p2",
            "participant_allocation": "sub",
            "ip": "169.254.201.141",
            "ssh_key_path": "~/.ssh/id_rsa",
            "username": "acwh025",
            "perftest_exec_path": "~/Documents/rtiperftest/srcCpp/objs/armv7Linux4gcc7.5.0/perftest_publisher"
        }
    ]
}]
```

# Experiment Status Spreadsheet (ESS)

ESS stands for Experiment Status Spreadsheet and is a csv file containing details about the run of each test.

It contains the following columns:
- start timestamp
- end timestamp
- test name
- pings count
- ssh check count
- end status
- qos settings
- comments

# Deprecated Content

# User Story

These are the general steps that take place when using AP:

1. Define experimental configurations.
2. Run AP.
3. Get notified if something goes wrong.

# How does the current system work?

1. Read config file and buffer duration in seconds from command line arguments.
2. For each campaign:
    1. Generate combinations.
    2. For each combination:
        1. Generate scripts for combination.
        2. Distribute scripts across machines.
        3. Check last 10 tests for failures.
            1. If last 10 tests have failed then stop the application.
            2. If last 10 tests have not ALL failed then continue.
        4. For each machine:
            1. Ping machine.
            2. Check SSH connection to machine.
        5. For each machine:
            1. Restart machine.
            2. Ping every other machine.
            3. SSH check every other machine.
            4. tbc...

# New things to add

- [ ] Set buffer duration straight from config file. Only thing being passed to the application is the config file.
- [ ] Add functionality to store and manipulate test statuses from the [ESS](#experiment-status-spreadsheet-ess).

