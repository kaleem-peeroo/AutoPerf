# Introduction

I want to improve and revamp Autoperf (AP). So I’m going to plan out everything here and record things as they go along.

Let’s start off with the purpose of AP:

- [ ]  Run lots of perftest tests automatically without minimal (if any) human input
- [ ]  Gather lots of juicy performance data

What are the extra things that AP should be able to do?

- [ ]  Record problematic tests (tests that don’t produce data for any reason)
- [ ]  Automatically deal with cases where the machines don’t respond for a while
- [ ]  Provide an interface to keep track of what tests are happening and which tests have been successful so far and which ones have failed

# Features

- [ ] 🔃 Retry failed tests x times before moving on to next test.
- [ ] 🗂️ Automatically compress test data after each test.
- [ ] 💿 Store test statuses in a spreadsheet for easy monitoring.

# Terminology

Tests refer to Perftest tests.

Experiments refer to AP tests where 1 AP test (1 experiment) can contain many Perftest tests (many tests).

# User Story

These are the general steps that take place when using AP:

1. Define experimental configurations.
2. Run AP.

# System Story

This is how AP generally works in terms of a sequence of steps.

1. Validate configs.
2. Validate connections to machines in config.
3. 
