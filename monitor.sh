#!/bin/bash

shopt -s expand_aliases

alias python='python3'
alias check3="python ~/Documents/software_development/PTST/monitor.py 10.210.35.27 3Pi '/home/acwh025/Documents/PTST' '/Users/kaleem/.ssh/id_rsa'"
alias check5="python ~/Documents/software_development/PTST/monitor.py 10.210.58.126 5Pi '/home/acwh025/Documents/PTST' '/Users/kaleem/.ssh/id_rsa'"
alias checkall="check3; check5;"

if [[ $1 =~ ^[0-9]+$ ]]; then
    arg=$1
else
    echo "Please provide a valid integer argument for the duration between checks."
    exit 1
fi

while true; do
    clear
    checkall
    sleep $1
done