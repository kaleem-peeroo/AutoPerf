#!/bin/bash

shopt -s expand_aliases

alias python='python3'
alias check3="python ~/Documents/software_development/PTST/monitor.py 10.210.35.27 3Pi '/home/acwh025/Documents/PTST' '/Users/kaleem/.ssh/id_rsa'"
alias check5="python ~/Documents/software_development/PTST/monitor.py 10.210.58.126 5Pi '/home/acwh025/Documents/PTST' '/Users/kaleem/.ssh/id_rsa'"
alias checkall="check3; check5;"

while true; do
    clear
    checkall
    sleep 30
done