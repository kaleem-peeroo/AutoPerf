# PS
python index.py configs/3Pi/qos_combination_capture_100B_PS.json 1.5 -skip-confirmation |& tee qos_combination_capture_100B_PS_output.txt
python index.py configs/3Pi/qos_combination_capture_1KB_PS.json 1.5 -skip-confirmation |& tee qos_combination_capture_1KB_PS_output.txt
python index.py configs/3Pi/qos_combination_capture_32KB_PS.json 1.5 -skip-confirmation |& tee qos_combination_capture_32KB_PS_output.txt
python index.py configs/3Pi/qos_combination_capture_64KB_PS.json 1.5 -skip-confirmation |& tee qos_combination_capture_64KB_PS_output.txt
python index.py configs/3Pi/qos_combination_capture_128KB_PS.json 1.5 -skip-confirmation |& tee qos_combination_capture_128KB_PS_output.txt

# AA
# python index.py configs/3Pi/qos_combination_capture_100B_PS.json 1.5 |& tee qos_combination_capture_100B_PS_output.txt
# python index.py configs/3Pi/qos_combination_capture_1KB_PS.json 1.5 |& tee qos_combination_capture_1KB_PS_output.txt
# python index.py configs/3Pi/qos_combination_capture_32KB_PS.json 1.5 |& tee qos_combination_capture_32KB_PS_output.txt
# python index.py configs/3Pi/qos_combination_capture_64KB_PS.json 1.5 |& tee qos_combination_capture_64KB_PS_output.txt
# python index.py configs/3Pi/qos_combination_capture_128KB_PS.json 1.5 |& tee qos_combination_capture_128KB_PS_output.txt