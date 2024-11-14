cd ..;
rm -rf output/data/*devtest*;
rm -rf output/summarised_data/*devtest*;
rm -rf output/datasets/*devtest*;
rm -rf output/ess/*devtest*;
python autoperf.py configs/3pi_devtest.json;
