!/usr/bin/env bash

# Uncontrolled version of experiment
# Change the parameter "is_controller_active" as "False" of aerospike_controller.py
python ./run_workloads.py 30
mkdir -p /home/ubuntu/experiment_results/uncontrol_90
rm /home/ubuntu/experiment_results/uncontrol_90/*.json
mv /home/ubuntu/experiment_results/172*.json /home/ubuntu/experiment_results/uncontrol_90/
sleep 100

python ./run_workloads.py 60
mkdir -p /home/ubuntu/experiment_results/uncontrol_180
rm /home/ubuntu/experiment_results/uncontrol_180/*.json
mv /home/ubuntu/experiment_results/172*.json /home/ubuntu/experiment_results/uncontrol_180/
sleep 100

python ./run_workloads.py 90
mkdir -p /home/ubuntu/experiment_results/uncontrol_270
rm /home/ubuntu/experiment_results/uncontrol_270/*.json
mv /home/ubuntu/experiment_results/172*.json /home/ubuntu/experiment_results/uncontrol_270/
