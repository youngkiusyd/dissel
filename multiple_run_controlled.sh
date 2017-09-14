!/usr/bin/env bash


# Controlled version of experiment
# Change the parameter "is_controller_active" as "True" of aerospike_controller.py

mkdir -p /home/ubuntu/experiment_results/control_90
rm /home/ubuntu/experiment_results/control_90/*.json
python ./run_workloads.py 30
mv /home/ubuntu/experiment_results/172*.json /home/ubuntu/experiment_results/control_90/
sleep 100

mkdir -p /home/ubuntu/experiment_results/control_180
rm /home/ubuntu/experiment_results/control_180/*.json
python ./run_workloads.py 60
mv /home/ubuntu/experiment_results/172*.json /home/ubuntu/experiment_results/control_180/
sleep 100

mkdir -p /home/ubuntu/experiment_results/control_270
rm /home/ubuntu/experiment_results/control_270/*.json
python ./run_workloads.py 90
mv /home/ubuntu/experiment_results/172*.json /home/ubuntu/experiment_results/control_270/
