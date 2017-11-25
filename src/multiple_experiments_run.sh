#!/usr/bin/env bash


# Proposed policy evaluation
mkdir -p /var/exp/control_90
rm /var/exp/control_90/*.json
python ./run_workloads.py 20 act
mv /var/exp/172*.json /var/exp/control_90/
sleep 10

mkdir -p /var/exp/control_180
rm /var/exp/control_180/*.json
python ./run_workloads.py 45 act
mv /var/exp/172*.json /var/exp/control_180/
sleep 10

mkdir -p /var/exp/control_270
rm /var/exp/control_270/*.json
python ./run_workloads.py 70 act
mv /var/exp/172*.json /var/exp/control_270/
sleep 2


# Default policy evaluation
mkdir -p /var/exp/uncontrol_90
rm /var/exp/uncontrol_90/*.json
python ./run_workloads.py 20 deact
mv /var/exp/172*.json /var/exp/uncontrol_90/

sleep 10
mkdir -p /var/exp/uncontrol_180
rm /var/exp/uncontrol_180/*.json
python ./run_workloads.py 45 deact
mv /var/exp/172*.json /var/exp/uncontrol_180/
sleep 10

mkdir -p /var/exp/uncontrol_270
rm /var/exp/uncontrol_270/*.json
python ./run_workloads.py 70 deact
mv /var/exp/172*.json /var/exp/uncontrol_270/

sleep 10


python ./after_experiment_statistics.py ops
python ./after_experiment_statistics.py latency_temp
python ./after_experiment_statistics.py latency_final

cd /var/exp

tar cvf results.tar ./*_*
