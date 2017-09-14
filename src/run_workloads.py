#!/usr/bin/env python
from gevent import monkey
monkey.patch_all()


import sys
import json
from multiprocessing import Process

import os; os.environ.setdefault("DJANGO_SETTINGS_MODULE", "control_server.settings"); import django; django.setup()
from performance.views import *
from experiment.views import *

from aerospike_controller import AerospikeClient as ASC
import remote_tasks


def aerospike_cluster_cleaning(aerospike_servers):
    result = ''
    print('TRUNCATING AEROSPIKE FOR STARTING NEW EXPERIMENT')
    aerospike_server = aerospike_servers[0]
    aerospike_server_user = aerospike_server.user
    aerospike_server_password = aerospike_server.password
    aerospike_server_ip = get_connectable_ip(aerospike_server)
    host = '{}@{}'.format(aerospike_server_user, aerospike_server_ip)
    command = '''asinfo -v "truncate:namespace=test;set=demo;"'''
    remote_tasks.execute_remote_command_using_fabric(host, aerospike_server_password, command, True)
    return result


def get_connectable_ip(virtual_machine_object):
    if virtual_machine_object.public_ip in ['0.0.0.0', 'NA']:
        return virtual_machine_object.private_ip
    else:
        return virtual_machine_object.public_ip


def run_workloads_at_client(aerospike_client, number_of_concurrent_runs, config, workloads, client_update):
    workload_name = workloads[0]['workload_name']
    remote_json_path = "./{}_experiments_transferred.json".format(workload_name)
    local_client_path = "./aerospike_controller.py"
    remote_client_path = "./aerospike_controller.py"
    aerospike_client_ip = get_connectable_ip(aerospike_client)
    json_path = "/tmp/experiments_{}_{}.json".format(aerospike_client_ip, workload_name)

    aerospike_client_user = aerospike_client.user
    aerospike_client_password = aerospike_client.password

    json_information = {'config_hosts': config['hosts'],
                      'workloads' : list(workloads),
                      'aerospike_client_ip' : aerospike_client_ip,
                      }
    json.dump(json_information, open(json_path, "wb"))

    host = '{}@{}'.format(aerospike_client_user,aerospike_client_ip)
    command_path='python {} '.format(remote_client_path)
    command_arguments=" {} {}".format(number_of_concurrent_runs,remote_json_path)
    command = command_path+command_arguments
    if client_update:
        remote_tasks.file_transfer_using_fabric(host,aerospike_client_password, local_client_path, remote_client_path)
    remote_tasks.file_transfer_using_fabric(host,aerospike_client_password, json_path, remote_json_path)
    remote_tasks.execute_remote_command_using_fabric(host, aerospike_client_password, command, False)


if __name__ == "__main__":
    # Get active controlling server from Evaluation framework
    active_aerospike_servers = get_aerospike_server_list(status='05')
    active_aerospike_clients = get_aerospike_client_list(status='05')

    config = ASC.build_aerospike_client_config(active_aerospike_servers)

    experiment_id = get_valid_experiment_id()

    # Initialize the Aerospike cluster for clean start
    aerospike_cluster_cleaning(aerospike_servers=active_aerospike_servers)

    print('Running experiment id: {}'.format(experiment_id))
    number_of_concurrent_runs = int(sys.argv[1])
    # number_of_concurrent_runs is the number of clients in each controlling server in proxy-tier

    total_experiment_workloads = get_workloads(experiment_id)
    qos_concurrent_runs = []

    qos_scenarios = [3, 3, 4]
    for qs in qos_scenarios:
        qos_concurrent_run = int(round(number_of_concurrent_runs*float(qs)/sum(qos_scenarios)))
        qos_concurrent_runs.append(qos_concurrent_run)

    multi_processes = []

    #Run Experiment at each controlling server in proxy-tier
    for active_aerospike_client in active_aerospike_clients:
        i=0
        for qos_concurrent_run in qos_concurrent_runs:
            print(total_experiment_workloads[i])
            print(qos_concurrent_run)
            if i == 0:
                client_update = True
            else:
                client_update = False

            p = Process(target=run_workloads_at_client, args=(active_aerospike_client, qos_concurrent_run, config, total_experiment_workloads[i], client_update))
            multi_processes.append(p)
            p.start()
            i += 1

        print('########## Process launched for active Aerospike client for:{}, number_concurrent_runs:{}'.format(active_aerospike_client, number_of_concurrent_runs))

    for p in multi_processes:
        p.join()

    #Collect result data from clients for analyzing the experiment result
    for active_aerospike_client in active_aerospike_clients:
        aerospike_client_ip = get_connectable_ip(active_aerospike_client)
        host = '{}@{}'.format(active_aerospike_client.user, aerospike_client_ip)
        remote_path_to_transfer = '/home/ubuntu/experiment_results/*.json'
        try:
            remote_tasks.file_transfer_using_fabric(host=host, password=active_aerospike_client.password,
                                                    local_path='/home/ubuntu/experiment_results/',
                                                    remote_path=remote_path_to_transfer,
                                                    transfer_type='get',
                                                    )
        except Exception as e:
            print('Exception occurred when file transferring: {}'.format(e.__str__()))

        try:
            remote_tasks.execute_remote_cmd(aerospike_client_ip, active_aerospike_client.user,
                                        active_aerospike_client.password,
                                        'rm {}'.format(remote_path_to_transfer)
                                        )
        except Exception as e:
            print('Exception occurred when deleting: {}'.format(e.__str__()))