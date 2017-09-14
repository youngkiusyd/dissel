#!/usr/bin/env python
from gevent import monkey
monkey.patch_all()


import os
import sys
import json
import gevent
import aerospike
import time
import numpy
from datetime import datetime
from multiprocessing import Process


class Queue():
    def __init__(self, task_id):
        self.queue = []
        self.task_id = task_id
        self.add_counter = 0
        self.remove_counter = 0
        self.accumulated_time = 0
        self.last_10_operation_latency = []
        self.all_latency = []
        self.ops = []
        self.perf = {}

    def add_workload(self, workload_unit):
        self.queue.append(workload_unit)
        self.add_counter += 1

    def remove_workload(self, delay=0):
        # is_controller_active = False # theta_1=0
        is_controller_active = True    # theta_1=1

        if is_controller_active:
            if delay > 0:
                gevent.sleep(delay)

        if len(self.queue) > 0:
            workload_unit = self.queue.pop(0)
            self.remove_counter += 1
            return workload_unit
        else:
            # print('QUEUE IS EMPTY')
            return None

    def update_latencies(self, time_gap, latency_for_current_operation):
        self.accumulated_time += time_gap
        if latency_for_current_operation > 0:
            if latency_for_current_operation == 0: print(latency_for_current_operation)
            latency = (self.accumulated_time, time_gap, latency_for_current_operation)
            self.all_latency.append(latency)
            if len(self.last_10_operation_latency) == 10:
                self.last_10_operation_latency.pop(0)
            self.last_10_operation_latency.append(latency_for_current_operation)

    def generate_ops(self):
        results = []
        max_time = max(i[0] for i in self.all_latency)
        for time_index in range(0,int(max_time)+2):
            operation_count = 0
            for opr in self.all_latency:
                if time_index <= opr[0] < time_index+1:
                    operation_count += 1
            results.append((time_index+1, operation_count))
        return results

    def update_stats(self, key, value):
        self.perf[key] = value
        # print(self.perf)

    def stats(self):
        result = dict()
        result['queue_len'] = len(self.queue)
        result['10_opr_avg_latency'] = sum(self.last_10_operation_latency[-10:])/10
        return result


class Controller():
    def __init__(self, qos_class, qos_target, alpha):
        self.qos_class = qos_class
        self.qos_target = qos_target
        self.alpha = alpha
        self.control_interval = 10
        self.previous_delay = 0

    def qos_control(self, performance_metrics, operation_counter):
        if operation_counter % self.control_interval == 0:
            error = performance_metrics['10_opr_avg_latency'] - self.qos_target['latency']
            if error > 0:
                delay = self.alpha * error
                self.previous_delay = delay
                return delay
            else:
                return 0
        else:
            return self.previous_delay


class AerospikeClient():
    def __init__(self, config, self_ip, task_id, queue, controller):
        try:
            self.client = aerospike.client(config).connect()
            self.self_ip = self_ip
            self.task_id = task_id
            self.queue = queue
            self.controller = controller
        except:
            print("failed to connect to the cluster with", config['hosts'])
            sys.exit(1)

    def write(self, as_key=None, as_value=None):
        try:
            # Write a record
            self.client.put(as_key, as_value)

        except Exception as e:
            print("error: {0}".format(e))

    # Read a record
    def read(self, as_key):
        (key, metadata, record) = self.client.get(as_key)
        print(key, metadata, record)

    def make_unique_length_key(self, key_value='', unique_length=15):
        return key_value.zfill(unique_length)

    def make_desired_value(self, input_value='input_string', desired_length=100):
        dic_key = ('LENGTH'+str(desired_length)).zfill(10)
        dic_value = input_value.zfill(desired_length)
        return {dic_key: dic_value}

    @staticmethod
    def generate_workloads(workloads):
        total_workloads=[]
        for wlu in workloads:
            if wlu['workload_distribution_type'] != "blank":
                workload_interval_of_unit = AerospikeClient.generate_workload_interval_from_unit(
                                            wlu['workload_distribution_type'],
                                            wlu['workload_distribution_parameters'], wlu['run_length'])

                number_of_iterations = len(workload_interval_of_unit)

                workload_datasize_of_unit = AerospikeClient.generate_workload_datasize_from_unit(
                    wlu['workload_datasize_distribution_type'],
                    wlu['workload_datasize_distribution_parameters'], wlu['workload_datasize_medium_value'], number_of_iterations)

                operation_types = [wlu['workload_operation_type']] * number_of_iterations
            else:
                workload_interval_of_unit=[wlu['run_length'],]
                operation_types=['blank',]
                workload_datasize_of_unit=[100,]

            total_workloads = total_workloads + zip(operation_types, workload_interval_of_unit, workload_datasize_of_unit)
        return total_workloads

    @staticmethod
    def generate_workload_datasize_from_unit(workload_datasize_distribution_type, workload_datasize_distribution_parameters=None,
                                             workload_datasize_medium_value=None, number_of_iterations=0):
        workload_datasizes_distribution = AerospikeClient.get_distributions(
                                                                distribution_type=workload_datasize_distribution_type,
                                                                distribution_parameters=workload_datasize_distribution_parameters,
                                                                run_length=None,
                                                                number_of_iterations=number_of_iterations,
                                                                seeds=None)
        workload_datasizes=workload_datasizes_distribution * workload_datasize_medium_value
        return workload_datasizes

    @staticmethod
    def generate_workload_interval_from_unit(workload_distribution_type, workload_distribution_parameters=None,
                                    workload_run_length=10):
        workload_intervals = AerospikeClient.get_distributions(distribution_type=workload_distribution_type,
                                                               distribution_parameters=workload_distribution_parameters,
                                                               run_length=workload_run_length,
                                                               number_of_iterations=None,
                                                               seeds=None)
        return workload_intervals

    @staticmethod
    def get_distributions(distribution_type, distribution_parameters, run_length=None, number_of_iterations=None, seeds=None):
        distributions = []

        if seeds is not None:
            numpy.random.seed(seed=seeds)
        else:
            numpy.random.seed()

        if distribution_type == 'uniform':
            if distribution_parameters is None:
                distribution_parameters = {"a": 0.001, "b": 0.001}
            if number_of_iterations is None:
                number_of_iterations = int(2 * run_length / (distribution_parameters['a'] + distribution_parameters['b']))
            distributions = numpy.random.uniform(low=distribution_parameters['a'],
                                                     high=distribution_parameters['b'],
                                                     size=number_of_iterations)
        elif distribution_type == 'exponential':
            if distribution_parameters is None:
                distribution_parameters = {'lambda': 1} # lambda: rate 1 means 1 milliseconds

            number_of_iterations = int(1000 * run_length / distribution_parameters['lambda'])
            scale = 1 / distribution_parameters['lambda']  # divided 1000 to convert millisecond to second
            unscaled_distributions = numpy.random.exponential(scale=scale, size=number_of_iterations)
            distributions = [di/1000 for di in unscaled_distributions]
        elif distribution_type == 'weibull':
            if distribution_parameters is None:
                distribution_parameters = {'shape': 1} # shape parameter represents weibull shape
            shape = distribution_parameters['shape']
            number_of_iterations = AerospikeClient.get_number_of_iteration_of_weibull_distribution(run_length, shape)
            unscaled_distributions = numpy.random.weibull(shape, number_of_iterations)
            distributions = [x/1000 for x in unscaled_distributions]
        else:
            return distributions
        return distributions

    @staticmethod
    def get_number_of_iteration_of_weibull_distribution(run_length=0, shape=1):
        weibull_distribution = numpy.random.weibull(shape, 100000)
        mean_value = numpy.mean(weibull_distribution)
        number_of_iterations = int(1000 * run_length / mean_value) #calculate number of iterations, multiply 1000
        return number_of_iterations

    def operate_workload_unit(self, database_operation_type='read', workload_interval=None, data=None):
        if database_operation_type == 'write':
            workload_data_key, workload_data_value = data
            operation_start_time = datetime.now()
            self.write(workload_data_key, workload_data_value)
            operation_end_time = datetime.now()
            unit_operation_time = operation_end_time - operation_start_time
            return unit_operation_time.total_seconds()
        elif database_operation_type == 'blank':
            return 0

    def build_workload_data(self, total_workloads, task_id, qos_class):
        final_total_workload = []
        for i in range(len(total_workloads)):
            workload_operation_type = total_workloads[i][0]
            workload_interval = total_workloads[i][1]
            workload_datasize = int(total_workloads[i][2])
            if workload_operation_type == 'write':
                unique_value = '{}-Q{}T{}W{}O{}'.format(client_ip, str(qos_class), str(task_id), workload_datasize, str(i))
                as_key = self.make_unique_length_key(unique_value, 50)
                # print(as_key)
                full_as_key = ('test', 'demo', as_key)
                as_value = self.make_desired_value(str(unique_value), workload_datasize)
            elif workload_operation_type == 'blank':
                full_as_key = ('test', 'demo', 'BLANK')
                as_value = 'BLANK'
            final_total_workload.append((workload_operation_type, workload_interval, (full_as_key, as_value)))

        return final_total_workload

    def write_result_to_file(self, workload_name, json_input):
        base_dir = '{}/experiment_results'.format(os.environ['HOME'])
        if not os.path.isdir(base_dir):
            os.system('mkdir -p {}'.format(base_dir))
        output_file_name = '{}/{}_{}_{}.json'.format(base_dir, self.self_ip, workload_name,  self.task_id)
        with open(output_file_name, 'w') as outfile:
            json.dump(json_input, outfile)

    def run_workloads(self, total_workloads, task_id, workload_name, qos_class):
        start_time = datetime.now()
        print('Workload Started, Name:{} TASK_ID: {} , Time: {}'.format(workload_name, task_id, start_time))
        operation_times = []
        final_total_workloads = self.build_workload_data(total_workloads, task_id, qos_class)
        total_operation_count = len(final_total_workloads)
        total_operation_time = 0
        sum_of_interval = 0
        total_elapsed_time = 0
        elasped_time = 0
        workload_start_time = datetime.now()
        loop_count=0
        inner_loop_count=0
        operation_counter =0

        while True:
            loop_start_time = datetime.now()
            performance_metrics = queue.stats()
            time.sleep(0.0000001)
            if len(final_total_workloads) == 0 and performance_metrics['queue_len'] == 0:
                break
            # Add newly arrived workload to the Queue based on elapsed time
            while sum_of_interval <= total_elapsed_time:
                if len(final_total_workloads) == 0:
                    break
                new_workload = final_total_workloads.pop(0)
                queue.add_workload(new_workload)
                sum_of_interval += new_workload[1]
                inner_loop_count += 1

            if performance_metrics['queue_len'] > 0:
                operation_counter += 1
                delay = self.controller.qos_control(performance_metrics=performance_metrics, operation_counter=operation_counter)
                waiting_workload = queue.remove_workload(delay=delay)
                operation_time = self.operate_workload_unit(waiting_workload[0], waiting_workload[1], waiting_workload[2])
                total_operation_time += operation_time
                operation_times.append(operation_time)
            else:
                operation_time = 0

            loop_end_time = datetime.now()
            elasped_time = (loop_end_time - loop_start_time).total_seconds()
            queue.update_latencies(time_gap=elasped_time, latency_for_current_operation=operation_time)
            total_elapsed_time += elasped_time
            loop_count += 1

        workload_end_time = datetime.now()
        total_loop_time = (workload_end_time - workload_start_time).total_seconds()
        average_operation_time = total_operation_time / total_operation_count * 1000000
        print('END OF EXPERIMENT | Workload_name:{}, Task_id:{}, total_loop_time: {}, total_elapsed_time:{} ,OPS:{}, AOT:{}'.format(workload_name, task_id, total_loop_time, total_elapsed_time, int(total_operation_count/total_loop_time),int(average_operation_time)))
        final_sleeping_time = 360 - total_loop_time
        print('Sleeping {} seconds'.format(final_sleeping_time))
        time.sleep(final_sleeping_time)
        result = {'operation_times_in_microsecond': self.queue.all_latency,
                'operation_per_second': self.queue.generate_ops(),
                'workload_starttime': workload_start_time.isoformat(),
                'workload_endtime': workload_end_time.isoformat(),
                'task_id': task_id,
                'client_ip': self.self_ip,
                'workload_name': workload_name
                }
        print('Saving experiment results')
        self.write_result_to_file(workload_name, result)
        exit(1)

    @staticmethod
    def build_aerospike_client_config(aerospike_server_object, aerospike_server_valid_ip_list=None):
        config={}
        aerospike_port=3000
        if aerospike_server_valid_ip_list is None:
            config['hosts']=[(aerospike_server.private_ip if aerospike_server.public_ip == 'NA' else aerospike_server.public_ip,
                             aerospike_port) for aerospike_server in aerospike_server_object]
        else:
            config['hosts']=[(aerospike_server_ip,
                             aerospike_port) for aerospike_server_ip in aerospike_server_valid_ip_list]
        return config

    def close_connection(self):
        self.client.close()


if __name__ == "__main__":
    number_of_concurrent_runs = int(sys.argv[1])
    experiment_json = json.load(open(sys.argv[2], "rb"))
    config_hosts = experiment_json['config_hosts']
    config = AerospikeClient.build_aerospike_client_config('empty_aerospike_server_object', [i[0] for i in config_hosts])
    workloads = experiment_json['workloads']
    workload_name = workloads[0]['workload_name']
    client_ip = experiment_json['aerospike_client_ip']
    start_time = datetime.now()
    gevent_spawn_list = []
    total_workloads_count = 0
    total_expected_time = 0

    for task_id in range(0, number_of_concurrent_runs):
        active_aerospike_client = task_id
        total_workloads = AerospikeClient.generate_workloads(workloads=workloads)
        total_workloads_count += len(total_workloads)
        expected_time = sum(ii[1] for ii in total_workloads)
        total_expected_time += expected_time

        queue = Queue(task_id)

        qos_targets = {1: {'latency': 0.060},
                       2: {'latency': 0.010},
                       3: {'latency': 0.0015}
                       }
        # p_max = 10
        # p_i in {9.999, 9, 5}
        # theta_1 = 1
        alphas = {1: 0.001,
                  2: 1,
                  3: 5}

        qos_class = int(workload_name[-1])
        controller = Controller(qos_class=qos_class, qos_target=qos_targets[qos_class], alpha=alphas[qos_class])
        asc = AerospikeClient(config, client_ip, task_id, queue, controller)
        Process(target=asc.run_workloads, args=(total_workloads, active_aerospike_client, workload_name, qos_class)).start()
        print('######## Launched new process for task number: {}'.format(active_aerospike_client))
