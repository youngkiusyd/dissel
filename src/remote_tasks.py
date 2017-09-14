import os
import paramiko
import subprocess
from fabric.api import *


def execute_remote_command_using_fabric(host, password, command, do_sudo, do_hide=False):
    env.host_string = host
    env.password = password
    if do_hide:
        with hide('output', 'running', 'warnings'), settings(warn_only=True):
            if do_sudo:
                return sudo(command)
            else:
                return run(command)
    else:
        if do_sudo:
            return sudo(command)
        else:
            return run(command)


def file_transfer_using_fabric(host, password, local_path, remote_path, transfer_type='put'):
    env.host_string = host
    env.password = password
    if '~' in local_path:
        local_path = os.path.expanduser(local_path)
    if transfer_type == 'put':
        put(local_path, remote_path)
    elif transfer_type == 'get':
        if not os.path.isdir(local_path):
            print("Path does not exist!! making the required directory: {}".format(local_path))
            execute_local_cmd('mkdir -p {}'.format(local_path))
        get(remote_path, local_path)


def execute_local_cmd(cmd, key_strokes_file="", debug=True):
    # print(cmd)
    if debug:
        print("EXECUTING [[ {} ]]".format(cmd))
    if key_strokes_file != "":
        cmd = cmd + "<" + key_strokes_file
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        output = e.output
    except Exception as e:
        output = e.__str__()
    # print(output)
    return output


def execute_remote_cmd(ip=None, user_id=None, pw=None, command=None, debug=True):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username=user_id, password=pw)
    stdin, stdout, stderr = client.exec_command(command)
    result = list()

    for line in stdout:
        if debug:
            print(line.strip('\n'))
        result.append(line.strip('\n'))
    for line in stdin:
        if debug:
            print(line.strip('\n'))
    for line in stderr:
        if debug:
            print(line.strip('\n'))
    client.close()
    return result
