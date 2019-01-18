#!/usr/bin/env python3
import os
import sys
import unittest
import traceback
import subprocess
import re
import time
from typing import List, Any


def import_all_packages():
    realpath=os.path.realpath(__file__)
    #print("os.path.realpath({})={}".format(__file__,realpath))
    dirname=os.path.dirname(realpath)
    #print("os.path.dirname({})={}".format(realpath,dirname))
    dirname_list=dirname.split('/')
    #print(dirname_list)
    for index in range(len(dirname_list)):
        module_path='/'.join(dirname_list[:index])
        #print("module_path={}".format(module_path))
        try:
            sys.path.append(module_path)
        except:
            #print("Invalid module path {}".format(module_path))
            pass

import_all_packages()

from infrastructure_components.log.log_file import logging_to_console_and_syslog
from tier3.auto_scaler.auto_scaler import AutoScaler
from infrastructure_components.redis_client.redis_interface import RedisInterface

class TestAutoScaler(unittest.TestCase):
    service_to_be_tested = 'briefcam_machine_learning_workers'

    def setUp(self):
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["total_job_enqueued_count_redis_name_key"] = "enqueue"
        os.environ["total_job_dequeued_count_redis_name_key"] = "dequeue"
        os.environ["redis_server_hostname_key"] = "localhost"
        os.environ["redis_server_port_key"] = "6379"
        os.environ["min_threshold_key"] = "1"
        os.environ["max_threshold_key"] = "100"
        os.environ["auto_scale_time_interval_key"] = "10"
        os.environ["auto_scale_service_name_key"] = TestAutoScaler.service_to_be_tested
        self.__create_docker_stack()
        self.auto_scaler = None
        time.sleep(30)

    def __create_docker_stack(self):
        completedProcess = subprocess.run(["docker",
                                           "stack",
                                           "deploy",
                                           "-c",
                                           "docker-compose-confluent-kafka.yml",
                                           "briefcam"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        logging_to_console_and_syslog(completedProcess.stdout.decode('utf8'))

    def test_auto_scaler(self):
        self.auto_scaler = AutoScaler()

    def __tear_down_docker_stack(self):
        completedProcess = subprocess.run(["docker",
                                           "stack",
                                           "rm",
                                           "briefcam"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        logging_to_console_and_syslog(completedProcess.stdout.decode('utf8'))

    def tearDown(self):
        self.__tear_down_docker_stack()


if __name__ == "__main__":
    try:
        unittest.main()
    except:
        logging_to_console_and_syslog("Exception occurred.{}".format(sys.exc_info()))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
