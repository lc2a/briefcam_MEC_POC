#!/usr/bin/env python3
import os
import sys
import unittest
import traceback
import subprocess
import re
import time
import threading

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
from infrastructure_components.docker.docker_service import DockerService
from infrastructure_components.build_ut_push_docker_image.docker_api_interface import DockerAPIInterface


class TestAutoScaler(unittest.TestCase):
    service_to_be_tested = 'machine_learning_workers'
    service_name = 'test_auto_scaler'
    sleep_time = 5
    min_threshold = 1
    max_threshold = 100
    retry_attempts = 3
    total_number_of_iterations = 3
    def setUp(self):
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["total_job_enqueued_count_redis_name_key"] = "enqueue"
        os.environ["total_job_dequeued_count_redis_name_key"] = "dequeue"
        os.environ["redis_server_hostname_key"] = "localhost"
        os.environ["redis_server_port_key"] = "6379"
        os.environ["min_threshold_key"] = str(TestAutoScaler.min_threshold)
        os.environ["max_threshold_key"] = str(TestAutoScaler.max_threshold)
        os.environ["auto_scale_time_interval_key"] = str(TestAutoScaler.sleep_time)
        os.environ["auto_scale_service_name_key"] = TestAutoScaler.service_to_be_tested
        self.__create_docker_stack()
        self.auto_scaler = None

    @staticmethod
    def run_auto_scaler():
        logging_to_console_and_syslog("Instantiating AutoScaler...")
        auto_scaler = AutoScaler()
        logging_to_console_and_syslog("Shutting down AutoScaler...")

    def __create_docker_stack(self):
        completedProcess = subprocess.run(["docker",
                                           "stack",
                                           "deploy",
                                           "-c",
                                           "docker-compose.yml",
                                           TestAutoScaler.service_name],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        logging_to_console_and_syslog(completedProcess.stdout.decode('utf8'))
        time.sleep(30)

    def create_auto_scaler_thread(self):
        self.auto_scaler_thread = threading.Thread(name="{}{}".format("thread", 1),
                                                  target=TestAutoScaler.run_auto_scaler
                                                  )
        self.auto_scaler_thread.do_run = True
        self.auto_scaler_thread.name = "{}_{}".format("auto_scaler_thread", 1)
        self.auto_scaler_thread.start()
        time.sleep(30)

    def get_current_count(self, service_name):
        docker_svc_instance = DockerService(service_name)
        self.assertIsNotNone(docker_svc_instance)
        service_id = docker_svc_instance.get_service_id_from_service_name()
        self.assertIsNotNone(service_id)
        logging_to_console_and_syslog("get_service_id_from_service_name:"
                                      "service_name={},service_id={}"
                                      .format(service_name,
                                              service_id))
        current_container_count = docker_svc_instance.get_current_number_of_containers_per_service()
        return current_container_count

    def validate_auto_scaler(self, jobs_in_pipe, scale_up):
        self.redis_instance.set_the_key_in_redis_db("enqueue", jobs_in_pipe)
        service_name = '{}_{}'.format(TestAutoScaler.service_name,
                                      TestAutoScaler.service_to_be_tested)
        expected_container_count = 0

        for index in range(TestAutoScaler.total_number_of_iterations):
            current_container_count = self.get_current_count(service_name)
            if index == 0:
                # Start off from the current value of the total number of containers.
                expected_container_count = current_container_count
            if current_container_count != expected_container_count:
                for count in range(TestAutoScaler.retry_attempts):
                    time.sleep(1)
                    current_container_count = self.get_current_count(service_name)
                    logging_to_console_and_syslog("Iteration={},"
                                                  "Retrying after a second."
                                                  "current_container_count={},"
                                                  "expected_container_count={}"
                                                  .format(index,
                                                          current_container_count,
                                                          expected_container_count))
                    if current_container_count == expected_container_count:
                        break
            logging_to_console_and_syslog("Iteration={},"
                                          "get_current_number_of_containers_per_service:"
                                          "current_container_count={},"
                                          "expected_container_count={}"
                                          .format(index,
                                                  current_container_count,
                                                  expected_container_count))

            self.assertEqual(current_container_count, expected_container_count)

            if scale_up:
                if 0 < jobs_in_pipe <= 10 and \
                        current_container_count + 1 < TestAutoScaler.max_threshold:
                    expected_container_count += 1
                elif 11 < jobs_in_pipe <= 50:
                    if current_container_count + 10 < TestAutoScaler.max_threshold:
                        expected_container_count += 10
                    else:
                        expected_container_count += TestAutoScaler.max_threshold-current_container_count
                elif 50 < jobs_in_pipe <= 100:
                    if current_container_count + 20 < TestAutoScaler.max_threshold:
                        expected_container_count += 20
                    else:
                        expected_container_count += TestAutoScaler.max_threshold - current_container_count
                elif 100 < jobs_in_pipe <= 200:
                    if current_container_count + 30 < TestAutoScaler.max_threshold:
                        expected_container_count += 30
                    else:
                        expected_container_count += TestAutoScaler.max_threshold - current_container_count
            else:
                if current_container_count == TestAutoScaler.max_threshold and \
                        current_container_count - 30 >= TestAutoScaler.min_threshold:
                    expected_container_count -= 30
                elif current_container_count <= TestAutoScaler.max_threshold // 2 and \
                        current_container_count - 20 >= TestAutoScaler.min_threshold:
                    expected_container_count -= 20
                elif current_container_count <= TestAutoScaler.max_threshold // 4 and \
                        current_container_count - 10 >= TestAutoScaler.min_threshold:
                    expected_container_count -= 10
                elif current_container_count - 1 >= TestAutoScaler.min_threshold:
                    expected_container_count -= 1

            logging_to_console_and_syslog("Sleeping for {} seconds."
                                          .format(TestAutoScaler.sleep_time))
            time.sleep(TestAutoScaler.sleep_time)
            logging_to_console_and_syslog("Waking up after {} seconds."
                                          .format(TestAutoScaler.sleep_time))

    def test_auto_scaler(self):

        logging_to_console_and_syslog("******************test_auto_scaler******************")
        self.redis_instance = RedisInterface(TestAutoScaler.service_name)
        self.redis_instance.set_the_key_in_redis_db("enqueue", 1)
        self.redis_instance.set_the_key_in_redis_db("dequeue", 1)

        self.create_auto_scaler_thread()
        #SCALE UP
        jobs_in_pipe=10
        logging_to_console_and_syslog("************Testing SCALE UP Jobs in pipe = {} ****************"
                                      .format(jobs_in_pipe))
        self.validate_auto_scaler(jobs_in_pipe, scale_up=True)
        #SCALE DOWN
        jobs_in_pipe=0
        logging_to_console_and_syslog("************Testing SCALE DOWN Jobs in pipe = {} ****************"
                                      .format(jobs_in_pipe))
        self.validate_auto_scaler(jobs_in_pipe, scale_up=False)
        #SCALE UP
        jobs_in_pipe=20
        logging_to_console_and_syslog("************Testing SCALE UP Jobs in pipe = {} ****************"
                                      .format(jobs_in_pipe))

        self.validate_auto_scaler(jobs_in_pipe, scale_up=True)
        #SCALE DOWN
        jobs_in_pipe=0
        logging_to_console_and_syslog("************Testing SCALE DOWN Jobs in pipe = {} ****************"
                                      .format(jobs_in_pipe))

        self.validate_auto_scaler(jobs_in_pipe, scale_up=False)
        #SCALE UP
        jobs_in_pipe=51
        logging_to_console_and_syslog("************Testing SCALE UP Jobs in pipe = {} ****************"
                                      .format(jobs_in_pipe))

        self.validate_auto_scaler(jobs_in_pipe, scale_up=True)
        #SCALE DOWN
        jobs_in_pipe=0
        logging_to_console_and_syslog("************Testing SCALE DOWN Jobs in pipe = {} ****************"
                                      .format(jobs_in_pipe))

        self.validate_auto_scaler(jobs_in_pipe, scale_up=False)
        #SCALE UP
        jobs_in_pipe=101
        logging_to_console_and_syslog("************Testing SCALE UP Jobs in pipe = {} ****************"
                                      .format(jobs_in_pipe))

        self.validate_auto_scaler(jobs_in_pipe, scale_up=True)
        #SCALE DOWN
        jobs_in_pipe=0
        logging_to_console_and_syslog("************Testing SCALE DOWN Jobs in pipe = {} ****************"
                                      .format(jobs_in_pipe))

        self.validate_auto_scaler(jobs_in_pipe, scale_up=False)

    def __tear_down_docker_stack(self):
        completedProcess = subprocess.run(["docker",
                                           "stack",
                                           "rm",
                                           TestAutoScaler.service_name],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        logging_to_console_and_syslog(completedProcess.stdout.decode('utf8'))

    def tearDown(self):
        self.__tear_down_docker_stack()
        self.auto_scaler_thread.join(1.0)
        try:
            docker_api_interface_instance = DockerAPIInterface(image_name=TestAutoScaler.service_to_be_tested)
            docker_api_interface_instance.stop_docker_container_by_name()
        except:
            logging_to_console_and_syslog("Caught an exception while stopping {}"
                                          .format(TestAutoScaler.service_to_be_tested))
            print("Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)


if __name__ == "__main__":
    # To avoid the end of execution traceback adding exit=False
    unittest.main(exit=False)

