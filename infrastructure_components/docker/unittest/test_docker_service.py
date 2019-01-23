#!/usr/bin/env python3
import os
import sys
import unittest
import traceback
import subprocess
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
from infrastructure_components.docker.docker_service import DockerService


class TestDockerService(unittest.TestCase):
    service_to_be_tested = 'machine_learning_workers'
    scale=20
    service_name = 'test_docker'

    def setUp(self):
        self.__create_docker_stack()
        time.sleep(30)

    def __create_docker_stack(self):
        completedProcess = subprocess.run(["docker",
                                           "stack",
                                           "deploy",
                                           "-c",
                                           "docker-compose.yml",
                                           TestDockerService.service_name],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        logging_to_console_and_syslog(completedProcess.stdout.decode('utf8'))

    def test_docker_service(self):
        service_name = '{}_{}'.format(TestDockerService.service_name,
                                      TestDockerService.service_to_be_tested)
        docker_svc_instance = DockerService(service_name)
        self.assertIsNotNone(docker_svc_instance)

        service_id = docker_svc_instance.get_service_id_from_service_name()
        self.assertIsNotNone(service_id)
        logging_to_console_and_syslog("__get_service_id_from_service_name:"
                                      "service_name={},service_id={}"
                                      .format(service_name,
                                              service_id))
        count = docker_svc_instance.count_list_of_containers_per_service(service_id)
        self.assertGreaterEqual(count, 1)
        logging_to_console_and_syslog("__count_list_of_containers_per_service:"
                                      "service_name={},service_id={},count={}"
                                      .format(service_name,
                                              service_id,
                                              count))
        count = docker_svc_instance.get_current_number_of_containers_per_service()
        self.assertEqual(count, 1)
        logging_to_console_and_syslog("get_current_number_of_containers_per_service:"
                                      "service_name={},service_id={},count={}"
                                      .format(service_name,
                                              service_id,
                                              count))
        docker_svc_instance.scale(TestDockerService.scale)
        time.sleep(30)
        service_id = docker_svc_instance.get_service_id_from_service_name()
        self.assertIsNotNone(service_id)
        logging_to_console_and_syslog("After scaling up by {},"
                                      "__get_service_id_from_service_name:"
                                      "service_name={},service_id={}"
                                      .format(TestDockerService.scale,
                                              service_name,
                                              service_id))

        count = docker_svc_instance.count_list_of_containers_per_service(service_id)
        logging_to_console_and_syslog("After scaling up by {},"
                                      "__count_list_of_containers_per_service:"
                                      "service_name={},service_id={},count={}"
                                      .format(TestDockerService.scale,
                                              service_name,
                                              service_id,
                                              count))

        self.assertGreaterEqual(count, TestDockerService.scale)
        count = docker_svc_instance.get_current_number_of_containers_per_service()
        self.assertEqual(count, TestDockerService.scale)
        logging_to_console_and_syslog("After scaling up by {},"
                                      "get_current_number_of_containers_per_service:"
                                      "service_name={},service_id={},count={}"
                                      .format(TestDockerService.scale,
                                              service_name,
                                              service_id,
                                              count))

    def __tear_down_docker_stack(self):
        completedProcess = subprocess.run(["docker",
                                           "stack",
                                           "rm",
                                           TestDockerService.service_name],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        logging_to_console_and_syslog(completedProcess.stdout.decode('utf8'))

    def tearDown(self):
        self.__tear_down_docker_stack()


if __name__ == "__main__":
    try:
        # To avoid the end of execution traceback adding exit=False
        unittest.main(exit=False)
    except:
        logging_to_console_and_syslog("Exception occurred.{}".format(sys.exc_info()))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
