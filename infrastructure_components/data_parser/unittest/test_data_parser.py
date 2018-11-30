import os
import time
import sys
import subprocess
import unittest


def import_all_packages():
    realpath = os.path.realpath(__file__)
    # print("os.path.realpath({})={}".format(__file__, realpath))
    dirname = os.path.dirname(realpath)
    # print("os.path.dirname({})={}".format(realpath, dirname))
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
from infrastructure_components.data_parser.data_parser_interface import DataParserInterface


class TestDataParser(unittest.TestCase):
    def setUp(self):
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        self.data_parser_instance = None
        self.filename = '00030.mp4'
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["total_job_enqueued_count_redis_name_key"] = "enqueue"
        os.environ["total_job_dequeued_count_redis_name_key"] = "dequeue"
        os.environ["redis_server_hostname_key"] = "localhost"
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["redis_server_port_key"] = "6379"
    def test_run(self):
        logging_to_console_and_syslog("Testing pytorch parser.")
        self.create_test_delete_test(DataParserInterface.PyTorch,
                                'docker-compose_pytorch.yml')
        logging_to_console_and_syslog("Testing tensorflow parser.")
        self.create_test_delete_test(DataParserInterface.TensorFlow,
                                'docker-compose_tensorflow.yml')
        logging_to_console_and_syslog("Testing briefcam parser.")
        #self.create_test_delete_test(DataParserInterface.BriefCam,
        #                             'docker-compose_briefcam.yml')

    def create_test_delete_test(self, parser_type, docker_compose_yml_file):
        self.create_docker_container(docker_compose_yml_file)
        os.environ["data_parser_type_key"] = parser_type
        self.data_parser_instance = DataParserInterface()
        logging_to_console_and_syslog("Validating Data Parser Instance to be not null.")
        self.assertIsNotNone(self.data_parser_instance)
        self.assertTrue(self.data_parser_instance.process_job(self.filename))
        self.delete_docker_container(docker_compose_yml_file)

    def create_docker_container(self,docker_compose_yml_file):
        completedProcess = subprocess.run(["docker-compose",
                                           "-f",
                                           "{}/{}".format(self.dirname,
                                                          docker_compose_yml_file),
                                           "up",
                                           "-d"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        time.sleep(30)

    def delete_docker_container(self, docker_compose_yml_file):
        completedProcess = subprocess.run(["docker-compose",
                                           "-f",
                                           "{}/{}".format(self.dirname,
                                                          docker_compose_yml_file),
                                           "down"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def tearDown(self):
        self.data_parser_instance.cleanup()